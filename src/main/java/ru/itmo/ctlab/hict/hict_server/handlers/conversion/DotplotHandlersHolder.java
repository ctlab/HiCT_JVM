package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.response.conversion.ConversionJobDTO;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@RequiredArgsConstructor
@Slf4j
public class DotplotHandlersHolder extends HandlersHolder {
  private static final long JOB_TTL_MS = 60 * 60 * 1000L;

  private final @NotNull Vertx vertx;
  private final @NotNull ConcurrentHashMap<String, DotplotJob> jobs = new ConcurrentHashMap<>();
  private final @NotNull ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, Math.min(2, Runtime.getRuntime().availableProcessors())));

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/dotplot/jobs").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.EXPORT,
        RequestTaskScheduler.CancellationDomain.EXPORT,
        () -> {
          cleanupOldJobs();
          final var body = ctx.body().asJsonObject();
          final var fastaFiles = body.getJsonArray("fastaFiles");
          if (fastaFiles == null || fastaFiles.isEmpty()) {
            throw new IllegalArgumentException("fastaFiles is required");
          }
          final var dataDirectory = requireDataDirectory();
          final var outputDirectory = resolveOutputDirectory(body.getString("outputDirectory"));
          final var options = new DotplotOptions(
            Math.max(1, body.getInteger("binSize", 1000)),
            body.getString("resolutions", ""),
            Math.max(1, body.getInteger("minimizerK", 17)),
            Math.max(1, body.getInteger("minimizerWindow", 5)),
            Math.max(0, body.getInteger("minChainScore", 40)),
            body.getBoolean("skipDiagonal", false),
            Math.max(0, body.getInteger("dropNearDiagonalBins", 0)),
            Math.max(1, body.getInteger("alignmentThreads", Runtime.getRuntime().availableProcessors())),
            Math.max(1, body.getInteger("conversionThreads", Runtime.getRuntime().availableProcessors())),
            body.getBoolean("overwrite", false)
          );
          final var ids = new ArrayList<String>();
          final var groupId = UUID.randomUUID().toString();
          for (int i = 0; i < fastaFiles.size(); i++) {
            final var filename = fastaFiles.getString(i);
            final var fastaPath = dataDirectory.resolve(filename).normalize();
            if (!fastaPath.startsWith(dataDirectory) || !Files.isRegularFile(fastaPath)) {
              throw new IllegalArgumentException("FASTA file not found: " + filename);
            }
            final var job = new DotplotJob(UUID.randomUUID().toString(), fastaPath, outputDirectory, options);
            jobs.put(job.jobId, job);
            ids.add(job.jobId);
            executor.submit(() -> runJob(job));
          }
          return Map.of("status", "submitted", "groupId", groupId, "jobIds", ids);
        },
        response -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(response)),
        () -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(Map.of("status", "cancelled")))
      );
    });

    router.post("/dotplot/jobs/list").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> {
          cleanupOldJobs();
          return jobs.values().stream().map(DotplotJob::toDto).toList();
        },
        response -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(response))
      );
    });
  }

  private void runJob(final @NotNull DotplotJob job) {
    job.status = "running";
    job.startedAtMs = Instant.now().toEpochMilli();
    try {
      job.log("Starting dotplot generation for " + job.sourcePath.getFileName());
      final var script = resolveSelfdotScript();
      if (!Files.isRegularFile(script)) {
        throw new IllegalStateException(
          "Dotplot backend is not configured: selfdot_mcool.sh was not found. " +
            "Set HICT_SELFDOT_SCRIPT or bundle the native/minimap2 dotplot pipeline."
        );
      }
      Files.createDirectories(job.outputDirectory);
      final var prefix = stripFastaSuffix(job.sourcePath.getFileName().toString()) + ".self.k" + job.options.minimizerK() + "w" + job.options.minimizerWindow();
      final var mcoolPath = job.outputDirectory.resolve(prefix + ".mcool");
      final var hictPath = job.outputDirectory.resolve(prefix + ".hict.hdf5");
      job.outputPath = hictPath;
      if (Files.exists(hictPath) && !job.options.overwrite()) {
        throw new IllegalArgumentException("Output file already exists: " + hictPath.getFileName());
      }
      job.currentStage = "align";
      job.currentStageLabel = "Running minimap2 self-alignment";
      job.overallProgress = 0.05d;
      final var command = new ArrayList<String>();
      command.add("bash");
      command.add(script.toString());
      command.add("--input");
      command.add(job.sourcePath.toString());
      command.add("--outdir");
      command.add(job.outputDirectory.toString());
      command.add("--prefix");
      command.add(prefix);
      command.add("--bin");
      command.add(Integer.toString(job.options.binSize()));
      if (!job.options.resolutions().isBlank()) {
        command.add("--resolutions");
        command.add(job.options.resolutions());
      }
      command.add("--k");
      command.add(Integer.toString(job.options.minimizerK()));
      command.add("--w");
      command.add(Integer.toString(job.options.minimizerWindow()));
      command.add("--min-chain-score");
      command.add(Integer.toString(job.options.minChainScore()));
      command.add("--drop-near-diag");
      command.add(Integer.toString(job.options.dropNearDiagonalBins()));
      command.add("--align-threads");
      command.add(Integer.toString(job.options.alignmentThreads()));
      command.add("--convert-procs");
      command.add(Integer.toString(job.options.conversionThreads()));
      if (job.options.skipDiagonal()) {
        command.add("--skip-diag");
      }
      if (job.options.overwrite()) {
        command.add("--force");
      }
      runCommand(job, command, job.outputDirectory);
      job.overallProgress = 0.75d;
      job.currentStage = "import_hict";
      job.currentStageLabel = "Importing generated .mcool into HiCT";
      new McoolToHictConverter().convert(
        new ConversionOptions(
          mcoolPath,
          hictPath,
          parseResolutions(job.options.resolutions()),
          8192,
          6,
          ConversionOptions.CompressionAlgorithm.DEFLATE,
          ConversionOptions.NO_AGP,
          false,
          job.options.conversionThreads()
        ),
        job::log
      );
      job.overallProgress = 1.0d;
      job.stageProgress = 1.0d;
      job.status = "finished";
      job.currentStageLabel = "Dotplot ready";
      job.log("Dotplot generated: " + hictPath.getFileName());
    } catch (Exception e) {
      job.status = job.cancelRequested.get() ? "cancelled" : "failed";
      job.error = e.getMessage() == null ? e.toString() : e.getMessage();
      job.log("ERROR: " + job.error);
      log.warn("Dotplot job failed", e);
    } finally {
      job.finishedAtMs = Instant.now().toEpochMilli();
    }
  }

  private static void runCommand(final @NotNull DotplotJob job,
                                 final @NotNull List<String> command,
                                 final @NotNull Path workingDirectory) throws IOException, InterruptedException {
    job.log("Executing external command: " + String.join(" ", command));
    final var process = new ProcessBuilder(command)
      .directory(workingDirectory.toFile())
      .redirectErrorStream(true)
      .start();
    job.activeProcess = process;
    try (final var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        job.log(line);
        if (line.contains("Writing chrom sizes")) {
          job.overallProgress = 0.10d;
          job.currentStageLabel = "Writing contig sizes";
        } else if (line.toLowerCase(Locale.ROOT).contains("minimap2")) {
          job.overallProgress = Math.max(job.overallProgress, 0.20d);
          job.currentStageLabel = "Running minimap2";
        } else if (line.toLowerCase(Locale.ROOT).contains("cooler zoomify")) {
          job.overallProgress = Math.max(job.overallProgress, 0.60d);
          job.currentStageLabel = "Building resolution pyramid";
        }
      }
    } finally {
      job.activeProcess = null;
    }
    final var exit = process.waitFor();
    if (exit != 0) {
      throw new IllegalStateException("Dotplot external pipeline failed with exit code " + exit);
    }
  }

  private @NotNull Path requireDataDirectory() {
    final var wrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
    if (wrapper == null) {
      throw new IllegalStateException("Data directory is not present in local map");
    }
    return wrapper.getPath();
  }

  private @NotNull Path resolveOutputDirectory(final String requested) {
    if (requested != null && !requested.isBlank()) {
      final var dataDirectory = requireDataDirectory();
      final var output = dataDirectory.resolve(requested).normalize();
      if (!output.startsWith(dataDirectory)) {
        throw new IllegalArgumentException("Invalid output directory");
      }
      return output;
    }
    final var wrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("processedDirectory");
    if (wrapper != null) {
      return wrapper.getPath();
    }
    return requireDataDirectory().resolve("processed").normalize();
  }

  private static @NotNull Path resolveSelfdotScript() {
    final var configured = System.getenv("HICT_SELFDOT_SCRIPT");
    if (configured != null && !configured.isBlank()) {
      return Path.of(configured).toAbsolutePath().normalize();
    }
    return Path.of("selfdot_mcool.sh").toAbsolutePath().normalize();
  }

  private static @NotNull List<Long> parseResolutions(final @NotNull String csv) {
    if (csv.isBlank()) {
      return List.of();
    }
    return java.util.Arrays.stream(csv.split(","))
      .map(String::trim)
      .filter(token -> !token.isBlank())
      .map(Long::parseLong)
      .toList();
  }

  private static @NotNull String stripFastaSuffix(final @NotNull String filename) {
    var name = filename;
    if (name.toLowerCase(Locale.ROOT).endsWith(".gz")) {
      name = name.substring(0, name.length() - 3);
    }
    for (final var suffix : List.of(".fasta", ".fa", ".fna", ".fas")) {
      if (name.toLowerCase(Locale.ROOT).endsWith(suffix)) {
        return name.substring(0, name.length() - suffix.length());
      }
    }
    return name;
  }

  private void cleanupOldJobs() {
    final var cutoff = Instant.now().toEpochMilli() - JOB_TTL_MS;
    jobs.entrySet().removeIf(entry -> {
      final var job = entry.getValue();
      return job.finishedAtMs > 0L && job.finishedAtMs < cutoff;
    });
  }

  private RequestTaskScheduler getScheduler(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
    if (wrapper == null) {
      ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
      return null;
    }
    return wrapper.getRequestTaskScheduler();
  }

  private record DotplotOptions(
    int binSize,
    @NotNull String resolutions,
    int minimizerK,
    int minimizerWindow,
    int minChainScore,
    boolean skipDiagonal,
    int dropNearDiagonalBins,
    int alignmentThreads,
    int conversionThreads,
    boolean overwrite
  ) {
  }

  private static final class DotplotJob {
    private final @NotNull String jobId;
    private final long createdAtMs = Instant.now().toEpochMilli();
    private final @NotNull Path sourcePath;
    private volatile @NotNull Path outputPath;
    private final @NotNull Path outputDirectory;
    private final @NotNull DotplotOptions options;
    private volatile @NotNull String status = "queued";
    private volatile @NotNull String error = "";
    private final @NotNull CopyOnWriteArrayList<String> logs = new CopyOnWriteArrayList<>();
    private volatile @NotNull String currentStage = "";
    private volatile @NotNull String currentStageLabel = "";
    private volatile double stageProgress = 0.0d;
    private volatile double overallProgress = 0.0d;
    private volatile long startedAtMs = 0L;
    private volatile long finishedAtMs = 0L;
    private volatile Process activeProcess = null;
    private final @NotNull AtomicBoolean cancelRequested = new AtomicBoolean(false);

    private DotplotJob(final @NotNull String jobId,
                       final @NotNull Path sourcePath,
                       final @NotNull Path outputDirectory,
                       final @NotNull DotplotOptions options) {
      this.jobId = jobId;
      this.sourcePath = sourcePath;
      this.outputDirectory = outputDirectory;
      this.outputPath = outputDirectory.resolve(stripFastaSuffix(sourcePath.getFileName().toString()) + ".self.hict.hdf5");
      this.options = options;
    }

    private void log(final @NotNull String message) {
      logs.add(message);
      synchronized (System.out) {
        System.out.println("[dotplot] " + message);
      }
    }

    private ConversionJobDTO toDto() {
      final var elapsed = startedAtMs <= 0L
        ? 0L
        : (finishedAtMs > 0L ? finishedAtMs : Instant.now().toEpochMilli()) - startedAtMs;
      return new ConversionJobDTO(
        jobId,
        status,
        sourcePath.getFileName().toString(),
        outputPath.getFileName().toString(),
        "fasta-to-dotplot",
        currentStage,
        currentStageLabel,
        "",
        stageProgress,
        overallProgress,
        0.0d,
        0L,
        elapsed,
        0L,
        0L,
        0L,
        safeSize(sourcePath),
        safeSize(outputPath),
        "dotplot",
        "Self-alignment dotplot pipeline",
        List.of("Current implementation uses configured external selfdot/minimap2 pipeline when available."),
        List.of("minimap2: Li H. Bioinformatics. 2018;34(18):3094-3100."),
        List.copyOf(logs),
        error
      );
    }

    private static long safeSize(final @NotNull Path path) {
      try {
        return Files.exists(path) ? Files.size(path) : 0L;
      } catch (IOException ignored) {
        return 0L;
      }
    }
  }
}
