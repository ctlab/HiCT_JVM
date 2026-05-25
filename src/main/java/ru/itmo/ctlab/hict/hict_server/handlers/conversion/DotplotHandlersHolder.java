package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.response.conversion.ConversionJobDTO;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
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
  private final @NotNull ExternalToolchainManager toolchainManager = new ExternalToolchainManager();
  private final @NotNull SelfDotplotPipeline dotplotPipeline = new SelfDotplotPipeline();

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
            body.getBoolean("overwrite", false),
            Math.max(1, body.getInteger("sampleBp", 250)),
            Math.max(0, body.getInteger("minAlignmentLength", 50)),
            body.getString("extraMinimap2Args", "")
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
      job.log("Starting integrated dotplot generation for " + job.sourcePath.getFileName());
      Files.createDirectories(job.outputDirectory);
      final var prefix = stripFastaSuffix(job.sourcePath.getFileName().toString()) + ".self.k" + job.options.minimizerK() + "w" + job.options.minimizerWindow();
      final var hictPath = job.outputDirectory.resolve(prefix + ".hict.hdf5");
      job.outputPath = hictPath;
      if (Files.exists(hictPath) && !job.options.overwrite()) {
        throw new IllegalArgumentException("Output file already exists: " + hictPath.getFileName());
      }
      final var toolchain = toolchainManager.requireDotplotToolchain();
      job.toolchainSource = toolchain.source();
      job.toolchainSummary = "minimap2 alignment with Java PAF/BG2 conversion and " + toolchain.source() + " hictk command " + toolchain.hictkCommand();
      job.toolchainNotices.clear();
      job.toolchainNotices.add("Dotplot generation uses minimap2 for self-alignment and integrated Java PAF/BG2 conversion; Python and Cooler are not required.");
      job.toolchainNotices.addAll(toolchain.notices());
      job.toolchainCitations.clear();
      job.toolchainCitations.addAll(toolchain.citations());
      final var output = dotplotPipeline.generate(
        new SelfDotplotPipeline.Options(
          job.sourcePath,
          job.outputDirectory,
          prefix,
          job.options.binSize(),
          job.options.resolutions(),
          job.options.minimizerK(),
          job.options.minimizerWindow(),
          job.options.minChainScore(),
          job.options.skipDiagonal(),
          job.options.dropNearDiagonalBins(),
          job.options.alignmentThreads(),
          job.options.conversionThreads(),
          job.options.overwrite(),
          false,
          job.options.sampleBp(),
          job.options.minAlignmentLength(),
          job.options.extraMinimap2Args()
        ),
        toolchain,
        job::log,
        process -> job.activeProcess = process,
        () -> job.cancelRequested.get()
      );
      job.outputPath = output;
      job.overallProgress = 1.0d;
      job.stageProgress = 1.0d;
      job.status = "finished";
      job.currentStageLabel = "Dotplot ready";
      job.log("Dotplot generated: " + output.getFileName());
    } catch (Exception e) {
      job.status = job.cancelRequested.get() ? "cancelled" : "failed";
      job.error = e.getMessage() == null ? e.toString() : e.getMessage();
      job.log("ERROR: " + job.error);
      log.warn("Dotplot job failed", e);
    } finally {
      job.finishedAtMs = Instant.now().toEpochMilli();
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
    boolean overwrite,
    int sampleBp,
    int minAlignmentLength,
    @NotNull String extraMinimap2Args
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
    private final @NotNull CopyOnWriteArrayList<String> toolchainNotices = new CopyOnWriteArrayList<>(
      List.of("Dotplot generation uses minimap2 plus integrated Java PAF/BG2 conversion and hictk load/zoomify; Python and Cooler are not required.")
    );
    private final @NotNull CopyOnWriteArrayList<String> toolchainCitations = new CopyOnWriteArrayList<>(
      List.of(
        "minimap2: Li H. Bioinformatics. 2018;34(18):3094-3100.",
        "hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408."
      )
    );
    private volatile @NotNull String toolchainSource = "hictk";
    private volatile @NotNull String toolchainSummary = "Integrated self-alignment dotplot pipeline";

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
      updateProgressFromStageMessage(message);
      synchronized (System.out) {
        System.out.println("[dotplot] " + message);
      }
    }

    private void updateProgressFromStageMessage(final @NotNull String message) {
      if (!message.startsWith("HICT_STAGE ")) {
        return;
      }
      final var detailIndex = message.indexOf(" detail=");
      final var metadata = detailIndex >= 0 ? message.substring("HICT_STAGE ".length(), detailIndex) : message.substring("HICT_STAGE ".length());
      if (detailIndex >= 0) {
        currentStageLabel = message.substring(detailIndex + " detail=".length());
      }
      for (final var token : metadata.split("\\s+")) {
        final var separator = token.indexOf('=');
        if (separator <= 0 || separator + 1 >= token.length()) {
          continue;
        }
        final var key = token.substring(0, separator);
        final var value = token.substring(separator + 1);
        try {
          switch (key) {
            case "stage" -> currentStage = value;
            case "progress" -> stageProgress = Double.parseDouble(value);
            case "overall" -> overallProgress = Double.parseDouble(value);
            default -> {
            }
          }
        } catch (NumberFormatException ignored) {
          // Keep the previous progress value if a malformed diagnostic line reaches the UI.
        }
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
        toolchainSource,
        toolchainSummary,
        List.copyOf(toolchainNotices),
        List.copyOf(toolchainCitations),
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
