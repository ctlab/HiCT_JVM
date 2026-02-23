package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.HictToMcoolConverter;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.dto.response.conversion.ConversionJobDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.conversion.ConversionSubmitResponseDTO;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RequiredArgsConstructor
@Slf4j
public class ConversionHandlersHolder extends HandlersHolder {

    private final Vertx vertx;
    private static final long MAX_UPLOAD_BYTES = 2L * 1024 * 1024 * 1024;
    private static final long JOB_TTL_MS = 60 * 60 * 1000;

    private static final Pattern OVERALL_PROGRESS_PATTERN = Pattern.compile(
      "Overall progress: (\\d+)% \\((\\d+)/(\\d+)\\), elapsed=([0-9:]+), eta=([0-9:]+)"
    );
    private static final Pattern RESOLUTION_PROGRESS_PATTERN = Pattern.compile(
      "Resolution (\\d+) write: (\\d+)% \\((\\d+)/(\\d+) stripes\\), elapsed=([0-9:]+), eta=([0-9:]+)"
    );

    private final ConcurrentHashMap<String, ConversionJob> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConversionJobGroup> groups = new ConcurrentHashMap<>();

    @Override
    public void addHandlersToRouter(final @NotNull Router router) {
        router.post("/convert/upload").blockingHandler(ctx -> {
            try {
              cleanupOldJobs();

              final var upload = ctx.fileUploads().stream().findFirst().orElseThrow(() -> new IllegalArgumentException("No file uploaded"));
              final var sourcePath = Path.of(upload.uploadedFileName());

              if (Files.size(sourcePath) > MAX_UPLOAD_BYTES) {
                  Files.deleteIfExists(sourcePath);
                  throw new IllegalArgumentException("Uploaded file is too large");
              }
              final var req = ctx.request();
              final var direction = req.getParam("direction");
              final var outputExt = "hict-to-mcool".equals(direction) ? ".mcool" : ".hict.hdf5";
              final var outputPath = Files.createTempFile("hict-converter-out-", outputExt);

              final var resolutionCsv = req.getParam("resolutions");
              final var resolutions = parseResolutions(resolutionCsv);
              final var compression = parseInteger(req.getParam("compression"), 0);
              final var compressionAlgorithm = ConversionOptions.CompressionAlgorithm.parse(req.getParam("compressionAlgorithm") == null ? "deflate" : req.getParam("compressionAlgorithm"));
              final var chunkSize = parseInteger(req.getParam("chunkSize"), 8192);
              final var applyAgpRaw = Boolean.parseBoolean(req.getParam("applyAgp"));
              final var agpPathRaw = req.getParam("agpPath") == null ? ConversionOptions.NO_AGP : req.getParam("agpPath");
              final var parallelism = parseInteger(req.getParam("parallelism"), Runtime.getRuntime().availableProcessors());

              final var useAgp = "hict-to-mcool".equals(direction) && applyAgpRaw;
              final var agpPath = useAgp ? agpPathRaw : ConversionOptions.NO_AGP;
              final var options = new ConversionOptions(sourcePath, outputPath, resolutions, chunkSize, compression, compressionAlgorithm, agpPath, useAgp, parallelism);

              final var job = createJob(sourcePath, outputPath, direction, parallelism, true, true);
              submitJob(job, options, ensureGroup("upload", 1));

              ctx.response().end(Json.encode(new ConversionSubmitResponseDTO("submitted", job.jobId)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        router.post("/convert/jobs").blockingHandler(ctx -> {
            cleanupOldJobs();
            final var requestJson = ctx.body().asJsonObject();
            final var filename = requestJson.getString("filename");
            if (filename == null || filename.isBlank()) {
                throw new IllegalArgumentException("filename is required");
            }
            final var direction = requestJson.getString("direction", "mcool-to-hict");
            final var parallelism = requestJson.getInteger("parallelism", Runtime.getRuntime().availableProcessors());
            final var resolutions = parseResolutions(requestJson.getString("resolutions"));
            final var compression = requestJson.getInteger("compression", 0);
            final var compressionAlgorithm = ConversionOptions.CompressionAlgorithm.parse(requestJson.getString("compressionAlgorithm", "deflate"));
            final var chunkSize = requestJson.getInteger("chunkSize", 8192);

            final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
            if (dataDirectoryWrapper == null) {
                throw new IllegalStateException("Data directory is not present in local map");
            }
            final var dataDirectory = dataDirectoryWrapper.getPath();
            final var sourcePath = dataDirectory.resolve(filename).normalize();
            if (!sourcePath.startsWith(dataDirectory)) {
                throw new IllegalArgumentException("Invalid filename");
            }
            if (!Files.exists(sourcePath)) {
                throw new IllegalArgumentException("Source file not found: " + filename);
            }

            final var outputPath = deriveOutputPath(sourcePath);
            if (Files.exists(outputPath)) {
                throw new IllegalArgumentException("Output file already exists: " + outputPath.getFileName());
            }

            final var options = new ConversionOptions(sourcePath, outputPath, resolutions, chunkSize, compression, compressionAlgorithm, ConversionOptions.NO_AGP, false, parallelism);
            final ConversionJob job;
            try {
                job = createJob(sourcePath, outputPath, direction, parallelism, false, false);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create conversion job", e);
            }
            submitJob(job, options, ensureGroup(UUID.randomUUID().toString(), 1));

            ctx.response().end(Json.encode(new ConversionSubmitResponseDTO("submitted", job.jobId)));
        });

        router.post("/convert/jobs/batch").blockingHandler(ctx -> {
            cleanupOldJobs();
            final var requestJson = ctx.body().asJsonObject();
            final var files = requestJson.getJsonArray("files", null);
            if (files == null || files.isEmpty()) {
                throw new IllegalArgumentException("files is required");
            }
            final var parallelJobs = Math.max(1, requestJson.getInteger("parallelJobs", 1));
            final var parallelism = requestJson.getInteger("parallelism", Runtime.getRuntime().availableProcessors());
            final var resolutions = parseResolutions(requestJson.getString("resolutions"));
            final var compression = requestJson.getInteger("compression", 0);
            final var compressionAlgorithm = ConversionOptions.CompressionAlgorithm.parse(requestJson.getString("compressionAlgorithm", "deflate"));
            final var chunkSize = requestJson.getInteger("chunkSize", 8192);

            final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
            if (dataDirectoryWrapper == null) {
                throw new IllegalStateException("Data directory is not present in local map");
            }
            final var dataDirectory = dataDirectoryWrapper.getPath();

            final var groupId = UUID.randomUUID().toString();
            final var group = ensureGroup(groupId, parallelJobs);
            final var jobIds = new ArrayList<String>();

            for (int i = 0; i < files.size(); i++) {
                final var filename = files.getString(i);
                final var sourcePath = dataDirectory.resolve(filename).normalize();
                if (!sourcePath.startsWith(dataDirectory)) {
                    throw new IllegalArgumentException("Invalid filename: " + filename);
                }
                if (!Files.exists(sourcePath)) {
                    throw new IllegalArgumentException("Source file not found: " + filename);
                }
                final var outputPath = deriveOutputPath(sourcePath);
                if (Files.exists(outputPath)) {
                    throw new IllegalArgumentException("Output file already exists: " + outputPath.getFileName());
                }
                final var options = new ConversionOptions(sourcePath, outputPath, resolutions, chunkSize, compression, compressionAlgorithm, ConversionOptions.NO_AGP, false, parallelism);
                final ConversionJob job;
                try {
                    job = createJob(sourcePath, outputPath, "mcool-to-hict", parallelism, false, false);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create conversion job for " + filename, e);
                }
                submitJob(job, options, group);
                jobIds.add(job.jobId);
            }

            ctx.response().end(Json.encode(Map.of("status", "submitted", "groupId", groupId, "jobIds", jobIds)));
        });

        router.get("/convert/jobs").blockingHandler(ctx -> {
            cleanupOldJobs();
            final var jobList = jobs.values().stream().map(ConversionJob::toDto).toList();
            ctx.response().end(Json.encode(jobList));
        });
        router.post("/convert/jobs/list").blockingHandler(ctx -> {
            cleanupOldJobs();
            final var jobList = jobs.values().stream().map(ConversionJob::toDto).toList();
            ctx.response().end(Json.encode(jobList));
        });

        router.get("/convert/jobs/:jobId").blockingHandler(ctx -> {
            final var job = jobs.get(ctx.pathParam("jobId"));
            if (job == null) {
                throw new IllegalArgumentException("Job not found");
            }
            ctx.response().end(Json.encode(job.toDto()));
        });
        router.post("/convert/jobs/:jobId").blockingHandler(ctx -> {
            final var job = jobs.get(ctx.pathParam("jobId"));
            if (job == null) {
                throw new IllegalArgumentException("Job not found");
            }
            ctx.response().end(Json.encode(job.toDto()));
        });

        router.post("/convert/jobs/:jobId/stop").blockingHandler(ctx -> {
            final var job = jobs.get(ctx.pathParam("jobId"));
            if (job == null) {
                throw new IllegalArgumentException("Job not found");
            }
            job.requestCancel();
            ctx.response().end(Json.encode(Map.of("status", "cancelling", "jobId", job.jobId)));
        });

        router.get("/convert/download/:jobId").blockingHandler(ctx -> {
            final var job = jobs.get(ctx.pathParam("jobId"));
            if (job == null) {
                throw new IllegalArgumentException("Job not found");
            }
            if (!"finished".equals(job.status)) {
                throw new IllegalStateException("Job is not finished yet");
            }
            if (!Files.exists(job.outputPath)) {
                throw new IllegalStateException("Converted file was already cleaned up");
            }
            ctx.response().putHeader("Content-Type", "application/octet-stream");
            ctx.response().putHeader("Content-Disposition", "attachment; filename=\"" + job.outputPath.getFileName() + "\"");
            ctx.response().sendFile(job.outputPath.toString());
        });
    }

    private static int parseInteger(final String value, final int defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static @NotNull List<Long> parseResolutions(final String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        final var out = new ArrayList<Long>();
        for (final var token : csv.split(",")) {
            final var trimmed = token.trim();
            if (!trimmed.isBlank()) {
                out.add(Long.parseLong(trimmed));
            }
        }
        return out;
    }

    private static Path deriveOutputPath(final @NotNull Path sourcePath) {
        final var filename = sourcePath.getFileName().toString();
        final var lower = filename.toLowerCase();
        final String base;
        if (lower.endsWith(".mcool")) {
            base = filename.substring(0, filename.length() - ".mcool".length());
        } else if (lower.endsWith(".cool")) {
            base = filename.substring(0, filename.length() - ".cool".length());
        } else {
            base = filename;
        }
        return sourcePath.getParent().resolve(base + ".hict.hdf5");
    }

    private ConversionJob createJob(final @NotNull Path sourcePath, final @NotNull Path outputPath, final @NotNull String direction, final int parallelism, final boolean deleteSourceOnCleanup, final boolean deleteOutputOnCleanup) throws IOException {
        final var jobId = UUID.randomUUID().toString();
        final var job = new ConversionJob(jobId, sourcePath, outputPath, direction, parallelism, deleteSourceOnCleanup, deleteOutputOnCleanup);
        job.inputSizeBytes = Files.exists(sourcePath) ? Files.size(sourcePath) : 0L;
        jobs.put(jobId, job);
        return job;
    }

    private ConversionJobGroup ensureGroup(final @NotNull String groupId, final int maxParallelJobs) {
        return groups.computeIfAbsent(groupId, id -> new ConversionJobGroup(groupId, maxParallelJobs));
    }

    private void submitJob(final @NotNull ConversionJob job, final @NotNull ConversionOptions options, final @NotNull ConversionJobGroup group) {
        group.totalJobs.incrementAndGet();
        group.executor.submit(() -> runJob(job, options, group));
    }

    private void runJob(final @NotNull ConversionJob job, final @NotNull ConversionOptions options, final @NotNull ConversionJobGroup group) {
        job.status = "running";
        job.startedAtMs = Instant.now().toEpochMilli();
        final java.util.function.Consumer<String> conversionLogger = message -> {
            synchronized (System.out) {
                System.out.println(message);
            }
            job.logs.add(message);
            parseProgress(job, message);
            job.updateOutputSize();
        };

        try {
            job.workerThread = Thread.currentThread();
            if ("hict-to-mcool".equals(job.direction)) {
                new HictToMcoolConverter().convert(options, conversionLogger);
            } else if ("mcool-to-hict".equals(job.direction)) {
                new McoolToHictConverter().convert(options, conversionLogger);
            } else {
                throw new IllegalArgumentException("Unknown conversion direction");
            }
            job.status = job.cancelRequested.get() ? "cancelled" : "finished";
        } catch (Exception e) {
            if (job.cancelRequested.get()) {
                job.status = "cancelled";
                job.error = "Cancelled";
            } else {
                job.status = "failed";
                job.error = e.getMessage();
                job.logs.add("ERROR: " + e.getMessage());
            }
        } finally {
            job.finishedAtMs = Instant.now().toEpochMilli();
            group.onJobFinished();
        }
    }

    private void parseProgress(final @NotNull ConversionJob job, final @NotNull String message) {
        Matcher overall = OVERALL_PROGRESS_PATTERN.matcher(message);
        if (overall.find()) {
            job.overallProgress = clampPercent(overall.group(1));
            job.elapsedMillis = parseDurationMillis(overall.group(4));
            job.etaMillis = parseDurationMillis(overall.group(5));
            return;
        }
        Matcher res = RESOLUTION_PROGRESS_PATTERN.matcher(message);
        if (res.find()) {
            job.currentResolution = Long.parseLong(res.group(1));
            job.resolutionProgress = clampPercent(res.group(2));
            job.resolutionElapsedMillis = parseDurationMillis(res.group(5));
            job.resolutionEtaMillis = parseDurationMillis(res.group(6));
        }
    }

    private static double clampPercent(final String value) {
        try {
            final var percent = Double.parseDouble(value);
            return Math.max(0.0d, Math.min(100.0d, percent)) / 100.0d;
        } catch (NumberFormatException ignored) {
            return 0.0d;
        }
    }

    private static long parseDurationMillis(final String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }
        final var parts = value.split(":");
        if (parts.length == 2) {
            final long minutes = Long.parseLong(parts[0]);
            final long seconds = Long.parseLong(parts[1]);
            return (minutes * 60 + seconds) * 1000L;
        }
        if (parts.length == 3) {
            final long hours = Long.parseLong(parts[0]);
            final long minutes = Long.parseLong(parts[1]);
            final long seconds = Long.parseLong(parts[2]);
            return (hours * 3600 + minutes * 60 + seconds) * 1000L;
        }
        return 0L;
    }

    private void cleanupOldJobs() {
        final var now = Instant.now().toEpochMilli();
        jobs.values().removeIf(job -> {
            final var expired = now - job.createdAtMs > JOB_TTL_MS;
            if (expired) {
                try {
                    if (job.deleteSourceOnCleanup) {
                        Files.deleteIfExists(job.sourcePath);
                    }
                    if (job.deleteOutputOnCleanup) {
                        Files.deleteIfExists(job.outputPath);
                    }
                } catch (IOException e) {
                    log.warn("Unable to cleanup temp files for {}", job.jobId, e);
                }
            }
            return expired;
        });
    }

    private static class ConversionJobGroup {
        private final String groupId;
        private final ExecutorService executor;
        private final AtomicInteger totalJobs = new AtomicInteger(0);
        private final AtomicInteger finishedJobs = new AtomicInteger(0);

        private ConversionJobGroup(final String groupId, final int maxParallelJobs) {
            this.groupId = groupId;
            this.executor = Executors.newFixedThreadPool(maxParallelJobs);
        }

        private void onJobFinished() {
            if (finishedJobs.incrementAndGet() >= totalJobs.get()) {
                executor.shutdown();
            }
        }
    }

    private static class ConversionJob {
        private final String jobId;
        private final long createdAtMs = Instant.now().toEpochMilli();
        private final Path sourcePath;
        private final Path outputPath;
        private final String direction;
        private final int parallelism;
        private final boolean deleteSourceOnCleanup;
        private final boolean deleteOutputOnCleanup;
        private volatile String status = "queued";
        private volatile String error = "";
        private final CopyOnWriteArrayList<String> logs = new CopyOnWriteArrayList<>();
        private volatile double overallProgress = 0.0d;
        private volatile double resolutionProgress = 0.0d;
        private volatile long currentResolution = 0L;
        private volatile long elapsedMillis = 0L;
        private volatile long etaMillis = 0L;
        private volatile long resolutionElapsedMillis = 0L;
        private volatile long resolutionEtaMillis = 0L;
        private volatile long inputSizeBytes = 0L;
        private volatile long outputSizeBytes = 0L;
        private volatile long startedAtMs = 0L;
        private volatile long finishedAtMs = 0L;
        private volatile Thread workerThread = null;
        private final AtomicBoolean cancelRequested = new AtomicBoolean(false);

        private ConversionJob(String jobId, Path sourcePath, Path outputPath, String direction, int parallelism, boolean deleteSourceOnCleanup, boolean deleteOutputOnCleanup) {
            this.jobId = jobId;
            this.sourcePath = sourcePath;
            this.outputPath = outputPath;
            this.direction = direction;
            this.parallelism = parallelism;
            this.deleteSourceOnCleanup = deleteSourceOnCleanup;
            this.deleteOutputOnCleanup = deleteOutputOnCleanup;
        }

        private void updateOutputSize() {
            try {
                if (Files.exists(outputPath)) {
                    outputSizeBytes = Files.size(outputPath);
                }
            } catch (IOException ignored) {
                // ignore
            }
        }

        private void requestCancel() {
            cancelRequested.set(true);
            if (workerThread != null) {
                workerThread.interrupt();
            }
        }

        private ConversionJobDTO toDto() {
            return new ConversionJobDTO(
                    jobId,
                    status,
                    sourcePath.getFileName().toString(),
                    outputPath.getFileName().toString(),
                    direction,
                    overallProgress,
                    resolutionProgress,
                    currentResolution,
                    elapsedMillis,
                    etaMillis,
                    resolutionElapsedMillis,
                    resolutionEtaMillis,
                    inputSizeBytes,
                    outputSizeBytes,
                    List.copyOf(logs),
                    error == null ? "" : error
            );
        }
    }
}
