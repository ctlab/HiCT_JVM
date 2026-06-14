package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.assembly.AssemblyLayoutConverter;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.HictToMcoolConverter;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.response.conversion.ConversionJobDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.conversion.ConversionSubmitResponseDTO;
import ru.itmo.ctlab.hict.hict_server.util.cache.FileFingerprintService;
import ru.itmo.ctlab.hict.hict_server.util.cache.MatrixConversionCacheManager;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RequiredArgsConstructor
@Slf4j
public class ConversionHandlersHolder extends HandlersHolder {

    private final Vertx vertx;
    private static final long MAX_UPLOAD_BYTES = 2L * 1024 * 1024 * 1024;
    private static final long JOB_TTL_MS = 60 * 60 * 1000;
    private static final Pattern STAGE_PROGRESS_PATTERN = Pattern.compile(
      "HICT_STAGE stage=(\\S+) progress=([0-9.]+) overall=([0-9.]+) detail=(.*)"
    );
    private static final Pattern TOOLCHAIN_PATTERN = Pattern.compile(
      "HICT_TOOLCHAIN source=(\\S+) platform=(\\S+)"
    );

    private static final Pattern OVERALL_PROGRESS_PATTERN = Pattern.compile(
      "Overall progress: (\\d+)% \\((\\d+)/(\\d+)\\), elapsed=([0-9:]+), eta=([0-9:]+)"
    );
    private static final Pattern RESOLUTION_PROGRESS_PATTERN = Pattern.compile(
      "Resolution (\\d+) write: (\\d+)% \\((\\d+)/(\\d+) stripes\\), elapsed=([0-9:]+), eta=([0-9:]+)"
    );

    private final ConcurrentHashMap<String, ConversionJob> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConversionJobGroup> groups = new ConcurrentHashMap<>();
    private final ExternalToolchainManager toolchainManager = new ExternalToolchainManager();
    private final HictkConversionPipeline hictkConversionPipeline = new HictkConversionPipeline(this.toolchainManager);
    private final FileFingerprintService fingerprintService = new FileFingerprintService();

    @Override
    public void addHandlersToRouter(final @NotNull Router router) {
        router.post("/convert/toolchain").handler(ctx -> {
            final var scheduler = getScheduler(ctx);
            if (scheduler == null) {
                return;
            }
            scheduler.submit(
              ctx,
              RequestTaskScheduler.RequestPriority.UI_UX,
              null,
              this.toolchainManager::inspect,
              response -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(response))
            );
        });

        router.post("/convert/toolchain/dotplot-aligner").handler(ctx -> {
            final var scheduler = getScheduler(ctx);
            if (scheduler == null) {
                return;
            }
            scheduler.submit(
              ctx,
              RequestTaskScheduler.RequestPriority.UI_UX,
              null,
              () -> {
                  final var body = ctx.body().asJsonObject();
                  ExternalToolchainManager.setDotplotAlignerPreference(body.getString("alignerPreference", "auto"));
                  return this.toolchainManager.inspect();
              },
              response -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(response))
            );
        });

        router.post("/convert/upload").handler(ctx -> {
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
                    try {
                        final var upload = ctx.fileUploads().stream()
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException("No file uploaded"));
                        final var sourcePath = Path.of(upload.uploadedFileName());

                        if (Files.size(sourcePath) > MAX_UPLOAD_BYTES) {
                            Files.deleteIfExists(sourcePath);
                            throw new IllegalArgumentException("Uploaded file is too large");
                        }
                        final var req = ctx.request();
                        final var direction = ConversionDirection.fromRequestOrSource(req.getParam("direction"), sourcePath);
                        final var outputPath = Files.createTempFile("hict-converter-out-", direction.outputExtension());

                        final var resolutionCsv = req.getParam("resolutions");
                        final var resolutions = parseResolutions(resolutionCsv);
                        final var compression = parseInteger(req.getParam("compression"), 6);
                        final var compressionAlgorithm = ConversionOptions.CompressionAlgorithm.parse(
                            req.getParam("compressionAlgorithm") == null ? "deflate" : req.getParam("compressionAlgorithm")
                        );
                        final var chunkSize = parseInteger(req.getParam("chunkSize"), 8192);
                        final var applyAgpRaw = Boolean.parseBoolean(req.getParam("applyAgp"));
                        final var agpPathRaw = req.getParam("agpPath") == null ? ConversionOptions.NO_AGP : req.getParam("agpPath");
                        final var parallelism = parseInteger(req.getParam("parallelism"), Runtime.getRuntime().availableProcessors());

                        final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
                        if (dataDirectoryWrapper == null) {
                            throw new IllegalStateException("Data directory is not present in local map");
                        }
                        final var dataDirectory = dataDirectoryWrapper.getPath();

                        final var useAgp = direction == ConversionDirection.HICT_TO_MCOOL && applyAgpRaw;
                        final var agpPath = useAgp
                          ? resolveOptionalAssemblyPath(dataDirectory, agpPathRaw).toString()
                          : ConversionOptions.NO_AGP;
                        final var options = new ConversionOptions(
                            sourcePath,
                            outputPath,
                            resolutions,
                            chunkSize,
                            compression,
                            compressionAlgorithm,
                            agpPath,
                            useAgp,
                            parallelism
                        );

                        final var job = createJob(sourcePath, outputPath, null, direction, parallelism, true, true, HictkLoadOptions.EMPTY);
                        submitJob(job, options, ensureGroup("upload", 1));
                        return new ConversionSubmitResponseDTO("submitted", job.jobId);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                response -> ctx.response().end(Json.encode(response)),
                () -> ctx.response().end(Json.encode(Map.of("status", "cancelled")))
            );
        });

        router.post("/convert/assembly-to-agp").handler(ctx -> {
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
                final var requestJson = ctx.body().asJsonObject();
                final var filename = requestJson.getString("filename");
                if (filename == null || filename.isBlank()) {
                  throw new IllegalArgumentException("filename is required");
                }
                final var overwrite = requestJson.getBoolean("overwrite", false);

                final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
                if (dataDirectoryWrapper == null) {
                  throw new IllegalStateException("Data directory is not present in local map");
                }
                final var dataDirectory = dataDirectoryWrapper.getPath();
                final var sourcePath = dataDirectory.resolve(filename).normalize();
                if (!sourcePath.startsWith(dataDirectory)) {
                  throw new IllegalArgumentException("Invalid filename");
                }
                if (!Files.isRegularFile(sourcePath)) {
                  throw new IllegalArgumentException("Source file not found: " + filename);
                }

                final var outputFilename = requestJson.getString("outputFilename", "");
                final var outputPath = outputFilename == null || outputFilename.isBlank()
                  ? deriveAgpOutputPath(sourcePath)
                  : dataDirectory.resolve(outputFilename).normalize();
                if (!outputPath.startsWith(dataDirectory)) {
                  throw new IllegalArgumentException("Invalid output filename");
                }
                if (!overwrite && Files.exists(outputPath)) {
                  throw new IllegalArgumentException("Output file already exists: " + dataDirectory.relativize(outputPath));
                }
                if (overwrite) {
                  Files.deleteIfExists(outputPath);
                }

                AssemblyLayoutConverter.convertToAgp(sourcePath, outputPath);
                return Map.of(
                  "status", "converted",
                  "inputFilename", filename,
                  "outputFilename", dataDirectory.relativize(outputPath).toString()
                );
              },
              response -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(response))
            );
        });

        router.post("/convert/jobs").handler(ctx -> {
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
                    final var requestJson = ctx.body().asJsonObject();
                    final var filename = requestJson.getString("filename");
                    if (filename == null || filename.isBlank()) {
                        throw new IllegalArgumentException("filename is required");
                    }
                    final var parallelism = requestJson.getInteger("parallelism", Runtime.getRuntime().availableProcessors());
                    final var overwrite = requestJson.getBoolean("overwrite", false);
                    final var resolutions = parseResolutions(requestJson.getString("resolutions"));
                    final var compression = requestJson.getInteger("compression", 6);
                    final var compressionAlgorithm = ConversionOptions.CompressionAlgorithm.parse(requestJson.getString("compressionAlgorithm", "deflate"));
                    final var chunkSize = requestJson.getInteger("chunkSize", 8192);
                    final var assemblyFilename = requestJson.getString("assemblyFilename", "");

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

                    final var direction = ConversionDirection.fromRequestOrSource(requestJson.getString("direction"), sourcePath);
                    final var outputPath = deriveOutputPath(sourcePath, direction);
                    prepareOutputPath(outputPath, overwrite);
                    final var assemblyPath = resolveOptionalAssemblyPath(dataDirectory, assemblyFilename);
                    final var assemblyPathForOptions = assemblyPath == null
                      ? ConversionOptions.NO_AGP
                      : assemblyPath.toString();
                    final var loadOptions = parseHictkLoadOptions(dataDirectory, requestJson);

                    final var options = new ConversionOptions(
                        sourcePath,
                        outputPath,
                        resolutions,
                        chunkSize,
                        compression,
                        compressionAlgorithm,
                        assemblyPathForOptions,
                        false,
                        parallelism
                    );
                    final ConversionJob job;
                    try {
                        job = createJob(sourcePath, outputPath, dataDirectory, direction, parallelism, false, false, loadOptions);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to create conversion job", e);
                    }
                    submitJob(job, options, ensureGroup(UUID.randomUUID().toString(), 1));
                    return new ConversionSubmitResponseDTO("submitted", job.jobId);
                },
                response -> ctx.response().end(Json.encode(response)),
                () -> ctx.response().end(Json.encode(Map.of("status", "cancelled")))
            );
        });

        router.post("/convert/jobs/batch").handler(ctx -> {
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
                    final var requestJson = ctx.body().asJsonObject();
                    final var files = requestJson.getJsonArray("files", null);
                    if (files == null || files.isEmpty()) {
                        throw new IllegalArgumentException("files is required");
                    }
                    final var parallelJobs = Math.max(1, requestJson.getInteger("parallelJobs", 1));
                    final var parallelism = requestJson.getInteger("parallelism", Runtime.getRuntime().availableProcessors());
                    final var overwrite = requestJson.getBoolean("overwrite", false);
                    final var resolutions = parseResolutions(requestJson.getString("resolutions"));
                    final var compression = requestJson.getInteger("compression", 6);
                    final var compressionAlgorithm = ConversionOptions.CompressionAlgorithm.parse(requestJson.getString("compressionAlgorithm", "deflate"));
                    final var chunkSize = requestJson.getInteger("chunkSize", 8192);
                    final var assemblyFilename = requestJson.getString("assemblyFilename", "");
                    final var assemblyByFileJson = requestJson.getJsonObject("assemblyFilenameByFile");

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
                        final var direction = ConversionDirection.fromRequestOrSource(null, sourcePath);
                        final var outputPath = deriveOutputPath(sourcePath, direction);
                        prepareOutputPath(outputPath, overwrite);
                        final var perFileAssembly = assemblyByFileJson == null
                          ? assemblyFilename
                          : assemblyByFileJson.getString(filename, assemblyFilename);
                        final var assemblyPath = resolveOptionalAssemblyPath(dataDirectory, perFileAssembly);
                        final var assemblyPathForOptions = assemblyPath == null
                          ? ConversionOptions.NO_AGP
                          : assemblyPath.toString();
                        final var loadOptions = parseHictkLoadOptions(dataDirectory, requestJson);
                        final var options = new ConversionOptions(
                            sourcePath,
                            outputPath,
                            resolutions,
                            chunkSize,
                            compression,
                            compressionAlgorithm,
                            assemblyPathForOptions,
                            false,
                            parallelism
                        );
                        final ConversionJob job;
                        try {
                            job = createJob(sourcePath, outputPath, dataDirectory, direction, parallelism, false, false, loadOptions);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to create conversion job for " + filename, e);
                        }
                        submitJob(job, options, group);
                        jobIds.add(job.jobId);
                    }
                    return Map.of("status", "submitted", "groupId", groupId, "jobIds", jobIds);
                },
                response -> ctx.response().end(Json.encode(response)),
                () -> ctx.response().end(Json.encode(Map.of("status", "cancelled")))
            );
        });

        router.get("/convert/jobs").handler(ctx -> {
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
                    return jobs.values().stream().map(ConversionJob::toDto).toList();
                },
                response -> ctx.response().end(Json.encode(response))
            );
        });
        router.post("/convert/jobs/list").handler(ctx -> {
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
                    return jobs.values().stream().map(ConversionJob::toDto).toList();
                },
                response -> ctx.response().end(Json.encode(response))
            );
        });

        router.get("/convert/jobs/:jobId").handler(ctx -> {
            final var scheduler = getScheduler(ctx);
            if (scheduler == null) {
                return;
            }
            scheduler.submit(
                ctx,
                RequestTaskScheduler.RequestPriority.UI_UX,
                null,
                () -> {
                    final var job = jobs.get(ctx.pathParam("jobId"));
                    if (job == null) {
                        throw new IllegalArgumentException("Job not found");
                    }
                    return job.toDto();
                },
                response -> ctx.response().end(Json.encode(response))
            );
        });
        router.post("/convert/jobs/:jobId").handler(ctx -> {
            final var scheduler = getScheduler(ctx);
            if (scheduler == null) {
                return;
            }
            scheduler.submit(
                ctx,
                RequestTaskScheduler.RequestPriority.UI_UX,
                null,
                () -> {
                    final var job = jobs.get(ctx.pathParam("jobId"));
                    if (job == null) {
                        throw new IllegalArgumentException("Job not found");
                    }
                    return job.toDto();
                },
                response -> ctx.response().end(Json.encode(response))
            );
        });

        router.post("/convert/jobs/:jobId/stop").handler(ctx -> {
            final var scheduler = getScheduler(ctx);
            if (scheduler == null) {
                return;
            }
            scheduler.submit(
                ctx,
                RequestTaskScheduler.RequestPriority.UI_UX,
                null,
                () -> {
                    final var job = jobs.get(ctx.pathParam("jobId"));
                    if (job == null) {
                        throw new IllegalArgumentException("Job not found");
                    }
                    job.requestCancel();
                    return Map.of("status", "cancelling", "jobId", job.jobId);
                },
                response -> ctx.response().end(Json.encode(response))
            );
        });

        router.get("/convert/download/:jobId").handler(ctx -> {
            final var scheduler = getScheduler(ctx);
            if (scheduler == null) {
                return;
            }
            scheduler.submit(
                ctx,
                RequestTaskScheduler.RequestPriority.EXPORT,
                RequestTaskScheduler.CancellationDomain.EXPORT,
                () -> {
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
                    return job.outputPath;
                },
                outputPath -> {
                    ctx.response().putHeader("Content-Type", "application/octet-stream");
                    ctx.response().putHeader("Content-Disposition", "attachment; filename=\"" + outputPath.getFileName() + "\"");
                    ctx.response().sendFile(outputPath.toString());
                },
                () -> ctx.response().setStatusCode(200).end()
            );
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

    private static Path deriveOutputPath(final @NotNull Path sourcePath,
                                         final @NotNull ConversionDirection direction) {
        return direction.deriveOutputPath(sourcePath);
    }

    private static Path deriveAgpOutputPath(final @NotNull Path sourcePath) {
        final var filename = sourcePath.getFileName().toString();
        final var lowerFilename = filename.toLowerCase(Locale.ROOT);
        if (lowerFilename.endsWith(".assembly")) {
            return sourcePath.resolveSibling(filename.substring(0, filename.length() - ".assembly".length()) + ".agp");
        }
        if (lowerFilename.endsWith(".agp")) {
            return sourcePath.resolveSibling(filename);
        }
        return sourcePath.resolveSibling(filename + ".agp");
    }

    private static void prepareOutputPath(final @NotNull Path outputPath, final boolean overwrite) {
        if (!Files.exists(outputPath)) {
            return;
        }
        if (!overwrite) {
            throw new IllegalArgumentException("Output file already exists: " + outputPath.getFileName());
        }
        try {
            Files.delete(outputPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to overwrite existing output file: " + outputPath.getFileName(), e);
        }
    }

    private static Path resolveOptionalAssemblyPath(final @NotNull Path dataDirectory, final String filename) {
        if (filename == null || filename.isBlank()) {
            return null;
        }
        final var lowerFilename = filename.toLowerCase(Locale.ROOT);
        if (!lowerFilename.endsWith(".assembly") && !lowerFilename.endsWith(".agp")) {
            throw new IllegalArgumentException("Assembly layout file must be .assembly or .agp: " + filename);
        }
        final var assemblyPath = dataDirectory.resolve(filename).normalize();
        if (!assemblyPath.startsWith(dataDirectory)) {
            throw new IllegalArgumentException("Invalid assembly layout filename");
        }
        if (!Files.isRegularFile(assemblyPath)) {
            throw new IllegalArgumentException("Assembly layout file not found: " + filename);
        }
        return assemblyPath;
    }

    private static @NotNull HictkLoadOptions parseHictkLoadOptions(final @NotNull Path dataDirectory,
                                                                   final @NotNull io.vertx.core.json.JsonObject requestJson) {
        final var binTablePath = resolveOptionalRegularDataPath(dataDirectory, requestJson.getString("binTableFilename", ""));
        final var chromSizesPath = resolveOptionalRegularDataPath(dataDirectory, requestJson.getString("chromSizesFilename", ""));
        final var binSizeValue = requestJson.getValue("binSize");
        final Long binSize = binSizeValue instanceof Number number ? number.longValue() : null;
        final var oneBased = requestJson.getBoolean("oneBased", false);
        final var countAsFloat = requestJson.getBoolean("countAsFloat", false);
        return new HictkLoadOptions(binTablePath, chromSizesPath, binSize, oneBased, countAsFloat);
    }

    private static Path resolveOptionalRegularDataPath(final @NotNull Path dataDirectory,
                                                       final String filename) {
        if (filename == null || filename.isBlank()) {
            return null;
        }
        final var path = dataDirectory.resolve(filename).normalize();
        if (!path.startsWith(dataDirectory)) {
            throw new IllegalArgumentException("Invalid sidecar filename");
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Sidecar file not found: " + filename);
        }
        return path;
    }

    private ConversionJob createJob(final @NotNull Path sourcePath,
                                    final @NotNull Path outputPath,
                                    final Path dataDirectory,
                                    final @NotNull ConversionDirection direction,
                                    final int parallelism,
                                    final boolean deleteSourceOnCleanup,
                                    final boolean deleteOutputOnCleanup,
                                    final @NotNull HictkLoadOptions loadOptions) throws IOException {
        final var jobId = UUID.randomUUID().toString();
        final var job = new ConversionJob(jobId, sourcePath, outputPath, dataDirectory, direction, parallelism, deleteSourceOnCleanup, deleteOutputOnCleanup, loadOptions);
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
            if (job.direction == ConversionDirection.HICT_TO_MCOOL) {
                new HictToMcoolConverter().convert(options, conversionLogger);
            } else if (job.direction == ConversionDirection.MCOOL_TO_HICT) {
                new McoolToHictConverter().convert(options, conversionLogger);
            } else if (job.direction.requiresExternalHicToolchain()) {
                final var toolchain = this.hictkConversionPipeline.requireToolchain();
                job.toolchainSource = toolchain.source();
                job.toolchainSummary = "Using hictk command " + toolchain.hictkCommand();
                job.toolchainNotices.clear();
                job.toolchainNotices.addAll(toolchain.notices());
                job.toolchainCitations.clear();
                job.toolchainCitations.addAll(toolchain.citations());
                this.hictkConversionPipeline.convert(
                  job.direction,
                  options,
                  toolchain,
                  conversionLogger,
                  process -> job.activeProcess = process,
                  () -> job.cancelRequested.get(),
                  job.loadOptions
                );
            } else {
                throw new IllegalArgumentException("Unknown conversion direction: " + job.direction.wireName());
            }
            recordSuccessfulConversionIfPossible(job);
            job.overallProgress = 1.0d;
            job.stageProgress = 1.0d;
            job.status = job.cancelRequested.get() ? "cancelled" : "finished";
        } catch (Exception e) {
            if (job.cancelRequested.get()) {
                job.status = "cancelled";
                job.error = "Cancelled";
            } else {
                job.status = "failed";
                job.error = e.getMessage() == null ? e.toString() : e.getMessage();
                job.logs.add("ERROR: " + job.error);
            }
        } finally {
            job.activeProcess = null;
            job.finishedAtMs = Instant.now().toEpochMilli();
            group.onJobFinished();
        }
    }

    private void recordSuccessfulConversionIfPossible(final @NotNull ConversionJob job) {
        final var map = this.vertx.sharedData().getLocalMap("hict_server");
        final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) map.get("dataDirectory");
        if (dataDirectoryWrapper == null) {
            return;
        }
        final var dataDirectory = dataDirectoryWrapper.getPath();
        final var processedDirectoryWrapper = (ShareableWrappers.PathWrapper) map.get("processedDirectory");
        final var processedDirectory = processedDirectoryWrapper != null
            ? processedDirectoryWrapper.getPath()
            : dataDirectory.resolve("processed").normalize().toAbsolutePath();
        final var cacheManager = new MatrixConversionCacheManager(dataDirectory, processedDirectory, this.fingerprintService);
        cacheManager.recordSuccessfulConversion(job.sourcePath, job.outputPath, job.direction, job.loadOptions.dependencyPaths());
    }

    private void parseProgress(final @NotNull ConversionJob job, final @NotNull String message) {
        Matcher stageMatcher = STAGE_PROGRESS_PATTERN.matcher(message);
        if (stageMatcher.find()) {
            job.currentStage = stageMatcher.group(1);
            job.currentStageLabel = humanizeStageName(job.currentStage);
            job.stageProgress = clampUnit(stageMatcher.group(2));
            job.overallProgress = clampUnit(stageMatcher.group(3));
            job.stageDetail = stageMatcher.group(4) == null ? "" : stageMatcher.group(4).trim();
            return;
        }
        Matcher toolchainMatcher = TOOLCHAIN_PATTERN.matcher(message);
        if (toolchainMatcher.find()) {
            job.toolchainSource = toolchainMatcher.group(1);
            if (job.toolchainSummary == null || job.toolchainSummary.isBlank()) {
                job.toolchainSummary = "Resolved external toolchain for platform " + toolchainMatcher.group(2);
            }
            return;
        }
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

    static @NotNull String toClientFilename(final @NotNull Path path, final Path dataDirectory) {
        if (dataDirectory == null) {
            final var fileName = path.getFileName();
            return fileName == null ? path.toString() : fileName.toString();
        }
        final var normalizedDataDirectory = dataDirectory.normalize().toAbsolutePath();
        final var normalizedPath = path.normalize().toAbsolutePath();
        if (normalizedPath.startsWith(normalizedDataDirectory)) {
            return normalizedDataDirectory.relativize(normalizedPath).toString();
        }
        final var fileName = normalizedPath.getFileName();
        return fileName == null ? normalizedPath.toString() : fileName.toString();
    }

    private static @NotNull String humanizeStageName(final @NotNull String stageName) {
        return switch (stageName) {
            case "metadata" -> "Metadata";
            case "convert_base" -> "Base .cool conversion";
            case "zoomify" -> "Zoomify";
            case "balance" -> "Balancing";
            case "import_hict" -> "HiCT import";
            default -> stageName.replace('_', ' ');
        };
    }

    private static double clampPercent(final String value) {
        try {
            final var percent = Double.parseDouble(value);
            return Math.max(0.0d, Math.min(100.0d, percent)) / 100.0d;
        } catch (NumberFormatException ignored) {
            return 0.0d;
        }
    }

    private static double clampUnit(final String value) {
        try {
            final var parsed = Double.parseDouble(value);
            return Math.max(0.0d, Math.min(1.0d, parsed));
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

    private RequestTaskScheduler getScheduler(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
        final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
        final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
        if (wrapper == null) {
            ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
            return null;
        }
        return wrapper.getRequestTaskScheduler();
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
        private final Path dataDirectory;
        private final ConversionDirection direction;
        private final int parallelism;
        private final boolean deleteSourceOnCleanup;
        private final boolean deleteOutputOnCleanup;
        private final HictkLoadOptions loadOptions;
        private volatile String status = "queued";
        private volatile String error = "";
        private final CopyOnWriteArrayList<String> logs = new CopyOnWriteArrayList<>();
        private volatile String currentStage = "";
        private volatile String currentStageLabel = "";
        private volatile String stageDetail = "";
        private volatile double stageProgress = 0.0d;
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
        private volatile Process activeProcess = null;
        private volatile String toolchainSource = "";
        private volatile String toolchainSummary = "";
        private final CopyOnWriteArrayList<String> toolchainNotices = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<String> toolchainCitations = new CopyOnWriteArrayList<>();
        private final AtomicBoolean cancelRequested = new AtomicBoolean(false);

        private ConversionJob(String jobId, Path sourcePath, Path outputPath, Path dataDirectory, ConversionDirection direction, int parallelism, boolean deleteSourceOnCleanup, boolean deleteOutputOnCleanup, HictkLoadOptions loadOptions) {
            this.jobId = jobId;
            this.sourcePath = sourcePath;
            this.outputPath = outputPath;
            this.dataDirectory = dataDirectory;
            this.direction = direction;
            this.parallelism = parallelism;
            this.deleteSourceOnCleanup = deleteSourceOnCleanup;
            this.deleteOutputOnCleanup = deleteOutputOnCleanup;
            this.loadOptions = loadOptions == null ? HictkLoadOptions.EMPTY : loadOptions;
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
            if (activeProcess != null) {
                activeProcess.destroyForcibly();
            }
            if (workerThread != null) {
                workerThread.interrupt();
            }
        }

        private ConversionJobDTO toDto() {
            return new ConversionJobDTO(
                    jobId,
                    status,
                    toClientFilename(sourcePath, dataDirectory),
                    toClientFilename(outputPath, dataDirectory),
                    direction.wireName(),
                    currentStage == null ? "" : currentStage,
                    currentStageLabel == null ? "" : currentStageLabel,
                    stageDetail == null ? "" : stageDetail,
                    stageProgress,
                    overallProgress,
                    resolutionProgress,
                    currentResolution,
                    elapsedMillis,
                    etaMillis,
                    resolutionElapsedMillis,
                    resolutionEtaMillis,
                    inputSizeBytes,
                    outputSizeBytes,
                    toolchainSource == null ? "" : toolchainSource,
                    toolchainSummary == null ? "" : toolchainSummary,
                    List.copyOf(toolchainNotices),
                    List.copyOf(toolchainCitations),
                    List.copyOf(logs),
                    error == null ? "" : error
            );
        }
    }
}
