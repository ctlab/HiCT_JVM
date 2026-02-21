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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RequiredArgsConstructor
@Slf4j
public class ConversionHandlersHolder extends HandlersHolder {

    private final Vertx vertx;
    private static final long MAX_UPLOAD_BYTES = 2L * 1024 * 1024 * 1024;
    private static final long JOB_TTL_MS = 60 * 60 * 1000;

    private final ConcurrentHashMap<String, ConversionJob> jobs = new ConcurrentHashMap<>();

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
              final var outputExt = "hict-to-mcool".equals(direction) ? ".mcool" : ".hict";
              final var outputPath = Files.createTempFile("hict-converter-out-", outputExt);

              final var resolutionCsv = req.getParam("resolutions");
              final var resolutions = parseResolutions(resolutionCsv);
              final var compression = parseInteger(req.getParam("compression"), 0);
              final var compressionAlgorithm = ConversionOptions.CompressionAlgorithm.parse(req.getParam("compressionAlgorithm") == null ? "deflate" : req.getParam("compressionAlgorithm"));
              final var chunkSize = parseInteger(req.getParam("chunkSize"), 8192);
              final var applyAgp = Boolean.parseBoolean(req.getParam("applyAgp"));
              final var agpPath = req.getParam("agpPath") == null ? ConversionOptions.NO_AGP : req.getParam("agpPath");
              final var parallelism = parseInteger(req.getParam("parallelism"), Runtime.getRuntime().availableProcessors());

              final var jobId = UUID.randomUUID().toString();
              final var job = new ConversionJob(jobId, sourcePath, outputPath);
              jobs.put(jobId, job);

              final var options = new ConversionOptions(sourcePath, outputPath, resolutions, chunkSize, compression, compressionAlgorithm, agpPath, applyAgp, parallelism);
              vertx.executeBlocking(promise -> {
                  try {
                      job.status = "running";
                      if ("hict-to-mcool".equals(direction)) {
                          new HictToMcoolConverter().convert(options, job.logs::add);
                      } else if ("mcool-to-hict".equals(direction)) {
                          new McoolToHictConverter().convert(options, job.logs::add);
                      } else {
                          throw new IllegalArgumentException("Unknown conversion direction");
                      }
                      job.status = "finished";
                      promise.complete();
                  } catch (Exception e) {
                      job.status = "failed";
                      job.error = e.getMessage();
                      job.logs.add("ERROR: " + e.getMessage());
                      promise.fail(e);
                  }
              }, false);

              ctx.response().end(Json.encode(new ConversionSubmitResponseDTO("submitted", jobId)));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });

        router.get("/convert/jobs/:jobId").blockingHandler(ctx -> {
            final var job = jobs.get(ctx.pathParam("jobId"));
            if (job == null) {
                throw new IllegalArgumentException("Job not found");
            }
            ctx.response().end(Json.encode(job.toDto()));
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

    private static @NotNull
    List<Long> parseResolutions(final String csv) {
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

    private void cleanupOldJobs() {
        final var now = Instant.now().toEpochMilli();
        jobs.values().removeIf(job -> {
            final var expired = now - job.createdAtMs > JOB_TTL_MS;
            if (expired) {
                try {
                    Files.deleteIfExists(job.sourcePath);
                    Files.deleteIfExists(job.outputPath);
                } catch (IOException e) {
                    log.warn("Unable to cleanup temp files for {}", job.jobId, e);
                }
            }
            return expired;
        });
    }

    private static class ConversionJob {

        private final String jobId;
        private final long createdAtMs = Instant.now().toEpochMilli();
        private final Path sourcePath;
        private final Path outputPath;
        private volatile String status = "queued";
        private volatile String error = "";
        private final CopyOnWriteArrayList<String> logs = new CopyOnWriteArrayList<>();

        private ConversionJob(String jobId, Path sourcePath, Path outputPath) {
            this.jobId = jobId;
            this.sourcePath = sourcePath;
            this.outputPath = outputPath;
        }

        private ConversionJobDTO toDto() {
            return new ConversionJobDTO(
                    jobId,
                    status,
                    sourcePath.getFileName().toString(),
                    outputPath.getFileName().toString(),
                    List.copyOf(logs),
                    error == null ? "" : error
            );
        }
    }
}
