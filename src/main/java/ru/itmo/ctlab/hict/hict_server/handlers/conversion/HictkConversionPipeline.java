package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class HictkConversionPipeline {
  private static final @NotNull Pattern INTERNAL_OVERALL_PROGRESS_PATTERN = Pattern.compile("Overall progress:\\s*(\\d+)%");
  private static final @NotNull Pattern ZOOMIFY_RESOLUTION_PATTERN = Pattern.compile("generating\\s+(\\d+)\\s+resolution", Pattern.CASE_INSENSITIVE);

  private final @NotNull ExternalToolchainManager toolchainManager;

  public HictkConversionPipeline(final @NotNull ExternalToolchainManager toolchainManager) {
    this.toolchainManager = toolchainManager;
  }

  public @NotNull ExternalToolchainManager.ResolvedToolchain requireToolchain() {
    return this.toolchainManager.requireHictkToolchain();
  }

  public void convert(final @NotNull ConversionDirection direction,
                      final @NotNull ConversionOptions options,
                      final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                      final @NotNull Consumer<String> logger,
                      final @NotNull Consumer<Process> processSink,
                      final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    if (!direction.requiresExternalHicToolchain()) {
      throw new IllegalArgumentException("Unsupported direction for hictk pipeline: " + direction.wireName());
    }
    if (toolchain.hictkCommand() == null) {
      throw new IllegalStateException("hictk command is not available");
    }

    final var stagePlan = direction == ConversionDirection.HIC_TO_HICT
      ? List.of(
      new StageDefinition("metadata", "Inspect .hic metadata", 0.05d),
      new StageDefinition("convert_base", "Convert finest .hic resolution to .cool", 0.20d),
      new StageDefinition("zoomify", "Build .mcool pyramid", 0.25d),
      new StageDefinition("balance", "Balance generated .mcool resolutions", 0.20d),
      new StageDefinition("import_hict", "Import .mcool into HiCT", 0.30d)
    )
      : List.of(
      new StageDefinition("metadata", "Inspect .hic metadata", 0.10d),
      new StageDefinition("convert_base", "Convert finest .hic resolution to .cool", 0.30d),
      new StageDefinition("zoomify", "Build .mcool pyramid", 0.35d),
      new StageDefinition("balance", "Balance generated .mcool resolutions", 0.25d)
    );

    logger.accept("HICT_TOOLCHAIN source=" + toolchain.source() + " platform=" + toolchain.platform());

    final var tmpDirectory = Files.createTempDirectory("hict-hic-pipeline-");
    try {
      final var metadataStage = stagePlan.get(0);
      emitStage(logger, stagePlan, metadataStage.id(), 0.0d, "Reading .hic metadata");
      final var metadata = readMetadata(options.inputPath(), toolchain, processSink, cancellationRequested);
      final var targetResolutions = resolveTargetResolutions(options.resolutions(), metadata.resolutions());
      final var baseResolution = targetResolutions.stream().mapToLong(Long::longValue).min()
        .orElseThrow(() -> new IllegalStateException("No target resolutions were resolved for " + options.inputPath().getFileName()));
      emitStage(
        logger,
        stagePlan,
        metadataStage.id(),
        1.0d,
        "Resolved " + targetResolutions.size() + " output resolution(s); finest=" + baseResolution
      );

      final var baseCoolPath = tmpDirectory.resolve(stripSuffix(options.inputPath().getFileName().toString()) + ".base.cool");
      final var mcoolOutputPath = direction == ConversionDirection.HIC_TO_HICT
        ? tmpDirectory.resolve(stripSuffix(options.inputPath().getFileName().toString()) + ".generated.mcool")
        : options.outputPath();

      final var convertStage = stagePlan.get(1);
      emitStage(logger, stagePlan, convertStage.id(), 0.0d, "Converting .hic base resolution " + baseResolution + " to .cool");
      runStreamingCommand(
        List.of(
          toolchain.hictkCommand().toString(),
          "convert",
          options.inputPath().toString(),
          baseCoolPath.toString(),
          "--output-fmt",
          "cool",
          "--resolutions",
          Long.toString(baseResolution),
          "--threads",
          Integer.toString(Math.max(2, normalizeParallelism(options.parallelism()))),
          "--compression-lvl",
          Integer.toString(options.compressionLevel()),
          "--force"
        ),
        tmpDirectory,
        logger,
        line -> {
          logger.accept(line);
          if (line.toLowerCase(Locale.ROOT).contains("begin processing")) {
            emitStage(logger, stagePlan, convertStage.id(), 0.5d, "Copying base resolution " + baseResolution + " from .hic");
          }
        },
        processSink,
        cancellationRequested
      );
      emitStage(logger, stagePlan, convertStage.id(), 1.0d, "Created temporary base .cool at " + baseCoolPath.getFileName());

      final var zoomifyStage = stagePlan.get(2);
      emitStage(logger, stagePlan, zoomifyStage.id(), 0.0d, "Zoomifying .cool to .mcool");
      final var zoomifyState = new ZoomifyProgressState(targetResolutions);
      final var zoomifyCommand = new ArrayList<String>();
      zoomifyCommand.add(toolchain.hictkCommand().toString());
      zoomifyCommand.add("zoomify");
      zoomifyCommand.add(baseCoolPath.toString());
      zoomifyCommand.add(mcoolOutputPath.toString());
      zoomifyCommand.add("--force");
      zoomifyCommand.add("--threads");
      zoomifyCommand.add(Integer.toString(Math.max(1, normalizeParallelism(options.parallelism()))));
      zoomifyCommand.add("--compression-lvl");
      zoomifyCommand.add(Integer.toString(options.compressionLevel()));
      zoomifyCommand.add("--resolutions");
      targetResolutions.stream().map(String::valueOf).forEach(zoomifyCommand::add);

      runStreamingCommand(
        zoomifyCommand,
        tmpDirectory,
        logger,
        line -> {
          logger.accept(line);
          zoomifyState.onLogLine(line).ifPresent(detail -> emitStage(
            logger,
            stagePlan,
            zoomifyStage.id(),
            zoomifyState.progress(),
            detail
          ));
        },
        processSink,
        cancellationRequested
      );
      emitStage(logger, stagePlan, zoomifyStage.id(), 1.0d, "Generated .mcool pyramid at " + mcoolOutputPath.getFileName());

      final var balanceStage = stagePlan.get(3);
      emitStage(logger, stagePlan, balanceStage.id(), 0.0d, "Balancing " + targetResolutions.size() + " resolution(s)");
      for (int i = 0; i < targetResolutions.size(); i++) {
        checkCancelled(cancellationRequested);
        final var resolution = targetResolutions.get(i);
        final var localProgress = i / (double) targetResolutions.size();
        emitStage(logger, stagePlan, balanceStage.id(), localProgress, "Balancing resolution " + resolution);
        runStreamingCommand(
          List.of(
            toolchain.hictkCommand().toString(),
            "balance",
            "ice",
            "--threads",
            Integer.toString(Math.max(1, normalizeParallelism(options.parallelism()))),
            "--tmpdir",
            tmpDirectory.toString(),
            "--ignore-diags",
            "2",
            mcoolOutputPath + "::/resolutions/" + resolution
          ),
          tmpDirectory,
          logger,
          logger::accept,
          processSink,
          cancellationRequested
        );
        emitStage(
          logger,
          stagePlan,
          balanceStage.id(),
          (i + 1) / (double) targetResolutions.size(),
          "Balanced resolution " + resolution
        );
      }

      if (direction == ConversionDirection.HIC_TO_MCOOL) {
        return;
      }

      final var importStage = stagePlan.get(4);
      emitStage(logger, stagePlan, importStage.id(), 0.0d, "Importing generated .mcool into HiCT");
      final var wrappedLogger = createWrappedImportLogger(logger, stagePlan, importStage.id());
      new McoolToHictConverter().convert(
        new ConversionOptions(
          mcoolOutputPath,
          options.outputPath(),
          targetResolutions,
          options.chunkSize(),
          options.compressionLevel(),
          options.compressionAlgorithm(),
          ConversionOptions.NO_AGP,
          false,
          options.parallelism()
        ),
        wrappedLogger
      );
      emitStage(logger, stagePlan, importStage.id(), 1.0d, "Created " + options.outputPath().getFileName());
    } finally {
      processSink.accept(null);
      deleteRecursively(tmpDirectory);
    }
  }

  private @NotNull HicMetadata readMetadata(final @NotNull Path inputPath,
                                            final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                            final @NotNull Consumer<Process> processSink,
                                            final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    final var command = List.of(
      Objects.requireNonNull(toolchain.hictkCommand()).toString(),
      "metadata",
      inputPath.toString(),
      "--output-format",
      "json"
    );
    final var output = runCollectingCommand(command, inputPath.getParent(), processSink, cancellationRequested);
    final var json = new JsonObject(output);
    final var resolutionsJson = json.getJsonArray("resolutions", new JsonArray());
    final var resolutions = new ArrayList<Long>(resolutionsJson.size());
    for (int i = 0; i < resolutionsJson.size(); i++) {
      final var value = resolutionsJson.getValue(i);
      if (value instanceof Number number) {
        resolutions.add(number.longValue());
      }
    }
    if (resolutions.isEmpty()) {
      throw new IllegalStateException("hictk metadata did not return any resolutions for " + inputPath.getFileName());
    }
    resolutions.sort(Comparator.naturalOrder());
    return new HicMetadata(resolutions, json.getString("assembly", ""));
  }

  private @NotNull List<Long> resolveTargetResolutions(final @NotNull List<Long> requested,
                                                       final @NotNull List<Long> available) {
    if (requested == null || requested.isEmpty()) {
      return List.copyOf(available);
    }
    final var availableSet = new LinkedHashSet<>(available);
    final var requestedUnique = requested.stream()
      .filter(availableSet::contains)
      .sorted()
      .distinct()
      .toList();
    if (requestedUnique.isEmpty()) {
      throw new IllegalArgumentException("None of the requested resolutions are available in the input .hic file");
    }
    return requestedUnique;
  }

  private @NotNull Consumer<String> createWrappedImportLogger(final @NotNull Consumer<String> logger,
                                                              final @NotNull List<StageDefinition> stagePlan,
                                                              final @NotNull String stageId) {
    return message -> {
      final var parsedProgress = tryParseOverallProgress(message);
      if (parsedProgress.isPresent()) {
        emitStage(logger, stagePlan, stageId, parsedProgress.getAsLong() / 100.0d, "Importing .mcool into HiCT");
        logger.accept("IMPORT: " + message);
        return;
      }
      logger.accept("IMPORT: " + message);
    };
  }

  private static @NotNull OptionalLong tryParseOverallProgress(final @NotNull String message) {
    final var matcher = INTERNAL_OVERALL_PROGRESS_PATTERN.matcher(message);
    if (!matcher.find()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(Long.parseLong(matcher.group(1)));
  }

  private static void runStreamingCommand(final @NotNull List<String> command,
                                          final @NotNull Path workingDirectory,
                                          final @NotNull Consumer<String> logger,
                                          final @NotNull Consumer<String> lineObserver,
                                          final @NotNull Consumer<Process> processSink,
                                          final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    checkCancelled(cancellationRequested);
    logger.accept("Executing external command: " + String.join(" ", command));
    final var process = new ProcessBuilder(command)
      .directory(workingDirectory.toFile())
      .redirectErrorStream(true)
      .start();
    processSink.accept(process);
    try (final var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (cancellationRequested.getAsBoolean()) {
          process.destroyForcibly();
          throw new InterruptedException("External conversion was cancelled");
        }
        lineObserver.accept(line);
      }
    } finally {
      processSink.accept(null);
    }
    final var exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IllegalStateException("External command failed with exit code " + exitCode + ": " + String.join(" ", command));
    }
  }

  private static @NotNull String runCollectingCommand(final @NotNull List<String> command,
                                                      final @NotNull Path workingDirectory,
                                                      final @NotNull Consumer<Process> processSink,
                                                      final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    checkCancelled(cancellationRequested);
    final var process = new ProcessBuilder(command)
      .directory(workingDirectory.toFile())
      .redirectErrorStream(true)
      .start();
    processSink.accept(process);
    final var output = new StringBuilder();
    try (final var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (cancellationRequested.getAsBoolean()) {
          process.destroyForcibly();
          throw new InterruptedException("External conversion was cancelled");
        }
        output.append(line).append(System.lineSeparator());
      }
    } finally {
      processSink.accept(null);
    }
    final var exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IllegalStateException("External command failed with exit code " + exitCode + ": " + String.join(" ", command));
    }
    return output.toString().trim();
  }

  private static void emitStage(final @NotNull Consumer<String> logger,
                                final @NotNull List<StageDefinition> stagePlan,
                                final @NotNull String stageId,
                                final double stageProgress,
                                final @NotNull String detail) {
    final var boundedStageProgress = Math.max(0.0d, Math.min(1.0d, stageProgress));
    double completedWeight = 0.0d;
    double totalWeight = 0.0d;
    double currentWeight = 0.0d;
    for (final var stage : stagePlan) {
      totalWeight += stage.weight();
      if (stage.id().equals(stageId)) {
        currentWeight = stage.weight();
        break;
      }
      completedWeight += stage.weight();
    }
    final var overall = totalWeight <= 0.0d ? boundedStageProgress : (completedWeight + currentWeight * boundedStageProgress) / totalWeight;
    logger.accept(
      "HICT_STAGE stage=" + stageId
        + " progress=" + String.format(Locale.ROOT, "%.4f", boundedStageProgress)
        + " overall=" + String.format(Locale.ROOT, "%.4f", overall)
        + " detail=" + sanitizeDetail(detail)
    );
  }

  private static @NotNull String sanitizeDetail(final @NotNull String detail) {
    return detail.replace('\n', ' ').replace('\r', ' ').trim();
  }

  private static void checkCancelled(final @NotNull BooleanSupplier cancellationRequested) throws InterruptedException {
    if (cancellationRequested.getAsBoolean()) {
      throw new InterruptedException("External conversion was cancelled");
    }
  }

  private static int normalizeParallelism(final int parallelism) {
    return parallelism <= 0 ? Runtime.getRuntime().availableProcessors() : parallelism;
  }

  private static @NotNull String stripSuffix(final @NotNull String filename) {
    final var lowered = filename.toLowerCase(Locale.ROOT);
    if (lowered.endsWith(".hic")) {
      return filename.substring(0, filename.length() - ".hic".length());
    }
    return filename;
  }

  private static void deleteRecursively(final @NotNull Path root) {
    if (!Files.exists(root)) {
      return;
    }
    try (final var walk = Files.walk(root)) {
      walk.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (final IOException ignored) {
          // Best-effort cleanup only.
        }
      });
    } catch (final IOException ignored) {
      // Best-effort cleanup only.
    }
  }

  private record StageDefinition(@NotNull String id, @NotNull String label, double weight) {
  }

  private record HicMetadata(@NotNull List<Long> resolutions, @NotNull String assembly) {
  }

  private static final class ZoomifyProgressState {
    private final @NotNull List<Long> targetResolutions;
    private final LinkedHashSet<Long> startedResolutions = new LinkedHashSet<>();

    private ZoomifyProgressState(final @NotNull List<Long> targetResolutions) {
      this.targetResolutions = targetResolutions;
    }

    private @NotNull java.util.Optional<String> onLogLine(final @NotNull String line) {
      final var matcher = ZOOMIFY_RESOLUTION_PATTERN.matcher(line);
      if (!matcher.find()) {
        if (line.toLowerCase(Locale.ROOT).contains("copying") && !this.targetResolutions.isEmpty()) {
          this.startedResolutions.add(this.targetResolutions.get(0));
          return java.util.Optional.of("Copied base resolution " + this.targetResolutions.get(0));
        }
        return java.util.Optional.empty();
      }
      final var resolution = Long.parseLong(matcher.group(1));
      this.startedResolutions.add(resolution);
      return java.util.Optional.of("Generating resolution " + resolution);
    }

    private double progress() {
      if (this.targetResolutions.isEmpty()) {
        return 0.0d;
      }
      return Math.min(1.0d, this.startedResolutions.size() / (double) this.targetResolutions.size());
    }
  }
}
