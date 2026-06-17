package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
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
import java.util.zip.GZIPInputStream;

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
    convert(direction, options, toolchain, logger, processSink, cancellationRequested, HictkLoadOptions.EMPTY);
  }

  public void convert(final @NotNull ConversionDirection direction,
                      final @NotNull ConversionOptions options,
                      final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                      final @NotNull Consumer<String> logger,
                      final @NotNull Consumer<Process> processSink,
                      final @NotNull BooleanSupplier cancellationRequested,
                      final @NotNull HictkLoadOptions loadOptions) throws Exception {
    if (!direction.requiresExternalHicToolchain()) {
      throw new IllegalArgumentException("Unsupported direction for hictk pipeline: " + direction.wireName());
    }
    if (toolchain.hictkCommand() == null) {
      throw new IllegalStateException("hictk command is not available");
    }
    if (direction.requiresHictkLoadToolchain()) {
      convertHictkLoad(direction, options, toolchain, logger, processSink, cancellationRequested, loadOptions);
      return;
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
    if (!ConversionOptions.NO_AGP.equals(options.agpPath())) {
      logger.accept("HICT_ASSEMBLY layout=" + options.agpPath() + " accepted for this .hic conversion request");
    }

    final var tmpDirectory = Files.createTempDirectory("hict-hic-pipeline-");
    try {
      final var metadataStage = stagePlan.get(0);
      emitStage(logger, stagePlan, metadataStage.id(), 0.0d, "Reading .hic metadata");
      final var metadata = readMetadata(options.inputPath(), toolchain, processSink, cancellationRequested);
      final var targetResolutions = resolveTargetResolutions(options.resolutions(), metadata.resolutions());
      final var automaticZoomifyPyramid = shouldUseAutomaticZoomifyPyramid(options.resolutions(), targetResolutions);
      final var baseResolution = targetResolutions.stream().mapToLong(Long::longValue).min()
        .orElseThrow(() -> new IllegalStateException("No target resolutions were resolved for " + options.inputPath().getFileName()));
      emitStage(
        logger,
        stagePlan,
        metadataStage.id(),
        1.0d,
        automaticZoomifyPyramid
          ? "Resolved sparse .hic resolution metadata; finest=" + baseResolution + ", hictk nice-step pyramid will be generated"
          : "Resolved " + targetResolutions.size() + " output resolution(s); finest=" + baseResolution
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
          Integer.toString(normalizeParallelism(options.parallelism())),
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
      zoomifyCommand.add(Integer.toString(normalizeParallelism(options.parallelism())));
      zoomifyCommand.add("--compression-lvl");
      zoomifyCommand.add(Integer.toString(options.compressionLevel()));
      if (automaticZoomifyPyramid) {
        zoomifyCommand.add("--nice-steps");
        zoomifyCommand.add("--copy-base-resolution");
      } else {
        zoomifyCommand.add("--resolutions");
        targetResolutions.stream().map(String::valueOf).forEach(zoomifyCommand::add);
      }

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
      final var generatedResolutions = automaticZoomifyPyramid
        ? readMetadata(mcoolOutputPath, toolchain, processSink, cancellationRequested).resolutions()
        : targetResolutions;
      emitStage(
        logger,
        stagePlan,
        zoomifyStage.id(),
        1.0d,
        "Generated .mcool pyramid at " + mcoolOutputPath.getFileName() + " with " + generatedResolutions.size() + " resolution(s)"
      );

      final var balanceStage = stagePlan.get(3);
      emitStage(logger, stagePlan, balanceStage.id(), 0.0d, "Balancing " + generatedResolutions.size() + " resolution(s)");
      for (int i = 0; i < generatedResolutions.size(); i++) {
        checkCancelled(cancellationRequested);
        final var resolution = generatedResolutions.get(i);
        final var localProgress = i / (double) generatedResolutions.size();
        emitStage(logger, stagePlan, balanceStage.id(), localProgress, "Balancing resolution " + resolution);
        runStreamingCommand(
          List.of(
            toolchain.hictkCommand().toString(),
            "balance",
            "ice",
            "--threads",
            Integer.toString(normalizeParallelism(options.parallelism())),
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
          (i + 1) / (double) generatedResolutions.size(),
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
          generatedResolutions,
          options.chunkSize(),
          options.compressionLevel(),
          options.compressionAlgorithm(),
          options.agpPath(),
          false,
          options.parallelism(),
          true,
          ConversionOptions.ExportMode.AUTO
        ),
        wrappedLogger
      );
      emitStage(logger, stagePlan, importStage.id(), 1.0d, "Created " + options.outputPath().getFileName());
    } finally {
      processSink.accept(null);
      deleteRecursively(tmpDirectory);
    }
  }

  private void convertHictkLoad(final @NotNull ConversionDirection direction,
                                final @NotNull ConversionOptions options,
                                final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                final @NotNull Consumer<String> logger,
                                final @NotNull Consumer<Process> processSink,
                                final @NotNull BooleanSupplier cancellationRequested,
                                final @NotNull HictkLoadOptions requestedLoadOptions) throws Exception {
    final var stagePlan = List.of(
      new StageDefinition("prepare_load", "Prepare hictk load inputs", 0.10d),
      new StageDefinition("load_base", "Load text interactions to .cool", 0.25d),
      new StageDefinition("metadata", "Inspect loaded .cool metadata", 0.05d),
      new StageDefinition("zoomify", "Build .mcool pyramid", 0.25d),
      new StageDefinition("balance", "Balance generated .mcool resolutions", 0.15d),
      new StageDefinition("import_hict", "Import .mcool into HiCT", 0.20d)
    );

    logger.accept("HICT_TOOLCHAIN source=" + toolchain.source() + " platform=" + toolchain.platform());
    logger.accept("HICT_LOAD_FORMAT direction=" + direction.wireName());

    final var tmpDirectory = Files.createTempDirectory("hict-hictk-load-pipeline-");
    try {
      emitStage(logger, stagePlan, "prepare_load", 0.0d, "Resolving hictk load options");
      final var loadOptions = resolveLoadOptions(direction, options.inputPath(), requestedLoadOptions, tmpDirectory);
      emitStage(logger, stagePlan, "prepare_load", 1.0d, describeLoadOptions(direction, loadOptions));

      final var baseCoolPath = tmpDirectory.resolve(stripSuffix(options.inputPath().getFileName().toString()) + ".base.cool");
      final var mcoolOutputPath = tmpDirectory.resolve(stripSuffix(options.inputPath().getFileName().toString()) + ".generated.mcool");

      emitStage(logger, stagePlan, "load_base", 0.0d, "Loading interactions with hictk");
      final var loadCommand = buildLoadCommand(direction, options, toolchain, loadOptions, baseCoolPath, tmpDirectory);
      runStreamingCommand(
        loadCommand,
        tmpDirectory,
        logger,
        line -> {
          logger.accept(line);
          final var lower = line.toLowerCase(Locale.ROOT);
          if (lower.contains("sorting") || lower.contains("loading") || lower.contains("writing")) {
            emitStage(logger, stagePlan, "load_base", 0.5d, "hictk load is processing interactions");
          }
        },
        processSink,
        cancellationRequested
      );
      emitStage(logger, stagePlan, "load_base", 1.0d, "Created temporary base .cool at " + baseCoolPath.getFileName());

      emitStage(logger, stagePlan, "metadata", 0.0d, "Reading loaded .cool metadata");
      final var metadata = readMetadata(baseCoolPath, toolchain, processSink, cancellationRequested);
      final var targetResolutions = resolveLoadTargetResolutions(options.resolutions(), metadata.resolutions());
      final var baseResolution = targetResolutions.stream().mapToLong(Long::longValue).min()
        .orElseThrow(() -> new IllegalStateException("No target resolutions were resolved for " + options.inputPath().getFileName()));
      emitStage(logger, stagePlan, "metadata", 1.0d, "Loaded base resolution " + baseResolution);

      emitStage(logger, stagePlan, "zoomify", 0.0d, "Zoomifying .cool to .mcool");
      final var zoomifyState = new ZoomifyProgressState(targetResolutions);
      final var zoomifyCommand = new ArrayList<String>();
      zoomifyCommand.add(Objects.requireNonNull(toolchain.hictkCommand()).toString());
      zoomifyCommand.add("zoomify");
      zoomifyCommand.add(baseCoolPath.toString());
      zoomifyCommand.add(mcoolOutputPath.toString());
      zoomifyCommand.add("--force");
      zoomifyCommand.add("--threads");
      zoomifyCommand.add(Integer.toString(normalizeParallelism(options.parallelism())));
      zoomifyCommand.add("--compression-lvl");
      zoomifyCommand.add(Integer.toString(normalizeHictkCompressionLevel(options.compressionLevel())));
      zoomifyCommand.add("--nice-steps");
      zoomifyCommand.add("--copy-base-resolution");
      if (options.resolutions() != null && !options.resolutions().isEmpty()) {
        zoomifyCommand.add("--resolutions");
        targetResolutions.stream().map(String::valueOf).forEach(zoomifyCommand::add);
      }
      runStreamingCommand(
        zoomifyCommand,
        tmpDirectory,
        logger,
        line -> {
          logger.accept(line);
          zoomifyState.onLogLine(line).ifPresent(detail -> emitStage(
            logger,
            stagePlan,
            "zoomify",
            zoomifyState.progress(),
            detail
          ));
        },
        processSink,
        cancellationRequested
      );
      final var generatedResolutions = readMetadata(mcoolOutputPath, toolchain, processSink, cancellationRequested).resolutions();
      emitStage(logger, stagePlan, "zoomify", 1.0d, "Generated " + generatedResolutions.size() + " .mcool resolution(s)");

      emitStage(logger, stagePlan, "balance", 0.0d, "Balancing " + generatedResolutions.size() + " resolution(s)");
      for (int i = 0; i < generatedResolutions.size(); i++) {
        checkCancelled(cancellationRequested);
        final var resolution = generatedResolutions.get(i);
        emitStage(logger, stagePlan, "balance", i / (double) generatedResolutions.size(), "Balancing resolution " + resolution);
        runStreamingCommand(
          List.of(
            Objects.requireNonNull(toolchain.hictkCommand()).toString(),
            "balance",
            "ice",
            "--threads",
            Integer.toString(normalizeParallelism(options.parallelism())),
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
        emitStage(logger, stagePlan, "balance", (i + 1) / (double) generatedResolutions.size(), "Balanced resolution " + resolution);
      }

      emitStage(logger, stagePlan, "import_hict", 0.0d, "Importing generated .mcool into HiCT");
      new McoolToHictConverter().convert(
        new ConversionOptions(
          mcoolOutputPath,
          options.outputPath(),
          generatedResolutions,
          options.chunkSize(),
          options.compressionLevel(),
          options.compressionAlgorithm(),
          ConversionOptions.NO_AGP,
          false,
          options.parallelism(),
          true,
          ConversionOptions.ExportMode.AUTO
        ),
        createWrappedImportLogger(logger, stagePlan, "import_hict")
      );
      emitStage(logger, stagePlan, "import_hict", 1.0d, "Created " + options.outputPath().getFileName());
    } finally {
      processSink.accept(null);
      deleteRecursively(tmpDirectory);
    }
  }

  public void convertCoolerToHictWithPyramid(final @NotNull ConversionOptions options,
                                             final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                             final @NotNull Consumer<String> logger,
                                             final @NotNull Consumer<Process> processSink,
                                             final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    if (toolchain.hictkCommand() == null) {
      throw new IllegalStateException("hictk command is not available");
    }
    final var stagePlan = List.of(
      new StageDefinition("metadata", "Inspect input Cooler metadata", 0.08d),
      new StageDefinition("extract_base", "Prepare finest .cool base", 0.12d),
      new StageDefinition("zoomify", "Build .mcool pyramid", 0.25d),
      new StageDefinition("balance", "Balance generated .mcool resolutions", 0.15d),
      new StageDefinition("import_hict", "Import .mcool into HiCT", 0.40d)
    );

    logger.accept("HICT_TOOLCHAIN source=" + toolchain.source() + " platform=" + toolchain.platform());
    final var tmpDirectory = Files.createTempDirectory("hict-mcool-pyramid-pipeline-");
    try {
      emitStage(logger, stagePlan, "metadata", 0.0d, "Reading Cooler metadata");
      final var metadata = readMetadata(options.inputPath(), toolchain, processSink, cancellationRequested);
      final var baseResolution = metadata.resolutions().stream().mapToLong(Long::longValue).min()
        .orElseThrow(() -> new IllegalStateException("No input resolutions were resolved for " + options.inputPath().getFileName()));
      final var requestedPyramidResolutions = resolvePyramidTargetResolutions(options.resolutions(), metadata.resolutions());
      emitStage(
        logger,
        stagePlan,
        "metadata",
        1.0d,
        "Resolved finest resolution " + baseResolution + " from " + metadata.resolutions().size() + " input resolution(s)"
      );

      emitStage(logger, stagePlan, "extract_base", 0.0d, "Preparing base .cool at resolution " + baseResolution);
      final Path baseCoolPath;
      if (isSingleResolutionCooler(options.inputPath())) {
        baseCoolPath = options.inputPath();
        emitStage(logger, stagePlan, "extract_base", 1.0d, "Using input .cool as base resolution");
      } else {
        baseCoolPath = tmpDirectory.resolve(stripSuffix(options.inputPath().getFileName().toString()) + "." + baseResolution + ".base.cool");
        convertMcoolResolutionToCool(options, toolchain, baseResolution, baseCoolPath, tmpDirectory, logger, processSink, cancellationRequested);
        emitStage(logger, stagePlan, "extract_base", 1.0d, "Extracted base resolution to " + baseCoolPath.getFileName());
      }

      final var generatedMcoolPath = tmpDirectory.resolve(stripSuffix(options.inputPath().getFileName().toString()) + ".pyramid.mcool");
      emitStage(logger, stagePlan, "zoomify", 0.0d, "Zoomifying base .cool to .mcool");
      final var zoomifyState = new ZoomifyProgressState(requestedPyramidResolutions);
      final var zoomifyCommand = new ArrayList<String>();
      zoomifyCommand.add(Objects.requireNonNull(toolchain.hictkCommand()).toString());
      zoomifyCommand.add("zoomify");
      zoomifyCommand.add(baseCoolPath.toString());
      zoomifyCommand.add(generatedMcoolPath.toString());
      zoomifyCommand.add("--force");
      zoomifyCommand.add("--threads");
      zoomifyCommand.add(Integer.toString(normalizeParallelism(options.parallelism())));
      zoomifyCommand.add("--compression-lvl");
      zoomifyCommand.add(Integer.toString(normalizeHictkCompressionLevel(options.compressionLevel())));
      if (options.resolutions() == null || options.resolutions().isEmpty()) {
        zoomifyCommand.add("--nice-steps");
        zoomifyCommand.add("--copy-base-resolution");
      } else {
        zoomifyCommand.add("--resolutions");
        requestedPyramidResolutions.stream().map(String::valueOf).forEach(zoomifyCommand::add);
      }
      runStreamingCommand(
        zoomifyCommand,
        tmpDirectory,
        logger,
        line -> {
          logger.accept(line);
          zoomifyState.onLogLine(line).ifPresent(detail -> emitStage(
            logger,
            stagePlan,
            "zoomify",
            zoomifyState.progress(),
            detail
          ));
        },
        processSink,
        cancellationRequested
      );
      final var generatedResolutions = readMetadata(generatedMcoolPath, toolchain, processSink, cancellationRequested).resolutions();
      emitStage(logger, stagePlan, "zoomify", 1.0d, "Generated " + generatedResolutions.size() + " resolution(s)");

      emitStage(logger, stagePlan, "balance", 0.0d, "Balancing " + generatedResolutions.size() + " generated resolution(s)");
      for (int i = 0; i < generatedResolutions.size(); i++) {
        checkCancelled(cancellationRequested);
        final var resolution = generatedResolutions.get(i);
        emitStage(logger, stagePlan, "balance", i / (double) generatedResolutions.size(), "Balancing resolution " + resolution);
        try {
          runStreamingCommand(
            List.of(
              Objects.requireNonNull(toolchain.hictkCommand()).toString(),
              "balance",
              "ice",
              "--force",
              "--threads",
              Integer.toString(normalizeParallelism(options.parallelism())),
              "--tmpdir",
              tmpDirectory.toString(),
              "--ignore-diags",
              "2",
              generatedMcoolPath + "::/resolutions/" + resolution
            ),
            tmpDirectory,
            logger,
            logger::accept,
            processSink,
            cancellationRequested
          );
        } catch (IllegalStateException balanceFailure) {
          logger.accept("WARNING: hictk balance failed for resolution " + resolution + "; keeping unbalanced generated pixels. " + balanceFailure.getMessage());
        }
        emitStage(logger, stagePlan, "balance", (i + 1) / (double) generatedResolutions.size(), "Processed balance for resolution " + resolution);
      }

      emitStage(logger, stagePlan, "import_hict", 0.0d, "Importing generated .mcool into HiCT");
      new McoolToHictConverter().convert(
        new ConversionOptions(
          generatedMcoolPath,
          options.outputPath(),
          generatedResolutions,
          options.chunkSize(),
          options.compressionLevel(),
          options.compressionAlgorithm(),
          options.agpPath(),
          false,
          options.parallelism(),
          true,
          false,
          ConversionOptions.ExportMode.AUTO
        ),
        createWrappedImportLogger(logger, stagePlan, "import_hict")
      );
      emitStage(logger, stagePlan, "import_hict", 1.0d, "Created " + options.outputPath().getFileName());
    } finally {
      processSink.accept(null);
      deleteRecursively(tmpDirectory);
    }
  }

  private void convertMcoolResolutionToCool(final @NotNull ConversionOptions options,
                                            final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                            final long baseResolution,
                                            final @NotNull Path baseCoolPath,
                                            final @NotNull Path tmpDirectory,
                                            final @NotNull Consumer<String> logger,
                                            final @NotNull Consumer<Process> processSink,
                                            final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    final var hictk = Objects.requireNonNull(toolchain.hictkCommand()).toString();
    final var uriCommand = List.of(
      hictk,
      "convert",
      options.inputPath() + "::/resolutions/" + baseResolution,
      baseCoolPath.toString(),
      "--output-fmt",
      "cool",
      "--threads",
      Integer.toString(normalizeParallelism(options.parallelism())),
      "--compression-lvl",
      Integer.toString(normalizeHictkCompressionLevel(options.compressionLevel())),
      "--force"
    );
    try {
      runStreamingCommand(uriCommand, tmpDirectory, logger, logger::accept, processSink, cancellationRequested);
      return;
    } catch (IllegalStateException uriFailure) {
      logger.accept("WARNING: hictk could not extract " + options.inputPath().getFileName() + "::/resolutions/" + baseResolution + "; retrying with --resolutions. " + uriFailure.getMessage());
    }
    runStreamingCommand(
      List.of(
        hictk,
        "convert",
        options.inputPath().toString(),
        baseCoolPath.toString(),
        "--output-fmt",
        "cool",
        "--resolutions",
        Long.toString(baseResolution),
        "--threads",
        Integer.toString(normalizeParallelism(options.parallelism())),
        "--compression-lvl",
        Integer.toString(normalizeHictkCompressionLevel(options.compressionLevel())),
        "--force"
      ),
      tmpDirectory,
      logger,
      logger::accept,
      processSink,
      cancellationRequested
    );
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
      final var binSize = json.getValue("bin-size");
      if (binSize instanceof Number number && number.longValue() > 0L) {
        resolutions.add(number.longValue());
      }
    }
    if (resolutions.isEmpty()) {
      throw new IllegalStateException("hictk metadata did not return any resolutions for " + inputPath.getFileName());
    }
    resolutions.sort(Comparator.naturalOrder());
    return new HicMetadata(resolutions, json.getString("assembly", ""));
  }

  private static @NotNull HictkLoadOptions resolveLoadOptions(final @NotNull ConversionDirection direction,
                                                              final @NotNull Path inputPath,
                                                              final @NotNull HictkLoadOptions requested,
                                                              final @NotNull Path tmpDirectory) throws IOException {
    final var binTable = requested.binTablePath() != null
      ? requested.binTablePath()
      : discoverSibling(inputPath, List.of(".bed", ".bins.bed", ".bin_table.bed"));
    final var chromSizes = requested.chromSizesPath() != null
      ? requested.chromSizesPath()
      : discoverSibling(inputPath, List.of(".chrom.sizes", ".chromsizes", ".chrom_sizes.txt"));
    final var binSize = requested.binSize();

    return switch (direction) {
      case HICPRO_MATRIX_TO_HICT -> {
        final var geometry = bedToChromSizes(
          requireRegularPath(
            binTable,
            "Hi-C Pro .matrix conversion requires the matching BED3+ bin table. Select it explicitly or place a same-stem .bed file next to the .matrix file."
          ),
          tmpDirectory
        );
        yield new HictkLoadOptions(null, geometry.chromSizesPath(), geometry.binSize(), true, requested.countAsFloat());
      }
      case COO_TO_HICT -> {
        if (binTable != null) {
          final var geometry = bedToChromSizes(
            requireRegularPath(binTable, "Selected COO bin table does not exist: " + binTable.getFileName()),
            tmpDirectory
          );
          yield new HictkLoadOptions(null, geometry.chromSizesPath(), geometry.binSize(), requested.oneBased(), requested.countAsFloat());
        }
        if (chromSizes != null && binSize != null && binSize > 0) {
          yield new HictkLoadOptions(null, chromSizes, binSize, requested.oneBased(), requested.countAsFloat());
        }
        final var synthetic = createSyntheticChromSizes(inputPath, tmpDirectory, binSize == null || binSize <= 0 ? 1L : binSize);
        yield new HictkLoadOptions(null, synthetic.chromSizesPath(), synthetic.binSize(), requested.oneBased(), requested.countAsFloat());
      }
      case BG2_TO_HICT, VALIDPAIRS_TO_HICT -> new HictkLoadOptions(
        null,
        requireRegularPath(
          chromSizes,
          direction == ConversionDirection.BG2_TO_HICT
            ? "BEDPE/bedGraph2 conversion requires a .chrom.sizes file and bin size."
            : "validPairs conversion requires a .chrom.sizes file and bin size."
        ),
        requirePositiveBinSize(binSize, direction),
        requested.oneBased(),
        requested.countAsFloat()
      );
      case PAIRS_TO_HICT -> requested;
      default -> throw new IllegalArgumentException("Unsupported hictk load direction: " + direction.wireName());
    };
  }

  private static @NotNull List<String> buildLoadCommand(final @NotNull ConversionDirection direction,
                                                        final @NotNull ConversionOptions options,
                                                        final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                                        final @NotNull HictkLoadOptions loadOptions,
                                                        final @NotNull Path baseCoolPath,
                                                        final @NotNull Path tmpDirectory) {
    final var command = new ArrayList<String>();
    command.add(Objects.requireNonNull(toolchain.hictkCommand()).toString());
    command.add("load");
    command.add(options.inputPath().toString());
    command.add(baseCoolPath.toString());
    command.add("--format");
    command.add(hictkLoadFormat(direction));
    command.add("--output-fmt");
    command.add("cool");
    command.add("--force");
    command.add("--tmpdir");
    command.add(tmpDirectory.toString());
    command.add("--threads");
    command.add(Integer.toString(normalizeParallelism(options.parallelism())));
    command.add("--compression-lvl");
    command.add(Integer.toString(normalizeHictkCompressionLevel(options.compressionLevel())));
    if (loadOptions.binTablePath() != null) {
      command.add("--bin-table");
      command.add(loadOptions.binTablePath().toString());
    }
    if (loadOptions.chromSizesPath() != null) {
      command.add("--chrom-sizes");
      command.add(loadOptions.chromSizesPath().toString());
    }
    if (loadOptions.binSize() != null) {
      command.add("--bin-size");
      command.add(Long.toString(loadOptions.binSize()));
    }
    if (direction == ConversionDirection.HICPRO_MATRIX_TO_HICT || direction == ConversionDirection.COO_TO_HICT || direction == ConversionDirection.BG2_TO_HICT) {
      if (loadOptions.oneBased()) {
        command.add("--one-based");
      } else {
        command.add("--zero-based");
      }
    }
    if (loadOptions.countAsFloat()) {
      command.add("--count-as-float");
    }
    if (direction == ConversionDirection.COO_TO_HICT || direction == ConversionDirection.HICPRO_MATRIX_TO_HICT) {
      command.add("--transpose-lower-triangular-pixels");
    }
    return command;
  }

  private static @NotNull String hictkLoadFormat(final @NotNull ConversionDirection direction) {
    return switch (direction) {
      case HICPRO_MATRIX_TO_HICT, COO_TO_HICT -> "coo";
      case BG2_TO_HICT -> "bg2";
      case PAIRS_TO_HICT -> "4dn";
      case VALIDPAIRS_TO_HICT -> "validpairs";
      default -> throw new IllegalArgumentException("Unsupported hictk load direction: " + direction.wireName());
    };
  }

  private static @NotNull List<Long> resolveLoadTargetResolutions(final @NotNull List<Long> requested,
                                                                  final @NotNull List<Long> loadedResolutions) {
    if (requested == null || requested.isEmpty()) {
      return List.copyOf(loadedResolutions);
    }
    final var baseResolution = loadedResolutions.stream().mapToLong(Long::longValue).min()
      .orElseThrow(() -> new IllegalStateException("hictk load did not create any base resolution"));
    final var out = new ArrayList<Long>();
    out.add(baseResolution);
    requested.stream()
      .filter(value -> value != null && value > 0)
      .sorted()
      .distinct()
      .forEach(value -> {
        if (!out.contains(value)) {
          out.add(value);
        }
      });
    out.sort(Comparator.naturalOrder());
    return List.copyOf(out);
  }

  private static @NotNull String describeLoadOptions(final @NotNull ConversionDirection direction,
                                                     final @NotNull HictkLoadOptions options) {
    final var parts = new ArrayList<String>();
    parts.add("format=" + hictkLoadFormat(direction));
    if (options.binTablePath() != null) {
      parts.add("bin-table=" + options.binTablePath().getFileName());
    }
    if (options.chromSizesPath() != null) {
      parts.add("chrom-sizes=" + options.chromSizesPath().getFileName());
    }
    if (options.binSize() != null) {
      parts.add("bin-size=" + options.binSize());
    }
    return "Resolved " + String.join(", ", parts);
  }

  private static Path discoverSibling(final @NotNull Path inputPath,
                                      final @NotNull List<String> suffixes) {
    final var parent = inputPath.getParent();
    if (parent == null) {
      return null;
    }
    final var base = stripCompressionSuffix(inputPath.getFileName().toString());
    final var stem = stripExtension(base);
    for (final var suffix : suffixes) {
      final var candidate = parent.resolve(stem + suffix);
      if (Files.isRegularFile(candidate)) {
        return candidate;
      }
    }
    for (final var suffix : suffixes) {
      final var candidate = parent.resolve(suffix.startsWith(".") ? suffix.substring(1) : suffix);
      if (Files.isRegularFile(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  private static @NotNull Path requireRegularPath(final Path path,
                                                  final @NotNull String message) {
    if (path == null || !Files.isRegularFile(path)) {
      throw new IllegalArgumentException(message);
    }
    return path;
  }

  private static long requirePositiveBinSize(final Long binSize,
                                             final @NotNull ConversionDirection direction) {
    if (binSize == null || binSize <= 0) {
      throw new IllegalArgumentException(
        direction.wireName() + " requires a positive bin size. Provide binSize in the conversion request."
      );
    }
    return binSize;
  }

  static @NotNull BedLoadGeometry createSyntheticChromSizes(final @NotNull Path inputPath,
                                                            final @NotNull Path tmpDirectory,
                                                            final long binSize) throws IOException {
    if (!isPlainOrGzip(inputPath)) {
      throw new IllegalArgumentException(
        "Generic COO conversion without explicit chrom sizes can only auto-scan plain text or .gz files. "
          + "For .xz/.zst/.bz2 and other compressed COO inputs, provide a .chrom.sizes file and bin size."
      );
    }
    long maxBin = -1L;
    try (final var reader = openTextReader(inputPath)) {
      String line;
      long lineNumber = 0L;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        final var trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        final var tokens = splitInteractionLine(trimmed);
        if (tokens.length < 3) {
          throw new IllegalArgumentException("COO row " + lineNumber + " has fewer than 3 columns");
        }
        final var row = Long.parseLong(tokens[0]);
        final var col = Long.parseLong(tokens[1]);
        if (row < 0 || col < 0) {
          throw new IllegalArgumentException("COO row " + lineNumber + " uses a negative bin index");
        }
        maxBin = Math.max(maxBin, Math.max(row, col));
      }
    }
    if (maxBin < 0) {
      throw new IllegalArgumentException("COO input does not contain any interaction rows");
    }
    final var chromSizes = tmpDirectory.resolve("synthetic-coo.chrom.sizes");
    try (final var writer = Files.newBufferedWriter(chromSizes, StandardCharsets.UTF_8)) {
      writer.write("assembly\t" + ((maxBin + 1L) * binSize));
      writer.newLine();
    }
    return new BedLoadGeometry(chromSizes, binSize);
  }

  private static @NotNull BedLoadGeometry bedToChromSizes(final @NotNull Path bedPath,
                                                          final @NotNull Path tmpDirectory) throws IOException {
    final var chromSizes = new java.util.LinkedHashMap<String, Long>();
    Long binSize = null;
    try (final var reader = Files.newBufferedReader(bedPath, StandardCharsets.UTF_8)) {
      String line;
      long lineNumber = 0L;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        final var trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        final var tokens = trimmed.split("\\s+");
        if (tokens.length < 3) {
          throw new IllegalArgumentException("BED bin table row " + lineNumber + " has fewer than 3 columns");
        }
        final var chrom = tokens[0];
        final var start = Long.parseLong(tokens[1]);
        final var end = Long.parseLong(tokens[2]);
        if (start < 0 || end <= start) {
          throw new IllegalArgumentException("BED bin table row " + lineNumber + " has invalid coordinates");
        }
        final var localBinSize = end - start;
        if (binSize == null) {
          binSize = localBinSize;
        } else if (!binSize.equals(localBinSize) && localBinSize > binSize) {
          throw new IllegalArgumentException("BED bin table must use a fixed bin size; row " + lineNumber + " has width " + localBinSize + " but expected " + binSize);
        }
        chromSizes.merge(chrom, end, Math::max);
      }
    }
    if (chromSizes.isEmpty() || binSize == null || binSize <= 0) {
      throw new IllegalArgumentException("BED bin table does not contain any bins");
    }
    final var chromSizesPath = tmpDirectory.resolve("bed-derived.chrom.sizes");
    try (final var writer = Files.newBufferedWriter(chromSizesPath, StandardCharsets.UTF_8)) {
      for (final var entry : chromSizes.entrySet()) {
        writer.write(entry.getKey() + "\t" + entry.getValue());
        writer.newLine();
      }
    }
    return new BedLoadGeometry(chromSizesPath, binSize);
  }

  private static @NotNull BufferedReader openTextReader(final @NotNull Path inputPath) throws IOException {
    final Reader reader;
    if (inputPath.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".gz")) {
      reader = new InputStreamReader(new GZIPInputStream(Files.newInputStream(inputPath)), StandardCharsets.UTF_8);
    } else {
      reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8);
    }
    return new BufferedReader(reader);
  }

  private static boolean isPlainOrGzip(final @NotNull Path inputPath) {
    final var lowered = inputPath.getFileName().toString().toLowerCase(Locale.ROOT);
    return !lowered.matches(".*\\.(xz|zst|zstd|bz2|lz4|lzo)$");
  }

  private static @NotNull String[] splitInteractionLine(final @NotNull String line) {
    return line.indexOf(',') >= 0 ? line.split("\\s*,\\s*") : line.split("\\s+");
  }

  static @NotNull List<Long> resolveTargetResolutions(final @NotNull List<Long> requested,
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

  static boolean shouldUseAutomaticZoomifyPyramid(final @NotNull List<Long> requested,
                                                 final @NotNull List<Long> targetResolutions) {
    if (requested != null && !requested.isEmpty()) {
      return false;
    }
    if (targetResolutions.size() < 2) {
      return true;
    }
    final var minResolution = targetResolutions.stream().mapToLong(Long::longValue).min().orElse(0L);
    final var maxResolution = targetResolutions.stream().mapToLong(Long::longValue).max().orElse(0L);
    return minResolution > 0L && maxResolution < minResolution * 10L;
  }

  static @NotNull List<Long> resolvePyramidTargetResolutions(final @NotNull List<Long> requested,
                                                             final @NotNull List<Long> available) {
    final var baseResolution = available.stream().mapToLong(Long::longValue).min()
      .orElseThrow(() -> new IllegalStateException("Input Cooler does not contain any resolutions"));
    if (requested == null || requested.isEmpty()) {
      return List.of(baseResolution);
    }
    final var out = new ArrayList<Long>();
    out.add(baseResolution);
    requested.stream()
      .filter(value -> value != null && value >= baseResolution)
      .sorted()
      .distinct()
      .forEach(value -> {
        if (!out.contains(value)) {
          out.add(value);
        }
      });
    out.sort(Comparator.naturalOrder());
    return List.copyOf(out);
  }

  private static boolean isSingleResolutionCooler(final @NotNull Path inputPath) {
    final var lowered = inputPath.getFileName().toString().toLowerCase(Locale.ROOT);
    return lowered.endsWith(".cool") && !lowered.endsWith(".mcool");
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
    final var output = new StringBuilder();
    try (final var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (cancellationRequested.getAsBoolean()) {
          process.destroyForcibly();
          throw new InterruptedException("External conversion was cancelled");
        }
        output.append(line).append(System.lineSeparator());
        lineObserver.accept(line);
      }
    } finally {
      processSink.accept(null);
    }
    final var exitCode = process.waitFor();
    if (exitCode != 0) {
      throw externalCommandFailure(command, exitCode, output.toString());
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
      throw externalCommandFailure(command, exitCode, output.toString());
    }
    return output.toString().trim();
  }

  private static @NotNull IllegalStateException externalCommandFailure(final @NotNull List<String> command,
                                                                       final int exitCode,
                                                                       final @NotNull String output) {
    final var message = new StringBuilder()
      .append("External command failed with exit code ")
      .append(exitCode)
      .append(": ")
      .append(String.join(" ", command));
    final var trimmedOutput = output.trim();
    if (!trimmedOutput.isBlank()) {
      message
        .append(System.lineSeparator())
        .append("External command output:")
        .append(System.lineSeparator())
        .append(trimmedOutput);
    }
    return new IllegalStateException(message.toString());
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
    final var availableProcessors = Math.max(1, Runtime.getRuntime().availableProcessors());
    if (parallelism <= 0) {
      return availableProcessors;
    }
    return Math.max(1, Math.min(parallelism, availableProcessors));
  }

  private static int normalizeHictkCompressionLevel(final int compressionLevel) {
    if (compressionLevel <= 0) {
      return 6;
    }
    return Math.max(1, Math.min(12, compressionLevel));
  }

  private static @NotNull String stripSuffix(final @NotNull String filename) {
    final var lowered = filename.toLowerCase(Locale.ROOT);
    if (lowered.endsWith(".hic")) {
      return filename.substring(0, filename.length() - ".hic".length());
    }
    return stripExtension(stripCompressionSuffix(filename));
  }

  private static @NotNull String stripCompressionSuffix(final @NotNull String filename) {
    return ConversionDirection.stripCompressionSuffix(filename);
  }

  private static @NotNull String stripExtension(final @NotNull String filename) {
    final var dot = filename.lastIndexOf('.');
    if (dot <= 0) {
      return filename;
    }
    return filename.substring(0, dot);
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

  record BedLoadGeometry(@NotNull Path chromSizesPath, long binSize) {
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
