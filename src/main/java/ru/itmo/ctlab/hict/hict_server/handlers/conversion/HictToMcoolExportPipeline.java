package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5DataClass;
import ch.systemsx.cisd.hdf5.HDF5FloatStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import io.vertx.core.json.JsonArray;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.Initializers;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.HictToMcoolConverter;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.zip.GZIPOutputStream;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockColsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockLengthDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockOffsetDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockRowsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockValuesDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getDenseBlockDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeLengthsBinsDatasetPath;

public final class HictToMcoolExportPipeline {
  private static final String DEFAULT_ASSEMBLY_NAME = "HiCT current assembly";
  private static final int DENSE_BLOCK_SIZE = 256;
  private static final int DEFAULT_COO_MERGE_FAN_IN = 64;
  private static final int MIN_COO_MERGE_FAN_IN = 2;
  private static final int MAX_COO_MERGE_FAN_IN = 512;
  private static final long DEFAULT_EXPORT_MAX_MEMORY_BYTES = 16L * 1024L * 1024L * 1024L;
  private static final long MIN_EXPORT_MAX_MEMORY_BYTES = 256L * 1024L * 1024L;
  private static final int MIN_COO_SORT_BATCH_SIZE = 250_000;
  private static final int MAX_COO_SORT_BATCH_SIZE = 8_000_000;
  private static final int MIN_COO_SOURCE_READ_BATCH_SIZE = 16_384;
  private static final int MAX_COO_SOURCE_READ_BATCH_SIZE = 4_000_000;
  private static final int DEFAULT_COO_SOURCE_READ_BATCH_SIZE = 1_048_576;
  private static final int MIN_HICTK_LOAD_CHUNK_SIZE = 1_000_000;
  private static final int MAX_HICTK_LOAD_CHUNK_SIZE = 100_000_000;
  private static final int DEFAULT_HICTK_LOAD_CHUNK_SIZE = 20_000_000;
  private static final long ESTIMATED_COO_RECORD_BYTES = 128L;
  private static final long PROGRESS_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30L);
  private static final long ETA_WARMUP_MIN_ITEMS = 1_000_000L;
  private final @NotNull ExternalToolchainManager toolchainManager;

  public HictToMcoolExportPipeline(final @NotNull ExternalToolchainManager toolchainManager) {
    this.toolchainManager = toolchainManager;
  }

  public void convert(final @NotNull ConversionOptions options,
                      final @NotNull Consumer<String> logger) throws Exception {
    convert(options, logger, process -> {
    }, () -> false);
  }

  public void convert(final @NotNull ConversionOptions options,
                      final @NotNull Consumer<String> logger,
                      final @NotNull Consumer<Process> processSink,
                      final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    final var effectiveMode = resolveEffectiveMode(options);
    if (effectiveMode == ConversionOptions.ExportMode.INTERNAL) {
      if (options.balanceExportedCoolers()) {
        logger.accept("WARNING: Direct internal Cooler exporter cannot run Cooler balancing; use hictk-assisted or auto mode for balanced output.");
      }
      new HictToMcoolConverter().convert(options, logger);
      return;
    }

    final var toolchain = toolchainManager.requireHictkToolchain();
    logger.accept("HICT_TOOLCHAIN source=" + toolchain.source() + " platform=" + toolchain.platform());
    convertViaHictk(options, toolchain, logger, processSink, cancellationRequested);
  }

  private @NotNull ConversionOptions.ExportMode resolveEffectiveMode(final @NotNull ConversionOptions options) {
    if (options.exportMode() == ConversionOptions.ExportMode.AUTO) {
      try {
        toolchainManager.requireHictkToolchain();
        return ConversionOptions.ExportMode.HICTK;
      } catch (IllegalStateException ignored) {
        return ConversionOptions.ExportMode.INTERNAL;
      }
    }
    if (options.exportMode() == ConversionOptions.ExportMode.HICTK && isSingleCoolerOutput(options.outputPath())) {
      return ConversionOptions.ExportMode.HICTK;
    }
    if (options.exportMode() == ConversionOptions.ExportMode.HICTK) {
      return ConversionOptions.ExportMode.HICTK;
    }
    if (options.exportMode() == ConversionOptions.ExportMode.INTERNAL) {
      return ConversionOptions.ExportMode.INTERNAL;
    }
    return options.exportMode();
  }

  private void convertViaHictk(final @NotNull ConversionOptions options,
                               final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                               final @NotNull Consumer<String> logger,
                               final @NotNull Consumer<Process> processSink,
                               final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    HDF5LibraryInitializer.initializeHDF5Library();
    final var synchronizedLogger = synchronizedLogger(logger);
    final var tmpDirectory = createExportTempDirectory();
    final var cooCompression = resolveCooTextCompression(synchronizedLogger);
    final long exportMaxMemoryBytes = resolveExportMaxMemoryBytes(synchronizedLogger);
    final int hictkThreads = resolveToolThreads(options.parallelism());
    synchronizedLogger.accept("Preparing hictk-assisted .mcool export in " + tmpDirectory);
    synchronizedLogger.accept("HiCT native processing: " + NativeProcessingService.getInstance().status().summary());
    synchronizedLogger.accept(
      "hictk-assisted export resources: hictkThreads=" + hictkThreads +
        ", cooCompression=" + cooCompression.logName() +
        " (legacy sorted-temp path only; hictk-assisted export streams COO directly)" +
        ", exportMemoryBudget=" + formatByteSize(exportMaxMemoryBytes)
    );

    try (final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(options.inputPath(), 2, 8))) {
      if (options.applyAgpBeforeExport() && !options.agpPath().isBlank()) {
        final var agpPath = Path.of(options.agpPath());
        try (final var reader = Files.newBufferedReader(agpPath, StandardCharsets.UTF_8)) {
          chunkedFile.importAGP(reader);
        }
        synchronizedLogger.accept("Applied AGP before export: " + agpPath);
      }

      final var selectedResolutions = HictToMcoolConverter.normalizeSelectedResolutionsForOutput(
        options.outputPath(),
        HictToMcoolConverter.requireUsableSelectedResolutions(
          options.inputPath(),
          resolveResolutions(chunkedFile, options),
          chunkedFile.getResolutions(),
          options.resolutions(),
          options.exportAllResolutions()
        ),
        synchronizedLogger
      );
      final var assemblyLayout = HictToMcoolConverter.buildCoolerAssemblyLayout(chunkedFile, selectedResolutions);
      final var totalSourcePixels = countSourcePixels(options.inputPath(), selectedResolutions);
      final var overallTracker = new OverallProgressTracker(
        Math.max(1L, totalSourcePixels * 2L),
        synchronizedLogger
      );
      if (isSingleCoolerOutput(options.outputPath())) {
        exportSingleCoolerViaHictk(
          options,
          toolchain,
          chunkedFile,
          assemblyLayout,
          selectedResolutions.get(0),
          tmpDirectory,
          cooCompression,
          exportMaxMemoryBytes,
          overallTracker,
          synchronizedLogger,
          processSink,
          cancellationRequested
        );
        overallTracker.finish("Finished .cool export");
        return;
      }

      Files.deleteIfExists(options.outputPath());
      try (final var dst = HDF5Factory.open(options.outputPath().toFile())) {
        dst.object().createGroup("/resolutions");
        dst.object().createGroup("/chroms");
        dst.string().setAttrVL("/", "format", "HDF5::MCOOL");
        dst.int64().setAttr("/", "format-version", 2L);
        dst.string().write("/source_format", "hict");
        dst.string().write("/selected_resolutions", new JsonArray(selectedResolutions).encode());

        writeChromsGroup(dst, "/chroms", assemblyLayout.chroms(), resolveIntStorageFeatures(options));
        HictToMcoolConverter.writeHictAssemblyMetadata(dst, assemblyLayout, resolveIntStorageFeatures(options));

        for (final var resolution : selectedResolutions.stream().sorted().toList()) {
          checkCancelled(cancellationRequested);
          final var resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
          if (resolutionOrder == null) {
            throw new IllegalStateException("Resolution " + resolution + " is not present in " + options.inputPath().getFileName());
          }

          final var resolutionTmpDir = Files.createDirectories(tmpDirectory.resolve("r" + resolution));
          final var chromSizesPath = resolutionTmpDir.resolve("chrom.sizes");
          final var coolPath = resolutionTmpDir.resolve("pixels.cool");

          writeChromSizesFile(assemblyLayout, chromSizesPath);
          final var mapper = buildMapper(chunkedFile, options.inputPath(), resolutionOrder);
          runHictkLoadFromTransformedCoo(
            toolchain,
            options.inputPath(),
            resolution,
            mapper,
            chromSizesPath,
            coolPath,
            options.parallelism(),
            resolutionTmpDir,
            options.chunkSize(),
            overallTracker,
            synchronizedLogger,
            processSink,
            cancellationRequested
          );
          if (options.balanceExportedCoolers()) {
            runHictkBalance(
              toolchain,
              coolPath.toString(),
              options.parallelism(),
              resolutionTmpDir,
              synchronizedLogger,
              processSink,
              cancellationRequested
            );
          } else {
            synchronizedLogger.accept("Skipping hictk balance for exported resolution " + resolution);
          }

          try (final var coolReader = HDF5Factory.openForReading(coolPath.toFile())) {
            mergeResolutionFromCool(
              coolReader,
              dst,
              assemblyLayout.resolutionLayout(resolution),
              resolution,
              options.chunkSize(),
              resolveIntStorageFeatures(options),
              resolveFloatStorageFeatures(options),
              synchronizedLogger
            );
          }
          synchronizedLogger.accept("Resolution " + resolution + ": 100% (" + resolution + "/" + resolution + "), elapsed=00:00, eta=00:00");
        }
      }

      overallTracker.finish("Finished .mcool export");
    } finally {
      processSink.accept(null);
      deleteRecursively(tmpDirectory);
    }
  }

  private static void runHictkLoad(final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                   final @NotNull Path cooPath,
                                   final @NotNull Path chromSizesPath,
                                   final long resolution,
                                   final @NotNull Path coolPath,
                                   final int parallelism,
                                   final @NotNull Path workDirectory,
                                   final @NotNull Consumer<String> logger,
                                   final @NotNull Consumer<Process> processSink,
                                   final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    final var command = List.of(
      Objects.requireNonNull(toolchain.hictkCommand()).toString(),
      "load",
      "--format",
      "coo",
      "--chrom-sizes",
      chromSizesPath.toString(),
      "--bin-size",
      Long.toString(resolution),
      "--assume-sorted",
      "--threads",
      Integer.toString(resolveToolThreads(parallelism)),
      "--tmpdir",
      workDirectory.toString(),
      "--force",
      cooPath.toString(),
      coolPath.toString()
    );
    runStreamingCommand(command, workDirectory, logger, processSink, cancellationRequested);
  }

  private static void runHictkLoadFromTransformedCoo(final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                                     final @NotNull Path inputPath,
                                                     final long resolution,
                                                     final @NotNull SourceToAssemblyMapper mapper,
                                                     final @NotNull Path chromSizesPath,
                                                     final @NotNull Path coolPath,
                                                     final int parallelism,
                                                     final @NotNull Path workDirectory,
                                                     final int chunkSize,
                                                     final @NotNull OverallProgressTracker overallTracker,
                                                     final @NotNull Consumer<String> logger,
                                                     final @NotNull Consumer<Process> processSink,
                                                     final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    final int hictkThreads = resolveToolThreads(parallelism);
    final int hictkChunkSize = resolveHictkLoadChunkSize(logger);
    final var command = List.of(
      Objects.requireNonNull(toolchain.hictkCommand()).toString(),
      "load",
      "--format",
      "coo",
      "--chrom-sizes",
      chromSizesPath.toString(),
      "--bin-size",
      Long.toString(resolution),
      "--assume-unsorted",
      "--transpose-lower-triangular-pixels",
      "--threads",
      Integer.toString(hictkThreads),
      "--chunk-size",
      Integer.toString(hictkChunkSize),
      "--tmpdir",
      workDirectory.toString(),
      "--force",
      "-",
      coolPath.toString()
    );
    logger.accept(
      "Starting hictk streaming load for resolution " + resolution +
        ": threads=" + hictkThreads +
        ", hictkChunkSize=" + hictkChunkSize +
        ", output=" + coolPath.getFileName()
    );
    final var process = new ProcessBuilder(command)
      .directory(workDirectory.toFile())
      .redirectErrorStream(true)
      .start();
    processSink.accept(process);
    final var streamThread = new Thread(() -> {
      try (final var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          logger.accept(line);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }, "hictk-streaming-load-log");
    streamThread.setDaemon(true);
    streamThread.start();

    long streamedRecords = 0L;
    Throwable writeFailure = null;
    try (final var writer = new FastCooTextWriter(process.getOutputStream())) {
      streamedRecords = streamTransformedCooToWriter(
        inputPath,
        resolution,
        writer,
        mapper,
        chunkSize,
        overallTracker,
        logger,
        cancellationRequested
      );
    } catch (IOException | RuntimeException e) {
      writeFailure = e;
      process.destroy();
    }

    if (writeFailure == null) {
      final long waitStartedNanos = System.nanoTime();
      long nextWaitLogNanos = waitStartedNanos;
      while (process.isAlive()) {
        checkCancelled(cancellationRequested);
        final long nowNanos = System.nanoTime();
        if (nowNanos >= nextWaitLogNanos) {
          logger.accept(
            "Resolution " + resolution + " COO stream finished; waiting for hictk load to finalize " +
              coolPath.getFileName() +
              " (elapsed=" + formatDuration((nowNanos - waitStartedNanos) / 1_000_000L) +
              ", currentSize=" + describeFileSize(coolPath) + ")"
          );
          nextWaitLogNanos = nowNanos + TimeUnit.SECONDS.toNanos(30L);
        }
        Thread.sleep(200L);
      }
    } else {
      process.waitFor(10L, TimeUnit.SECONDS);
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
    streamThread.join();
    final int exitCode = process.waitFor();
    processSink.accept(null);
    if (writeFailure != null) {
      throw new IllegalStateException("Failed while streaming transformed COO records into hictk load", writeFailure);
    }
    if (exitCode != 0) {
      throw new IllegalStateException(String.join(" ", command) + " failed with exit code " + exitCode);
    }
    overallTracker.add(streamedRecords, "Resolution " + resolution + " hictk load finalized");
    logger.accept(
      "Resolution " + resolution + " hictk load finalized " + coolPath.getFileName() +
        " (size=" + describeFileSize(coolPath) + ")"
    );
  }

  private void exportSingleCoolerViaHictk(final @NotNull ConversionOptions options,
                                          final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                          final @NotNull ChunkedFile chunkedFile,
                                          final @NotNull HictToMcoolConverter.CoolerAssemblyLayout assemblyLayout,
                                          final long resolution,
                                          final @NotNull Path tmpDirectory,
                                          final @NotNull CooTextCompression cooCompression,
                                          final long exportMaxMemoryBytes,
                                          final @NotNull OverallProgressTracker overallTracker,
                                          final @NotNull Consumer<String> logger,
                                          final @NotNull Consumer<Process> processSink,
                                          final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    checkCancelled(cancellationRequested);
    final var resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
    if (resolutionOrder == null) {
      throw new IllegalStateException("Resolution " + resolution + " is not present in " + options.inputPath().getFileName());
    }
    final var outputParent = options.outputPath().getParent();
    if (outputParent != null) {
      Files.createDirectories(outputParent);
    }
    Files.deleteIfExists(options.outputPath());
    final var resolutionTmpDir = Files.createDirectories(tmpDirectory.resolve("single-r" + resolution));
    final var chromSizesPath = resolutionTmpDir.resolve("chrom.sizes");

    writeChromSizesFile(assemblyLayout, chromSizesPath);
    final var mapper = buildMapper(chunkedFile, options.inputPath(), resolutionOrder);
    logger.accept("hictk-assisted single-resolution export streams transformed COO directly to hictk; no HiCT sorted COO temp file is created.");
    runHictkLoadFromTransformedCoo(
      toolchain,
      options.inputPath(),
      resolution,
      mapper,
      chromSizesPath,
      options.outputPath(),
      options.parallelism(),
      resolutionTmpDir,
      options.chunkSize(),
      overallTracker,
      logger,
      processSink,
      cancellationRequested
    );
    if (options.balanceExportedCoolers()) {
      runHictkBalance(
        toolchain,
        options.outputPath().toString(),
        options.parallelism(),
        resolutionTmpDir,
        logger,
        processSink,
        cancellationRequested
      );
    } else {
      logger.accept("Skipping hictk balance for exported .cool");
    }
  }

  private static void runHictkBalance(final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                      final @NotNull String coolerUri,
                                      final int parallelism,
                                      final @NotNull Path workDirectory,
                                      final @NotNull Consumer<String> logger,
                                      final @NotNull Consumer<Process> processSink,
                                      final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    try {
      runStreamingCommand(
        List.of(
          Objects.requireNonNull(toolchain.hictkCommand()).toString(),
          "balance",
          "ice",
          "--force",
          "--threads",
          Integer.toString(resolveToolThreads(parallelism)),
          "--tmpdir",
          workDirectory.toString(),
          "--ignore-diags",
          "2",
          coolerUri
        ),
        workDirectory,
        logger,
        processSink,
        cancellationRequested
      );
    } catch (IllegalStateException balanceFailure) {
      logger.accept("WARNING: hictk balance failed for exported Cooler; output remains unbalanced. " + balanceFailure.getMessage());
    }
  }

  private static boolean isSingleCoolerOutput(final @NotNull Path outputPath) {
    final var lowered = outputPath.getFileName().toString().toLowerCase();
    return lowered.endsWith(".cool") && !lowered.endsWith(".mcool");
  }

  private static void mergeResolutionFromCool(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                              final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                              final @NotNull HictToMcoolConverter.ResolutionLayout layout,
                                              final long resolution,
                                              final int chunkSize,
                                              final @NotNull HDF5IntStorageFeatures compression,
                                              final @NotNull HDF5FloatStorageFeatures floatCompression,
                                              final @NotNull Consumer<String> logger) {
    final var root = "/resolutions/" + resolution;
    dst.object().createGroup(root);
    dst.object().createGroup(root + "/chroms");
    dst.object().createGroup(root + "/bins");
    dst.object().createGroup(root + "/pixels");
    dst.object().createGroup(root + "/indexes");

    copyStringArray(src, dst, "/chroms/name", root + "/chroms/name");
    copyIntArrayChunked(src, dst, "/chroms/length", root + "/chroms/length", chunkSize, compression);
    copyIntArrayChunked(src, dst, "/bins/chrom", root + "/bins/chrom", chunkSize, compression);
    copyIntArrayChunked(src, dst, "/bins/start", root + "/bins/start", chunkSize, compression);
    copyIntArrayChunked(src, dst, "/bins/end", root + "/bins/end", chunkSize, compression);
    if (src.object().isDataSet("/bins/weight")) {
      copyDoubleArrayChunked(src, dst, "/bins/weight", root + "/bins/weight", chunkSize, floatCompression);
    } else {
      writeBinWeightsForCopiedCoolerBins(src, dst, root + "/bins/weight", layout, resolution, chunkSize, floatCompression);
    }
    copyLongArrayChunked(src, dst, "/pixels/bin1_id", root + "/pixels/bin1_id", chunkSize, compression);
    copyLongArrayChunked(src, dst, "/pixels/bin2_id", root + "/pixels/bin2_id", chunkSize, compression);
    copyIntArrayChunked(src, dst, "/pixels/count", root + "/pixels/count", chunkSize, compression);
    copyLongArrayChunked(src, dst, "/indexes/bin1_offset", root + "/indexes/bin1_offset", chunkSize, compression);
    copyLongArrayChunked(src, dst, "/indexes/chrom_offset", root + "/indexes/chrom_offset", chunkSize, compression);

    final long binsCount = datasetLength(src, "/bins/end");
    final long chromCount = src.string().readArray("/chroms/name").length;
    final long nonzeroPixelCount = datasetLength(src, "/pixels/bin1_id");
    final long totalCounts = sumIntArrayChunked(src, "/pixels/count", chunkSize);
    writeResolutionMetadata(dst, root, resolution, binsCount, chromCount, nonzeroPixelCount, totalCounts);
    logger.accept("Merged resolution " + resolution + " to final output");
  }

  private static void writeChromSizesFile(final @NotNull HictToMcoolConverter.CoolerAssemblyLayout layout,
                                          final @NotNull Path outputPath) throws IOException {
    try (final var writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
      for (final var chrom : layout.chroms()) {
        writer.write(chrom.name());
        writer.write('\t');
        writer.write(Long.toString(chrom.lengthBp()));
        writer.newLine();
      }
    }
  }

  private static long streamTransformedCooToWriter(final @NotNull Path inputPath,
                                                  final long resolution,
                                                  final @NotNull FastCooTextWriter writer,
                                                  final @NotNull SourceToAssemblyMapper mapper,
                                                  final int chunkSize,
                                                  final @NotNull OverallProgressTracker overallTracker,
                                                  final @NotNull Consumer<String> logger,
                                                  final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    final int sourceReadBatchSize = resolveCooSourceReadBatchSize(logger);
    final var batch = new CooRecordBatch(sourceReadBatchSize);
    long total = 0L;
    try (final var src = HDF5Factory.openForReading(inputPath.toFile())) {
      final var blockLengthsPath = getBlockLengthDatasetPath(resolution);
      final var blockOffsetsPath = getBlockOffsetDatasetPath(resolution);
      final var rowsPath = getBlockRowsDatasetPath(resolution);
      final var colsPath = getBlockColsDatasetPath(resolution);
      final var valuesPath = getBlockValuesDatasetPath(resolution);
      final var denseBlocksPath = getDenseBlockDatasetPath(resolution);
      final long[] stripeLengths = src.int64().readArray(getStripeLengthsBinsDatasetPath(resolution));
      final long[] stripeOffsets = new long[stripeLengths.length];
      long stripeCursor = 0L;
      for (int i = 0; i < stripeLengths.length; i++) {
        stripeOffsets[i] = stripeCursor;
        stripeCursor += stripeLengths[i];
      }
      final int stripeCount = stripeLengths.length;
      final boolean floatingPointSignal = isFloatingPointDataset(src, valuesPath);
      if (floatingPointSignal) {
        throw new IllegalStateException("hictk-assisted .mcool export currently supports integer HiCT matrices only");
      }
      for (int rowStripe = 0; rowStripe < stripeCount; rowStripe++) {
        final long rowBase = (long) rowStripe * stripeCount;
        final long[] rowBlockLengths = src.int64().readArrayBlockWithOffset(blockLengthsPath, stripeCount, rowBase);
        for (int colStripe = rowStripe; colStripe < stripeCount; colStripe++) {
          total += rowBlockLengths[colStripe];
        }
      }
      final long startedNanos = System.nanoTime();
      logger.accept(
        "Resolution " + resolution + " streaming COO export started: source entries=" + total +
          ", sourceReadBatchSize=" + sourceReadBatchSize +
          ", chunkSize=" + chunkSize +
          ". This stage streams transformed HiCT pixels directly into hictk load; HiCT does not create sorted COO temp chunks."
      );
      final var progress = new CooExportProgressTracker(resolution, total, startedNanos, overallTracker, logger);
      final var rowMapperCursor = mapper.cursor();
      final var colMapperCursor = mapper.cursor();
      final var nativeProcessingService = NativeProcessingService.getInstance();
      final var sparseRun = new ArrayList<SparseBlockRef>();
      long sparseRunStartOffset = -1L;
      long sparseRunLength = 0L;
      for (int rowStripe = 0; rowStripe < stripeCount; rowStripe++) {
        checkCancelled(cancellationRequested);
        final long rowBase = (long) rowStripe * stripeCount;
        final long[] rowBlockLengths = src.int64().readArrayBlockWithOffset(blockLengthsPath, stripeCount, rowBase);
        final long[] rowBlockOffsets = src.int64().readArrayBlockWithOffset(blockOffsetsPath, stripeCount, rowBase);
        final long rowStripeOffset = stripeOffsets[rowStripe];
        final int rowStripeLength = Math.toIntExact(stripeLengths[rowStripe]);
        for (int colStripe = rowStripe; colStripe < stripeCount; colStripe++) {
          final long blockLen = rowBlockLengths[colStripe];
          if (blockLen <= 0L) {
            continue;
          }
          final long blockOffset = rowBlockOffsets[colStripe];
          final long colStripeOffset = stripeOffsets[colStripe];
          final int colStripeLength = Math.toIntExact(stripeLengths[colStripe]);
          if (blockOffset >= 0L) {
            long localOffset = 0L;
            while (localOffset < blockLen) {
              checkCancelled(cancellationRequested);
              final long remainingInBlock = blockLen - localOffset;
              final long sourceOffset = blockOffset + localOffset;
              final boolean startsNewRun = sparseRun.isEmpty();
              final boolean contiguous = startsNewRun || sourceOffset == sparseRunStartOffset + sparseRunLength;
              final long availableRunCapacity = sourceReadBatchSize - sparseRunLength;
              if (!startsNewRun && (!contiguous || availableRunCapacity <= 0L)) {
                flushSparseRunToWriter(
                  src,
                  rowsPath,
                  colsPath,
                  valuesPath,
                  sparseRun,
                  sparseRunStartOffset,
                  sparseRunLength,
                  batch,
                  writer,
                  mapper,
                  rowMapperCursor,
                  colMapperCursor,
                  nativeProcessingService,
                  cancellationRequested
                );
                progress.add(sparseRunLength);
                sparseRun.clear();
                sparseRunStartOffset = -1L;
                sparseRunLength = 0L;
              }

              final long runAppendLength = Math.min(remainingInBlock, Math.max(1L, sourceReadBatchSize - sparseRunLength));
              if (sparseRun.isEmpty()) {
                sparseRunStartOffset = sourceOffset;
              }
              sparseRun.add(new SparseBlockRef(sourceOffset, runAppendLength, rowStripeOffset, colStripeOffset));
              sparseRunLength += runAppendLength;
              localOffset += runAppendLength;
            }
          } else {
            if (!sparseRun.isEmpty()) {
              flushSparseRunToWriter(
                src,
                rowsPath,
                colsPath,
                valuesPath,
                sparseRun,
                sparseRunStartOffset,
                sparseRunLength,
                batch,
                writer,
                mapper,
                rowMapperCursor,
                colMapperCursor,
                nativeProcessingService,
                cancellationRequested
              );
              progress.add(sparseRunLength);
              sparseRun.clear();
              sparseRunStartOffset = -1L;
              sparseRunLength = 0L;
            }
            final long denseIndex = -(blockOffset + 1L);
            final var denseValues = readDenseLongBlock(src, denseBlocksPath, denseIndex);
            for (int row = 0; row < rowStripeLength; row++) {
              final int colStart = (rowStripe == colStripe) ? row : 0;
              for (int col = colStart; col < colStripeLength; col++) {
                final long value = denseValues[(row * DENSE_BLOCK_SIZE) + col];
                if (value == 0L) {
                  continue;
                }
                appendMappedRecord(
                  batch,
                  rowMapperCursor,
                  colMapperCursor,
                  rowStripeOffset + row,
                  colStripeOffset + col,
                  value
                );
                if (batch.remainingCapacity() == 0) {
                  writeCooBatch(writer, batch);
                }
              }
            }
            progress.add(blockLen);
          }
        }
        if (!sparseRun.isEmpty()) {
          flushSparseRunToWriter(
            src,
            rowsPath,
            colsPath,
            valuesPath,
            sparseRun,
            sparseRunStartOffset,
            sparseRunLength,
            batch,
            writer,
            mapper,
            rowMapperCursor,
            colMapperCursor,
            nativeProcessingService,
            cancellationRequested
          );
          progress.add(sparseRunLength);
          sparseRun.clear();
          sparseRunStartOffset = -1L;
          sparseRunLength = 0L;
        }
      }
    }
    writeCooBatch(writer, batch);
    writer.flush();
    return total;
  }

  private static void exportTransformedCoo(final @NotNull Path inputPath,
                                           final long resolution,
                                           final @NotNull Path outputPath,
                                           final @NotNull SourceToAssemblyMapper mapper,
                                           final int chunkSize,
                                           final long exportMaxMemoryBytes,
                                           final @NotNull CooTextCompression cooCompression,
                                           final @NotNull OverallProgressTracker overallTracker,
                                           final @NotNull Consumer<String> logger,
                                           final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    final var workDir = Files.createDirectories(outputPath.getParent());
    final var chunkPaths = new ArrayList<Path>();
    final int sortBatchSize = resolveCooSortBatchSize(chunkSize, exportMaxMemoryBytes, logger);
    final int sourceReadBatchSize = Math.min(sortBatchSize, resolveCooSourceReadBatchSize(logger));
    final var batch = new CooRecordBatch(sortBatchSize);
    try (final var src = HDF5Factory.openForReading(inputPath.toFile())) {
      final var blockLengthsPath = getBlockLengthDatasetPath(resolution);
      final var blockOffsetsPath = getBlockOffsetDatasetPath(resolution);
      final var rowsPath = getBlockRowsDatasetPath(resolution);
      final var colsPath = getBlockColsDatasetPath(resolution);
      final var valuesPath = getBlockValuesDatasetPath(resolution);
      final var denseBlocksPath = getDenseBlockDatasetPath(resolution);
      final long[] stripeLengths = src.int64().readArray(getStripeLengthsBinsDatasetPath(resolution));
      final long[] stripeOffsets = new long[stripeLengths.length];
      long stripeCursor = 0L;
      for (int i = 0; i < stripeLengths.length; i++) {
        stripeOffsets[i] = stripeCursor;
        stripeCursor += stripeLengths[i];
      }
      final int stripeCount = stripeLengths.length;
      final boolean floatingPointSignal = isFloatingPointDataset(src, valuesPath);
      if (floatingPointSignal) {
        throw new IllegalStateException("hictk-assisted .mcool export currently supports integer HiCT matrices only");
      }
      long total = 0L;
      for (int rowStripe = 0; rowStripe < stripeCount; rowStripe++) {
        final long rowBase = (long) rowStripe * stripeCount;
        final long[] rowBlockLengths = src.int64().readArrayBlockWithOffset(blockLengthsPath, stripeCount, rowBase);
        for (int colStripe = rowStripe; colStripe < stripeCount; colStripe++) {
          total += rowBlockLengths[colStripe];
        }
      }
      final long startedNanos = System.nanoTime();
      logger.accept(
        "Resolution " + resolution + " COO export started: source entries=" + total +
          ", sortBatchSize=" + sortBatchSize +
          ", sourceReadBatchSize=" + sourceReadBatchSize +
          ", chunkSize=" + chunkSize +
          ", tempCompression=" + cooCompression.logName() +
          ". This stage streams, remaps and sorts the selected HiCT pixels before hictk loads them."
      );
      final var progress = new CooExportProgressTracker(resolution, total, startedNanos, overallTracker, logger);
      final var rowMapperCursor = mapper.cursor();
      final var colMapperCursor = mapper.cursor();
      final var nativeProcessingService = NativeProcessingService.getInstance();
      final var sparseRun = new ArrayList<SparseBlockRef>();
      long sparseRunStartOffset = -1L;
      long sparseRunLength = 0L;
      for (int rowStripe = 0; rowStripe < stripeCount; rowStripe++) {
        checkCancelled(cancellationRequested);
        final long rowBase = (long) rowStripe * stripeCount;
        final long[] rowBlockLengths = src.int64().readArrayBlockWithOffset(blockLengthsPath, stripeCount, rowBase);
        final long[] rowBlockOffsets = src.int64().readArrayBlockWithOffset(blockOffsetsPath, stripeCount, rowBase);
        final long rowStripeOffset = stripeOffsets[rowStripe];
        final int rowStripeLength = Math.toIntExact(stripeLengths[rowStripe]);
        for (int colStripe = rowStripe; colStripe < stripeCount; colStripe++) {
          final long blockLen = rowBlockLengths[colStripe];
          if (blockLen <= 0L) {
            continue;
          }
          final long blockOffset = rowBlockOffsets[colStripe];
          final long colStripeOffset = stripeOffsets[colStripe];
          final int colStripeLength = Math.toIntExact(stripeLengths[colStripe]);
          if (blockOffset >= 0L) {
            long localOffset = 0L;
            while (localOffset < blockLen) {
              checkCancelled(cancellationRequested);
              final long remainingInBlock = blockLen - localOffset;
              final long sourceOffset = blockOffset + localOffset;
              final boolean startsNewRun = sparseRun.isEmpty();
              final boolean contiguous = startsNewRun || sourceOffset == sparseRunStartOffset + sparseRunLength;
              final long availableRunCapacity = sourceReadBatchSize - sparseRunLength;
              if (!startsNewRun && (!contiguous || availableRunCapacity <= 0L)) {
                flushSparseRun(
                  src,
                  rowsPath,
                  colsPath,
                  valuesPath,
                  sparseRun,
                  sparseRunStartOffset,
                  sparseRunLength,
                  batch,
                  mapper,
                  rowMapperCursor,
                  colMapperCursor,
                  nativeProcessingService,
                  workDir,
                  resolution,
                  sortBatchSize,
                  chunkPaths,
                  cancellationRequested
                );
                progress.add(sparseRunLength);
                sparseRun.clear();
                sparseRunStartOffset = -1L;
                sparseRunLength = 0L;
              }

              final long runAppendLength = Math.min(remainingInBlock, Math.max(1L, sourceReadBatchSize - sparseRunLength));
              if (sparseRun.isEmpty()) {
                sparseRunStartOffset = sourceOffset;
              }
              sparseRun.add(new SparseBlockRef(sourceOffset, runAppendLength, rowStripeOffset, colStripeOffset));
              sparseRunLength += runAppendLength;
              localOffset += runAppendLength;
            }
          } else {
            if (!sparseRun.isEmpty()) {
              flushSparseRun(
                src,
                rowsPath,
                colsPath,
                valuesPath,
                sparseRun,
                sparseRunStartOffset,
                sparseRunLength,
                batch,
                mapper,
                rowMapperCursor,
                colMapperCursor,
                nativeProcessingService,
                workDir,
                resolution,
                sortBatchSize,
                chunkPaths,
                cancellationRequested
              );
              progress.add(sparseRunLength);
              sparseRun.clear();
              sparseRunStartOffset = -1L;
              sparseRunLength = 0L;
            }
            final long denseIndex = -(blockOffset + 1L);
            final var denseValues = readDenseLongBlock(src, denseBlocksPath, denseIndex);
            for (int row = 0; row < rowStripeLength; row++) {
              final int colStart = (rowStripe == colStripe) ? row : 0;
              for (int col = colStart; col < colStripeLength; col++) {
                final long value = denseValues[(row * DENSE_BLOCK_SIZE) + col];
                if (value == 0L) {
                  continue;
                }
                appendMappedRecord(
                  batch,
                  rowMapperCursor,
                  colMapperCursor,
                  rowStripeOffset + row,
                  colStripeOffset + col,
                  value
                );
                if (batch.size() >= sortBatchSize) {
                  final var chunkPath = flushSortedChunk(workDir, resolution, chunkPaths.size(), batch);
                  chunkPaths.add(chunkPath);
                  batch.clear();
                }
              }
            }
            progress.add(blockLen);
          }
        }
        if (!sparseRun.isEmpty()) {
          flushSparseRun(
            src,
            rowsPath,
            colsPath,
            valuesPath,
            sparseRun,
            sparseRunStartOffset,
            sparseRunLength,
            batch,
            mapper,
            rowMapperCursor,
            colMapperCursor,
            nativeProcessingService,
            workDir,
            resolution,
            sortBatchSize,
            chunkPaths,
            cancellationRequested
          );
          progress.add(sparseRunLength);
          sparseRun.clear();
          sparseRunStartOffset = -1L;
          sparseRunLength = 0L;
        }
      }
    }
    if (!batch.isEmpty()) {
      final var chunkPath = flushSortedChunk(workDir, resolution, chunkPaths.size(), batch);
      chunkPaths.add(chunkPath);
      batch.clear();
    }
    mergeSortedChunks(chunkPaths, outputPath, cooCompression, logger, cancellationRequested);
  }

  private static @NotNull Path flushSortedChunk(final @NotNull Path workDir,
                                                final long resolution,
                                                final int chunkIndex,
                                                final @NotNull CooRecordBatch records) throws IOException {
    final var rows = records.copyRows();
    final var cols = records.copyCols();
    final var counts = records.copyCounts();
    final int compactedLength = HictToMcoolConverter.sortAndCompactCoolerRecordsRowMajor(rows, cols, counts);
    final var chunkPath = workDir.resolve(String.format("pixels-r%d-%05d.bin", resolution, chunkIndex));
    try (final var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(chunkPath)))) {
      for (int i = 0; i < compactedLength; i++) {
        out.writeLong(rows[i]);
        out.writeLong(cols[i]);
        out.writeLong(counts[i]);
      }
    }
    return chunkPath;
  }

  private static void mergeSortedChunks(final @NotNull List<Path> chunkPaths,
                                        final @NotNull Path outputPath,
                                        final @NotNull CooTextCompression cooCompression,
                                        final @NotNull Consumer<String> logger,
                                        final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    if (chunkPaths.isEmpty()) {
      writeEmptyCoo(outputPath, cooCompression);
      return;
    }

    final int mergeFanIn = resolveCooMergeFanIn(logger);
    logger.accept("Merging " + chunkPaths.size() + " sorted COO chunks with fan-in " + mergeFanIn);
    final var liveChunks = new ArrayList<Path>(chunkPaths);
    var current = new ArrayList<Path>(chunkPaths);
    var pass = 0;
    try {
      while (current.size() > mergeFanIn) {
        checkCancelled(cancellationRequested);
        pass++;
        final int mergedChunkCount = (current.size() + mergeFanIn - 1) / mergeFanIn;
        logger.accept(
          "COO merge pass " + pass + ": " + current.size() + " chunks -> " + mergedChunkCount + " chunks"
        );
        final var next = new ArrayList<Path>(mergedChunkCount);
        for (int start = 0; start < current.size(); start += mergeFanIn) {
          checkCancelled(cancellationRequested);
          final int end = Math.min(current.size(), start + mergeFanIn);
          final var group = new ArrayList<>(current.subList(start, end));
          final var mergedPath = outputPath.getParent().resolve(String.format("pixels-merge-p%02d-%05d.bin", pass, next.size()));
          final var stats = mergeChunkGroupToBinary(group, mergedPath, logger, cancellationRequested);
          liveChunks.add(mergedPath);
          deleteChunkFiles(group);
          liveChunks.removeAll(group);
          next.add(mergedPath);
          logger.accept(
            "COO merge pass " + pass + ": wrote " + mergedPath.getFileName() + " from " + group.size()
              + " chunks, raw=" + stats.rawRecords() + ", unique=" + stats.uniqueRecords()
          );
        }
        current = next;
      }
      mergeChunkGroupToText(current, outputPath, cooCompression, logger, cancellationRequested);
      deleteChunkFiles(current);
      liveChunks.removeAll(current);
    } finally {
      deleteChunkFiles(liveChunks);
    }
  }

  private static @NotNull MergeStats mergeChunkGroupToBinary(final @NotNull List<Path> chunkPaths,
                                                             final @NotNull Path outputPath,
                                                             final @NotNull Consumer<String> logger,
                                                             final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    Files.deleteIfExists(outputPath);
    try (final var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(outputPath)))) {
      return mergeChunkGroup(
        chunkPaths,
        logger,
        cancellationRequested,
        (row, col, count) -> {
          out.writeLong(row);
          out.writeLong(col);
          out.writeLong(count);
        }
      );
    } catch (IOException | RuntimeException e) {
      try {
        Files.deleteIfExists(outputPath);
      } catch (IOException deleteFailure) {
        e.addSuppressed(deleteFailure);
      }
      throw e;
    }
  }

  private static void writeEmptyCoo(final @NotNull Path outputPath,
                                    final @NotNull CooTextCompression cooCompression) throws IOException {
    try (final var writer = cooCompression.openWriter(outputPath)) {
      writer.flush();
    }
  }

  private static @NotNull MergeStats mergeChunkGroupToText(final @NotNull List<Path> chunkPaths,
                                                           final @NotNull Path outputPath,
                                                           final @NotNull CooTextCompression cooCompression,
                                                           final @NotNull Consumer<String> logger,
                                                           final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    Files.deleteIfExists(outputPath);
    try (final var writer = cooCompression.openWriter(outputPath)) {
      return mergeChunkGroup(
        chunkPaths,
        logger,
        cancellationRequested,
        (row, col, count) -> {
          writer.write(Long.toString(row));
          writer.write('\t');
          writer.write(Long.toString(col));
          writer.write('\t');
          writer.write(Long.toString(count));
          writer.newLine();
        }
      );
    } catch (IOException | RuntimeException e) {
      try {
        Files.deleteIfExists(outputPath);
      } catch (IOException deleteFailure) {
        e.addSuppressed(deleteFailure);
      }
      throw e;
    }
  }

  private static @NotNull MergeStats mergeChunkGroup(final @NotNull List<Path> chunkPaths,
                                                     final @NotNull Consumer<String> logger,
                                                     final @NotNull BooleanSupplier cancellationRequested,
                                                     final @NotNull MergedRecordSink sink) throws IOException {
    final var cursors = new ArrayList<ChunkCursor>(chunkPaths.size());
    final var queue = new PriorityQueue<ChunkCursor>(Comparator
      .comparingLong((ChunkCursor cursor) -> cursor.record().row())
      .thenComparingLong(cursor -> cursor.record().col())
      .thenComparingLong(cursor -> cursor.record().count()));
    try {
      for (final var chunkPath : chunkPaths) {
        final var cursor = new ChunkCursor(chunkPath);
        if (cursor.advance()) {
          cursors.add(cursor);
          queue.add(cursor);
        } else {
          cursor.close();
        }
      }
      long rawRecords = 0L;
      long written = 0L;
      boolean hasPending = false;
      long pendingRow = 0L;
      long pendingCol = 0L;
      long pendingCount = 0L;
      while (!queue.isEmpty()) {
        checkCancelled(cancellationRequested);
        final var cursor = queue.poll();
        final var record = cursor.record();
        if (!hasPending) {
          pendingRow = record.row();
          pendingCol = record.col();
          pendingCount = record.count();
          hasPending = true;
        } else if (pendingRow == record.row() && pendingCol == record.col()) {
          pendingCount += record.count();
        } else {
          sink.write(pendingRow, pendingCol, pendingCount);
          written++;
          pendingRow = record.row();
          pendingCol = record.col();
          pendingCount = record.count();
        }
        rawRecords++;
        if (cursor.advance()) {
          queue.add(cursor);
        }
        if (rawRecords % 1_000_000L == 0L) {
          logger.accept("Merged sorted COO records: raw=" + rawRecords + ", unique=" + written);
        }
      }
      if (hasPending) {
        sink.write(pendingRow, pendingCol, pendingCount);
        written++;
      }
      logger.accept("Merged sorted COO records complete: raw=" + rawRecords + ", unique=" + written);
      return new MergeStats(rawRecords, written);
    } finally {
      IOException closeFailure = null;
      for (final var cursor : cursors) {
        try {
          cursor.close();
        } catch (IOException e) {
          if (closeFailure == null) {
            closeFailure = e;
          } else {
            closeFailure.addSuppressed(e);
          }
        }
      }
      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }

  private static @NotNull SourceToAssemblyMapper buildMapper(final @NotNull ChunkedFile chunkedFile,
                                                             final @NotNull Path inputPath,
                                                             final int resolutionOrder) throws IOException {
    final var currentContigs = chunkedFile.getAssemblyInfo().contigs();
    try (final var reader = HDF5Factory.openForReading(inputPath.toFile())) {
      final var resolution = chunkedFile.getResolutions()[resolutionOrder];
      final var stripes = Initializers.readStripeDescriptors(resolution, reader);
      final long[] stripeOffsets = new long[stripes.size()];
      long stripeCursor = 0L;
      for (int i = 0; i < stripes.size(); i++) {
        stripeOffsets[i] = stripeCursor;
        stripeCursor += stripes.get(i).stripeLengthBins();
      }

      final var segments = new ArrayList<SourceSegment>();
      long targetCursor = 0L;
      for (final ContigTree.ContigTuple tuple : currentContigs) {
        final var descriptor = tuple.descriptor();
        final var contigAtus = descriptor.getAtus().get(resolutionOrder);
        for (final ATUDescriptor atu : contigAtus) {
          final var stripe = atu.getStripeDescriptor();
          final long sourceStart = stripeOffsets[stripe.stripeId()] + atu.getStartIndexInStripeIncl();
          final long sourceEnd = stripeOffsets[stripe.stripeId()] + atu.getEndIndexInStripeExcl();
          final long targetStart = targetCursor;
          final long targetEnd = targetStart + atu.getLength();
          segments.add(new SourceSegment(sourceStart, sourceEnd, targetStart, targetEnd, atu.getDirection()));
          targetCursor = targetEnd;
        }
      }

      segments.sort(Comparator.comparingLong(SourceSegment::sourceStart));
      return new SourceToAssemblyMapper(segments);
    }
  }

  private static @NotNull List<Long> resolveResolutions(final @NotNull ChunkedFile chunkedFile,
                                                        final @NotNull ConversionOptions options) {
    final var available = new ArrayList<Long>();
    for (int i = 1; i < chunkedFile.getResolutions().length; i++) {
      available.add(chunkedFile.getResolutions()[i]);
    }
    if (!options.resolutions().isEmpty()) {
      return available.stream().filter(options.resolutions()::contains).toList();
    }
    if (options.exportAllResolutions()) {
      return available;
    }
    return available.stream().min(Long::compareTo).map(List::of).orElse(List.of());
  }

  private static long countSourcePixels(final @NotNull Path inputPath,
                                        final @NotNull List<Long> resolutions) {
    try (final var src = HDF5Factory.openForReading(inputPath.toFile())) {
      long total = 0L;
      for (final var resolution : resolutions) {
        final var blockLengthsPath = getBlockLengthDatasetPath(resolution);
        final long[] stripeLengths = src.int64().readArray(getStripeLengthsBinsDatasetPath(resolution));
        final int stripeCount = stripeLengths.length;
        for (int rowStripe = 0; rowStripe < stripeCount; rowStripe++) {
          final long rowBase = (long) rowStripe * stripeCount;
          final long[] rowBlockLengths = src.int64().readArrayBlockWithOffset(blockLengthsPath, stripeCount, rowBase);
          for (int colStripe = rowStripe; colStripe < stripeCount; colStripe++) {
            total += rowBlockLengths[colStripe];
          }
        }
      }
      return total;
    }
  }

  private static void appendMappedRecord(final @NotNull CooRecordBatch batch,
                                         final @NotNull SourceToAssemblyMapper.Cursor rowMapperCursor,
                                         final @NotNull SourceToAssemblyMapper.Cursor colMapperCursor,
                                         final long sourceRow,
                                         final long sourceCol,
                                         final long count) {
    long mappedRow = rowMapperCursor.map(sourceRow);
    long mappedCol = colMapperCursor.map(sourceCol);
    if (mappedRow > mappedCol) {
      final long tmp = mappedRow;
      mappedRow = mappedCol;
      mappedCol = tmp;
    }
    batch.add(mappedRow, mappedCol, count);
  }

  private static int appendMappedRecords(final @NotNull CooRecordBatch batch,
                                         final @NotNull SourceToAssemblyMapper mapper,
                                         final @NotNull SourceToAssemblyMapper.Cursor rowMapperCursor,
                                         final @NotNull SourceToAssemblyMapper.Cursor colMapperCursor,
                                         final long rowStripeOffset,
                                         final long colStripeOffset,
                                         final long @NotNull [] sourceRows,
                                         final long @NotNull [] sourceColumns,
                                         final long @NotNull [] sourceValues,
                                         final int sourceOffset,
                                         final int length,
                                         final @NotNull NativeProcessingService nativeProcessingService) {
    final int toAppend = Math.min(length, batch.remainingCapacity());
    if (toAppend <= 0) {
      return 0;
    }

    final int nativeMapped = nativeProcessingService.tryMapCoolerRecordsToBatch(
      sourceRows,
      sourceColumns,
      sourceValues,
      sourceOffset,
      toAppend,
      rowStripeOffset,
      colStripeOffset,
      batch.rows,
      batch.cols,
      batch.counts,
      batch.size(),
      mapper.sourceStarts,
      mapper.sourceEnds,
      mapper.targetStarts,
      mapper.targetEnds,
      mapper.reversedBytes
    );
    if (nativeMapped == toAppend) {
      batch.advanceSize(nativeMapped);
      return nativeMapped;
    }

    for (int i = 0; i < toAppend; i++) {
      final int sourceIndex = sourceOffset + i;
      appendMappedRecord(
        batch,
        rowMapperCursor,
        colMapperCursor,
        rowStripeOffset + sourceRows[sourceIndex],
        colStripeOffset + sourceColumns[sourceIndex],
        sourceValues[sourceIndex]
      );
    }
    return toAppend;
  }

  private static void flushSparseRun(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                     final @NotNull String rowsPath,
                                     final @NotNull String colsPath,
                                     final @NotNull String valuesPath,
                                     final @NotNull List<SparseBlockRef> sparseRun,
                                     final long sparseRunStartOffset,
                                     final long sparseRunLength,
                                     final @NotNull CooRecordBatch batch,
                                     final @NotNull SourceToAssemblyMapper mapper,
                                     final @NotNull SourceToAssemblyMapper.Cursor rowMapperCursor,
                                     final @NotNull SourceToAssemblyMapper.Cursor colMapperCursor,
                                     final @NotNull NativeProcessingService nativeProcessingService,
                                     final @NotNull Path workDir,
                                     final long resolution,
                                     final int sortBatchSize,
                                     final @NotNull List<Path> chunkPaths,
                                     final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    if (sparseRun.isEmpty() || sparseRunLength <= 0L) {
      return;
    }
    if (sparseRunStartOffset < 0L) {
      throw new IllegalStateException("Sparse COO run has no source offset");
    }

    final int runLength = Math.toIntExact(sparseRunLength);
    final var sourceRows = src.int64().readArrayBlockWithOffset(rowsPath, runLength, sparseRunStartOffset);
    final var sourceColumns = src.int64().readArrayBlockWithOffset(colsPath, runLength, sparseRunStartOffset);
    final var sourceValues = src.int64().readArrayBlockWithOffset(valuesPath, runLength, sparseRunStartOffset);

    for (final var block : sparseRun) {
      checkCancelled(cancellationRequested);
      int consumed = 0;
      final int blockLength = Math.toIntExact(block.length());
      final int blockSourceOffset = Math.toIntExact(block.blockOffset() - sparseRunStartOffset);
      while (consumed < blockLength) {
        if (batch.size() >= sortBatchSize) {
          final var chunkPath = flushSortedChunk(workDir, resolution, chunkPaths.size(), batch);
          chunkPaths.add(chunkPath);
          batch.clear();
        }

        final int appended = appendMappedRecords(
          batch,
          mapper,
          rowMapperCursor,
          colMapperCursor,
          block.rowStripeOffset(),
          block.colStripeOffset(),
          sourceRows,
          sourceColumns,
          sourceValues,
          blockSourceOffset + consumed,
          blockLength - consumed,
          nativeProcessingService
        );
        if (appended <= 0) {
          throw new IllegalStateException("Failed to append transformed COO records to the export sort batch");
        }
        consumed += appended;
      }
    }
  }

  private static void flushSparseRunToWriter(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                             final @NotNull String rowsPath,
                                             final @NotNull String colsPath,
                                             final @NotNull String valuesPath,
                                             final @NotNull List<SparseBlockRef> sparseRun,
                                             final long sparseRunStartOffset,
                                             final long sparseRunLength,
                                             final @NotNull CooRecordBatch batch,
                                             final @NotNull FastCooTextWriter writer,
                                             final @NotNull SourceToAssemblyMapper mapper,
                                             final @NotNull SourceToAssemblyMapper.Cursor rowMapperCursor,
                                             final @NotNull SourceToAssemblyMapper.Cursor colMapperCursor,
                                             final @NotNull NativeProcessingService nativeProcessingService,
                                             final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    if (sparseRun.isEmpty() || sparseRunLength <= 0L) {
      return;
    }
    if (sparseRunStartOffset < 0L) {
      throw new IllegalStateException("Sparse COO run has no source offset");
    }

    final int runLength = Math.toIntExact(sparseRunLength);
    final var sourceRows = src.int64().readArrayBlockWithOffset(rowsPath, runLength, sparseRunStartOffset);
    final var sourceColumns = src.int64().readArrayBlockWithOffset(colsPath, runLength, sparseRunStartOffset);
    final var sourceValues = src.int64().readArrayBlockWithOffset(valuesPath, runLength, sparseRunStartOffset);

    for (final var block : sparseRun) {
      checkCancelled(cancellationRequested);
      int consumed = 0;
      final int blockLength = Math.toIntExact(block.length());
      final int blockSourceOffset = Math.toIntExact(block.blockOffset() - sparseRunStartOffset);
      while (consumed < blockLength) {
        if (batch.remainingCapacity() == 0) {
          writeCooBatch(writer, batch);
        }

        final int appended = appendMappedRecords(
          batch,
          mapper,
          rowMapperCursor,
          colMapperCursor,
          block.rowStripeOffset(),
          block.colStripeOffset(),
          sourceRows,
          sourceColumns,
          sourceValues,
          blockSourceOffset + consumed,
          blockLength - consumed,
          nativeProcessingService
        );
        if (appended <= 0) {
          throw new IllegalStateException("Failed to append transformed COO records to the streaming export batch");
        }
        consumed += appended;
      }
    }
  }

  private static void writeCooBatch(final @NotNull FastCooTextWriter writer,
                                    final @NotNull CooRecordBatch batch) throws IOException {
    if (batch.isEmpty()) {
      return;
    }
    writer.writeBatch(batch);
    batch.clear();
  }

  private static boolean isFloatingPointDataset(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
                                                final @NotNull String path) {
    return reader.object().getDataSetInformation(path).getTypeInformation().getDataClass() == HDF5DataClass.FLOAT;
  }

  private static long @NotNull [] readDenseLongBlock(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
                                                     final @NotNull String path,
                                                     final long denseIndex) {
    final var block = reader.int64().readMDArrayBlockWithOffset(
      path,
      new int[]{1, 1, DENSE_BLOCK_SIZE, DENSE_BLOCK_SIZE},
      new long[]{denseIndex, 0L, 0L, 0L}
    );
    return block.getAsFlatArray();
  }

  private static void runStreamingCommand(final @NotNull List<String> command,
                                          final @NotNull Path workDirectory,
                                          final @NotNull Consumer<String> logger,
                                          final @NotNull Consumer<Process> processSink,
                                          final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    final var process = new ProcessBuilder(command)
      .directory(workDirectory.toFile())
      .redirectErrorStream(true)
      .start();
    processSink.accept(process);
    final var streamThread = new Thread(() -> {
      try (final var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          logger.accept(line);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }, "hictk-export-log");
    streamThread.setDaemon(true);
    streamThread.start();
    while (process.isAlive()) {
      checkCancelled(cancellationRequested);
      Thread.sleep(200L);
    }
    streamThread.join();
    final int exitCode = process.waitFor();
    processSink.accept(null);
    if (exitCode != 0) {
      throw new IllegalStateException(String.join(" ", command) + " failed with exit code " + exitCode);
    }
  }

  private static void checkCancelled(final @NotNull BooleanSupplier cancellationRequested) {
    if (cancellationRequested.getAsBoolean()) {
      throw new IllegalStateException("Cancelled");
    }
  }

  private static void copyStringArray(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                      final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                      final @NotNull String srcPath,
                                      final @NotNull String dstPath) {
    dst.string().writeArray(dstPath, src.string().readArray(srcPath));
  }

  private static void copyIntArrayChunked(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                          final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                          final @NotNull String srcPath,
                                          final @NotNull String dstPath,
                                          final int chunkSize,
                                          final @NotNull HDF5IntStorageFeatures compression) {
    final long length = datasetLength(src, srcPath);
    dst.int32().createArray(dstPath, length, safeChunkLen(length, chunkSize), compression);
    long offset = 0L;
    while (offset < length) {
      final int blockLen = (int) Math.min(chunkSize, length - offset);
      final var block = src.int32().readArrayBlockWithOffset(srcPath, blockLen, offset);
      dst.int32().writeArrayBlockWithOffset(dstPath, block, blockLen, offset);
      offset += blockLen;
    }
  }

  private static void copyLongArrayChunked(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                           final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                           final @NotNull String srcPath,
                                           final @NotNull String dstPath,
                                           final int chunkSize,
                                           final @NotNull HDF5IntStorageFeatures compression) {
    final long length = datasetLength(src, srcPath);
    dst.int64().createArray(dstPath, length, safeChunkLen(length, chunkSize), compression);
    long offset = 0L;
    while (offset < length) {
      final int blockLen = (int) Math.min(chunkSize, length - offset);
      final var block = src.int64().readArrayBlockWithOffset(srcPath, blockLen, offset);
      dst.int64().writeArrayBlockWithOffset(dstPath, block, blockLen, offset);
      offset += blockLen;
    }
  }

  private static void copyDoubleArrayChunked(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                             final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                             final @NotNull String srcPath,
                                             final @NotNull String dstPath,
                                             final int chunkSize,
                                             final @NotNull HDF5FloatStorageFeatures compression) {
    final long length = datasetLength(src, srcPath);
    dst.float64().createArray(dstPath, length, safeChunkLen(length, chunkSize), compression);
    long offset = 0L;
    while (offset < length) {
      final int blockLen = (int) Math.min(chunkSize, length - offset);
      final var block = src.float64().readArrayBlockWithOffset(srcPath, blockLen, offset);
      dst.float64().writeArrayBlockWithOffset(dstPath, block, blockLen, offset);
      offset += blockLen;
    }
  }

  private static void writeBinWeightsForCopiedCoolerBins(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                                         final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                                         final @NotNull String dstPath,
                                                         final @NotNull HictToMcoolConverter.ResolutionLayout layout,
                                                         final long resolution,
                                                         final int chunkSize,
                                                         final @NotNull HDF5FloatStorageFeatures compression) {
    final long length = datasetLength(src, "/bins/chrom");
    final int safeChunkSize = safeChunkLen(length, chunkSize);
    dst.float64().createArray(dstPath, length, safeChunkSize, compression);

    @SuppressWarnings("unchecked") final List<HictToMcoolConverter.ContigBinSpan>[] spansByChrom = new List[layout.chroms().size()];
    for (final var span : layout.spans()) {
      spansByChrom[span.chromIndex()] = List.of(span);
    }

    long offset = 0L;
    while (offset < length) {
      final int blockLen = (int) Math.min(safeChunkSize, length - offset);
      final var chroms = src.int32().readArrayBlockWithOffset("/bins/chrom", blockLen, offset);
      final var starts = src.int32().readArrayBlockWithOffset("/bins/start", blockLen, offset);
      final var weights = new double[blockLen];
      for (int i = 0; i < blockLen; i++) {
        weights[i] = weightForCopiedCoolerBin(spansByChrom, chroms[i], Math.floorDiv((long) starts[i], resolution));
      }
      dst.float64().writeArrayBlockWithOffset(dstPath, weights, blockLen, offset);
      offset += blockLen;
    }
  }

  private static double weightForCopiedCoolerBin(
    final @NotNull List<HictToMcoolConverter.ContigBinSpan> @NotNull [] spansByChrom,
    final int chromIndex,
    final long localBin
  ) {
    if (chromIndex < 0 || chromIndex >= spansByChrom.length || localBin < 0) {
      return 1.0d;
    }
    final var spans = spansByChrom[chromIndex];
    if (spans == null || spans.isEmpty()) {
      return 1.0d;
    }
    final var span = spans.get(0);
    long atuLocalStart = 0L;
    for (final var atu : span.atus()) {
      final long atuEnd = atuLocalStart + atu.getLength();
      if (localBin < atuEnd) {
        return weightFromAtu(atu, (int) (localBin - atuLocalStart));
      }
      atuLocalStart = atuEnd;
    }
    return 1.0d;
  }

  private static double weightFromAtu(final @NotNull ATUDescriptor atu, final int offsetInAtu) {
    final int sourceIndex = switch (atu.getDirection()) {
      case FORWARD -> atu.getStartIndexInStripeIncl() + offsetInAtu;
      case REVERSED -> atu.getEndIndexInStripeExcl() - 1 - offsetInAtu;
    };
    final var weights = atu.getStripeDescriptor().bin_weights();
    return sourceIndex >= 0 && sourceIndex < weights.length ? weights[sourceIndex] : 1.0d;
  }

  private static long datasetLength(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
                                    final @NotNull String path) {
    final var dims = reader.object().getDataSetInformation(path).getDimensions();
    if (dims.length != 1) {
      throw new IllegalStateException(path + " rank mismatch: " + dims.length);
    }
    return dims[0];
  }

  private static long sumIntArrayChunked(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                         final @NotNull String path,
                                         final int chunkSize) {
    final long length = datasetLength(src, path);
    long offset = 0L;
    long sum = 0L;
    while (offset < length) {
      final int blockLen = (int) Math.min(chunkSize, length - offset);
      final var block = src.int32().readArrayBlockWithOffset(path, blockLen, offset);
      for (final int value : block) {
        sum += value;
      }
      offset += blockLen;
    }
    return sum;
  }

  private static void writeChromsGroup(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                       final @NotNull String groupPath,
                                       final @NotNull List<HictToMcoolConverter.CoolerChrom> chroms,
                                       final @NotNull HDF5IntStorageFeatures compression) {
    final var names = new String[chroms.size()];
    final var lengths = new int[chroms.size()];
    for (int i = 0; i < chroms.size(); i++) {
      names[i] = chroms.get(i).name();
      lengths[i] = Math.toIntExact(chroms.get(i).lengthBp());
    }
    dst.string().writeArray(groupPath + "/name", names);
    dst.int32().writeArray(groupPath + "/length", lengths, compression);
  }

  private static void writeResolutionMetadata(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                              final @NotNull String root,
                                              final long resolution,
                                              final long binsCount,
                                              final long chromCount,
                                              final long nonzeroPixelCount,
                                              final long totalCounts) {
    dst.string().setAttrVL(root, "assembly", DEFAULT_ASSEMBLY_NAME);
    dst.int64().setAttr(root, "bin-size", resolution);
    dst.string().setAttrVL(root, "bin-type", "fixed");
    dst.int64().setAttr(root, "cis", totalCounts);
    dst.string().setAttrVL(root, "creation-date", OffsetDateTime.now(ZoneOffset.UTC).toString());
    dst.string().setAttrVL(root, "format", "HDF5::Cooler");
    dst.string().setAttrVL(root, "format-url", "https://github.com/open2c/cooler");
    dst.int64().setAttr(root, "format-version", 3L);
    dst.string().setAttrVL(root, "generated-by", "HiCT hict-to-mcool hictk-assisted exporter");
    dst.string().setAttrVL(root, "metadata", "{}");
    dst.int64().setAttr(root, "nbins", binsCount);
    dst.int64().setAttr(root, "nchroms", chromCount);
    dst.int64().setAttr(root, "nnz", nonzeroPixelCount);
    dst.string().setAttrVL(root, "storage-mode", "symmetric-upper");
    dst.int64().setAttr(root, "sum", totalCounts);
  }

  private static @NotNull HDF5IntStorageFeatures resolveIntStorageFeatures(final @NotNull ConversionOptions options) {
    if (options.compressionLevel() <= 0) {
      return HDF5IntStorageFeatures.INT_CHUNKED;
    }
    return HDF5IntStorageFeatures.createDeflation(options.compressionLevel());
  }

  private static @NotNull HDF5FloatStorageFeatures resolveFloatStorageFeatures(final @NotNull ConversionOptions options) {
    if (options.compressionLevel() <= 0) {
      return HDF5FloatStorageFeatures.FLOAT_CHUNKED;
    }
    return HDF5FloatStorageFeatures.createDeflation(options.compressionLevel());
  }

  private static int safeChunkLen(final long length, final int preferred) {
    final long base = Math.max(1L, Math.min(Math.max(1L, (long) preferred), Math.max(1L, length)));
    return (int) Math.min(base, Integer.MAX_VALUE);
  }

  private static long estimateEtaMillis(final long done, final long total, final long elapsedMillis) {
    if (done <= 0 || total <= 0 || done >= total || elapsedMillis <= 0) {
      return 0L;
    }
    return (elapsedMillis * (total - done)) / done;
  }

  private static @NotNull String formatEta(final long done,
                                           final long total,
                                           final long elapsedMillis,
                                           final long etaMillis) {
    if (done <= 0L || total <= 0L || done >= total) {
      return "00:00";
    }
    final long warmupThreshold = Math.min(ETA_WARMUP_MIN_ITEMS, Math.max(1L, total / 1_000L));
    if (done < warmupThreshold || elapsedMillis < 5_000L) {
      return "warming-up";
    }
    return formatDuration(etaMillis);
  }

  private static @NotNull String formatItemsPerSecond(final long done, final long elapsedMillis) {
    if (done <= 0L || elapsedMillis <= 0L) {
      return "0/s";
    }
    final double rate = (done * 1000.0d) / elapsedMillis;
    if (rate >= 1_000_000_000.0d) {
      return String.format(Locale.ROOT, "%.2fG/s", rate / 1_000_000_000.0d);
    }
    if (rate >= 1_000_000.0d) {
      return String.format(Locale.ROOT, "%.2fM/s", rate / 1_000_000.0d);
    }
    if (rate >= 1_000.0d) {
      return String.format(Locale.ROOT, "%.2fk/s", rate / 1_000.0d);
    }
    return String.format(Locale.ROOT, "%.0f/s", rate);
  }

  private static @NotNull String formatDuration(final long millis) {
    if (millis <= 0) {
      return "00:00";
    }
    final long totalSeconds = millis / 1000L;
    final long hours = totalSeconds / 3600L;
    final long minutes = (totalSeconds % 3600L) / 60L;
    final long seconds = totalSeconds % 60L;
    if (hours > 0L) {
      return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }
    return String.format("%02d:%02d", minutes, seconds);
  }

  private static @NotNull Consumer<String> synchronizedLogger(final @NotNull Consumer<String> delegate) {
    final Object lock = new Object();
    return message -> {
      synchronized (lock) {
        delegate.accept(message);
      }
    };
  }

  private static @NotNull Path createExportTempDirectory() throws IOException {
    final var configuredRoot = firstNonBlank(
      System.getProperty("hict.export.tmpDir"),
      System.getenv("HICT_EXPORT_TMPDIR"),
      System.getenv("TMPDIR")
    );
    if (configuredRoot == null) {
      return Files.createTempDirectory("hict-hict-to-mcool-");
    }
    final var root = Files.createDirectories(Path.of(configuredRoot));
    return Files.createTempDirectory(root, "hict-hict-to-mcool-");
  }

  private static int resolveToolThreads(final int parallelism) {
    if (parallelism <= 0) {
      return Math.max(1, Runtime.getRuntime().availableProcessors());
    }
    return Math.max(1, parallelism);
  }

  private static int resolveCooSortBatchSize(final int chunkSize,
                                             final long exportMaxMemoryBytes,
                                             final @NotNull Consumer<String> logger) {
    final var configured = firstNonBlank(
      System.getProperty("hict.export.cooSortBatchSize"),
      System.getenv("HICT_EXPORT_COO_SORT_BATCH_SIZE")
    );
    if (configured != null) {
      try {
        final int requested = Integer.parseInt(configured);
        final int clamped = Math.max(MIN_COO_SORT_BATCH_SIZE, Math.min(MAX_COO_SORT_BATCH_SIZE, requested));
        logger.accept("COO sort batch size=" + clamped + " records (configured)");
        return clamped;
      } catch (NumberFormatException ignored) {
        logger.accept("WARNING: Ignoring invalid HICT_EXPORT_COO_SORT_BATCH_SIZE/hict.export.cooSortBatchSize value: " + configured);
      }
    }
    final long memoryBoundRecords = exportMaxMemoryBytes == Long.MAX_VALUE
      ? MAX_COO_SORT_BATCH_SIZE
      : Math.max(MIN_COO_SORT_BATCH_SIZE, exportMaxMemoryBytes / 8L / ESTIMATED_COO_RECORD_BYTES);
    final int batchSize = (int) Math.max(
      MIN_COO_SORT_BATCH_SIZE,
      Math.min(MAX_COO_SORT_BATCH_SIZE, Math.max((long) chunkSize, memoryBoundRecords))
    );
    logger.accept("COO sort batch size=" + batchSize + " records");
    return batchSize;
  }

  private static int resolveCooSourceReadBatchSize(final @NotNull Consumer<String> logger) {
    final var configured = firstNonBlank(
      System.getProperty("hict.export.cooReadBatchSize"),
      System.getenv("HICT_EXPORT_COO_READ_BATCH_SIZE")
    );
    if (configured != null) {
      try {
        final int requested = Integer.parseInt(configured);
        final int clamped = Math.max(MIN_COO_SOURCE_READ_BATCH_SIZE, Math.min(MAX_COO_SOURCE_READ_BATCH_SIZE, requested));
        logger.accept("COO source read batch size=" + clamped + " records (configured)");
        return clamped;
      } catch (NumberFormatException ignored) {
        logger.accept("WARNING: Ignoring invalid HICT_EXPORT_COO_READ_BATCH_SIZE/hict.export.cooReadBatchSize value: " + configured);
      }
    }
    logger.accept("COO source read batch size=" + DEFAULT_COO_SOURCE_READ_BATCH_SIZE + " records");
    return DEFAULT_COO_SOURCE_READ_BATCH_SIZE;
  }

  private static int resolveHictkLoadChunkSize(final @NotNull Consumer<String> logger) {
    final var configured = firstNonBlank(
      System.getProperty("hict.export.hictkLoadChunkSize"),
      System.getenv("HICT_EXPORT_HICTK_LOAD_CHUNK_SIZE")
    );
    if (configured != null) {
      try {
        final int requested = Integer.parseInt(configured);
        final int clamped = Math.max(MIN_HICTK_LOAD_CHUNK_SIZE, Math.min(MAX_HICTK_LOAD_CHUNK_SIZE, requested));
        logger.accept("hictk load chunk size=" + clamped + " records (configured)");
        return clamped;
      } catch (NumberFormatException ignored) {
        logger.accept("WARNING: Ignoring invalid HICT_EXPORT_HICTK_LOAD_CHUNK_SIZE/hict.export.hictkLoadChunkSize value: " + configured);
      }
    }
    logger.accept("hictk load chunk size=" + DEFAULT_HICTK_LOAD_CHUNK_SIZE + " records");
    return DEFAULT_HICTK_LOAD_CHUNK_SIZE;
  }

  private static long resolveExportMaxMemoryBytes(final @NotNull Consumer<String> logger) {
    final var configured = firstNonBlank(
      System.getProperty("hict.export.maxMemoryBytes"),
      System.getenv("HICT_EXPORT_MAX_MEMORY_BYTES"),
      System.getenv("HICT_CONVERSION_MAX_MEMORY_BYTES")
    );
    if (configured == null) {
      return DEFAULT_EXPORT_MAX_MEMORY_BYTES;
    }
    try {
      if (isUnlimitedMemoryValue(configured)) {
        return Long.MAX_VALUE;
      }
      return Math.max(MIN_EXPORT_MAX_MEMORY_BYTES, parseByteSize(configured));
    } catch (RuntimeException e) {
      logger.accept("WARNING: Ignoring invalid HiCT export memory limit '" + configured + "': " + e.getMessage());
      return DEFAULT_EXPORT_MAX_MEMORY_BYTES;
    }
  }

  private static long parseByteSize(final @NotNull String rawValue) {
    final var value = rawValue.trim().toLowerCase(Locale.ROOT);
    if (value.isBlank()) {
      throw new IllegalArgumentException("blank byte size");
    }
    int suffixStart = value.length();
    while (suffixStart > 0 && Character.isLetter(value.charAt(suffixStart - 1))) {
      suffixStart--;
    }
    final var numberPart = value.substring(0, suffixStart).trim();
    final var suffix = value.substring(suffixStart).trim();
    final double number = Double.parseDouble(numberPart);
    if (!Double.isFinite(number) || number <= 0.0d) {
      throw new IllegalArgumentException("invalid byte size");
    }
    final long multiplier = switch (suffix) {
      case "", "b", "bytes" -> 1L;
      case "k", "kb", "kib" -> 1024L;
      case "m", "mb", "mib" -> 1024L * 1024L;
      case "g", "gb", "gib" -> 1024L * 1024L * 1024L;
      case "t", "tb", "tib" -> 1024L * 1024L * 1024L * 1024L;
      default -> throw new IllegalArgumentException("unknown byte-size suffix: " + suffix);
    };
    return Math.max(1L, (long) Math.floor(number * multiplier));
  }

  private static boolean isUnlimitedMemoryValue(final @NotNull String rawValue) {
    final var value = rawValue.trim().toLowerCase(Locale.ROOT);
    return value.equals("0")
      || value.equals("-1")
      || value.equals("false")
      || value.equals("no")
      || value.equals("none")
      || value.equals("off")
      || value.equals("unlimited");
  }

  private static @NotNull String formatByteSize(final long bytes) {
    if (bytes == Long.MAX_VALUE) {
      return "unlimited";
    }
    final double gib = bytes / (1024.0d * 1024.0d * 1024.0d);
    if (gib >= 1.0d) {
      return String.format(Locale.ROOT, "%.1f GiB", gib);
    }
    final double mib = bytes / (1024.0d * 1024.0d);
    if (mib >= 1.0d) {
      return String.format(Locale.ROOT, "%.1f MiB", mib);
    }
    return bytes + " B";
  }

  private static @NotNull String describeFileSize(final @NotNull Path path) {
    try {
      if (!Files.exists(path)) {
        return "not-created";
      }
      return formatByteSize(Files.size(path));
    } catch (IOException e) {
      return "unknown";
    }
  }

  private static @NotNull CooTextCompression resolveCooTextCompression(final @NotNull Consumer<String> logger) {
    final var configured = firstNonBlank(
      System.getProperty("hict.export.cooCompression"),
      System.getenv("HICT_EXPORT_COO_COMPRESSION")
    );
    if (configured == null) {
      return CooTextCompression.GZIP;
    }
    return switch (configured.trim().toLowerCase(Locale.ROOT)) {
      case "none", "plain", "text", "false", "no", "off", "0" -> CooTextCompression.NONE;
      case "gzip", "gz", "true", "yes", "on", "1" -> CooTextCompression.GZIP;
      default -> {
        logger.accept("WARNING: Ignoring invalid HICT_EXPORT_COO_COMPRESSION/hict.export.cooCompression value: " + configured);
        yield CooTextCompression.GZIP;
      }
    };
  }

  private static int resolveCooMergeFanIn(final @NotNull Consumer<String> logger) {
    final var configured = firstNonBlank(
      System.getProperty("hict.export.mergeFanIn"),
      System.getenv("HICT_EXPORT_MERGE_FAN_IN")
    );
    if (configured == null) {
      return DEFAULT_COO_MERGE_FAN_IN;
    }
    try {
      final int requested = Integer.parseInt(configured);
      return Math.max(MIN_COO_MERGE_FAN_IN, Math.min(MAX_COO_MERGE_FAN_IN, requested));
    } catch (NumberFormatException ignored) {
      logger.accept("WARNING: Ignoring invalid HICT_EXPORT_MERGE_FAN_IN/hict.export.mergeFanIn value: " + configured);
      return DEFAULT_COO_MERGE_FAN_IN;
    }
  }

  private static String firstNonBlank(final String... values) {
    for (final var value : values) {
      if (value != null && !value.isBlank()) {
        return value.trim();
      }
    }
    return null;
  }

  private static void deleteChunkFiles(final @NotNull List<Path> paths) throws IOException {
    IOException deleteFailure = null;
    for (final var path : paths) {
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        if (deleteFailure == null) {
          deleteFailure = e;
        } else {
          deleteFailure.addSuppressed(e);
        }
      }
    }
    if (deleteFailure != null) {
      throw deleteFailure;
    }
  }

  private static void deleteRecursively(final @NotNull Path path) {
    if (!Files.exists(path)) {
      return;
    }
    try (final var stream = Files.walk(path)) {
      stream.sorted(Comparator.reverseOrder()).forEach(candidate -> {
        try {
          Files.deleteIfExists(candidate);
        } catch (IOException ignored) {
        }
      });
    } catch (IOException ignored) {
    }
  }

  private record SourceSegment(long sourceStart, long sourceEnd, long targetStart, long targetEnd,
                               @NotNull ATUDirection direction) {
    private long map(final long sourceBin) {
      final long local = sourceBin - sourceStart;
      return switch (direction) {
        case FORWARD -> targetStart + local;
        case REVERSED -> targetEnd - 1L - local;
      };
    }
  }

  private record SparseBlockRef(long blockOffset, long length, long rowStripeOffset, long colStripeOffset) {
  }

  private static final class SourceToAssemblyMapper {
    private final long[] sourceStarts;
    private final long[] sourceEnds;
    private final long[] targetStarts;
    private final long[] targetEnds;
    private final boolean[] reversed;
    private final byte[] reversedBytes;

    private SourceToAssemblyMapper(final @NotNull List<SourceSegment> segments) {
      this.sourceStarts = new long[segments.size()];
      this.sourceEnds = new long[segments.size()];
      this.targetStarts = new long[segments.size()];
      this.targetEnds = new long[segments.size()];
      this.reversed = new boolean[segments.size()];
      this.reversedBytes = new byte[segments.size()];
      for (int i = 0; i < segments.size(); i++) {
        final var segment = segments.get(i);
        sourceStarts[i] = segment.sourceStart();
        sourceEnds[i] = segment.sourceEnd();
        targetStarts[i] = segment.targetStart();
        targetEnds[i] = segment.targetEnd();
        reversed[i] = segment.direction() == ATUDirection.REVERSED;
        reversedBytes[i] = (byte) (reversed[i] ? 1 : 0);
      }
    }

    private @NotNull Cursor cursor() {
      return new Cursor();
    }

    private int findSegment(final long sourceBin) {
      int left = 0;
      int right = sourceStarts.length - 1;
      while (left <= right) {
        final int mid = (left + right) >>> 1;
        if (sourceBin < sourceStarts[mid]) {
          right = mid - 1;
        } else if (sourceBin >= sourceEnds[mid]) {
          left = mid + 1;
        } else {
          return mid;
        }
      }
      throw new IllegalStateException("Source bin " + sourceBin + " is not covered by current assembly mapping");
    }

    private long mapAt(final int index, final long sourceBin) {
      final long local = sourceBin - sourceStarts[index];
      if (reversed[index]) {
        return targetEnds[index] - 1L - local;
      }
      return targetStarts[index] + local;
    }

    private final class Cursor {
      private int segmentIndex = -1;

      private long map(final long sourceBin) {
        final int current = segmentIndex;
        if (current >= 0 && current < sourceStarts.length && sourceBin >= sourceStarts[current] && sourceBin < sourceEnds[current]) {
          return mapAt(current, sourceBin);
        }
        final int found = findSegment(sourceBin);
        segmentIndex = found;
        return mapAt(found, sourceBin);
      }
    }
  }

  enum CooTextCompression {
    NONE("none", ".coo"),
    GZIP("gzip", ".coo.gz");

    private final @NotNull String logName;
    private final @NotNull String suffix;

    CooTextCompression(final @NotNull String logName, final @NotNull String suffix) {
      this.logName = logName;
      this.suffix = suffix;
    }

    private @NotNull String logName() {
      return logName;
    }

    private @NotNull Path resolvePath(final @NotNull Path directory, final @NotNull String basename) {
      return directory.resolve(basename + suffix);
    }

    private @NotNull BufferedWriter openWriter(final @NotNull Path path) throws IOException {
      return switch (this) {
        case NONE -> Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        case GZIP -> new BufferedWriter(
          new OutputStreamWriter(
            new GZIPOutputStream(Files.newOutputStream(path)),
            StandardCharsets.UTF_8
          )
        );
      };
    }
  }

  private static final class FastCooTextWriter implements AutoCloseable {
    private final BufferedOutputStream output;
    private final byte[] digits = new byte[20];

    private FastCooTextWriter(final @NotNull OutputStream output) {
      this.output = new BufferedOutputStream(output, 1 << 20);
    }

    private void writeBatch(final @NotNull CooRecordBatch batch) throws IOException {
      for (int i = 0; i < batch.size(); i++) {
        writeLong(batch.rows[i]);
        output.write('\t');
        writeLong(batch.cols[i]);
        output.write('\t');
        writeLong(batch.counts[i]);
        output.write('\n');
      }
    }

    private void flush() throws IOException {
      output.flush();
    }

    private void writeLong(final long value) throws IOException {
      if (value == 0L) {
        output.write('0');
        return;
      }
      if (value == Long.MIN_VALUE) {
        final var text = Long.toString(value);
        output.write(text.getBytes(StandardCharsets.US_ASCII));
        return;
      }

      long current = value;
      if (current < 0L) {
        output.write('-');
        current = -current;
      }
      int offset = digits.length;
      while (current > 0L) {
        digits[--offset] = (byte) ('0' + (current % 10L));
        current /= 10L;
      }
      output.write(digits, offset, digits.length - offset);
    }

    @Override
    public void close() throws IOException {
      output.close();
    }
  }

  private record CooRecord(long row, long col, long count) {
  }

  private static final class CooRecordBatch {
    private final long[] rows;
    private final long[] cols;
    private final long[] counts;
    private int size;

    private CooRecordBatch(final int capacity) {
      if (capacity <= 0) {
        throw new IllegalArgumentException("COO batch capacity must be positive");
      }
      this.rows = new long[capacity];
      this.cols = new long[capacity];
      this.counts = new long[capacity];
    }

    private void add(final long row, final long col, final long count) {
      if (size >= rows.length) {
        throw new IllegalStateException("COO batch is full");
      }
      rows[size] = row;
      cols[size] = col;
      counts[size] = count;
      size++;
    }

    private int remainingCapacity() {
      return rows.length - size;
    }

    private void advanceSize(final int count) {
      if (count < 0 || count > remainingCapacity()) {
        throw new IllegalArgumentException("Invalid COO batch size increment: " + count);
      }
      size += count;
    }

    private int size() {
      return size;
    }

    private boolean isEmpty() {
      return size == 0;
    }

    private void clear() {
      size = 0;
    }

    private long @NotNull [] copyRows() {
      return Arrays.copyOf(rows, size);
    }

    private long @NotNull [] copyCols() {
      return Arrays.copyOf(cols, size);
    }

    private long @NotNull [] copyCounts() {
      return Arrays.copyOf(counts, size);
    }
  }

  private record MergeStats(long rawRecords, long uniqueRecords) {
  }

  @FunctionalInterface
  private interface MergedRecordSink {
    void write(long row, long col, long count) throws IOException;
  }

  private static final class ChunkCursor implements AutoCloseable {
    private final DataInputStream input;
    private CooRecord record;

    private ChunkCursor(final @NotNull Path path) throws IOException {
      this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
    }

    private boolean advance() throws IOException {
      try {
        this.record = new CooRecord(input.readLong(), input.readLong(), input.readLong());
        return true;
      } catch (IOException e) {
        if (e instanceof java.io.EOFException) {
          this.record = null;
          return false;
        }
        throw e;
      }
    }

    private @NotNull CooRecord record() {
      return Objects.requireNonNull(record, "Current record is not available");
    }

    @Override
    public void close() throws IOException {
      input.close();
    }
  }

  private static final class CooExportProgressTracker {
    private final long resolution;
    private final long totalItems;
    private final long startedNanos;
    private final OverallProgressTracker overallTracker;
    private final Consumer<String> logger;
    private long doneItems;
    private long lastLoggedNanos;
    private int lastLoggedPercent = -1;

    private CooExportProgressTracker(final long resolution,
                                     final long totalItems,
                                     final long startedNanos,
                                     final @NotNull OverallProgressTracker overallTracker,
                                     final @NotNull Consumer<String> logger) {
      this.resolution = resolution;
      this.totalItems = totalItems;
      this.startedNanos = startedNanos;
      this.overallTracker = overallTracker;
      this.logger = logger;
      this.lastLoggedNanos = startedNanos;
    }

    private void add(final long items) {
      if (items <= 0L || totalItems <= 0L) {
        return;
      }
      final long remaining = Math.max(0L, totalItems - doneItems);
      final long actualItems = Math.min(items, remaining);
      if (actualItems <= 0L) {
        return;
      }
      doneItems += actualItems;
      overallTracker.add(actualItems, "Resolution " + resolution + " COO export");
      final int percent = (int) ((doneItems * 100L) / totalItems);
      final long nowNanos = System.nanoTime();
      if (percent < 100 && percent == lastLoggedPercent && nowNanos - lastLoggedNanos < PROGRESS_LOG_INTERVAL_NANOS) {
        return;
      }
      lastLoggedPercent = percent;
      lastLoggedNanos = nowNanos;
      final long elapsedMillis = (nowNanos - startedNanos) / 1_000_000L;
      final long etaMillis = estimateEtaMillis(doneItems, totalItems, elapsedMillis);
      logger.accept(
        String.format(
          "Resolution %d COO export: %d%% (%d/%d), elapsed=%s, eta=%s, rate=%s",
          resolution,
          percent,
          doneItems,
          totalItems,
          formatDuration(elapsedMillis),
          formatEta(doneItems, totalItems, elapsedMillis, etaMillis),
          formatItemsPerSecond(doneItems, elapsedMillis)
        )
      );
    }
  }

  private static final class OverallProgressTracker {
    private final long totalItems;
    private final long startedNanos = System.nanoTime();
    private final Consumer<String> logger;
    private final AtomicLong doneItems = new AtomicLong(0L);
    private final AtomicLong lastLoggedNanos = new AtomicLong(startedNanos);
    private volatile int lastPercent = -1;

    private OverallProgressTracker(final long totalItems, final @NotNull Consumer<String> logger) {
      this.totalItems = totalItems;
      this.logger = logger;
    }

    private void add(final long items, final @NotNull String detail) {
      final long done = Math.min(totalItems, doneItems.addAndGet(items));
      final int percent = (int) ((done * 100L) / Math.max(1L, totalItems));
      final long nowNanos = System.nanoTime();
      if (percent < 100 && percent == lastPercent && nowNanos - lastLoggedNanos.get() < PROGRESS_LOG_INTERVAL_NANOS) {
        return;
      }
      lastLoggedNanos.set(nowNanos);
      lastPercent = percent;
      final long elapsedMillis = (nowNanos - startedNanos) / 1_000_000L;
      final long etaMillis = estimateEtaMillis(done, totalItems, elapsedMillis);
      logger.accept(
        String.format(
          "Overall progress: %d%% (%d/%d), elapsed=%s, eta=%s, rate=%s - %s",
          percent,
          done,
          totalItems,
          formatDuration(elapsedMillis),
          formatEta(done, totalItems, elapsedMillis, etaMillis),
          formatItemsPerSecond(done, elapsedMillis),
          detail
        )
      );
    }

    private void finish(final @NotNull String detail) {
      doneItems.set(totalItems);
      lastPercent = 100;
      final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
      logger.accept(
        String.format(
          "Overall progress: 100%% (%d/%d), elapsed=%s, eta=00:00 - %s",
          totalItems,
          totalItems,
          formatDuration(elapsedMillis),
          detail
        )
      );
    }
  }
}
