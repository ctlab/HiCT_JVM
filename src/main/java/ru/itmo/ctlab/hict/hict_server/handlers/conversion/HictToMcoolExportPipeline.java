package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5DataClass;
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
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.PriorityQueue;
import java.util.Objects;
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
      new HictToMcoolConverter().convert(options, logger);
      return;
    }

    final var toolchain = toolchainManager.requireHictkToolchain();
    logger.accept("HICT_TOOLCHAIN source=" + toolchain.source() + " platform=" + toolchain.platform());
    convertViaHictk(options, toolchain, logger, processSink, cancellationRequested);
  }

  private @NotNull ConversionOptions.ExportMode resolveEffectiveMode(final @NotNull ConversionOptions options) {
    if (options.exportMode() == ConversionOptions.ExportMode.AUTO) {
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
    final var tmpDirectory = Files.createTempDirectory("hict-hict-to-mcool-");
    synchronizedLogger.accept("Preparing hictk-assisted .mcool export in " + tmpDirectory);

    try (final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(options.inputPath(), 2, 8))) {
      if (options.applyAgpBeforeExport() && !options.agpPath().isBlank()) {
        final var agpPath = Path.of(options.agpPath());
        try (final var reader = Files.newBufferedReader(agpPath, StandardCharsets.UTF_8)) {
          chunkedFile.importAGP(reader);
        }
        synchronizedLogger.accept("Applied AGP before export: " + agpPath);
      }

      final var selectedResolutions = resolveResolutions(chunkedFile, options);
      final var assemblyLayout = HictToMcoolConverter.buildCoolerAssemblyLayout(chunkedFile, selectedResolutions);
      final var totalSourcePixels = countSourcePixels(options.inputPath(), selectedResolutions);
      final var overallTracker = new OverallProgressTracker(
        Math.max(1L, totalSourcePixels * 2L),
        synchronizedLogger
      );

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
          final var cooPath = resolutionTmpDir.resolve("pixels.coo.gz");
          final var coolPath = resolutionTmpDir.resolve("pixels.cool");

          writeChromSizesFile(assemblyLayout, chromSizesPath);
          final var mapper = buildMapper(chunkedFile, options.inputPath(), resolutionOrder);
          exportTransformedCoo(
            options.inputPath(),
            resolution,
            cooPath,
            mapper,
            options.chunkSize(),
            overallTracker,
            synchronizedLogger,
            cancellationRequested
          );

          runHictkLoad(
            toolchain,
            cooPath,
            chromSizesPath,
            resolution,
            coolPath,
            options.parallelism(),
            resolutionTmpDir,
            synchronizedLogger,
            processSink,
            cancellationRequested
          );

          try (final var coolReader = HDF5Factory.openForReading(coolPath.toFile())) {
            mergeResolutionFromCool(
              coolReader,
              dst,
              resolution,
              options.chunkSize(),
              resolveIntStorageFeatures(options),
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
      Integer.toString(Math.max(2, Math.min(Math.max(2, parallelism), 24))),
      "--tmpdir",
      workDirectory.toString(),
      "--force",
      cooPath.toString(),
      coolPath.toString()
    );
    runStreamingCommand(command, workDirectory, logger, processSink, cancellationRequested);
  }

  private static void mergeResolutionFromCool(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
                                              final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
                                              final long resolution,
                                              final int chunkSize,
                                              final @NotNull HDF5IntStorageFeatures compression,
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

  private static void exportTransformedCoo(final @NotNull Path inputPath,
                                           final long resolution,
                                           final @NotNull Path outputPath,
                                           final @NotNull SourceToAssemblyMapper mapper,
                                           final int chunkSize,
                                           final @NotNull OverallProgressTracker overallTracker,
                                           final @NotNull Consumer<String> logger,
                                           final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    final var workDir = Files.createDirectories(outputPath.getParent());
    final var chunkPaths = new ArrayList<Path>();
    final int sortBatchSize = Math.max(50_000, Math.min(Math.max(50_000, chunkSize), 250_000));
    final var batch = new ArrayList<CooRecord>(sortBatchSize);
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
      long processedPixels = 0L;
      long total = 0L;
      for (int rowStripe = 0; rowStripe < stripeCount; rowStripe++) {
        final long rowBase = (long) rowStripe * stripeCount;
        final long[] rowBlockLengths = src.int64().readArrayBlockWithOffset(blockLengthsPath, stripeCount, rowBase);
        for (int colStripe = rowStripe; colStripe < stripeCount; colStripe++) {
          total += rowBlockLengths[colStripe];
        }
      }
      final long startedNanos = System.nanoTime();
      int lastLoggedPercent = -1;
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
            final int actualBlockLen = Math.toIntExact(blockLen);
            final var rows = src.int64().readArrayBlockWithOffset(rowsPath, actualBlockLen, blockOffset);
            final var cols = src.int64().readArrayBlockWithOffset(colsPath, actualBlockLen, blockOffset);
            final var vals = src.int64().readArrayBlockWithOffset(valuesPath, actualBlockLen, blockOffset);
            for (int i = 0; i < actualBlockLen; i++) {
              appendMappedRecord(
                batch,
                mapper,
                rowStripeOffset + rows[i],
                colStripeOffset + cols[i],
                vals[i]
              );
              if (batch.size() >= sortBatchSize) {
                final var chunkPath = flushSortedChunk(workDir, resolution, chunkPaths.size(), batch);
                chunkPaths.add(chunkPath);
                batch.clear();
              }
            }
          } else {
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
                  mapper,
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
          }
          processedPixels += blockLen;
          overallTracker.add(blockLen, "Resolution " + resolution + " COO export");
          if (total > 0) {
            final int percent = (int) ((processedPixels * 100L) / total);
            if (percent >= 100 || percent - lastLoggedPercent >= 10) {
              lastLoggedPercent = percent;
              final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
              final long etaMillis = estimateEtaMillis(processedPixels, total, elapsedMillis);
              logger.accept(
                String.format(
                  "Resolution %d COO export: %d%% (%d/%d), elapsed=%s, eta=%s",
                  resolution,
                  percent,
                  processedPixels,
                  total,
                  formatDuration(elapsedMillis),
                  formatDuration(etaMillis)
                )
              );
            }
          }
        }
      }
    }
    if (!batch.isEmpty()) {
      final var chunkPath = flushSortedChunk(workDir, resolution, chunkPaths.size(), batch);
      chunkPaths.add(chunkPath);
      batch.clear();
    }
    mergeSortedChunks(chunkPaths, outputPath, logger, cancellationRequested);
  }

  private static @NotNull Path flushSortedChunk(final @NotNull Path workDir,
                                                final long resolution,
                                                final int chunkIndex,
                                                final @NotNull List<CooRecord> records) throws IOException {
    records.sort(Comparator
      .comparingLong(CooRecord::row)
      .thenComparingLong(CooRecord::col)
      .thenComparingLong(CooRecord::count));
    final var chunkPath = workDir.resolve(String.format("pixels-r%d-%05d.bin", resolution, chunkIndex));
    try (final var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(chunkPath)))) {
      for (final var record : records) {
        out.writeLong(record.row());
        out.writeLong(record.col());
        out.writeLong(record.count());
      }
    }
    return chunkPath;
  }

  private static void mergeSortedChunks(final @NotNull List<Path> chunkPaths,
                                        final @NotNull Path outputPath,
                                        final @NotNull Consumer<String> logger,
                                        final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    if (chunkPaths.isEmpty()) {
      try (final var rawOut = Files.newOutputStream(outputPath);
           final var gzipOut = new GZIPOutputStream(rawOut);
           final var writer = new BufferedWriter(new OutputStreamWriter(gzipOut, StandardCharsets.UTF_8))) {
        writer.flush();
      }
      return;
    }

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
      try (final var rawOut = Files.newOutputStream(outputPath);
           final var gzipOut = new GZIPOutputStream(rawOut);
           final var writer = new BufferedWriter(new OutputStreamWriter(gzipOut, StandardCharsets.UTF_8))) {
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
            writer.write(Long.toString(pendingRow));
            writer.write('\t');
            writer.write(Long.toString(pendingCol));
            writer.write('\t');
            writer.write(Long.toString(pendingCount));
            writer.newLine();
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
          writer.write(Long.toString(pendingRow));
          writer.write('\t');
          writer.write(Long.toString(pendingCol));
          writer.write('\t');
          writer.write(Long.toString(pendingCount));
          writer.newLine();
          written++;
        }
        logger.accept("Merged sorted COO records complete: raw=" + rawRecords + ", unique=" + written);
      }
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
      IOException deleteFailure = null;
      for (final var chunkPath : chunkPaths) {
        try {
          Files.deleteIfExists(chunkPath);
        } catch (IOException e) {
          if (deleteFailure == null) {
            deleteFailure = e;
          } else {
            deleteFailure.addSuppressed(e);
          }
        }
      }
      if (closeFailure != null) {
        throw closeFailure;
      }
      if (deleteFailure != null) {
        throw deleteFailure;
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

  private static void appendMappedRecord(final @NotNull List<CooRecord> batch,
                                         final @NotNull SourceToAssemblyMapper mapper,
                                         final long sourceRow,
                                         final long sourceCol,
                                         final long count) {
    long mappedRow = mapper.map(sourceRow);
    long mappedCol = mapper.map(sourceCol);
    if (mappedRow > mappedCol) {
      final long tmp = mappedRow;
      mappedRow = mappedCol;
      mappedCol = tmp;
    }
    batch.add(new CooRecord(mappedRow, mappedCol, count));
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

  private record SourceToAssemblyMapper(@NotNull List<SourceSegment> segments) {
    private long map(final long sourceBin) {
      int left = 0;
      int right = segments.size() - 1;
      while (left <= right) {
        final int mid = (left + right) >>> 1;
        final var segment = segments.get(mid);
        if (sourceBin < segment.sourceStart()) {
          right = mid - 1;
        } else if (sourceBin >= segment.sourceEnd()) {
          left = mid + 1;
        } else {
          return segment.map(sourceBin);
        }
      }
      throw new IllegalStateException("Source bin " + sourceBin + " is not covered by current assembly mapping");
    }
  }

  private record CooRecord(long row, long col, long count) {
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

  private static final class OverallProgressTracker {
    private final long totalItems;
    private final long startedNanos = System.nanoTime();
    private final Consumer<String> logger;
    private final AtomicLong doneItems = new AtomicLong(0L);
    private volatile int lastPercent = -1;

    private OverallProgressTracker(final long totalItems, final @NotNull Consumer<String> logger) {
      this.totalItems = totalItems;
      this.logger = logger;
    }

    private void add(final long items, final @NotNull String detail) {
      final long done = Math.min(totalItems, doneItems.addAndGet(items));
      final int percent = (int) ((done * 100L) / Math.max(1L, totalItems));
      if (percent < 100 && percent == lastPercent) {
        return;
      }
      lastPercent = percent;
      final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
      final long etaMillis = estimateEtaMillis(done, totalItems, elapsedMillis);
      logger.accept(
        String.format(
          "Overall progress: %d%% (%d/%d), elapsed=%s, eta=%s - %s",
          percent,
          done,
          totalItems,
          formatDuration(elapsedMillis),
          formatDuration(etaMillis),
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
