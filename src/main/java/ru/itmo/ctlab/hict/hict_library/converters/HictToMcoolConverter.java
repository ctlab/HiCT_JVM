package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import io.vertx.core.json.JsonArray;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

public class HictToMcoolConverter {

  private static final String DEFAULT_CHROM_NAME = "assembly";
  private static final String ROOT_CHROMS_GROUP = "/chroms";
  private static final List<DatasetCopySpec> MCOOL_PIXEL_DATASETS = List.of(
    new DatasetCopySpec("pixels/bin1_id", HictToMcoolConverter::blockRowsPath, "pixels/bin1_id"),
    new DatasetCopySpec("pixels/bin2_id", HictToMcoolConverter::blockColsPath, "pixels/bin2_id"),
    new DatasetCopySpec("pixels/counts", HictToMcoolConverter::blockValuesPath, "pixels/counts")
  );

  public void convert(final @NotNull ConversionOptions options, final @NotNull Consumer<String> logConsumer) throws IOException, NoSuchFieldException {
    HDF5LibraryInitializer.initializeHDF5Library();
    final var synchronizedLogConsumer = synchronizedLogger(logConsumer);
    final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(options.inputPath(), 2, 8));
    try {
      if (options.applyAgpBeforeExport() && !options.agpPath().isBlank()) {
        final var requestedAgpPath = Path.of(options.agpPath());
        final var resolvedAgpPath = requestedAgpPath.isAbsolute()
          ? requestedAgpPath
          : options.inputPath().resolveSibling(requestedAgpPath).normalize();
        try (final var reader = Files.newBufferedReader(resolvedAgpPath, StandardCharsets.UTF_8)) {
          chunkedFile.importAGP(reader);
        }
        synchronizedLogConsumer.accept("Applied AGP before export: " + resolvedAgpPath);
      }

      final var selectedResolutions = resolveResolutions(chunkedFile.getResolutions(), options.resolutions());
      final var compression = resolveIntStorageFeatures(options, synchronizedLogConsumer);
      final var requestedWorkers = resolveRequestedWorkers(options.parallelism());
      final var workers = Math.max(1, Math.min(requestedWorkers, selectedResolutions.size()));
      final var totalVectorItems = countVectorItems(options.inputPath(), selectedResolutions, synchronizedLogConsumer);
      final var progressTracker = new ExportProgressTracker(
        Math.max(0L, (totalVectorItems * 2L) + countBins(chunkedFile, selectedResolutions)),
        System.nanoTime(),
        synchronizedLogConsumer
      );

      synchronizedLogConsumer.accept("Converting in parallel with workers=" + workers + ", chunkSize=" + options.chunkSize());

      final var stagedResolutionFiles = convertResolutionsInParallel(
        options.inputPath(),
        selectedResolutions,
        options.chunkSize(),
        compression,
        workers,
        progressTracker,
        synchronizedLogConsumer
      );

      try (final var dst = HDF5Factory.open(options.outputPath().toFile())) {
        dst.object().createGroup("/resolutions");
        dst.string().setAttrVL("/", "format", "HDF5::MCOOL");
        dst.int64().setAttr("/", "format-version", 2L);
        dst.object().createGroup(ROOT_CHROMS_GROUP);
        writeChromsGroup(dst, ROOT_CHROMS_GROUP, DEFAULT_CHROM_NAME, chunkedFile.getMatrixSizeBins()[0], compression);
        dst.string().write("/source_format", "hict");
        dst.string().write("/selected_resolutions", new JsonArray(selectedResolutions).encode());

        for (final var staged : stagedResolutionFiles.stream().sorted(Comparator.comparingLong(StagedResolutionFile::resolution)).toList()) {
          try (final var stagedReader = HDF5Factory.openForReading(staged.path().toFile())) {
            mergeResolution(stagedReader, dst, chunkedFile, staged.resolution(), options.chunkSize(), compression, progressTracker, logConsumer);
            synchronizedLogConsumer.accept("Merged resolution " + staged.resolution() + " to final output");
          } finally {
            try {
              Files.deleteIfExists(staged.path());
            } catch (IOException e) {
              synchronizedLogConsumer.accept("Failed to delete temp file " + staged.path() + ": " + e.getMessage());
            }
          }
        }
      }
      progressTracker.finish();
    } finally {
      chunkedFile.close();
    }
  }

  private static @NotNull List<StagedResolutionFile> convertResolutionsInParallel(
    final @NotNull Path inputPath,
    final @NotNull List<Long> selectedResolutions,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final int workers,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logConsumer
  ) {
    final ExecutorService executor = Executors.newFixedThreadPool(workers);
    final List<Future<StagedResolutionFile>> futures = new ArrayList<>();

    for (final var resolution : selectedResolutions) {
      futures.add(executor.submit(() -> {
        HDF5LibraryInitializer.initializeHDF5Library();
        final var stagedFile = Files.createTempFile("hict-to-mcool-r" + resolution + "-", ".h5");
        try (final var src = HDF5Factory.openForReading(inputPath.toFile());
             final var dst = HDF5Factory.open(stagedFile.toFile())) {
          stageResolution(src, dst, resolution, chunkSize, compression, progressTracker, logConsumer);
          return new StagedResolutionFile(resolution, stagedFile);
        } catch (Exception e) {
          Files.deleteIfExists(stagedFile);
          throw e;
        }
      }));
    }

    final var out = new ArrayList<StagedResolutionFile>();
    try {
      for (final var f : futures) {
        out.add(f.get());
      }
      return out;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    } finally {
      executor.shutdownNow();
    }
  }

  private static void stageResolution(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final long resolution,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logConsumer
  ) {
    final var root = "/resolutions/" + resolution;
    dst.object().createGroup("/resolutions");
    dst.object().createGroup(root);
    dst.object().createGroup(root + "/pixels");

    final var prefix = "Resolution " + resolution;
    for (final var spec : MCOOL_PIXEL_DATASETS) {
      copyLongArrayChunked(
        src,
        dst,
        spec.sourcePath(resolution),
        root + "/" + spec.destinationRelativePath(),
        chunkSize,
        compression,
        progressTracker,
        logConsumer,
        prefix + " " + spec.progressName()
      );
    }
    logConsumer.accept("Staged resolution " + resolution + " in worker=" + Thread.currentThread().getName());
  }

  private static void mergeResolution(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader stagedReader,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull ChunkedFile chunkedFile,
    final long resolution,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logConsumer
  ) {
    final var root = "/resolutions/" + resolution;
    dst.object().createGroup(root);
    dst.object().createGroup(root + "/chroms");
    dst.object().createGroup(root + "/bins");
    dst.object().createGroup(root + "/pixels");
    dst.object().createGroup(root + "/indexes");

    final var prefix = "Merge resolution " + resolution;
    copyLongArrayChunked(stagedReader, dst, root + "/pixels/bin1_id", root + "/pixels/bin1_id", chunkSize, compression, progressTracker, logConsumer, prefix + " pixels/bin1_id");
    copyLongArrayChunked(stagedReader, dst, root + "/pixels/bin2_id", root + "/pixels/bin2_id", chunkSize, compression, progressTracker, logConsumer, prefix + " pixels/bin2_id");
    copyLongArrayToIntChunked(stagedReader, dst, root + "/pixels/counts", root + "/pixels/count", chunkSize, compression, progressTracker, logConsumer, prefix + " pixels/count");

    final int resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
    final long assemblyLengthBp = chunkedFile.getMatrixSizeBins()[0];
    final long binsCount = chunkedFile.getMatrixSizeBins()[resolutionOrder];
    final long nonzeroPixelCount = datasetLength(stagedReader, root + "/pixels/bin1_id");
    final long totalCounts = sumLongArrayChunked(stagedReader, root + "/pixels/counts", chunkSize);

    writeChromsGroup(dst, root + "/chroms", DEFAULT_CHROM_NAME, assemblyLengthBp, compression);
    writeBinsGroup(dst, root + "/bins", resolution, binsCount, assemblyLengthBp, compression, progressTracker, logConsumer, prefix + " bins");
    writeIndexesGroup(stagedReader, dst, root, binsCount, nonzeroPixelCount, chunkSize, compression);
    writeResolutionMetadata(dst, root, resolution, binsCount, nonzeroPixelCount, totalCounts);
  }

  static void copyLongArrayChunked(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String srcPath,
    final @NotNull String dstPath,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logConsumer,
    final @NotNull String progressLabel
  ) {
    if (!src.object().isDataSet(srcPath)) {
      logConsumer.accept("Skipped missing dataset " + srcPath);
      return;
    }

    final var dims = src.object().getDataSetInformation(srcPath).getDimensions();
    if (dims.length != 1) {
      logConsumer.accept("Skipped non-vector dataset " + srcPath + " (rank=" + dims.length + ")");
      return;
    }
    final long length = dims.length == 0 ? 0 : dims[0];
    dst.int64().createArray(dstPath, length, chunkSize, compression);

    long offset = 0L;
    final long startedNanos = System.nanoTime();
    int lastLoggedPercent = -1;
    while (offset < length) {
      final int blockLen = (int) Math.min(chunkSize, length - offset);
      final var block = src.int64().readArrayBlockWithOffset(srcPath, blockLen, offset);
      dst.int64().writeArrayBlockWithOffset(dstPath, block, blockLen, offset);
      offset += blockLen;
      progressTracker.add(blockLen, progressLabel);
      if (length > 0) {
        final int percent = (int) ((offset * 100L) / length);
        if (percent >= 100 || percent - lastLoggedPercent >= 10) {
          lastLoggedPercent = percent;
          final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
          final long etaMillis = estimateEtaMillis(offset, length, elapsedMillis);
          logConsumer.accept(
            String.format(
              "%s: %d%% (%d/%d), elapsed=%s, eta=%s",
              progressLabel,
              percent,
              offset,
              length,
              formatDuration(elapsedMillis),
              formatDuration(etaMillis)
            )
          );
        }
      }
    }
    logConsumer.accept("Copied " + srcPath + " -> " + dstPath + " (" + length + " items)");
  }

  private static void writeResolutionMetadata(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String root,
    final long resolution,
    final long binsCount,
    final long nonzeroPixelCount,
    final long totalCounts
  ) {
    dst.string().setAttrVL(root, "assembly", DEFAULT_CHROM_NAME);
    dst.int64().setAttr(root, "bin-size", resolution);
    dst.string().setAttrVL(root, "bin-type", "fixed");
    dst.int64().setAttr(root, "cis", totalCounts);
    dst.string().setAttrVL(root, "creation-date", OffsetDateTime.now(ZoneOffset.UTC).toString());
    dst.string().setAttrVL(root, "format", "HDF5::Cooler");
    dst.string().setAttrVL(root, "format-url", "https://github.com/open2c/cooler");
    dst.int64().setAttr(root, "format-version", 3L);
    dst.string().setAttrVL(root, "generated-by", "HiCT hict-to-mcool exporter");
    dst.string().setAttrVL(root, "metadata", "{}");
    dst.int64().setAttr(root, "nbins", binsCount);
    dst.int64().setAttr(root, "nchroms", 1L);
    dst.int64().setAttr(root, "nnz", nonzeroPixelCount);
    dst.string().setAttrVL(root, "storage-mode", "symmetric-upper");
    dst.int64().setAttr(root, "sum", totalCounts);
  }

  private static void writeChromsGroup(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String groupPath,
    final @NotNull String chromName,
    final long chromLengthBp,
    final @NotNull HDF5IntStorageFeatures compression
  ) {
    dst.string().writeArray(groupPath + "/name", new String[]{chromName});
    dst.int32().writeArray(groupPath + "/length", new int[]{Math.toIntExact(chromLengthBp)}, compression);
  }

  private static void writeBinsGroup(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String groupPath,
    final long resolution,
    final long binsCount,
    final long assemblyLengthBp,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logConsumer,
    final @NotNull String progressLabel
  ) {
    final int chunkLength = safeChunkLen(binsCount, 8192);
    final int compressionChunkLength = Math.max(1, chunkLength);
    dst.int32().createArray(groupPath + "/chrom", binsCount, compressionChunkLength, compression);
    dst.int32().createArray(groupPath + "/start", binsCount, compressionChunkLength, compression);
    dst.int32().createArray(groupPath + "/end", binsCount, compressionChunkLength, compression);

    long written = 0L;
    int lastLoggedPercent = -1;
    while (written < binsCount) {
      final int blockLen = (int) Math.min(chunkLength, binsCount - written);
      final var chrom = new int[blockLen];
      final var starts = new int[blockLen];
      final var ends = new int[blockLen];
      for (int i = 0; i < blockLen; i++) {
        final long binIndex = written + i;
        final long startBp = binIndex * resolution;
        starts[i] = Math.toIntExact(startBp);
        ends[i] = Math.toIntExact(Math.min(assemblyLengthBp, startBp + resolution));
      }
      dst.int32().writeArrayBlockWithOffset(groupPath + "/chrom", chrom, blockLen, written);
      dst.int32().writeArrayBlockWithOffset(groupPath + "/start", starts, blockLen, written);
      dst.int32().writeArrayBlockWithOffset(groupPath + "/end", ends, blockLen, written);
      written += blockLen;
      progressTracker.add(blockLen * 3L, progressLabel);
      if (binsCount > 0) {
        final int percent = (int) ((written * 100L) / binsCount);
        if (percent >= 100 || percent - lastLoggedPercent >= 10) {
          lastLoggedPercent = percent;
          logConsumer.accept(String.format("%s: %d%% (%d/%d)", progressLabel, percent, written, binsCount));
        }
      }
    }
  }

  static void copyLongArrayToIntChunked(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String srcPath,
    final @NotNull String dstPath,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logConsumer,
    final @NotNull String progressLabel
  ) {
    if (!src.object().isDataSet(srcPath)) {
      logConsumer.accept("Skipped missing dataset " + srcPath);
      return;
    }

    final var dims = src.object().getDataSetInformation(srcPath).getDimensions();
    if (dims.length != 1) {
      logConsumer.accept("Skipped non-vector dataset " + srcPath + " (rank=" + dims.length + ")");
      return;
    }
    final long length = dims.length == 0 ? 0 : dims[0];
    dst.int32().createArray(dstPath, length, chunkSize, compression);

    long offset = 0L;
    final long startedNanos = System.nanoTime();
    int lastLoggedPercent = -1;
    while (offset < length) {
      final int blockLen = (int) Math.min(chunkSize, length - offset);
      final var block = src.int64().readArrayBlockWithOffset(srcPath, blockLen, offset);
      final var intBlock = new int[blockLen];
      for (int i = 0; i < blockLen; i++) {
        intBlock[i] = Math.toIntExact(block[i]);
      }
      dst.int32().writeArrayBlockWithOffset(dstPath, intBlock, blockLen, offset);
      offset += blockLen;
      progressTracker.add(blockLen, progressLabel);
      if (length > 0) {
        final int percent = (int) ((offset * 100L) / length);
        if (percent >= 100 || percent - lastLoggedPercent >= 10) {
          lastLoggedPercent = percent;
          final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
          final long etaMillis = estimateEtaMillis(offset, length, elapsedMillis);
          logConsumer.accept(
            String.format(
              "%s: %d%% (%d/%d), elapsed=%s, eta=%s",
              progressLabel,
              percent,
              offset,
              length,
              formatDuration(elapsedMillis),
              formatDuration(etaMillis)
            )
          );
        }
      }
    }
    logConsumer.accept("Copied " + srcPath + " -> " + dstPath + " (" + length + " items)");
  }

  private static void writeIndexesGroup(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader stagedReader,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String root,
    final long binsCount,
    final long nonzeroPixelCount,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression
  ) {
    final long[] bin1Offset = buildBin1Offset(stagedReader, root + "/pixels/bin1_id", binsCount, nonzeroPixelCount, chunkSize);
    dst.int64().writeArray(root + "/indexes/bin1_offset", bin1Offset, compression);
    dst.int64().writeArray(root + "/indexes/chrom_offset", new long[]{0L, binsCount}, compression);
  }

  private static long @NotNull [] buildBin1Offset(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
    final @NotNull String bin1Path,
    final long binsCount,
    final long nonzeroPixelCount,
    final int chunkSize
  ) {
    final long[] offsets = new long[Math.toIntExact(binsCount + 1L)];
    if (nonzeroPixelCount == 0L) {
      return offsets;
    }

    long globalIndex = 0L;
    long lastBin1 = -1L;
    boolean seenAnyPixel = false;

    while (globalIndex < nonzeroPixelCount) {
      final int blockLen = (int) Math.min(chunkSize, nonzeroPixelCount - globalIndex);
      final var block = src.int64().readArrayBlockWithOffset(bin1Path, blockLen, globalIndex);
      for (int i = 0; i < block.length; i++) {
        final long bin1 = block[i];
        final long pixelIndex = globalIndex + i;
        if (!seenAnyPixel) {
          for (long emptyBin = 1L; emptyBin <= Math.min(bin1, binsCount); emptyBin++) {
            offsets[(int) emptyBin] = 0L;
          }
          lastBin1 = bin1;
          seenAnyPixel = true;
          continue;
        }
        if (bin1 != lastBin1) {
          for (long nextBin = lastBin1 + 1L; nextBin <= Math.min(bin1, binsCount); nextBin++) {
            offsets[(int) nextBin] = pixelIndex;
          }
          lastBin1 = bin1;
        }
      }
      globalIndex += blockLen;
    }

    for (long nextBin = Math.max(0L, lastBin1 + 1L); nextBin <= binsCount; nextBin++) {
      offsets[(int) nextBin] = nonzeroPixelCount;
    }
    return offsets;
  }

  private static long sumLongArrayChunked(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
    final @NotNull String path,
    final int chunkSize
  ) {
    if (!src.object().isDataSet(path)) {
      return 0L;
    }
    final var dims = src.object().getDataSetInformation(path).getDimensions();
    if (dims.length != 1) {
      return 0L;
    }
    final long length = dims[0];
    long offset = 0L;
    long sum = 0L;
    while (offset < length) {
      final int blockLen = (int) Math.min(chunkSize, length - offset);
      final var block = src.int64().readArrayBlockWithOffset(path, blockLen, offset);
      for (final long value : block) {
        sum += value;
      }
      offset += blockLen;
    }
    return sum;
  }

  private static long countVectorItems(
    final @NotNull Path inputPath,
    final @NotNull List<Long> selectedResolutions,
    final @NotNull Consumer<String> logConsumer
  ) {
    try (final var src = HDF5Factory.openForReading(inputPath.toFile())) {
      long total = 0L;
      for (final var resolution : selectedResolutions) {
        for (final var spec : MCOOL_PIXEL_DATASETS) {
          final var path = spec.sourcePath(resolution);
          if (!src.object().isDataSet(path)) {
            continue;
          }
          final var dims = src.object().getDataSetInformation(path).getDimensions();
          if (dims.length == 1) {
            total += dims[0];
          } else {
            logConsumer.accept("Will not export non-vector dataset " + path + " to .mcool (rank=" + dims.length + ")");
          }
        }
      }
      return total;
    }
  }

  private static long countBins(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull List<Long> selectedResolutions
  ) {
    long total = 0L;
    for (final var resolution : selectedResolutions) {
      final Integer resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
      if (resolutionOrder == null) {
        continue;
      }
      total += chunkedFile.getMatrixSizeBins()[resolutionOrder] * 3L;
    }
    return total;
  }

  private static long datasetLength(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
    final @NotNull String path
  ) {
    final var dims = src.object().getDataSetInformation(path).getDimensions();
    return dims.length == 0 ? 0L : dims[0];
  }

  private static int safeChunkLen(final long length, final int preferred) {
    final long base = Math.max(1L, Math.min(Math.max(1L, (long) preferred), Math.max(1L, length)));
    return (int) Math.min(base, Integer.MAX_VALUE);
  }

  private @NotNull List<Long> resolveResolutions(final long @NotNull [] availableResolutions, final @NotNull List<Long> requested) {
    final var available = new ArrayList<Long>();
    for (int i = 1; i < availableResolutions.length; i++) {
      available.add(availableResolutions[i]);
    }
    if (requested == null || requested.isEmpty()) {
      return available;
    }
    return available.stream().filter(requested::contains).toList();
  }

  private record StagedResolutionFile(long resolution, @NotNull Path path) {
  }

  private record DatasetCopySpec(
    @NotNull String progressName,
    @NotNull ResolutionPathFactory sourcePathFactory,
    @NotNull String destinationRelativePath
  ) {
    private @NotNull String sourcePath(final long resolution) {
      return sourcePathFactory.path(resolution);
    }
  }

  @FunctionalInterface
  private interface ResolutionPathFactory {
    @NotNull String path(long resolution);
  }

  private static @NotNull String blockCountPath(final long resolution) {
    return getBlockLengthDatasetPath(resolution);
  }

  private static @NotNull String blockRowsPath(final long resolution) {
    return getBlockRowsDatasetPath(resolution);
  }

  private static @NotNull String blockColsPath(final long resolution) {
    return getBlockColsDatasetPath(resolution);
  }

  private static @NotNull String blockValuesPath(final long resolution) {
    return getBlockValuesDatasetPath(resolution);
  }

  private static final class ExportProgressTracker {
    private final long totalItems;
    private final long startedNanos;
    private final Consumer<String> logConsumer;
    private final AtomicLong copiedItems = new AtomicLong(0L);
    private final AtomicInteger lastLoggedPercent = new AtomicInteger(-1);

    private ExportProgressTracker(
      final long totalItems,
      final long startedNanos,
      final @NotNull Consumer<String> logConsumer
    ) {
      this.totalItems = totalItems;
      this.startedNanos = startedNanos;
      this.logConsumer = logConsumer;
    }

    private void add(final long copied, final @NotNull String detail) {
      if (copied <= 0 || totalItems <= 0) {
        return;
      }
      final long done = Math.min(totalItems, copiedItems.addAndGet(copied));
      final int percent = (int) ((done * 100L) / totalItems);
      int previous;
      do {
        previous = lastLoggedPercent.get();
        if (percent < 100 && percent - previous < 1) {
          return;
        }
      } while (!lastLoggedPercent.compareAndSet(previous, percent));
      logOverall(done, detail);
    }

    private void finish() {
      if (totalItems <= 0) {
        logConsumer.accept("Overall progress: 100% (0/0), elapsed=00:00, eta=00:00");
        return;
      }
      copiedItems.set(totalItems);
      lastLoggedPercent.set(100);
      logOverall(totalItems, "Finished .mcool export");
    }

    private void logOverall(final long done, final @NotNull String detail) {
      final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
      final long etaMillis = estimateEtaMillis(done, totalItems, elapsedMillis);
      logConsumer.accept(
        String.format(
          "Overall progress: %d%% (%d/%d), elapsed=%s, eta=%s - %s",
          (int) ((done * 100L) / totalItems),
          done,
          totalItems,
          formatDuration(elapsedMillis),
          formatDuration(etaMillis),
          detail
        )
      );
    }
  }

  private static @NotNull HDF5IntStorageFeatures resolveIntStorageFeatures(final @NotNull ConversionOptions options, final @NotNull Consumer<String> logConsumer) {
    if (options.compressionLevel() <= 0) {
      return HDF5IntStorageFeatures.INT_CHUNKED;
    }
    return switch (options.compressionAlgorithm()) {
      case DEFLATE -> HDF5IntStorageFeatures.createDeflation(options.compressionLevel());
      case ZSTD, LZF -> {
        logConsumer.accept(
          "Compression algorithm " + options.compressionAlgorithm() +
            " requested, but current JHDF5 high-level writer path supports deflate features only. Falling back to Deflate."
        );
        yield HDF5IntStorageFeatures.createDeflation(options.compressionLevel());
      }
    };
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
    if (hours > 0) {
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

  private static int resolveRequestedWorkers(final int parallelismOption) {
    if (parallelismOption == -1 || parallelismOption <= 0) {
      return Math.max(1, Runtime.getRuntime().availableProcessors());
    }
    return parallelismOption;
  }
}
