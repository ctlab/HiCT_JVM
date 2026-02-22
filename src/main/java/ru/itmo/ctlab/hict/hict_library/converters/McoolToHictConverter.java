package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5FloatStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.StripeDescriptor;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBasisATUDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockColsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockLengthDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockOffsetDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockRowsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockValuesDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigDirectionDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigHideTypeDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigLengthBinsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigLengthBpDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigNameDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigOrderDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigsATLDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getDenseBlockDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeBinWeightsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeLengthsBinsDatasetPath;

public class McoolToHictConverter {
  private static final int SUBMATRIX_SIZE = 256;
  private static final long HDF5_MAX_CHUNK_SIZE = 32L * 1024L * 1024L * 8L;
  private static final int DENSE_THRESHOLD = (SUBMATRIX_SIZE * SUBMATRIX_SIZE) / 2;

  public void convert(final @NotNull ConversionOptions options, final @NotNull Consumer<String> logConsumer) {
    final var synchronizedLogConsumer = synchronizedLogger(logConsumer);
    try (final var src = HDF5Factory.openForReading(options.inputPath().toFile())) {
      final var selectedResolutions = resolveResolutions(src, options.resolutions());
      if (selectedResolutions.isEmpty()) {
        throw new IllegalArgumentException("No numeric resolutions found in input file");
      }

      final var conversionOrder = selectedResolutions.stream().sorted(Comparator.reverseOrder()).toList();
      final var intStorageFeatures = resolveIntStorageFeatures(options, synchronizedLogConsumer);
      final var floatStorageFeatures = resolveFloatStorageFeatures(options, synchronizedLogConsumer);
      final var progressTracker = new ConversionProgressTracker((conversionOrder.size() * 2) + 1, synchronizedLogConsumer);

      final var requestedWorkers = resolveRequestedWorkers(options.parallelism());
      synchronizedLogConsumer.accept(
        "Converting .mcool -> .hict.hdf5, workers=" + requestedWorkers + ", resolutions=" + conversionOrder +
          ", compressionAlgorithm=" + options.compressionAlgorithm() + ", compressionLevel=" + options.compressionLevel()
      );

      Files.deleteIfExists(options.outputPath());
      try (final var dst = HDF5Factory.open(options.outputPath().toFile());
           final var srcAgain = HDF5Factory.openForReading(options.inputPath().toFile())) {
        dst.object().createGroup("/resolutions");
        dst.string().setAttr("/resolutions", "hict_version", "0.1.3.1a");

        dumpContigData(srcAgain, dst, selectedResolutions, requestedWorkers, intStorageFeatures, floatStorageFeatures);
        progressTracker.markStep("Dumped contig metadata");

        for (final var resolution : conversionOrder) {
          writeResolutionDirect(
            srcAgain,
            dst,
            resolution,
            options.chunkSize(),
            intStorageFeatures,
            floatStorageFeatures,
            requestedWorkers,
            synchronizedLogConsumer
          );
          progressTracker.markStep("Wrote resolution " + resolution);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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
            " requested, but current JHDF5 high-level writer path supports deflate features only. Falling back to uncompressed chunked datasets."
        );
        yield HDF5IntStorageFeatures.INT_CHUNKED;
      }
    };
  }

  private static @NotNull HDF5FloatStorageFeatures resolveFloatStorageFeatures(final @NotNull ConversionOptions options, final @NotNull Consumer<String> logConsumer) {
    if (options.compressionLevel() <= 0) {
      return HDF5FloatStorageFeatures.FLOAT_CHUNKED;
    }
    return switch (options.compressionAlgorithm()) {
      case DEFLATE -> HDF5FloatStorageFeatures.createDeflation(options.compressionLevel());
      case ZSTD, LZF -> {
        logConsumer.accept(
          "Compression algorithm " + options.compressionAlgorithm() +
            " requested, but current JHDF5 high-level writer path supports deflate features only. Falling back to uncompressed chunked datasets."
        );
        yield HDF5FloatStorageFeatures.FLOAT_CHUNKED;
      }
    };
  }

  private static void writeResolutionDirect(
    final @NotNull IHDF5Reader src,
    final @NotNull IHDF5Writer dst,
    final long resolution,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures intStorageFeatures,
    final @NotNull HDF5FloatStorageFeatures floatStorageFeatures,
    final int stripeWorkersRequested,
    final @NotNull Consumer<String> logConsumer
  ) {
    final long startedNanos = System.nanoTime();
    final var resolutionRoot = "/resolutions/" + resolution;
    if (!dst.object().isGroup(resolutionRoot)) {
      dst.object().createGroup(resolutionRoot);
    }

    final var nameLengthPath = resolveNameLengthPath(src, resolution);
    final var stripes = dumpStripeData(src, dst, resolution, nameLengthPath, floatStorageFeatures);

    final var treapRoot = resolutionRoot + "/treap_coo";
    dst.object().createGroup(treapRoot);
    dst.int64().setAttr(treapRoot, "dense_submatrix_size", SUBMATRIX_SIZE);
    dst.int64().setAttr(treapRoot, "hdf5_max_chunk_size", HDF5_MAX_CHUNK_SIZE);

    final var binsCount = datasetLength(src, resolutionRoot + "/bins/end");
    final var nonzeroPixelCount = datasetLength(src, resolutionRoot + "/pixels/bin1_id");
    final var stripeCount = stripes.size();
    dst.int64().setAttr(treapRoot, "bins_count", binsCount);
    dst.int64().setAttr(treapRoot, "stripes_count", stripeCount);

    final var allRowsStartIndices = src.int64().readArray(resolutionRoot + "/indexes/bin1_offset");

    logConsumer.accept("Resolution " + resolution + ": counting sparse and dense blocks");
    final var countingProgress = new PhaseProgressTracker(
      "Resolution " + resolution + " count",
      stripeCount,
      logConsumer
    );
    final int stripeWorkers = Math.max(1, Math.min(stripeWorkersRequested, Math.max(1, stripeCount)));
    final var counts = countDenseAndSparse(src, resolution, stripeCount, allRowsStartIndices, stripeWorkers, countingProgress::report);
    countingProgress.finish();
    final var denseBlockCount = counts.denseBlockCount();
    logConsumer.accept("Resolution " + resolution + ": finished counting blocks, denseBlocks=" + denseBlockCount);

    final var blockRowsPath = getBlockRowsDatasetPath(resolution);
    final var blockColsPath = getBlockColsDatasetPath(resolution);
    final var blockValsPath = getBlockValuesDatasetPath(resolution);
    final var blockOffsetPath = getBlockOffsetDatasetPath(resolution);
    final var blockLengthPath = getBlockLengthDatasetPath(resolution);
    final var denseBlocksPath = getDenseBlockDatasetPath(resolution);

    dst.int64().createArray(blockRowsPath, nonzeroPixelCount, safeChunkLen(nonzeroPixelCount, chunkSize), intStorageFeatures);
    dst.int64().createArray(blockColsPath, nonzeroPixelCount, safeChunkLen(nonzeroPixelCount, chunkSize), intStorageFeatures);
    dst.int64().createArray(blockValsPath, nonzeroPixelCount, safeChunkLen(nonzeroPixelCount, chunkSize), intStorageFeatures);

    final var totalBlockCount = (long) stripeCount * stripeCount;
    dst.int64().createArray(blockOffsetPath, totalBlockCount, safeChunkLen(totalBlockCount, chunkSize), intStorageFeatures);
    dst.int64().createArray(blockLengthPath, totalBlockCount, safeChunkLen(totalBlockCount, chunkSize), intStorageFeatures);

    final var denseDatasetSize = Math.max(1L, denseBlockCount);
    dst.int64().createMDArray(
      denseBlocksPath,
      new long[]{denseDatasetSize, 1L, SUBMATRIX_SIZE, SUBMATRIX_SIZE},
      new int[]{1, 1, SUBMATRIX_SIZE, SUBMATRIX_SIZE},
      intStorageFeatures
    );

    long currentSparseOffset = 0L;
    long currentDenseOffset = 0L;
    final var writeProgress = new PhaseProgressTracker(
      "Resolution " + resolution + " write",
      stripeCount,
      logConsumer
    );
    if (stripeCount == 0) {
      writeProgress.finish();
      return;
    }

    final var sortedStripes = new AtomicReferenceArray<SortedStripePixels>(stripeCount);
    final var errorRef = new AtomicReference<Throwable>();
    final Object lock = new Object();
    final ExecutorService stripeExecutor = Executors.newFixedThreadPool(stripeWorkers);
    final Object readLock = new Object();
    final List<Future<?>> futures = new ArrayList<>(stripeCount);

    for (int rowStripe = 0; rowStripe < stripeCount; rowStripe++) {
      final int stripeIdx = rowStripe;
      futures.add(stripeExecutor.submit(() -> {
        try {
          final PixelBlock block;
          synchronized (readLock) {
            block = readRowStripePixels(src, resolution, stripeIdx, allRowsStartIndices);
          }
          final SortedStripePixels sorted = block.length() > 0
            ? sortStripePixels(block.rows(), block.cols(), block.values())
            : EMPTY_STRIPE;
          sortedStripes.set(stripeIdx, sorted);
        } catch (Throwable t) {
          errorRef.compareAndSet(null, t);
        } finally {
          synchronized (lock) {
            lock.notifyAll();
          }
        }
      }));
    }

    try {
      for (int rowStripe = 0; rowStripe < stripeCount; rowStripe++) {
        SortedStripePixels sorted = sortedStripes.get(rowStripe);
        while (sorted == null) {
          if (errorRef.get() != null) {
            throw new RuntimeException(errorRef.get());
          }
          synchronized (lock) {
            try {
              lock.wait(50L);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          }
          sorted = sortedStripes.get(rowStripe);
        }

        if (sorted != EMPTY_STRIPE) {
          final var saveResult = saveIndirectBlock(
            dst,
            rowStripe,
            stripeCount,
            sorted,
            currentSparseOffset,
            currentDenseOffset,
            blockRowsPath,
            blockColsPath,
            blockValsPath,
            blockOffsetPath,
            blockLengthPath,
            denseBlocksPath
          );
          currentSparseOffset = saveResult.sparseOffset();
          currentDenseOffset = saveResult.denseOffset();
        }
        sortedStripes.set(rowStripe, null);
        writeProgress.report(rowStripe + 1);

        final int percent = (int) ((rowStripe + 1L) * 100L / stripeCount);
        final var elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
        final var etaMillis = estimateEtaMillis(rowStripe + 1L, stripeCount, elapsedMillis);
        logConsumer.accept(
          String.format(
            "Resolution %d write: %d%% (%d/%d stripes), elapsed=%s, eta=%s",
            resolution,
            percent,
            rowStripe + 1,
            stripeCount,
            formatDuration(elapsedMillis),
            formatDuration(etaMillis)
          )
        );
      }
    } finally {
      stripeExecutor.shutdown();
    }

    try {
      for (final var f : futures) {
        f.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
    writeProgress.finish();
  }

  private static @NotNull Consumer<String> synchronizedLogger(final @NotNull Consumer<String> delegate) {
    final Object lock = new Object();
    return msg -> {
      synchronized (lock) {
        delegate.accept(msg);
      }
    };
  }

  private static SaveBlockResult saveIndirectBlock(
    final @NotNull IHDF5Writer dst,
    final int rowStripe,
    final int stripeCount,
    final @NotNull SortedStripePixels sorted,
    final long currentSparseOffset,
    final long currentDenseOffset,
    final @NotNull String blockRowsPath,
    final @NotNull String blockColsPath,
    final @NotNull String blockValsPath,
    final @NotNull String blockOffsetPath,
    final @NotNull String blockLengthPath,
    final @NotNull String denseBlocksPath
  ) {
    long sparseOffset = currentSparseOffset;
    long denseOffset = currentDenseOffset;

    final var colStripes = sorted.colStripes();
    final var intraRows = sorted.intraRows();
    final var intraCols = sorted.intraCols();
    final var values = sorted.values();

    int start = 0;
    while (start < colStripes.length) {
      int end = start + 1;
      final long colStripe = colStripes[start];
      while (end < colStripes.length && colStripes[end] == colStripe) {
        end++;
      }

      final int blockLen = end - start;
      if (blockLen > 0) {
        final long blockIndex = (long) rowStripe * stripeCount + colStripe;

        if (blockLen >= DENSE_THRESHOLD) {
          dst.int64().writeArrayBlockWithOffset(blockOffsetPath, new long[]{-denseOffset - 1L}, 1, blockIndex);
          dst.int64().writeArrayBlockWithOffset(blockLengthPath, new long[]{blockLen}, 1, blockIndex);

          final var denseFlat = new long[SUBMATRIX_SIZE * SUBMATRIX_SIZE];
          for (int i = start; i < end; i++) {
            final int r = intraRows[i];
            final int c = intraCols[i];
            denseFlat[r * SUBMATRIX_SIZE + c] += values[i];
          }

          final var denseMd = new MDLongArray(denseFlat, new int[]{1, 1, SUBMATRIX_SIZE, SUBMATRIX_SIZE});
          dst.int64().writeMDArrayBlockWithOffset(denseBlocksPath, denseMd, new long[]{denseOffset, 0L, 0L, 0L});
          denseOffset++;
        } else {
          dst.int64().writeArrayBlockWithOffset(blockOffsetPath, new long[]{sparseOffset}, 1, blockIndex);
          dst.int64().writeArrayBlockWithOffset(blockLengthPath, new long[]{blockLen}, 1, blockIndex);

          final var blockRows = new long[blockLen];
          final var blockCols = new long[blockLen];
          final var blockVals = new long[blockLen];
          for (int i = 0; i < blockLen; i++) {
            blockRows[i] = intraRows[start + i];
            blockCols[i] = intraCols[start + i];
            blockVals[i] = values[start + i];
          }

          dst.int64().writeArrayBlockWithOffset(blockRowsPath, blockRows, blockLen, sparseOffset);
          dst.int64().writeArrayBlockWithOffset(blockColsPath, blockCols, blockLen, sparseOffset);
          dst.int64().writeArrayBlockWithOffset(blockValsPath, blockVals, blockLen, sparseOffset);

          sparseOffset += blockLen;
        }
      }

      start = end;
    }

    return new SaveBlockResult(sparseOffset, denseOffset);
  }

  private static @NotNull StripeCounts countDenseAndSparse(
    final @NotNull IHDF5Reader src,
    final long resolution,
    final int stripeCount,
    final long @NotNull [] allRowsStartIndices,
    final int stripeWorkers,
    final @NotNull java.util.function.IntConsumer countingProgressReporter
  ) {
    long sparseCount = 0L;
    long denseCount = 0L;

    final int batchSize = Math.max(1, stripeWorkers * 2);
    final ExecutorService stripeExecutor = stripeWorkers > 1 ? Executors.newFixedThreadPool(stripeWorkers) : null;
    try {
      for (int batchStart = 0; batchStart < stripeCount; batchStart += batchSize) {
        final int batchEnd = Math.min(stripeCount, batchStart + batchSize);
        final int localCount = batchEnd - batchStart;
        final var blocks = new PixelBlock[localCount];
        for (int i = 0; i < localCount; i++) {
          blocks[i] = readRowStripePixels(src, resolution, batchStart + i, allRowsStartIndices);
        }
        final var sortedBatch = sortStripeBatch(blocks, stripeExecutor);

        for (int i = 0; i < localCount; i++) {
          final int rowStripe = batchStart + i;
          final var sorted = sortedBatch[i];
          if (sorted != null) {
            final var colStripes = sorted.colStripes();
            int start = 0;
            while (start < colStripes.length) {
              int end = start + 1;
              while (end < colStripes.length && colStripes[end] == colStripes[start]) {
                end++;
              }
              final int blockLen = end - start;
              if (blockLen >= DENSE_THRESHOLD) {
                denseCount++;
              } else {
                sparseCount += blockLen;
              }
              start = end;
            }
          }
          countingProgressReporter.accept(rowStripe + 1);
        }
      }
    } finally {
      if (stripeExecutor != null) {
        stripeExecutor.shutdownNow();
      }
    }

    return new StripeCounts(sparseCount, denseCount);
  }

  private static @NotNull SortedStripePixels[] sortStripeBatch(
    final PixelBlock @NotNull [] blocks,
    final ExecutorService stripeExecutor
  ) {
    final var sortedBatch = new SortedStripePixels[blocks.length];
    if (stripeExecutor == null) {
      for (int i = 0; i < blocks.length; i++) {
        final var block = blocks[i];
        if (block.length() > 0) {
          sortedBatch[i] = sortStripePixels(block.rows(), block.cols(), block.values());
        }
      }
      return sortedBatch;
    }

    final List<Future<?>> futures = new ArrayList<>(blocks.length);
    for (int i = 0; i < blocks.length; i++) {
      final int idx = i;
      futures.add(stripeExecutor.submit(() -> {
        final var block = blocks[idx];
        if (block.length() > 0) {
          sortedBatch[idx] = sortStripePixels(block.rows(), block.cols(), block.values());
        }
      }));
    }

    try {
      for (final var f : futures) {
        f.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
    return sortedBatch;
  }

  private static long estimateEtaMillis(final long done, final long total, final long elapsedMillis) {
    if (done <= 0 || total <= 0 || done >= total || elapsedMillis <= 0) {
      return 0L;
    }
    final var remaining = total - done;
    return (elapsedMillis * remaining) / done;
  }

  private static int resolveRequestedWorkers(final int parallelismOption) {
    if (parallelismOption == -1 || parallelismOption <= 0) {
      return Math.max(1, Runtime.getRuntime().availableProcessors());
    }
    return parallelismOption;
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

  private static @NotNull PixelBlock readRowStripePixels(
    final @NotNull IHDF5Reader src,
    final long resolution,
    final int rowStripe,
    final long @NotNull [] allRowsStartIndices
  ) {
    final long startOffset = allRowsStartIndices[rowStripe * SUBMATRIX_SIZE];
    final int nextBin = Math.min((rowStripe + 1) * SUBMATRIX_SIZE, allRowsStartIndices.length - 1);
    final long endOffset = allRowsStartIndices[nextBin];
    final int length = (int) (endOffset - startOffset);

    if (length <= 0) {
      return new PixelBlock(new long[0], new long[0], new long[0]);
    }

    final var base = "/resolutions/" + resolution + "/pixels/";
    final var rows = src.int64().readArrayBlockWithOffset(base + "bin1_id", length, startOffset);
    final var cols = src.int64().readArrayBlockWithOffset(base + "bin2_id", length, startOffset);
    final var vals = src.int64().readArrayBlockWithOffset(base + "count", length, startOffset);
    return new PixelBlock(rows, cols, vals);
  }

  private static @NotNull SortedStripePixels sortStripePixels(
    final long @NotNull [] rows,
    final long @NotNull [] cols,
    final long @NotNull [] values
  ) {
    final int n = rows.length;
    final var order = new Integer[n];
    for (int i = 0; i < n; i++) {
      order[i] = i;
    }

    Arrays.sort(order, Comparator
      .comparingLong((Integer i) -> cols[i] / SUBMATRIX_SIZE)
      .thenComparingLong(i -> rows[i] / SUBMATRIX_SIZE)
      .thenComparingLong(i -> rows[i] % SUBMATRIX_SIZE)
      .thenComparingLong(i -> cols[i] % SUBMATRIX_SIZE)
    );

    final var sortedColStripes = new long[n];
    final var sortedIntraRows = new int[n];
    final var sortedIntraCols = new int[n];
    final var sortedValues = new long[n];

    for (int i = 0; i < n; i++) {
      final int srcIdx = order[i];
      sortedColStripes[i] = cols[srcIdx] / SUBMATRIX_SIZE;
      sortedIntraRows[i] = (int) (rows[srcIdx] % SUBMATRIX_SIZE);
      sortedIntraCols[i] = (int) (cols[srcIdx] % SUBMATRIX_SIZE);
      sortedValues[i] = values[srcIdx];
    }

    return new SortedStripePixels(sortedColStripes, sortedIntraRows, sortedIntraCols, sortedValues);
  }

  private static @NotNull List<StripeDescriptor> dumpStripeData(
    final @NotNull IHDF5Reader src,
    final @NotNull IHDF5Writer dst,
    final long resolution,
    final @NotNull String nameLengthPath,
    final @NotNull HDF5FloatStorageFeatures floatStorageFeatures
  ) {
    final var chromLengthPath = nameLengthPath + "/length";
    final var chromNamePath = nameLengthPath + "/name";

    final long[] chromLengths = src.int64().readArray(chromLengthPath);
    final String[] chromNames = src.string().readArray(chromNamePath);
    if (chromLengths.length != chromNames.length) {
      throw new IllegalStateException("Chromosome lengths and names have different sizes at " + nameLengthPath);
    }

    final var stripesRoot = "/resolutions/" + resolution + "/stripes";
    dst.object().createGroup(stripesRoot);

    final var binsRoot = "/resolutions/" + resolution + "/bins";
    final long binCount = datasetLength(src, binsRoot + "/chrom");
    final boolean hasWeights = src.object().isDataSet(binsRoot + "/weight");

    final int stripeCount = (int) ((binCount / SUBMATRIX_SIZE) + Math.min(binCount % SUBMATRIX_SIZE, 1L));
    final List<StripeDescriptor> stripes = new ArrayList<>(stripeCount);
    final var stripeLengths = new long[stripeCount];
    final var stripeWeights = new double[stripeCount][SUBMATRIX_SIZE];

    for (int stripeId = 0; stripeId < stripeCount; stripeId++) {
      final long start = (long) stripeId * SUBMATRIX_SIZE;
      final long end = Math.min((long) (stripeId + 1) * SUBMATRIX_SIZE, binCount);
      final int stripeLen = (int) (end - start);

      final double[] weights;
      if (hasWeights) {
        weights = src.float64().readArrayBlockWithOffset(binsRoot + "/weight", stripeLen, start);
      } else {
        weights = new double[SUBMATRIX_SIZE];
        Arrays.fill(weights, 1.0d);
      }

      final var padded = new double[SUBMATRIX_SIZE];
      Arrays.fill(padded, 1.0d);
      System.arraycopy(weights, 0, padded, 0, Math.min(weights.length, SUBMATRIX_SIZE));

      stripeLengths[stripeId] = stripeLen;
      stripeWeights[stripeId] = padded;
      stripes.add(new StripeDescriptor(stripeId, stripeLen, weights));
    }

    dst.int64().writeArray(getStripeLengthsBinsDatasetPath(resolution), stripeLengths, HDF5IntStorageFeatures.INT_CHUNKED);
    dst.float64().writeMatrix(getStripeBinWeightsDatasetPath(resolution), stripeWeights, floatStorageFeatures);

    return stripes;
  }

  private static void dumpContigData(
    final @NotNull IHDF5Reader src,
    final @NotNull IHDF5Writer dst,
    final @NotNull List<Long> resolutions,
    final int parallelism,
    final @NotNull HDF5IntStorageFeatures intStorageFeatures,
    final @NotNull HDF5FloatStorageFeatures floatStorageFeatures
  ) {
    final long anyResolution = resolutions.get(0);
    final String nameLengthPath = resolveNameLengthPath(src, anyResolution);

    dst.object().createGroup("/contig_info");
    final String[] contigNames = src.string().readArray(nameLengthPath + "/name");
    final int contigCount = contigNames.length;
    dst.string().writeArray(getContigNameDatasetPath(), contigNames);

    final long[] contigDirections = new long[contigCount];
    Arrays.fill(contigDirections, ContigDirection.FORWARD.ordinal());
    dst.int64().writeArray(getContigDirectionDatasetPath(), contigDirections, intStorageFeatures);

    final long[] orderedContigIds = new long[contigCount];
    final long[] contigScaffoldIds = new long[contigCount];
    Arrays.fill(contigScaffoldIds, -1L);
    for (int i = 0; i < contigCount; i++) {
      orderedContigIds[i] = i;
    }
    dst.int64().writeArray(getContigOrderDatasetPath(), orderedContigIds, intStorageFeatures);
    dst.int64().writeArray("/contig_info/contig_scaffold_id", contigScaffoldIds, intStorageFeatures);

    final long[] contigLengthBp;
    if (src.object().isDataSet(nameLengthPath + "/length")) {
      contigLengthBp = src.int64().readArray(nameLengthPath + "/length");
    } else {
      final var chromOffsets = src.int64().readArray("/resolutions/" + anyResolution + "/indexes/chrom_offset");
      final var binEnds = src.int64().readArray("/resolutions/" + anyResolution + "/bins/end");
      contigLengthBp = new long[contigCount];
      for (int i = 0; i < contigCount - 1; i++) {
        contigLengthBp[i] = binEnds[(int) (chromOffsets[i + 1] - 1L)];
      }
      contigLengthBp[contigCount - 1] = binEnds[binEnds.length - 1];
    }
    dst.int64().writeArray(getContigLengthBpDatasetPath(), contigLengthBp, intStorageFeatures);

    final Map<Long, long[]> contigStartBinsByResolution = new HashMap<>();
    final Map<Long, long[]> contigLengthBinsByResolution = new HashMap<>();
    final Map<Long, List<StripeDescriptor>> stripesByResolution = new HashMap<>();

    for (final var resolution : resolutions) {
      final var chromOffsets = src.int64().readArray("/resolutions/" + resolution + "/indexes/chrom_offset");
      final var lengthBins = new long[chromOffsets.length - 1];
      for (int i = 0; i < lengthBins.length; i++) {
        lengthBins[i] = chromOffsets[i + 1] - chromOffsets[i];
        if (lengthBins[i] <= 0) {
          throw new IllegalStateException("Zero-length contig found at resolution " + resolution + " contig=" + i);
        }
      }

      contigStartBinsByResolution.put(resolution, chromOffsets);
      contigLengthBinsByResolution.put(resolution, lengthBins);
      final var path = resolveNameLengthPath(src, resolution);
      stripesByResolution.put(resolution, buildStripeDescriptorsOnly(src, resolution, path));
    }

    for (final var resolution : resolutions) {
      final var resolutionRoot = "/resolutions/" + resolution;
      dst.object().createGroup(resolutionRoot + "/contigs");
      dst.object().createGroup(resolutionRoot + "/atl");

      final var contigLengthBins = contigLengthBinsByResolution.get(resolution);
      final var hideTypes = new byte[contigCount];
      for (int i = 0; i < contigCount; i++) {
        hideTypes[i] = (byte) ((contigLengthBins[i] > 1L) ? ContigHideType.SHOWN.ordinal() : ContigHideType.HIDDEN.ordinal());
      }

      dst.int64().writeArray(getContigLengthBinsDatasetPath(resolution), contigLengthBins, intStorageFeatures);
      dst.int8().writeArray(getContigHideTypeDatasetPath(resolution), hideTypes);

      final AtomicReferenceArray<List<ATUDescriptor>> atusByContig = new AtomicReferenceArray<>(contigCount);
      runParallelFor(parallelism, contigCount, contigId -> {
        final var atus = generateAtusForContig(
          contigId,
          resolution,
          contigStartBinsByResolution,
          contigLengthBinsByResolution,
          stripesByResolution
        );
        atusByContig.set(contigId, atus);
      });

      long totalAtuCount = 0L;
      for (int i = 0; i < contigCount; i++) {
        totalAtuCount += Objects.requireNonNull(atusByContig.get(i)).size();
      }

      final long[][] basisAtu = new long[(int) totalAtuCount][4];
      final long[][] contigsAtl = new long[(int) totalAtuCount][2];

      int atuCursor = 0;
      for (int contigId = 0; contigId < contigCount; contigId++) {
        final var atus = atusByContig.get(contigId);
        for (int i = 0; i < atus.size(); i++) {
          final var atu = atus.get(i);
          contigsAtl[atuCursor][0] = contigId;
          contigsAtl[atuCursor][1] = atuCursor;

          basisAtu[atuCursor][0] = atu.getStripeDescriptor().stripeId();
          basisAtu[atuCursor][1] = atu.getStartIndexInStripeIncl();
          basisAtu[atuCursor][2] = atu.getEndIndexInStripeExcl();
          basisAtu[atuCursor][3] = atu.getDirection().ordinal();

          atuCursor++;
        }
      }

      dst.int64().writeMatrix(getContigsATLDatasetPath(resolution), contigsAtl);
      dst.int64().writeMatrix(getBasisATUDatasetPath(resolution), basisAtu);
    }
  }

  private static @NotNull List<ATUDescriptor> generateAtusForContig(
    final int contigId,
    final long resolution,
    final @NotNull Map<Long, long[]> contigStartBinsByResolution,
    final @NotNull Map<Long, long[]> contigLengthBinsByResolution,
    final @NotNull Map<Long, List<StripeDescriptor>> stripesByResolution
  ) {
    long startBin = contigStartBinsByResolution.get(resolution)[contigId];
    final long endBin = startBin + contigLengthBinsByResolution.get(resolution)[contigId];
    final long startStripeId = startBin / SUBMATRIX_SIZE;

    final var stripes = stripesByResolution.get(resolution);
    final var atus = new ArrayList<ATUDescriptor>();

    atus.add(new ATUDescriptor(
      stripes.get((int) startStripeId),
      (int) (startBin % SUBMATRIX_SIZE),
      (int) (((startBin / SUBMATRIX_SIZE) < (endBin / SUBMATRIX_SIZE)) ? SUBMATRIX_SIZE : (1 + ((endBin - 1L) % SUBMATRIX_SIZE))),
      ATUDirection.FORWARD
    ));

    startBin = ((startBin / SUBMATRIX_SIZE) + 1L) * SUBMATRIX_SIZE;
    final long equalPartsCount = (endBin - startBin) / 256L;

    for (int part = 0; part < equalPartsCount; part++) {
      atus.add(new ATUDescriptor(
        stripes.get((int) (startStripeId + part + 1L)),
        0,
        SUBMATRIX_SIZE,
        ATUDirection.FORWARD
      ));
    }

    startBin += (long) (atus.size() - 1) * SUBMATRIX_SIZE;

    if (startBin < endBin) {
      atus.add(new ATUDescriptor(
        stripes.get((int) (startStripeId + 1L + equalPartsCount)),
        0,
        (int) (1L + ((endBin - 1L) % SUBMATRIX_SIZE)),
        ATUDirection.FORWARD
      ));
    }

    return atus;
  }

  private static @NotNull List<StripeDescriptor> buildStripeDescriptorsOnly(
    final @NotNull IHDF5Reader src,
    final long resolution,
    final @NotNull String nameLengthPath
  ) {
    final var chromLengthPath = nameLengthPath + "/length";
    final var chromNamePath = nameLengthPath + "/name";

    final long[] chromLengths = src.int64().readArray(chromLengthPath);
    final String[] chromNames = src.string().readArray(chromNamePath);
    if (chromLengths.length != chromNames.length) {
      throw new IllegalStateException("Chromosome lengths and names have different sizes at " + nameLengthPath);
    }

    final var binsRoot = "/resolutions/" + resolution + "/bins";
    final long binCount = datasetLength(src, binsRoot + "/chrom");
    final boolean hasWeights = src.object().isDataSet(binsRoot + "/weight");

    final int stripeCount = (int) ((binCount / SUBMATRIX_SIZE) + Math.min(binCount % SUBMATRIX_SIZE, 1L));
    final List<StripeDescriptor> stripes = new ArrayList<>(stripeCount);

    for (int stripeId = 0; stripeId < stripeCount; stripeId++) {
      final long start = (long) stripeId * SUBMATRIX_SIZE;
      final long end = Math.min((long) (stripeId + 1) * SUBMATRIX_SIZE, binCount);
      final int stripeLen = (int) (end - start);

      final double[] weights;
      if (hasWeights) {
        weights = src.float64().readArrayBlockWithOffset(binsRoot + "/weight", stripeLen, start);
      } else {
        weights = new double[SUBMATRIX_SIZE];
        Arrays.fill(weights, 1.0d);
      }

      stripes.add(new StripeDescriptor(stripeId, stripeLen, weights));
    }

    return stripes;
  }

  private static void runParallelFor(final int parallelism, final int itemCount, final @NotNull java.util.function.IntConsumer task) {
    if (parallelism <= 1 || itemCount <= 1) {
      for (int i = 0; i < itemCount; i++) {
        task.accept(i);
      }
      return;
    }

    final var pool = Executors.newFixedThreadPool(parallelism);
    try {
      final List<Future<?>> futures = new ArrayList<>(itemCount);
      for (int i = 0; i < itemCount; i++) {
        final int idx = i;
        futures.add(pool.submit(() -> task.accept(idx)));
      }
      for (final var f : futures) {
        f.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    } finally {
      pool.shutdownNow();
    }
  }

  private static @NotNull String resolveNameLengthPath(final @NotNull IHDF5Reader src, final long resolution) {
    final var perResolution = "/resolutions/" + resolution + "/chroms";
    if (src.object().isGroup(perResolution) && src.object().isDataSet(perResolution + "/name") && src.object().isDataSet(perResolution + "/length")) {
      return perResolution;
    }

    final var global = "/chroms";
    if (src.object().isGroup(global) && src.object().isDataSet(global + "/name") && src.object().isDataSet(global + "/length")) {
      return global;
    }

    throw new IllegalStateException("Cannot resolve chromosome name/length path for resolution " + resolution);
  }

  private static long datasetLength(final @NotNull IHDF5Reader src, final @NotNull String path) {
    final var dims = src.object().getDataSetInformation(path).getDimensions();
    return dims.length == 0 ? 0L : dims[0];
  }

  private static int safeChunkLen(final long length, final int preferred) {
    final long base = Math.max(1L, Math.min(Math.max(1L, (long) preferred), Math.max(1L, length)));
    return (int) Math.min(base, Integer.MAX_VALUE);
  }

  private @NotNull List<Long> resolveResolutions(final @NotNull IHDF5Reader src, final @NotNull List<Long> requested) {
    final var available = src.object().getAllGroupMembers("/resolutions").stream().map(s -> {
      try {
        return Long.parseLong(s);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }).filter(Objects::nonNull).toList();

    if (requested == null || requested.isEmpty()) {
      return available;
    }

    final var availableSet = new java.util.HashSet<>(available);
    return requested.stream().filter(availableSet::contains).toList();
  }

  @FunctionalInterface
  private interface StripeProgressReporter {
    void report(int processedStripes, int stripeCount);
  }

  private static final class ConversionProgressTracker {
    private final int totalSteps;
    private final @NotNull Consumer<String> logger;
    private final @NotNull AtomicInteger completedSteps = new AtomicInteger(0);
    private final long startedNanos = System.nanoTime();

    private ConversionProgressTracker(final int totalSteps, final @NotNull Consumer<String> logger) {
      this.totalSteps = Math.max(1, totalSteps);
      this.logger = logger;
    }

    private void markStep(final @NotNull String stepDescription) {
      final int done = completedSteps.incrementAndGet();
      final int percent = (int) ((done * 100L) / totalSteps);
      final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
      final long etaMillis = estimateEtaMillis(done, totalSteps, elapsedMillis);
      logger.accept(
        String.format(
          "Overall progress: %d%% (%d/%d), elapsed=%s, eta=%s - %s",
          percent,
          done,
          totalSteps,
          formatDuration(elapsedMillis),
          formatDuration(etaMillis),
          stepDescription
        )
      );
    }
  }

  private static final class PhaseProgressTracker {
    private final @NotNull String label;
    private final int totalItems;
    private final @NotNull Consumer<String> logger;
    private final long startedNanos = System.nanoTime();
    private int lastLoggedPercent = -1;

    private PhaseProgressTracker(
      final @NotNull String label,
      final int totalItems,
      final @NotNull Consumer<String> logger
    ) {
      this.label = label;
      this.totalItems = Math.max(0, totalItems);
      this.logger = logger;
      if (this.totalItems == 0) {
        logger.accept(label + ": 100% (0/0), elapsed=00:00, eta=00:00");
      }
    }

    private void report(final int doneItems) {
      if (totalItems <= 0) {
        return;
      }
      final int clampedDone = Math.max(0, Math.min(doneItems, totalItems));
      final int percent = (int) ((clampedDone * 100L) / totalItems);
      if (lastLoggedPercent >= 0 && percent < 100 && percent - lastLoggedPercent < 5) {
        return;
      }
      lastLoggedPercent = percent;
      final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
      final long etaMillis = estimateEtaMillis(clampedDone, totalItems, elapsedMillis);
      logger.accept(
        String.format(
          "%s: %d%% (%d/%d), elapsed=%s, eta=%s",
          label,
          percent,
          clampedDone,
          totalItems,
          formatDuration(elapsedMillis),
          formatDuration(etaMillis)
        )
      );
    }

    private void finish() {
      report(totalItems);
    }
  }

  private static final SortedStripePixels EMPTY_STRIPE =
    new SortedStripePixels(new long[0], new int[0], new int[0], new long[0]);

  private record PixelBlock(long @NotNull [] rows, long @NotNull [] cols, long @NotNull [] values) {
    int length() {
      return rows.length;
    }
  }

  private record SortedStripePixels(
    long @NotNull [] colStripes,
    int @NotNull [] intraRows,
    int @NotNull [] intraCols,
    long @NotNull [] values
  ) {
  }

  private record StripeCounts(long sparseElementCount, long denseBlockCount) {
  }

  private record SaveBlockResult(long sparseOffset, long denseOffset) {
  }
}
