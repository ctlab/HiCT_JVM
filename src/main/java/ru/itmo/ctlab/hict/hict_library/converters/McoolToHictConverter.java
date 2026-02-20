package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

public class McoolToHictConverter {

  public void convert(final @NotNull ConversionOptions options, final @NotNull Consumer<String> logConsumer) {
    final var compression = options.compressionLevel() > 0 ? HDF5IntStorageFeatures.createDeflation(options.compressionLevel()) : HDF5IntStorageFeatures.INT_CHUNKED;
    try (final var src = HDF5Factory.openForReading(options.inputPath().toFile())) {
      final var selectedResolutions = resolveResolutions(src, options.resolutions());
      final var workers = Math.max(1, Math.min(options.parallelism(), selectedResolutions.size()));

      logConsumer.accept("Converting in parallel with workers=" + workers + ", chunkSize=" + options.chunkSize());

      final var staged = convertResolutionsInParallel(options.inputPath(), selectedResolutions, options.chunkSize(), compression, workers, logConsumer);

      try (final var dst = HDF5Factory.open(options.outputPath().toFile())) {
        dst.object().createGroup("/resolutions");
        for (final var item : staged.stream().sorted(Comparator.comparingLong(StagedResolutionFile::resolution)).toList()) {
          try (final var stagedReader = HDF5Factory.openForReading(item.path().toFile())) {
            mergeResolution(stagedReader, dst, item.resolution(), options.chunkSize(), compression, logConsumer);
          } finally {
            try {
              Files.deleteIfExists(item.path());
            } catch (IOException e) {
              logConsumer.accept("Failed to delete temp file " + item.path() + ": " + e.getMessage());
            }
          }
        }
      }
    }
  }

  private static @NotNull List<StagedResolutionFile> convertResolutionsInParallel(
    final @NotNull Path inputPath,
    final @NotNull List<Long> resolutions,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final int workers,
    final @NotNull Consumer<String> logConsumer
  ) {
    final ExecutorService executor = Executors.newFixedThreadPool(workers);
    final List<Future<StagedResolutionFile>> futures = new ArrayList<>();

    for (final var resolution : resolutions) {
      futures.add(executor.submit(() -> {
        final var stagedFile = Files.createTempFile("mcool-to-hict-r" + resolution + "-", ".h5");
        try (final var src = HDF5Factory.openForReading(inputPath.toFile());
             final var dst = HDF5Factory.open(stagedFile.toFile())) {
          stageResolution(src, dst, resolution, chunkSize, compression, logConsumer);
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
    final @NotNull Consumer<String> logConsumer
  ) {
    final var srcRoot = "/resolutions/" + resolution;
    final var dstRoot = "/resolutions/" + resolution;
    dst.object().createGroup("/resolutions");
    dst.object().createGroup(dstRoot);
    dst.object().createGroup(dstRoot + "/treap_coo");

    HictToMcoolConverter.copyLongArrayChunked(src, dst, srcRoot + "/pixels/count", getBlockLengthDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(src, dst, srcRoot + "/indexes/block_offset", getBlockOffsetDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(src, dst, srcRoot + "/pixels/bin1_id", getBlockRowsDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(src, dst, srcRoot + "/pixels/bin2_id", getBlockColsDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(src, dst, srcRoot + "/pixels/counts", getBlockValuesDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(src, dst, srcRoot + "/pixels/dense_blocks", getDenseBlockDatasetPath(resolution), chunkSize, compression, logConsumer);
    dst.int64().setAttr(dstRoot + "/treap_coo", "dense_submatrix_size", chunkSize);

    logConsumer.accept("Staged resolution " + resolution + " in worker=" + Thread.currentThread().getName());
  }

  private static void mergeResolution(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader stagedReader,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final long resolution,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull Consumer<String> logConsumer
  ) {
    final var dstRoot = "/resolutions/" + resolution;
    dst.object().createGroup(dstRoot);
    dst.object().createGroup(dstRoot + "/treap_coo");

    HictToMcoolConverter.copyLongArrayChunked(stagedReader, dst, getBlockLengthDatasetPath(resolution), getBlockLengthDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(stagedReader, dst, getBlockOffsetDatasetPath(resolution), getBlockOffsetDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(stagedReader, dst, getBlockRowsDatasetPath(resolution), getBlockRowsDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(stagedReader, dst, getBlockColsDatasetPath(resolution), getBlockColsDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(stagedReader, dst, getBlockValuesDatasetPath(resolution), getBlockValuesDatasetPath(resolution), chunkSize, compression, logConsumer);
    HictToMcoolConverter.copyLongArrayChunked(stagedReader, dst, getDenseBlockDatasetPath(resolution), getDenseBlockDatasetPath(resolution), chunkSize, compression, logConsumer);
    dst.int64().setAttr(dstRoot + "/treap_coo", "dense_submatrix_size", chunkSize);
    logConsumer.accept("Merged resolution " + resolution + " to final output");
  }

  private @NotNull List<Long> resolveResolutions(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src, final @NotNull List<Long> requested) {
    final var all = src.object().getAllGroupMembers("/resolutions").stream().map(Long::parseLong).sorted().toList();
    if (requested == null || requested.isEmpty()) {
      return all;
    }
    return all.stream().filter(requested::contains).toList();
  }

  private record StagedResolutionFile(long resolution, @NotNull Path path) {
  }
}
