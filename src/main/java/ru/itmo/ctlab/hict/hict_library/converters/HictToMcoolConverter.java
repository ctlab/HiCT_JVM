package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import io.vertx.core.json.JsonArray;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

public class HictToMcoolConverter {

  public void convert(final @NotNull ConversionOptions options, final @NotNull Consumer<String> logConsumer) throws IOException, NoSuchFieldException {
    final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(options.inputPath(), 2, 8));
    try {
      if (options.applyAgpBeforeExport() && !options.agpPath().isBlank()) {
        try (final var reader = Files.newBufferedReader(options.inputPath().resolveSibling(options.agpPath()), StandardCharsets.UTF_8)) {
          chunkedFile.importAGP(reader);
        }
        logConsumer.accept("Applied AGP before export");
      }

      final var selectedResolutions = resolveResolutions(chunkedFile.getResolutions(), options.resolutions());
      final var compression = resolveIntStorageFeatures(options, logConsumer);
      final var workers = Math.max(1, Math.min(options.parallelism(), selectedResolutions.size()));

      logConsumer.accept("Converting in parallel with workers=" + workers + ", chunkSize=" + options.chunkSize());

      final var stagedResolutionFiles = convertResolutionsInParallel(
        options.inputPath(),
        selectedResolutions,
        options.chunkSize(),
        compression,
        workers,
        logConsumer
      );

      try (final var dst = HDF5Factory.open(options.outputPath().toFile())) {
        dst.object().createGroup("/resolutions");
        dst.string().write("/format", "mcool-lite");
        dst.string().write("/source_format", "hict");
        dst.string().write("/selected_resolutions", new JsonArray(selectedResolutions).encode());

        for (final var staged : stagedResolutionFiles.stream().sorted(Comparator.comparingLong(StagedResolutionFile::resolution)).toList()) {
          try (final var stagedReader = HDF5Factory.openForReading(staged.path().toFile())) {
            mergeResolution(stagedReader, dst, staged.resolution(), options.chunkSize(), compression, logConsumer);
          } finally {
            try {
              Files.deleteIfExists(staged.path());
            } catch (IOException e) {
              logConsumer.accept("Failed to delete temp file " + staged.path() + ": " + e.getMessage());
            }
          }
        }
      }
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
    final @NotNull Consumer<String> logConsumer
  ) {
    final ExecutorService executor = Executors.newFixedThreadPool(workers);
    final List<Future<StagedResolutionFile>> futures = new ArrayList<>();

    for (final var resolution : selectedResolutions) {
      futures.add(executor.submit(() -> {
        final var stagedFile = Files.createTempFile("hict-to-mcool-r" + resolution + "-", ".h5");
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
    final var root = "/resolutions/" + resolution;
    dst.object().createGroup("/resolutions");
    dst.object().createGroup(root);
    dst.object().createGroup(root + "/pixels");
    dst.object().createGroup(root + "/indexes");

    copyLongArrayChunked(src, dst, getBlockLengthDatasetPath(resolution), root + "/pixels/count", chunkSize, compression, logConsumer);
    copyLongArrayChunked(src, dst, getBlockOffsetDatasetPath(resolution), root + "/indexes/block_offset", chunkSize, compression, logConsumer);
    copyLongArrayChunked(src, dst, getBlockRowsDatasetPath(resolution), root + "/pixels/bin1_id", chunkSize, compression, logConsumer);
    copyLongArrayChunked(src, dst, getBlockColsDatasetPath(resolution), root + "/pixels/bin2_id", chunkSize, compression, logConsumer);
    copyLongArrayChunked(src, dst, getBlockValuesDatasetPath(resolution), root + "/pixels/counts", chunkSize, compression, logConsumer);
    copyLongArrayChunked(src, dst, getDenseBlockDatasetPath(resolution), root + "/pixels/dense_blocks", chunkSize, compression, logConsumer);
    dst.int64().setAttr(root, "bin_size", resolution);
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
    final var root = "/resolutions/" + resolution;
    dst.object().createGroup(root);
    dst.object().createGroup(root + "/pixels");
    dst.object().createGroup(root + "/indexes");

    copyLongArrayChunked(stagedReader, dst, root + "/pixels/count", root + "/pixels/count", chunkSize, compression, logConsumer);
    copyLongArrayChunked(stagedReader, dst, root + "/indexes/block_offset", root + "/indexes/block_offset", chunkSize, compression, logConsumer);
    copyLongArrayChunked(stagedReader, dst, root + "/pixels/bin1_id", root + "/pixels/bin1_id", chunkSize, compression, logConsumer);
    copyLongArrayChunked(stagedReader, dst, root + "/pixels/bin2_id", root + "/pixels/bin2_id", chunkSize, compression, logConsumer);
    copyLongArrayChunked(stagedReader, dst, root + "/pixels/counts", root + "/pixels/counts", chunkSize, compression, logConsumer);
    copyLongArrayChunked(stagedReader, dst, root + "/pixels/dense_blocks", root + "/pixels/dense_blocks", chunkSize, compression, logConsumer);
    dst.int64().setAttr(root, "bin_size", resolution);
    logConsumer.accept("Merged resolution " + resolution + " to final output");
  }

  static void copyLongArrayChunked(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader src,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String srcPath,
    final @NotNull String dstPath,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull Consumer<String> logConsumer
  ) {
    if (!src.object().isDataSet(srcPath)) {
      logConsumer.accept("Skipped missing dataset " + srcPath);
      return;
    }

    final var dims = src.object().getDataSetInformation(srcPath).getDimensions();
    final long length = dims.length == 0 ? 0 : dims[0];
    dst.int64().createArray(dstPath, length, chunkSize, compression);

    long offset = 0L;
    while (offset < length) {
      final int blockLen = (int) Math.min(chunkSize, length - offset);
      final var block = src.int64().readArrayBlockWithOffset(srcPath, blockLen, offset);
      dst.int64().writeArrayBlockWithOffset(dstPath, block, blockLen, offset);
      offset += blockLen;
    }
    logConsumer.accept("Copied " + srcPath + " -> " + dstPath + " (" + length + " items)");
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
}
