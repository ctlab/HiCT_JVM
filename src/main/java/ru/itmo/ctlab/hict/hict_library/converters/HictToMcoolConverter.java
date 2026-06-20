package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5DataClass;
import ch.systemsx.cisd.hdf5.HDF5FloatStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import io.vertx.core.json.JsonArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.Initializers;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

public class HictToMcoolConverter {

  private static final String DEFAULT_ASSEMBLY_NAME = "HiCT current assembly";
  private static final String ROOT_CHROMS_GROUP = "/chroms";
  public static final String HICT_METADATA_GROUP = "/hict_metadata";
  public static final String HICT_ASSEMBLY_METADATA_GROUP = HICT_METADATA_GROUP + "/assembly";
  public static final String HICT_METADATA_CONTIG_NAME_PATH = HICT_ASSEMBLY_METADATA_GROUP + "/contig_name";
  public static final String HICT_METADATA_CONTIG_LENGTH_BP_PATH = HICT_ASSEMBLY_METADATA_GROUP + "/contig_length_bp";
  public static final String HICT_METADATA_CONTIG_DIRECTION_PATH = HICT_ASSEMBLY_METADATA_GROUP + "/contig_direction";
  public static final String HICT_METADATA_CONTIG_ORDER_PATH = HICT_ASSEMBLY_METADATA_GROUP + "/ordered_contig_ids";
  public static final String HICT_METADATA_CONTIG_SCAFFOLD_ID_PATH = HICT_ASSEMBLY_METADATA_GROUP + "/contig_scaffold_id";
  private static final long DEFAULT_EXPORT_MEMORY_LIMIT_BYTES = 16L * 1024L * 1024L * 1024L;
  private static final int MIN_SORT_BATCH_SIZE = 50_000;
  private static final int MAX_SORT_BATCH_SIZE = 8_000_000;
  private static final int ESTIMATED_SORT_BYTES_PER_RECORD = 112;
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

      final var selectedResolutions = normalizeSelectedResolutionsForOutput(
        options.outputPath(),
        requireUsableSelectedResolutions(
          options.inputPath(),
          resolveResolutions(chunkedFile.getResolutions(), options.resolutions(), options.exportAllResolutions()),
          chunkedFile.getResolutions(),
          options.resolutions(),
          options.exportAllResolutions()
        ),
        synchronizedLogConsumer
      );
      final var assemblyLayout = buildCoolerAssemblyLayout(chunkedFile, selectedResolutions);
      final var compression = resolveIntStorageFeatures(options, synchronizedLogConsumer);
      final var floatCompression = resolveFloatStorageFeatures(options, synchronizedLogConsumer);
      final var totalSourcePixels = countSourcePixels(options.inputPath(), selectedResolutions);
      final var progressTracker = new ExportProgressTracker(
        Math.max(1L, (totalSourcePixels * 4L) + countBins(assemblyLayout)),
        System.nanoTime(),
        synchronizedLogConsumer
      );
      final int sortBatchSize = resolveSortBatchSize(options.chunkSize(), options.parallelism(), synchronizedLogConsumer);
      final boolean singleCoolerOutput = isSingleCoolerOutput(options.outputPath());

      synchronizedLogConsumer.accept(
        "Converting internally with chunkSize=" + options.chunkSize() +
          ", sortBatchSize=" + sortBatchSize +
          ", parallelism=" + options.parallelism()
      );

      Files.deleteIfExists(options.outputPath());
      try (final var dst = HDF5Factory.open(options.outputPath().toFile())) {
        if (!singleCoolerOutput) {
          dst.object().createGroup("/resolutions");
          dst.string().setAttrVL("/", "format", "HDF5::MCOOL");
          dst.int64().setAttr("/", "format-version", 2L);
          dst.object().createGroup(ROOT_CHROMS_GROUP);
          writeChromsGroup(dst, ROOT_CHROMS_GROUP, assemblyLayout.chroms(), compression);
        }
        writeHictAssemblyMetadata(dst, assemblyLayout, compression);
        dst.string().write("/source_format", "hict");
        dst.string().write("/selected_resolutions", new JsonArray(selectedResolutions).encode());

        for (final var resolution : selectedResolutions.stream().sorted().toList()) {
          final Integer resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
          if (resolutionOrder == null) {
            throw new IllegalStateException("Resolution " + resolution + " is not present in " + options.inputPath().getFileName());
          }
          final var resolutionTmpDir = Files.createTempDirectory("hict-to-mcool-r" + resolution + "-");
          try {
            final var cooPath = resolutionTmpDir.resolve("pixels.coo.bin");
            final var mapper = buildMapper(chunkedFile, options.inputPath(), resolutionOrder);
            exportTransformedCoo(
              options.inputPath(),
              resolution,
              cooPath,
              mapper,
              options.chunkSize(),
              sortBatchSize,
              progressTracker,
              synchronizedLogConsumer
            );
            if (singleCoolerOutput) {
              mergeSingleResolutionFromSortedCoo(
                cooPath,
                dst,
                assemblyLayout.resolutionLayout(resolution),
                options.chunkSize(),
                compression,
                floatCompression,
                progressTracker,
                synchronizedLogConsumer
              );
              synchronizedLogConsumer.accept("Merged resolution " + resolution + " to root Cooler output");
            } else {
              mergeResolutionFromSortedCoo(
                cooPath,
                dst,
                assemblyLayout.resolutionLayout(resolution),
                options.chunkSize(),
                compression,
                floatCompression,
                progressTracker,
                synchronizedLogConsumer
              );
              synchronizedLogConsumer.accept("Merged resolution " + resolution + " to final output");
            }
          } finally {
            deleteRecursively(resolutionTmpDir, synchronizedLogConsumer);
          }
        }
      }
      progressTracker.finish();
    } finally {
      chunkedFile.close();
    }
  }

  public static @NotNull List<Long> normalizeSelectedResolutionsForOutput(
    final @NotNull Path outputPath,
    final @NotNull List<Long> selectedResolutions,
    final @NotNull Consumer<String> logConsumer
  ) {
    if (!isSingleCoolerOutput(outputPath) || selectedResolutions.size() <= 1) {
      return selectedResolutions;
    }

    final var finestResolution = selectedResolutions.stream().min(Long::compareTo).orElseThrow();
    logConsumer.accept(
      "Requested .cool output with " + selectedResolutions.size() + " selected resolutions. " +
        "Cooler .cool files contain exactly one resolution, so HiCT will export only the finest selected resolution " +
        finestResolution + ". Use an .mcool output path to export multiple resolutions."
    );
    return List.of(finestResolution);
  }

  public static @NotNull List<Long> requireUsableSelectedResolutions(
    final @NotNull Path inputPath,
    final @NotNull List<Long> selectedResolutions,
    final long @NotNull [] availableResolutions,
    final @NotNull List<Long> requestedResolutions,
    final boolean exportAllResolutions
  ) {
    if (!selectedResolutions.isEmpty()) {
      return selectedResolutions;
    }

    final var available = new ArrayList<Long>();
    for (int i = 1; i < availableResolutions.length; i++) {
      available.add(availableResolutions[i]);
    }
    final var inputName = inputPath.getFileName() == null ? inputPath.toString() : inputPath.getFileName().toString();
    final String requested = requestedResolutions.isEmpty()
      ? (exportAllResolutions ? "all available resolutions" : "finest available resolution")
      : requestedResolutions.toString();
    throw new IllegalArgumentException(
      "No matching resolutions were selected for " + inputName + ". " +
        "Available resolutions are " + available + "; requested " + requested + ". " +
        "Use --resolutions with one of the available values, omit --resolutions for the finest resolution, " +
        "or use --all-resolutions for multi-resolution .mcool export."
    );
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

    final var layout = buildSingleChromLayout(DEFAULT_ASSEMBLY_NAME, assemblyLengthBp, resolution, binsCount);
    writeChromsGroup(dst, root + "/chroms", layout.chroms(), compression);
    writeBinsGroup(dst, root + "/bins", layout, compression, HDF5FloatStorageFeatures.FLOAT_CHUNKED, progressTracker, logConsumer, prefix + " bins");
    writeIndexesGroup(stagedReader, dst, root, binsCount, layout.chromOffsets(), nonzeroPixelCount, chunkSize, compression);
    writeResolutionMetadata(dst, root, resolution, binsCount, layout.chroms().size(), nonzeroPixelCount, totalCounts);
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
    final long chromCount,
    final long nonzeroPixelCount,
    final long totalCounts
  ) {
    dst.string().setAttrVL(root, "assembly", DEFAULT_ASSEMBLY_NAME);
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
    dst.int64().setAttr(root, "nchroms", chromCount);
    dst.int64().setAttr(root, "nnz", nonzeroPixelCount);
    dst.string().setAttrVL(root, "storage-mode", "symmetric-upper");
    dst.int64().setAttr(root, "sum", totalCounts);
  }

  private static void writeChromsGroup(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String groupPath,
    final @NotNull List<CoolerChrom> chroms,
    final @NotNull HDF5IntStorageFeatures compression
  ) {
    final var names = new String[chroms.size()];
    final var lengths = new int[chroms.size()];
    for (int i = 0; i < chroms.size(); i++) {
      names[i] = chroms.get(i).name();
      lengths[i] = Math.toIntExact(chroms.get(i).lengthBp());
    }
    dst.string().writeArray(groupPath + "/name", names);
    dst.int32().writeArray(groupPath + "/length", lengths, compression);
  }

  public static void writeHictAssemblyMetadata(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull CoolerAssemblyLayout layout,
    final @NotNull HDF5IntStorageFeatures compression
  ) {
    if (!dst.object().isGroup(HICT_METADATA_GROUP)) {
      dst.object().createGroup(HICT_METADATA_GROUP);
    }
    if (!dst.object().isGroup(HICT_ASSEMBLY_METADATA_GROUP)) {
      dst.object().createGroup(HICT_ASSEMBLY_METADATA_GROUP);
    }
    dst.string().setAttrVL(HICT_METADATA_GROUP, "format", "hict-mcool-metadata");
    dst.int64().setAttr(HICT_METADATA_GROUP, "format-version", 1L);
    dst.string().setAttrVL(HICT_ASSEMBLY_METADATA_GROUP, "description", "HiCT assembly state used to export this .mcool file");

    final var chroms = layout.chroms();
    final var names = new String[chroms.size()];
    final var lengths = new long[chroms.size()];
    final var directions = new long[chroms.size()];
    final var orderedIds = new long[chroms.size()];
    final var scaffoldIds = new long[chroms.size()];
    for (int i = 0; i < chroms.size(); i++) {
      final var chrom = chroms.get(i);
      names[i] = chrom.name();
      lengths[i] = chrom.lengthBp();
      directions[i] = chrom.direction();
      orderedIds[i] = i;
      scaffoldIds[i] = chrom.scaffoldId();
    }

    dst.string().writeArray(HICT_METADATA_CONTIG_NAME_PATH, names);
    dst.int64().writeArray(HICT_METADATA_CONTIG_LENGTH_BP_PATH, lengths, compression);
    dst.int64().writeArray(HICT_METADATA_CONTIG_DIRECTION_PATH, directions, compression);
    dst.int64().writeArray(HICT_METADATA_CONTIG_ORDER_PATH, orderedIds, compression);
    dst.int64().writeArray(HICT_METADATA_CONTIG_SCAFFOLD_ID_PATH, scaffoldIds, compression);
  }

  private static void writeBinsGroup(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String groupPath,
    final @NotNull ResolutionLayout layout,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull HDF5FloatStorageFeatures floatCompression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logConsumer,
    final @NotNull String progressLabel
  ) {
    final long resolution = layout.resolution();
    final long binsCount = layout.binsCount();
    final int chunkLength = safeChunkLen(binsCount, 8192);
    final int compressionChunkLength = Math.max(1, chunkLength);
    dst.int32().createArray(groupPath + "/chrom", binsCount, compressionChunkLength, compression);
    dst.int32().createArray(groupPath + "/start", binsCount, compressionChunkLength, compression);
    dst.int32().createArray(groupPath + "/end", binsCount, compressionChunkLength, compression);
    dst.float64().createArray(groupPath + "/weight", binsCount, compressionChunkLength, floatCompression);

    long written = 0L;
    int spanIndex = 0;
    final var weightCursor = new BinWeightCursor(layout);
    int lastLoggedPercent = -1;
    while (written < binsCount) {
      final int blockLen = (int) Math.min(chunkLength, binsCount - written);
      final var chrom = new int[blockLen];
      final var starts = new int[blockLen];
      final var ends = new int[blockLen];
      final var weights = new double[blockLen];
      for (int i = 0; i < blockLen; i++) {
        final long binIndex = written + i;
        while (spanIndex + 1 < layout.spans().size() && binIndex >= layout.spans().get(spanIndex).globalBinEnd()) {
          spanIndex++;
        }
        final var span = layout.spans().get(spanIndex);
        final long localBin = binIndex - span.globalBinStart();
        final long startBp = localBin * resolution;
        chrom[i] = span.chromIndex();
        starts[i] = Math.toIntExact(startBp);
        ends[i] = Math.toIntExact(Math.min(span.chrom().lengthBp(), startBp + resolution));
        weights[i] = weightCursor.weightAt(binIndex);
      }
      dst.int32().writeArrayBlockWithOffset(groupPath + "/chrom", chrom, blockLen, written);
      dst.int32().writeArrayBlockWithOffset(groupPath + "/start", starts, blockLen, written);
      dst.int32().writeArrayBlockWithOffset(groupPath + "/end", ends, blockLen, written);
      dst.float64().writeArrayBlockWithOffset(groupPath + "/weight", weights, blockLen, written);
      written += blockLen;
      progressTracker.add(blockLen * 4L, progressLabel);
      if (binsCount > 0) {
        final int percent = (int) ((written * 100L) / binsCount);
        if (percent >= 100 || percent - lastLoggedPercent >= 10) {
          lastLoggedPercent = percent;
          logConsumer.accept(String.format("%s: %d%% (%d/%d)", progressLabel, percent, written, binsCount));
        }
      }
    }
  }

  public static void writeBinWeights(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String binsGroupPath,
    final @NotNull ResolutionLayout layout,
    final int chunkSize,
    final @NotNull HDF5FloatStorageFeatures floatCompression
  ) {
    final long binsCount = layout.binsCount();
    final int chunkLength = safeChunkLen(binsCount, Math.max(1, chunkSize));
    dst.float64().createArray(binsGroupPath + "/weight", binsCount, chunkLength, floatCompression);

    final var cursor = new BinWeightCursor(layout);
    long written = 0L;
    while (written < binsCount) {
      final int blockLen = (int) Math.min(chunkLength, binsCount - written);
      final var weights = new double[blockLen];
      for (int i = 0; i < blockLen; i++) {
        weights[i] = cursor.weightAt(written + i);
      }
      dst.float64().writeArrayBlockWithOffset(binsGroupPath + "/weight", weights, blockLen, written);
      written += blockLen;
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
    final long @NotNull [] chromOffsets,
    final long nonzeroPixelCount,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression
  ) {
    final long[] bin1Offset = buildBin1Offset(stagedReader, root + "/pixels/bin1_id", binsCount, nonzeroPixelCount, chunkSize);
    dst.int64().writeArray(root + "/indexes/bin1_offset", bin1Offset, compression);
    dst.int64().writeArray(root + "/indexes/chrom_offset", chromOffsets, compression);
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

  private static long countSourcePixels(
    final @NotNull Path inputPath,
    final @NotNull List<Long> resolutions
  ) {
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

  private static long countBins(final @NotNull CoolerAssemblyLayout layout) {
    long total = 0L;
    for (final var resolutionLayout : layout.resolutionLayouts()) {
      total += resolutionLayout.binsCount() * 4L;
    }
    return total;
  }

  public static @NotNull CoolerAssemblyLayout buildCoolerAssemblyLayout(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull List<Long> selectedResolutions
  ) {
    final var currentContigs = chunkedFile.getAssemblyInfo().contigs();
    final var currentScaffolds = chunkedFile.getAssemblyInfo().scaffolds();
    final var exportContigs = filterExportableContigs(chunkedFile, currentContigs, selectedResolutions);
    final var scaffoldIds = buildExportScaffoldIds(chunkedFile, exportContigs, currentScaffolds);
    final var chroms = new ArrayList<CoolerChrom>(exportContigs.size());
    final var seenNames = new java.util.HashMap<String, Integer>();
    for (int i = 0; i < exportContigs.size(); i++) {
      final var descriptor = exportContigs.get(i).descriptor();
      final var rawName = descriptor.getContigName();
      final var baseName = rawName == null || rawName.isBlank() ? "contig_" + descriptor.getContigId() : rawName;
      final int occurrence = seenNames.merge(baseName, 1, Integer::sum);
      final var uniqueName = occurrence == 1 ? baseName : baseName + "__" + occurrence;
      chroms.add(new CoolerChrom(
        uniqueName,
        descriptor.getLengthBp(),
        exportContigs.get(i).direction().ordinal(),
        scaffoldIds[i]
      ));
    }

    final var resolutionLayouts = new ArrayList<ResolutionLayout>(selectedResolutions.size());
    for (final var resolution : selectedResolutions) {
      final Integer resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
      if (resolutionOrder == null) {
        throw new IllegalStateException("Resolution " + resolution + " is not present in the input HiCT file");
      }
      resolutionLayouts.add(buildResolutionLayout(exportContigs, chroms, resolution, resolutionOrder));
    }
    return new CoolerAssemblyLayout(List.copyOf(chroms), List.copyOf(resolutionLayouts));
  }

  private static @NotNull List<ContigTree.ContigTuple> filterExportableContigs(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull List<ContigTree.ContigTuple> currentContigs,
    final @NotNull List<Long> selectedResolutions
  ) {
    final var resolutionOrders = new ArrayList<Integer>(selectedResolutions.size());
    for (final var resolution : selectedResolutions) {
      final Integer resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
      if (resolutionOrder == null) {
        throw new IllegalStateException("Resolution " + resolution + " is not present in the input HiCT file");
      }
      resolutionOrders.add(resolutionOrder);
    }

    final var exportContigs = currentContigs.stream()
      .filter(tuple -> hasExportedBins(tuple.descriptor(), resolutionOrders))
      .toList();
    if (exportContigs.isEmpty() && !currentContigs.isEmpty()) {
      throw new IllegalStateException("Cannot export Cooler layout because all contigs are hidden or have no bins at selected resolutions");
    }
    return exportContigs;
  }

  private static boolean hasExportedBins(
    final @NotNull ContigDescriptor descriptor,
    final @NotNull List<Integer> resolutionOrders
  ) {
    for (final int resolutionOrder : resolutionOrders) {
      if (resolutionOrder < 0 || resolutionOrder >= descriptor.getAtus().size()) {
        continue;
      }
      for (final var atu : descriptor.getAtus().get(resolutionOrder)) {
        if (atu.getLength() > 0L) {
          return true;
        }
      }
    }
    return false;
  }

  private static long @NotNull [] buildExportScaffoldIds(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull List<ContigTree.ContigTuple> currentContigs,
    final @NotNull List<ScaffoldTree.ScaffoldTuple> currentScaffolds
  ) {
    final var storedScaffoldIds = readStoredScaffoldIds(chunkedFile, currentContigs);
    if (storedScaffoldIds != null) {
      return storedScaffoldIds;
    }
    return buildScaffoldIds(currentContigs, currentScaffolds);
  }

  private static long @Nullable [] readStoredScaffoldIds(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull List<ContigTree.ContigTuple> currentContigs
  ) {
    final var scaffoldIdPath = "/contig_info/contig_scaffold_id";
    try (final var reader = HDF5Factory.openForReading(chunkedFile.getHdfFilePath().toFile())) {
      if (!reader.object().isDataSet(scaffoldIdPath)) {
        return null;
      }
      final var originalScaffoldIds = reader.int64().readArray(scaffoldIdPath);
      final var scaffoldIds = new long[currentContigs.size()];
      for (int i = 0; i < currentContigs.size(); i++) {
        final int originalContigId = currentContigs.get(i).descriptor().getContigId();
        scaffoldIds[i] = originalContigId >= 0 && originalContigId < originalScaffoldIds.length
          ? originalScaffoldIds[originalContigId]
          : -1L;
      }
      return scaffoldIds;
    } catch (Exception ignored) {
      return null;
    }
  }

  private static long @NotNull [] buildScaffoldIds(
    final @NotNull List<ContigTree.ContigTuple> currentContigs,
    final @NotNull List<ScaffoldTree.ScaffoldTuple> currentScaffolds
  ) {
    final var scaffoldIds = new long[currentContigs.size()];
    Arrays.fill(scaffoldIds, -1L);
    if (currentScaffolds.isEmpty() || currentContigs.isEmpty()) {
      return scaffoldIds;
    }

    int scaffoldIndex = 0;
    long assemblyPosition = 0L;
    for (int contigIndex = 0; contigIndex < currentContigs.size(); contigIndex++) {
      while (scaffoldIndex < currentScaffolds.size()
        && currentScaffolds.get(scaffoldIndex).scaffoldBordersBP().endBP() <= assemblyPosition) {
        scaffoldIndex++;
      }
      if (scaffoldIndex < currentScaffolds.size()) {
        final var scaffold = currentScaffolds.get(scaffoldIndex);
        final ScaffoldDescriptor descriptor = scaffold.scaffoldDescriptor();
        if (descriptor != null
          && scaffold.scaffoldBordersBP().startBP() <= assemblyPosition
          && assemblyPosition < scaffold.scaffoldBordersBP().endBP()) {
          scaffoldIds[contigIndex] = descriptor.scaffoldId();
        }
      }
      assemblyPosition += currentContigs.get(contigIndex).descriptor().getLengthBp();
    }
    return scaffoldIds;
  }

  private static @NotNull ResolutionLayout buildResolutionLayout(
    final @NotNull List<ContigTree.ContigTuple> currentContigs,
    final @NotNull List<CoolerChrom> chroms,
    final long resolution,
    final int resolutionOrder
  ) {
    final var spans = new ArrayList<ContigBinSpan>(currentContigs.size());
    final var chromOffsets = new long[currentContigs.size() + 1];
    long cursor = 0L;
    for (int chromIndex = 0; chromIndex < currentContigs.size(); chromIndex++) {
      chromOffsets[chromIndex] = cursor;
      long binCount = 0L;
      for (final var atu : currentContigs.get(chromIndex).descriptor().getAtus().get(resolutionOrder)) {
        binCount += atu.getLength();
      }
      spans.add(new ContigBinSpan(
        chromIndex,
        chroms.get(chromIndex),
        cursor,
        cursor + binCount,
        List.copyOf(currentContigs.get(chromIndex).descriptor().getAtus().get(resolutionOrder))
      ));
      cursor += binCount;
    }
    chromOffsets[currentContigs.size()] = cursor;
    return new ResolutionLayout(resolution, List.copyOf(chroms), List.copyOf(spans), chromOffsets, cursor);
  }

  private static @NotNull ResolutionLayout buildSingleChromLayout(
    final @NotNull String chromName,
    final long chromLengthBp,
    final long resolution,
    final long binsCount
  ) {
    final var chrom = new CoolerChrom(chromName, chromLengthBp);
    return new ResolutionLayout(
      resolution,
      List.of(chrom),
      List.of(new ContigBinSpan(0, chrom, 0L, binsCount, List.of())),
      new long[]{0L, binsCount},
      binsCount
    );
  }

  private static void exportTransformedCoo(
    final @NotNull Path inputPath,
    final long resolution,
    final @NotNull Path outputPath,
    final @NotNull SourceToAssemblyMapper mapper,
    final int chunkSize,
    final int sortBatchSize,
    final @NotNull ExportProgressTracker overallTracker,
    final @NotNull Consumer<String> logger
  ) throws IOException {
    final var workDir = Files.createDirectories(outputPath.getParent());
    final var chunkPaths = new ArrayList<Path>();
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
      if (isFloatingPointDataset(src, valuesPath)) {
        throw new IllegalStateException("Internal .mcool export currently supports integer HiCT matrices only");
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
              appendMappedRecord(batch, mapper, rowStripeOffset + rows[i], colStripeOffset + cols[i], vals[i]);
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
                final long value = denseValues[(row * 256) + col];
                if (value == 0L) {
                  continue;
                }
                appendMappedRecord(batch, mapper, rowStripeOffset + row, colStripeOffset + col, value);
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
    mergeSortedChunks(chunkPaths, outputPath, logger);
  }

  private static void mergeResolutionFromSortedCoo(
    final @NotNull Path cooPath,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull ResolutionLayout layout,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull HDF5FloatStorageFeatures floatCompression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logger
  ) throws IOException {
    mergeResolutionFromSortedCoo(
      cooPath,
      dst,
      layout,
      "/resolutions/" + layout.resolution(),
      chunkSize,
      compression,
      floatCompression,
      progressTracker,
      logger
    );
  }

  private static void mergeSingleResolutionFromSortedCoo(
    final @NotNull Path cooPath,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull ResolutionLayout layout,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull HDF5FloatStorageFeatures floatCompression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logger
  ) throws IOException {
    mergeResolutionFromSortedCoo(
      cooPath,
      dst,
      layout,
      "/",
      chunkSize,
      compression,
      floatCompression,
      progressTracker,
      logger
    );
  }

  private static void mergeResolutionFromSortedCoo(
    final @NotNull Path cooPath,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull ResolutionLayout layout,
    final @NotNull String root,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull HDF5FloatStorageFeatures floatCompression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logger
  ) throws IOException {
    final long resolution = layout.resolution();
    final long binsCount = layout.binsCount();
    final var summary = summarizeSortedCoo(cooPath, binsCount);
    if (!"/".equals(root)) {
      dst.object().createGroup(root);
    }
    dst.object().createGroup(coolerChildPath(root, "chroms"));
    dst.object().createGroup(coolerChildPath(root, "bins"));
    dst.object().createGroup(coolerChildPath(root, "pixels"));
    dst.object().createGroup(coolerChildPath(root, "indexes"));

    writeChromsGroup(dst, coolerChildPath(root, "chroms"), layout.chroms(), compression);
    writeBinsGroup(dst, coolerChildPath(root, "bins"), layout, compression, floatCompression, progressTracker, logger, "Merge resolution " + resolution + " bins");
    writePixelsFromSortedCoo(cooPath, dst, root, summary.nonzeroPixelCount(), chunkSize, compression, progressTracker, logger, resolution);
    dst.int64().writeArray(coolerChildPath(root, "indexes/bin1_offset"), summary.bin1Offset(), compression);
    dst.int64().writeArray(coolerChildPath(root, "indexes/chrom_offset"), layout.chromOffsets(), compression);
    writeResolutionMetadata(dst, root, resolution, binsCount, layout.chroms().size(), summary.nonzeroPixelCount(), summary.totalCounts());
  }

  private static @NotNull String coolerChildPath(final @NotNull String root, final @NotNull String relativePath) {
    return "/".equals(root) ? "/" + relativePath : root + "/" + relativePath;
  }

  private static @NotNull SortedCooSummary summarizeSortedCoo(
    final @NotNull Path cooPath,
    final long binsCount
  ) throws IOException {
    final var bin1Offset = new long[Math.toIntExact(binsCount + 1L)];
    long nonzeroPixelCount = 0L;
    long totalCounts = 0L;
    long lastRow = -1L;
    long lastCol = -1L;
    boolean seenAnyPixel = false;
    try (final var reader = new DataInputStream(new BufferedInputStream(Files.newInputStream(cooPath)))) {
      while (true) {
        final long row;
        final long col;
        final long count;
        try {
          row = reader.readLong();
          col = reader.readLong();
          count = reader.readLong();
        } catch (java.io.EOFException eof) {
          break;
        }
        if (row < 0L || row >= binsCount || col < row || col >= binsCount) {
          throw new IllegalStateException("Sorted COO record is out of Cooler bounds: " + row + "\t" + col + "\t" + count);
        }
        if (!seenAnyPixel) {
          for (long emptyBin = 1L; emptyBin <= row; emptyBin++) {
            bin1Offset[(int) emptyBin] = 0L;
          }
          lastRow = row;
          lastCol = col;
          seenAnyPixel = true;
        } else {
          if (row < lastRow || (row == lastRow && col < lastCol)) {
            throw new IllegalStateException("Sorted COO stream is not row-major sorted at record: " + row + "\t" + col + "\t" + count);
          }
          if (row != lastRow) {
            for (long nextBin = lastRow + 1L; nextBin <= row; nextBin++) {
              bin1Offset[(int) nextBin] = nonzeroPixelCount;
            }
            lastRow = row;
          }
          lastCol = col;
        }
        nonzeroPixelCount++;
        totalCounts += count;
      }
    }
    if (!seenAnyPixel) {
      return new SortedCooSummary(0L, 0L, bin1Offset);
    }
    for (long nextBin = Math.max(0L, lastRow + 1L); nextBin <= binsCount; nextBin++) {
      bin1Offset[(int) nextBin] = nonzeroPixelCount;
    }
    return new SortedCooSummary(nonzeroPixelCount, totalCounts, bin1Offset);
  }

  private static void writePixelsFromSortedCoo(
    final @NotNull Path cooPath,
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer dst,
    final @NotNull String root,
    final long nonzeroPixelCount,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures compression,
    final @NotNull ExportProgressTracker progressTracker,
    final @NotNull Consumer<String> logger,
    final long resolution
  ) throws IOException {
    final int datasetChunk = safeChunkLen(nonzeroPixelCount, chunkSize);
    final var bin1IdPath = coolerChildPath(root, "pixels/bin1_id");
    final var bin2IdPath = coolerChildPath(root, "pixels/bin2_id");
    final var countPath = coolerChildPath(root, "pixels/count");
    dst.int64().createArray(bin1IdPath, nonzeroPixelCount, datasetChunk, compression);
    dst.int64().createArray(bin2IdPath, nonzeroPixelCount, datasetChunk, compression);
    dst.int32().createArray(countPath, nonzeroPixelCount, datasetChunk, compression);

    final long startedNanos = System.nanoTime();
    long offset = 0L;
    int lastLoggedPercent = -1;
    final var rows = new long[datasetChunk];
    final var cols = new long[datasetChunk];
    final var counts = new int[datasetChunk];
    int buffered = 0;
    try (final var reader = new DataInputStream(new BufferedInputStream(Files.newInputStream(cooPath)))) {
      while (true) {
        final long row;
        final long col;
        final long count;
        try {
          row = reader.readLong();
          col = reader.readLong();
          count = reader.readLong();
        } catch (java.io.EOFException eof) {
          break;
        }
        rows[buffered] = row;
        cols[buffered] = col;
        counts[buffered] = Math.toIntExact(count);
        buffered++;
        if (buffered >= datasetChunk) {
          dst.int64().writeArrayBlockWithOffset(bin1IdPath, rows, buffered, offset);
          dst.int64().writeArrayBlockWithOffset(bin2IdPath, cols, buffered, offset);
          dst.int32().writeArrayBlockWithOffset(countPath, counts, buffered, offset);
          offset += buffered;
          progressTracker.add((long) buffered * 3L, "Merge resolution " + resolution + " pixels");
          if (nonzeroPixelCount > 0L) {
            final int percent = (int) ((offset * 100L) / nonzeroPixelCount);
            if (percent >= 100 || percent - lastLoggedPercent >= 10) {
              lastLoggedPercent = percent;
              final long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
              final long etaMillis = estimateEtaMillis(offset, nonzeroPixelCount, elapsedMillis);
              logger.accept(
                String.format(
                  "Resolution %d pixels write: %d%% (%d/%d), elapsed=%s, eta=%s",
                  resolution,
                  percent,
                  offset,
                  nonzeroPixelCount,
                  formatDuration(elapsedMillis),
                  formatDuration(etaMillis)
                )
              );
            }
          }
          buffered = 0;
        }
      }
    }
    if (buffered > 0) {
      final var rowsTail = java.util.Arrays.copyOf(rows, buffered);
      final var colsTail = java.util.Arrays.copyOf(cols, buffered);
      final var countsTail = java.util.Arrays.copyOf(counts, buffered);
      dst.int64().writeArrayBlockWithOffset(bin1IdPath, rowsTail, buffered, offset);
      dst.int64().writeArrayBlockWithOffset(bin2IdPath, colsTail, buffered, offset);
      dst.int32().writeArrayBlockWithOffset(countPath, countsTail, buffered, offset);
      offset += buffered;
      progressTracker.add((long) buffered * 3L, "Merge resolution " + resolution + " pixels");
    }
    logger.accept("Resolution " + resolution + " pixels write: 100% (" + offset + "/" + nonzeroPixelCount + "), elapsed=" +
      formatDuration((System.nanoTime() - startedNanos) / 1_000_000L) + ", eta=00:00");
  }

  private static boolean isSingleCoolerOutput(final @NotNull Path outputPath) {
    final var fileName = outputPath.getFileName();
    final var lowered = (fileName == null ? outputPath.toString() : fileName.toString()).toLowerCase(Locale.ROOT);
    return lowered.endsWith(".cool") && !lowered.endsWith(".mcool");
  }

  private static void appendMappedRecord(
    final @NotNull CooRecordBatch batch,
    final @NotNull SourceToAssemblyMapper mapper,
    final long sourceRow,
    final long sourceCol,
    final long count
  ) {
    long mappedRow = mapper.map(sourceRow);
    long mappedCol = mapper.map(sourceCol);
    if (mappedRow > mappedCol) {
      final long tmp = mappedRow;
      mappedRow = mappedCol;
      mappedCol = tmp;
    }
    batch.add(mappedRow, mappedCol, count);
  }

  private static @NotNull Path flushSortedChunk(
    final @NotNull Path workDir,
    final long resolution,
    final int chunkIndex,
    final @NotNull CooRecordBatch batch
  ) throws IOException {
    final var rows = batch.copyRows();
    final var cols = batch.copyCols();
    final var counts = batch.copyCounts();
    if (!NativeProcessingService.getInstance().trySortCoolerRecordsRowMajor(rows, cols, counts)) {
      sortCoolerRecordsJava(rows, cols, counts);
    }
    final var chunkPath = workDir.resolve(String.format("pixels-r%d-%05d.bin", resolution, chunkIndex));
    try (final var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(chunkPath)))) {
      for (int i = 0; i < rows.length; i++) {
        out.writeLong(rows[i]);
        out.writeLong(cols[i]);
        out.writeLong(counts[i]);
      }
    }
    return chunkPath;
  }

  static void sortCoolerRecordsJava(final long @NotNull [] rows,
                                    final long @NotNull [] cols,
                                    final long @NotNull [] counts) {
    if (rows.length != cols.length || rows.length != counts.length) {
      throw new IllegalArgumentException("COO arrays must have identical lengths");
    }
    if (rows.length <= 1) {
      return;
    }
    quickSortCoolerRecords(rows, cols, counts, 0, rows.length - 1);
  }

  private static void quickSortCoolerRecords(final long @NotNull [] rows,
                                             final long @NotNull [] cols,
                                             final long @NotNull [] counts,
                                             final int left,
                                             final int right) {
    if (right - left <= 32) {
      insertionSortCoolerRecords(rows, cols, counts, left, right);
      return;
    }
    int i = left;
    int j = right;
    final int pivotIndex = left + ((right - left) >>> 1);
    final long pivotRow = rows[pivotIndex];
    final long pivotCol = cols[pivotIndex];
    final long pivotCount = counts[pivotIndex];
    while (i <= j) {
      while (compareRecordToPivot(rows, cols, counts, i, pivotRow, pivotCol, pivotCount) < 0) {
        i++;
      }
      while (compareRecordToPivot(rows, cols, counts, j, pivotRow, pivotCol, pivotCount) > 0) {
        j--;
      }
      if (i <= j) {
        swapCoolerRecords(rows, cols, counts, i, j);
        i++;
        j--;
      }
    }
    if (left < j) {
      quickSortCoolerRecords(rows, cols, counts, left, j);
    }
    if (i < right) {
      quickSortCoolerRecords(rows, cols, counts, i, right);
    }
  }

  private static void insertionSortCoolerRecords(final long @NotNull [] rows,
                                                 final long @NotNull [] cols,
                                                 final long @NotNull [] counts,
                                                 final int left,
                                                 final int right) {
    for (int i = left + 1; i <= right; i++) {
      final long row = rows[i];
      final long col = cols[i];
      final long count = counts[i];
      int j = i - 1;
      while (j >= left && compareRecord(rows[j], cols[j], counts[j], row, col, count) > 0) {
        rows[j + 1] = rows[j];
        cols[j + 1] = cols[j];
        counts[j + 1] = counts[j];
        j--;
      }
      rows[j + 1] = row;
      cols[j + 1] = col;
      counts[j + 1] = count;
    }
  }

  private static int compareRecordToPivot(final long @NotNull [] rows,
                                          final long @NotNull [] cols,
                                          final long @NotNull [] counts,
                                          final int index,
                                          final long pivotRow,
                                          final long pivotCol,
                                          final long pivotCount) {
    return compareRecord(rows[index], cols[index], counts[index], pivotRow, pivotCol, pivotCount);
  }

  private static int compareRecord(final long row,
                                   final long col,
                                   final long count,
                                   final long otherRow,
                                   final long otherCol,
                                   final long otherCount) {
    int cmp = Long.compare(row, otherRow);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compare(col, otherCol);
    if (cmp != 0) {
      return cmp;
    }
    return Long.compare(count, otherCount);
  }

  private static void swapCoolerRecords(final long @NotNull [] rows,
                                        final long @NotNull [] cols,
                                        final long @NotNull [] counts,
                                        final int first,
                                        final int second) {
    if (first == second) {
      return;
    }
    long tmp = rows[first];
    rows[first] = rows[second];
    rows[second] = tmp;
    tmp = cols[first];
    cols[first] = cols[second];
    cols[second] = tmp;
    tmp = counts[first];
    counts[first] = counts[second];
    counts[second] = tmp;
  }

  private static void mergeSortedChunks(
    final @NotNull List<Path> chunkPaths,
    final @NotNull Path outputPath,
    final @NotNull Consumer<String> logger
  ) throws IOException {
    if (chunkPaths.isEmpty()) {
      Files.write(outputPath, new byte[0]);
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
      try (final var writer = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(outputPath)))) {
        long rawRecords = 0L;
        long written = 0L;
        boolean hasPending = false;
        long pendingRow = 0L;
        long pendingCol = 0L;
        long pendingCount = 0L;
        while (!queue.isEmpty()) {
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
            writer.writeLong(pendingRow);
            writer.writeLong(pendingCol);
            writer.writeLong(pendingCount);
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
          writer.writeLong(pendingRow);
          writer.writeLong(pendingCol);
          writer.writeLong(pendingCount);
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

  private static @NotNull SourceToAssemblyMapper buildMapper(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull Path inputPath,
    final int resolutionOrder
  ) throws IOException {
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

  private static boolean isFloatingPointDataset(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
    final @NotNull String path
  ) {
    return reader.object().getDataSetInformation(path).getTypeInformation().getDataClass() == HDF5DataClass.FLOAT;
  }

  private static long @NotNull [] readDenseLongBlock(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
    final @NotNull String path,
    final long denseIndex
  ) {
    final var block = reader.int64().readMDArrayBlockWithOffset(
      path,
      new int[]{1, 1, 256, 256},
      new long[]{denseIndex, 0L, 0L, 0L}
    );
    return block.getAsFlatArray();
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

  private @NotNull List<Long> resolveResolutions(final long @NotNull [] availableResolutions,
                                                 final @NotNull List<Long> requested,
                                                 final boolean exportAllResolutions) {
    final var available = new ArrayList<Long>();
    for (int i = 1; i < availableResolutions.length; i++) {
      available.add(availableResolutions[i]);
    }
    if (requested == null || requested.isEmpty()) {
      if (exportAllResolutions) {
        return available;
      }
      return available.stream().min(Long::compareTo).map(List::of).orElse(List.of());
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

  private static @NotNull HDF5FloatStorageFeatures resolveFloatStorageFeatures(final @NotNull ConversionOptions options, final @NotNull Consumer<String> logConsumer) {
    if (options.compressionLevel() <= 0) {
      return HDF5FloatStorageFeatures.FLOAT_CHUNKED;
    }
    return switch (options.compressionAlgorithm()) {
      case DEFLATE -> HDF5FloatStorageFeatures.createDeflation(options.compressionLevel());
      case ZSTD, LZF -> {
        logConsumer.accept(
          "Compression algorithm " + options.compressionAlgorithm() +
            " requested, but current JHDF5 high-level writer path supports deflate features only. Falling back to uncompressed chunked float datasets."
        );
        yield HDF5FloatStorageFeatures.FLOAT_CHUNKED;
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

  private static int resolveSortBatchSize(final int chunkSize,
                                          final int parallelism,
                                          final @NotNull Consumer<String> logger) {
    final long memoryLimitBytes = resolveExportMemoryLimitBytes(logger);
    final long perWorkerBudget = Math.max(64L * 1024L * 1024L, memoryLimitBytes / Math.max(1, parallelism));
    final long memoryLimitedRecords = perWorkerBudget / ESTIMATED_SORT_BYTES_PER_RECORD;
    final long requestedRecords = Math.max((long) MIN_SORT_BATCH_SIZE, memoryLimitedRecords);
    logger.accept(
      "HiCT -> Cooler export memory budget=" + formatByteSize(memoryLimitBytes) +
        ", per-worker sort budget=" + formatByteSize(perWorkerBudget) +
        ", estimated bytes/record=" + ESTIMATED_SORT_BYTES_PER_RECORD
    );
    return (int) Math.max(
      MIN_SORT_BATCH_SIZE,
      Math.min((long) MAX_SORT_BATCH_SIZE, Math.min(requestedRecords, Integer.MAX_VALUE - 8L))
    );
  }

  private static long resolveExportMemoryLimitBytes(final @NotNull Consumer<String> logger) {
    final var configured = firstNonBlank(
      System.getProperty("hict.export.maxMemoryBytes"),
      System.getenv("HICT_EXPORT_MAX_MEMORY_BYTES"),
      System.getenv("HICT_CONVERSION_MAX_MEMORY_BYTES")
    );
    if (configured == null) {
      return DEFAULT_EXPORT_MEMORY_LIMIT_BYTES;
    }
    try {
      final long parsed = parseByteSize(configured);
      if (parsed <= 0L) {
        throw new IllegalArgumentException("must be positive");
      }
      return parsed;
    } catch (final RuntimeException err) {
      logger.accept("Ignoring invalid HiCT export memory limit '" + configured + "': " + err.getMessage());
      return DEFAULT_EXPORT_MEMORY_LIMIT_BYTES;
    }
  }

  private static long parseByteSize(final @NotNull String rawValue) {
    final var value = rawValue.trim().toLowerCase(java.util.Locale.ROOT);
    if (value.isBlank()) {
      throw new IllegalArgumentException("blank byte size");
    }
    int suffixStart = value.length();
    while (suffixStart > 0 && Character.isLetter(value.charAt(suffixStart - 1))) {
      suffixStart--;
    }
    final var numberPart = value.substring(0, suffixStart).trim();
    final var suffix = value.substring(suffixStart).trim();
    final long multiplier = switch (suffix) {
      case "", "b", "bytes" -> 1L;
      case "k", "kb", "kib" -> 1024L;
      case "m", "mb", "mib" -> 1024L * 1024L;
      case "g", "gb", "gib" -> 1024L * 1024L * 1024L;
      case "t", "tb", "tib" -> 1024L * 1024L * 1024L * 1024L;
      default -> throw new IllegalArgumentException("unsupported size suffix: " + suffix);
    };
    final double number = Double.parseDouble(numberPart);
    if (!Double.isFinite(number) || number <= 0.0d) {
      throw new IllegalArgumentException("byte size must be positive");
    }
    final double bytes = number * multiplier;
    if (bytes > Long.MAX_VALUE) {
      throw new IllegalArgumentException("byte size is too large");
    }
    return Math.max(1L, (long) bytes);
  }

  private static @NotNull String formatByteSize(final long bytes) {
    final double gib = bytes / (1024.0d * 1024.0d * 1024.0d);
    if (gib >= 1.0d) {
      return String.format(java.util.Locale.ROOT, "%.1f GiB", gib);
    }
    final double mib = bytes / (1024.0d * 1024.0d);
    if (mib >= 1.0d) {
      return String.format(java.util.Locale.ROOT, "%.1f MiB", mib);
    }
    final double kib = bytes / 1024.0d;
    if (kib >= 1.0d) {
      return String.format(java.util.Locale.ROOT, "%.1f KiB", kib);
    }
    return bytes + " B";
  }

  private static @Nullable String firstNonBlank(final @Nullable String... values) {
    for (final var value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  private record SortedCooSummary(long nonzeroPixelCount, long totalCounts, long @NotNull [] bin1Offset) {
  }

  public record CoolerAssemblyLayout(
    @NotNull List<CoolerChrom> chroms,
    @NotNull List<ResolutionLayout> resolutionLayouts
  ) {
    public @NotNull ResolutionLayout resolutionLayout(final long resolution) {
      for (final var layout : resolutionLayouts) {
        if (layout.resolution() == resolution) {
          return layout;
        }
      }
      throw new IllegalArgumentException("Resolution " + resolution + " is not present in the Cooler assembly layout");
    }
  }

  public record CoolerChrom(@NotNull String name, long lengthBp, long direction, long scaffoldId) {
    public CoolerChrom(final @NotNull String name, final long lengthBp) {
      this(name, lengthBp, ContigDirection.FORWARD.ordinal(), -1L);
    }
  }

  public record ResolutionLayout(
    long resolution,
    @NotNull List<CoolerChrom> chroms,
    @NotNull List<ContigBinSpan> spans,
    long @NotNull [] chromOffsets,
    long binsCount
  ) {
  }

  public record ContigBinSpan(
    int chromIndex,
    @NotNull CoolerChrom chrom,
    long globalBinStart,
    long globalBinEnd,
    @NotNull List<ATUDescriptor> atus
  ) {
  }

  private static final class BinWeightCursor {
    private final @NotNull ResolutionLayout layout;
    private int spanIndex = 0;
    private int atuIndex = 0;
    private long atuLocalStart = 0L;

    private BinWeightCursor(final @NotNull ResolutionLayout layout) {
      this.layout = layout;
    }

    private double weightAt(final long binIndex) {
      if (layout.spans().isEmpty()) {
        return 1.0d;
      }
      while (spanIndex + 1 < layout.spans().size() && binIndex >= layout.spans().get(spanIndex).globalBinEnd()) {
        spanIndex++;
        atuIndex = 0;
        atuLocalStart = 0L;
      }

      final var span = layout.spans().get(spanIndex);
      if (binIndex < span.globalBinStart() || binIndex >= span.globalBinEnd() || span.atus().isEmpty()) {
        return 1.0d;
      }

      final long localBin = binIndex - span.globalBinStart();
      while (atuIndex < span.atus().size()) {
        final var atu = span.atus().get(atuIndex);
        final long atuEnd = atuLocalStart + atu.getLength();
        if (localBin < atuEnd) {
          return weightFromAtu(atu, (int) (localBin - atuLocalStart));
        }
        atuLocalStart = atuEnd;
        atuIndex++;
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
      } catch (java.io.EOFException eof) {
        this.record = null;
        return false;
      }
    }

    private @NotNull CooRecord record() {
      if (record == null) {
        throw new IllegalStateException("Chunk cursor is not positioned on a record");
      }
      return record;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }
  }

  private static void deleteRecursively(final @NotNull Path path, final @NotNull Consumer<String> logger) {
    if (!Files.exists(path)) {
      return;
    }
    try (final var stream = Files.walk(path)) {
      stream.sorted(Comparator.reverseOrder()).forEach(candidate -> {
        try {
          Files.deleteIfExists(candidate);
        } catch (IOException e) {
          logger.accept("Failed to delete temp path " + candidate + ": " + e.getMessage());
        }
      });
    } catch (IOException e) {
      logger.accept("Failed to cleanup temp directory " + path + ": " + e.getMessage());
    }
  }
}
