package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5DataClass;
import ch.systemsx.cisd.hdf5.HDF5FloatStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.assembly.AssemblyLayoutConverter;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.StripeDescriptor;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.nio.file.Path;

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
  private static final long DEFAULT_IMPORT_MAX_MEMORY_BYTES = 16L * 1024L * 1024L * 1024L;
  private static final long MIN_IMPORT_MAX_MEMORY_BYTES = 256L * 1024L * 1024L;

  public void convert(final @NotNull ConversionOptions options, final @NotNull Consumer<String> logConsumer) {
    HDF5LibraryInitializer.initializeHDF5Library();
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
      final var importMaxMemoryBytes = resolveImportMaxMemoryBytes();
      synchronizedLogConsumer.accept(
        "Converting .mcool -> .hict.hdf5, workers=" + requestedWorkers + ", resolutions=" + conversionOrder +
          ", compressionAlgorithm=" + options.compressionAlgorithm() + ", compressionLevel=" + options.compressionLevel() +
          ", importMemoryBudget=" + formatByteSize(importMaxMemoryBytes)
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
            options.inputPath(),
            resolution,
            options.chunkSize(),
            intStorageFeatures,
            floatStorageFeatures,
            requestedWorkers,
            importMaxMemoryBytes,
            synchronizedLogConsumer
          );
          progressTracker.markStep("Wrote resolution " + resolution);
        }

        if (!options.agpPath().isBlank()) {
          applyAssemblyLayoutToOutput(srcAgain, dst, selectedResolutions, options, requestedWorkers, intStorageFeatures, synchronizedLogConsumer);
          synchronizedLogConsumer.accept("Applied assembly layout to generated .hict from " + resolveLayoutPath(options).getFileName());
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

  private static void applyAssemblyLayoutToOutput(
    final @NotNull IHDF5Reader src,
    final @NotNull IHDF5Writer dst,
    final @NotNull List<Long> selectedResolutions,
    final @NotNull ConversionOptions options,
    final int parallelism,
    final @NotNull HDF5IntStorageFeatures intStorageFeatures,
    final @NotNull Consumer<String> logConsumer
  ) throws IOException {
    final var layoutPath = resolveLayoutPath(options);
    if (!Files.isRegularFile(layoutPath)) {
      throw new IllegalArgumentException("Assembly layout file not found: " + layoutPath);
    }

    final List<AGPProcessor.AGPFileRecord> agpRecords;
    try {
      agpRecords = AssemblyLayoutConverter.loadAgpRecords(layoutPath);
    } catch (NoSuchFieldException e) {
      throw new IOException("Failed to parse assembly layout " + layoutPath.getFileName(), e);
    }
    final var contigRecords = agpRecords.stream()
      .filter(AGPProcessor.ContigAGPRecord.class::isInstance)
      .map(AGPProcessor.ContigAGPRecord.class::cast)
      .toList();
    if (contigRecords.isEmpty()) {
      throw new IllegalArgumentException("Assembly layout does not contain any contig records: " + layoutPath.getFileName());
    }

    final String[] contigNames = new String[contigRecords.size()];
    final long[] contigDirections = new long[contigRecords.size()];
    final long[] orderedContigIds = new long[contigRecords.size()];
    final long[] contigScaffoldIds = new long[contigRecords.size()];
    final long[] contigLengthBp = new long[contigRecords.size()];
    final int[] sourceChromIds = new int[contigRecords.size()];
    final long[] componentStartBp0 = new long[contigRecords.size()];
    final long[] componentEndBpExclusive = new long[contigRecords.size()];
    final var scaffoldIds = new java.util.LinkedHashMap<String, Long>();
    final var sourceChromLayout = readSourceChromLayout(src, selectedResolutions.get(0));
    final boolean isJuiceboxAssemblyLayout = layoutPath.getFileName().toString().toLowerCase().endsWith(".assembly");
    final boolean allContigNamesResolve = contigRecords.stream()
      .allMatch(record -> canResolveSourceChromIndex(sourceChromLayout, record.getContigName()));
    final boolean useSingleAssemblyChromosome = !allContigNamesResolve && sourceChromLayout.lengthsBp().length == 1;
    final boolean allowUnmappedJuiceboxContigs = !allContigNamesResolve && !useSingleAssemblyChromosome && isJuiceboxAssemblyLayout;
    if (!allContigNamesResolve && !useSingleAssemblyChromosome && !allowUnmappedJuiceboxContigs) {
      final var missingContigName = contigRecords.stream()
        .map(AGPProcessor.ContigAGPRecord::getContigName)
        .filter(name -> !canResolveSourceChromIndex(sourceChromLayout, name))
        .findFirst()
        .orElse("<unknown>");
      throw missingSourceChromException(missingContigName, layoutPath);
    }
    long assemblyChromosomeOffsetBp = 0L;
    final Map<String, SourceComponentRange> singleAssemblySourceRanges = useSingleAssemblyChromosome
      ? readSingleAssemblySourceRanges(layoutPath)
      : Map.of();
    int unmappedJuiceboxContigs = 0;

    for (int i = 0; i < contigRecords.size(); i++) {
      final var record = contigRecords.get(i);
      final long componentLengthBp = record.getIntraContigEndBpIncl() - record.getIntraContigStartBpIncl() + 1L;
      contigNames[i] = record.getContigName();
      contigDirections[i] = switch (record.getContigOrientation()) {
        case PLUS, UNKNOWN, IRRELEVANT -> 0L;
        case MINUS -> 1L;
      };
      orderedContigIds[i] = i;
      if (useSingleAssemblyChromosome) {
        sourceChromIds[i] = 0;
        final var sourceRange = singleAssemblySourceRanges.get(record.getContigName());
        if (sourceRange != null) {
          componentStartBp0[i] = sourceRange.startBp0() + record.getIntraContigStartBpIncl() - 1L;
          componentEndBpExclusive[i] = sourceRange.startBp0() + record.getIntraContigEndBpIncl();
        } else {
          componentStartBp0[i] = assemblyChromosomeOffsetBp;
          componentEndBpExclusive[i] = assemblyChromosomeOffsetBp + componentLengthBp;
        }
        assemblyChromosomeOffsetBp = componentEndBpExclusive[i];
      } else if (allowUnmappedJuiceboxContigs && !canResolveSourceChromIndex(sourceChromLayout, record.getContigName())) {
        sourceChromIds[i] = -1;
        componentStartBp0[i] = 0L;
        componentEndBpExclusive[i] = componentLengthBp;
        unmappedJuiceboxContigs++;
      } else {
        sourceChromIds[i] = resolveSourceChromIndex(sourceChromLayout, record.getContigName(), layoutPath);
        componentStartBp0[i] = record.getIntraContigStartBpIncl() - 1L;
        componentEndBpExclusive[i] = record.getIntraContigEndBpIncl();
      }
      if (sourceChromIds[i] >= 0) {
        final long sourceChromLengthBp = sourceChromLayout.lengthsBp()[sourceChromIds[i]];
        if (componentStartBp0[i] < 0L || componentEndBpExclusive[i] <= componentStartBp0[i] || componentEndBpExclusive[i] > sourceChromLengthBp) {
          throw new IllegalArgumentException(
            "Assembly layout component " + record.getContigName() + ":" + record.getIntraContigStartBpIncl() +
              "-" + record.getIntraContigEndBpIncl() + " is outside source contig length " + sourceChromLengthBp +
              " from " + layoutPath.getFileName()
          );
        }
      }
      contigLengthBp[i] = componentLengthBp;
      contigScaffoldIds[i] = scaffoldIds.computeIfAbsent(record.getScaffoldName(), ignored -> (long) scaffoldIds.size());
    }
    if (unmappedJuiceboxContigs > 0) {
      logConsumer.accept(
        "Assembly layout contains " + unmappedJuiceboxContigs +
          " contig(s) that are absent from source .mcool chromosome metadata; keeping them as hidden zero-bin contigs."
      );
    }

    final long totalAssemblyLengthBp = Arrays.stream(contigLengthBp).sum();
    if (totalAssemblyLengthBp <= 0L) {
      throw new IllegalArgumentException("Assembly layout contains no sequence: " + layoutPath.getFileName());
    }

    final Map<Long, long[]> contigStartBinsByResolution = new HashMap<>();
    final Map<Long, long[]> contigLengthBinsByResolution = new HashMap<>();
    final Map<Long, List<StripeDescriptor>> stripesByResolution = new HashMap<>();

    for (final var resolution : selectedResolutions) {
      final long[] chromOffsets = src.int64().readArray("/resolutions/" + resolution + "/indexes/chrom_offset");
      if (chromOffsets.length != sourceChromLayout.lengthsBp().length + 1) {
        throw new IllegalStateException(
          "Resolution " + resolution + " has " + chromOffsets.length +
            " chromosome offset entries, but source chromosome metadata has " + sourceChromLayout.lengthsBp().length + " chromosomes"
        );
      }
      final long[] startBins = new long[contigRecords.size()];
      final long[] lengthBins = new long[contigRecords.size()];
      final long[] sourceBinStarts = src.int64().readArray("/resolutions/" + resolution + "/bins/start");
      final long[] sourceBinEnds = src.int64().readArray("/resolutions/" + resolution + "/bins/end");
      for (int i = 0; i < contigRecords.size(); i++) {
        if (sourceChromIds[i] < 0) {
          startBins[i] = 0L;
          lengthBins[i] = 0L;
          continue;
        }
        final var range = resolveSourceBinRange(
          resolution,
          sourceChromIds[i],
          componentStartBp0[i],
          componentEndBpExclusive[i],
          chromOffsets,
          sourceBinStarts,
          sourceBinEnds
        );
        startBins[i] = range.startBin();
        lengthBins[i] = range.lengthBins();
      }
      contigStartBinsByResolution.put(resolution, startBins);
      contigLengthBinsByResolution.put(resolution, lengthBins);
      stripesByResolution.put(resolution, buildStripeDescriptorsOnly(src, resolution, resolveNameLengthPath(src, resolution)));
    }

    dst.string().writeArray(getContigNameDatasetPath(), contigNames);
    dst.int64().writeArray(getContigDirectionDatasetPath(), contigDirections, intStorageFeatures);
    dst.int64().writeArray(getContigOrderDatasetPath(), orderedContigIds, intStorageFeatures);
    dst.int64().writeArray("/contig_info/contig_scaffold_id", contigScaffoldIds, intStorageFeatures);
    dst.int64().writeArray(getContigLengthBpDatasetPath(), contigLengthBp, intStorageFeatures);

    for (final var resolution : selectedResolutions) {
      final var resolutionRoot = "/resolutions/" + resolution;
      if (!dst.object().isGroup(resolutionRoot + "/contigs")) {
        dst.object().createGroup(resolutionRoot + "/contigs");
      }
      if (!dst.object().isGroup(resolutionRoot + "/atl")) {
        dst.object().createGroup(resolutionRoot + "/atl");
      }

      final long[] contigLengthBins = contigLengthBinsByResolution.get(resolution);
      final byte[] hideTypes = new byte[contigRecords.size()];
      for (int i = 0; i < contigRecords.size(); i++) {
        hideTypes[i] = autoHideType(contigLengthBins[i], contigLengthBp[i], resolution);
      }

      dst.int64().writeArray(getContigLengthBinsDatasetPath(resolution), contigLengthBins, intStorageFeatures);
      dst.int8().writeArray(getContigHideTypeDatasetPath(resolution), hideTypes);

      final AtomicReferenceArray<List<ATUDescriptor>> atusByContig = new AtomicReferenceArray<>(contigRecords.size());
      runParallelFor(parallelism, contigRecords.size(), contigId -> {
        final var atus = contigLengthBins[contigId] > 0L
          ? generateAtusForContig(
            contigId,
            resolution,
            contigStartBinsByResolution,
            contigLengthBinsByResolution,
            stripesByResolution
          )
          : List.<ATUDescriptor>of();
        atusByContig.set(contigId, atus);
      });

      long totalAtuCount = 0L;
      for (int i = 0; i < contigRecords.size(); i++) {
        totalAtuCount += Objects.requireNonNull(atusByContig.get(i)).size();
      }

      final long[][] basisAtu = new long[(int) totalAtuCount][4];
      final long[][] contigsAtl = new long[(int) totalAtuCount][2];

      int atuCursor = 0;
      for (int contigId = 0; contigId < contigRecords.size(); contigId++) {
        final var atus = Objects.requireNonNull(atusByContig.get(contigId));
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

    logConsumer.accept(
      "Rewrote contig metadata from assembly layout using source bin offsets: contigs=" +
        contigRecords.size() + ", scaffolds=" + scaffoldIds.size() +
        (useSingleAssemblyChromosome ? ", source=single-assembly-chromosome" : ", source=named-contigs")
    );
  }

  private static @NotNull Map<String, SourceComponentRange> readSingleAssemblySourceRanges(final @NotNull Path layoutPath) throws IOException {
    if (!layoutPath.getFileName().toString().toLowerCase().endsWith(".assembly")) {
      return Map.of();
    }
    final Map<String, SourceComponentRange> rangesByContigName = new HashMap<>();
    try (final var reader = Files.newBufferedReader(layoutPath)) {
      for (final var contig : AssemblyLayoutConverter.parseJuiceboxContigs(reader)) {
        final var previous = rangesByContigName.put(
          contig.contigName(),
          new SourceComponentRange(contig.sourceStartBp0(), contig.sourceStartBp0() + contig.lengthBp())
        );
        if (previous != null) {
          throw new IllegalArgumentException(
            "Juicebox assembly file contains duplicate contig name '" + contig.contigName() +
              "'; source offsets for hictk single-assembly conversion would be ambiguous"
          );
        }
      }
    }
    return rangesByContigName;
  }

  private static @NotNull SourceChromLayout readSourceChromLayout(final @NotNull IHDF5Reader src, final long resolution) {
    final var nameLengthPath = resolveNameLengthPath(src, resolution);
    final long[] chromLengths = src.int64().readArray(nameLengthPath + "/length");
    final String[] chromNames = src.string().readArray(nameLengthPath + "/name");
    if (chromLengths.length != chromNames.length) {
      throw new IllegalStateException("Chromosome lengths and names have different sizes at " + nameLengthPath);
    }
    final Map<String, Integer> chromIdsByName = new HashMap<>();
    for (int i = 0; i < chromNames.length; i++) {
      final var name = chromNames[i];
      if (name == null || name.isBlank()) {
        throw new IllegalStateException("Blank chromosome name at index " + i + " in " + nameLengthPath);
      }
      chromIdsByName.putIfAbsent(name, i);
      chromIdsByName.putIfAbsent(name.trim(), i);
    }
    return new SourceChromLayout(chromIdsByName, chromLengths);
  }

  private static int resolveSourceChromIndex(
    final @NotNull SourceChromLayout sourceChromLayout,
    final @NotNull String contigName,
    final @NotNull Path layoutPath
  ) {
    final var sourceChromId = sourceChromLayout.chromIdsByName().get(contigName);
    if (sourceChromId != null) {
      return sourceChromId;
    }
    final var trimmed = contigName.trim();
    final var trimmedSourceChromId = sourceChromLayout.chromIdsByName().get(trimmed);
    if (trimmedSourceChromId != null) {
      return trimmedSourceChromId;
    }
    throw missingSourceChromException(contigName, layoutPath);
  }

  private static boolean canResolveSourceChromIndex(
    final @NotNull SourceChromLayout sourceChromLayout,
    final @NotNull String contigName
  ) {
    return sourceChromLayout.chromIdsByName().containsKey(contigName) || sourceChromLayout.chromIdsByName().containsKey(contigName.trim());
  }

  private static @NotNull IllegalArgumentException missingSourceChromException(
    final @NotNull String contigName,
    final @NotNull Path layoutPath
  ) {
    return new IllegalArgumentException(
      "Assembly layout contig '" + contigName + "' from " + layoutPath.getFileName() +
        " is not present in source .mcool chromosome metadata"
    );
  }

  private static @NotNull BinRange resolveSourceBinRange(
    final long resolution,
    final int sourceChromId,
    final long componentStartBp0,
    final long componentEndBpExclusive,
    final long @NotNull [] chromOffsets,
    final long @NotNull [] sourceBinStarts,
    final long @NotNull [] sourceBinEnds
  ) {
    final long chromStartBin = chromOffsets[sourceChromId];
    final long chromEndBin = chromOffsets[sourceChromId + 1];
    if (chromStartBin < 0L || chromEndBin < chromStartBin || chromEndBin > sourceBinStarts.length || chromEndBin > sourceBinEnds.length) {
      throw new IllegalStateException(
        "Resolution " + resolution + " has invalid bin offsets for source chromosome index " + sourceChromId +
          ": [" + chromStartBin + ", " + chromEndBin + ")"
      );
    }
    if (chromStartBin == chromEndBin || componentStartBp0 == componentEndBpExclusive) {
      return new BinRange(chromStartBin, chromStartBin);
    }

    long firstBin = chromStartBin;
    if (componentStartBp0 > 0L) {
      firstBin = chromEndBin;
      for (long bin = chromStartBin; bin < chromEndBin; bin++) {
        if (sourceBinStarts[(int) bin] >= componentStartBp0) {
          firstBin = bin;
          break;
        }
      }
    }

    long endBinExclusive = chromEndBin;
    for (long bin = firstBin; bin < chromEndBin; bin++) {
      if (sourceBinStarts[(int) bin] >= componentEndBpExclusive) {
        endBinExclusive = bin;
        break;
      }
    }

    if (firstBin > endBinExclusive) {
      firstBin = endBinExclusive;
    }
    return new BinRange(firstBin, endBinExclusive);
  }

  private record SourceChromLayout(@NotNull Map<String, Integer> chromIdsByName, long @NotNull [] lengthsBp) {
  }

  private record SourceComponentRange(long startBp0, long endBpExclusive) {
  }

  private record BinRange(long startBin, long endBinExclusive) {
    private long lengthBins() {
      return endBinExclusive - startBin;
    }
  }

  private static @NotNull Path resolveLayoutPath(final @NotNull ConversionOptions options) {
    final var layoutPath = Path.of(options.agpPath());
    if (layoutPath.isAbsolute()) {
      return layoutPath.normalize();
    }
    return options.inputPath().resolveSibling(layoutPath).normalize();
  }

  private static void writeResolutionDirect(
    final @NotNull IHDF5Reader src,
    final @NotNull IHDF5Writer dst,
    final @NotNull java.nio.file.Path inputPath,
    final long resolution,
    final int chunkSize,
    final @NotNull HDF5IntStorageFeatures intStorageFeatures,
    final @NotNull HDF5FloatStorageFeatures floatStorageFeatures,
    final int stripeWorkersRequested,
    final long importMaxMemoryBytes,
    final @NotNull Consumer<String> logConsumer
  ) {
    final long startedNanos = System.nanoTime();
    checkInterrupted();
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
    final var sourceCountsPath = resolutionRoot + "/pixels/count";
    final var floatingPointSignal = isFloatingPointDataset(src, sourceCountsPath);

    logConsumer.accept("Resolution " + resolution + ": counting sparse and dense blocks");
    final var countingProgress = new PhaseProgressTracker(
      "Resolution " + resolution + " count",
      stripeCount,
      logConsumer
    );
    final int stripeWorkers = Math.max(1, Math.min(stripeWorkersRequested, Math.max(1, stripeCount)));
    final var counts = countDenseAndSparse(inputPath, resolution, stripeCount, allRowsStartIndices, stripeWorkers, floatingPointSignal, importMaxMemoryBytes, countingProgress::report);
    countingProgress.finish();
    final var denseBlockCount = counts.denseTotal();
    logConsumer.accept("Resolution " + resolution + ": finished counting blocks, denseBlocks=" + denseBlockCount);

    final var blockRowsPath = getBlockRowsDatasetPath(resolution);
    final var blockColsPath = getBlockColsDatasetPath(resolution);
    final var blockValsPath = getBlockValuesDatasetPath(resolution);
    final var blockOffsetPath = getBlockOffsetDatasetPath(resolution);
    final var blockLengthPath = getBlockLengthDatasetPath(resolution);
    final var denseBlocksPath = getDenseBlockDatasetPath(resolution);

    dst.int64().createArray(blockRowsPath, nonzeroPixelCount, safeChunkLen(nonzeroPixelCount, chunkSize), intStorageFeatures);
    dst.int64().createArray(blockColsPath, nonzeroPixelCount, safeChunkLen(nonzeroPixelCount, chunkSize), intStorageFeatures);
    createNumericArray(dst, blockValsPath, nonzeroPixelCount, chunkSize, floatingPointSignal, intStorageFeatures, floatStorageFeatures);

    final var totalBlockCount = (long) stripeCount * stripeCount;
    dst.int64().createArray(blockOffsetPath, totalBlockCount, safeChunkLen(totalBlockCount, chunkSize), intStorageFeatures);
    dst.int64().createArray(blockLengthPath, totalBlockCount, safeChunkLen(totalBlockCount, chunkSize), intStorageFeatures);

    final var denseDatasetSize = Math.max(1L, denseBlockCount);
    createNumericMDArray(
      dst,
      denseBlocksPath,
      new long[]{denseDatasetSize, 1L, SUBMATRIX_SIZE, SUBMATRIX_SIZE},
      new int[]{1, 1, SUBMATRIX_SIZE, SUBMATRIX_SIZE},
      floatingPointSignal,
      intStorageFeatures,
      floatStorageFeatures
    );

    final var stripeSparseBase = new long[stripeCount];
    final var stripeDenseBase = new long[stripeCount];
    long sparseCursor = 0L;
    long denseCursor = 0L;
    for (int i = 0; i < stripeCount; i++) {
      stripeSparseBase[i] = sparseCursor;
      stripeDenseBase[i] = denseCursor;
      sparseCursor += counts.sparseCounts()[i];
      denseCursor += counts.denseCounts()[i];
    }
    final var writeProgress = new PhaseProgressTracker(
      "Resolution " + resolution + " write",
      stripeCount,
      logConsumer
    );
    if (stripeCount == 0) {
      writeProgress.finish();
      return;
    }

    final var errorRef = new AtomicReference<Throwable>();
    final ExecutorService stripeExecutor = Executors.newFixedThreadPool(stripeWorkers);
    final var readers = java.util.Collections.synchronizedList(new ArrayList<IHDF5Reader>());
    final ThreadLocal<IHDF5Reader> readerHolder = ThreadLocal.withInitial(() -> {
      HDF5LibraryInitializer.initializeHDF5Library();
      final IHDF5Reader reader = HDF5Factory.openForReading(inputPath.toFile());
      readers.add(reader);
      return reader;
    });
    final int batchSize = resolveBatchSize(stripeWorkers, stripeCount, nonzeroPixelCount, importMaxMemoryBytes);
    final int queueCapacity = resolveWriteQueueCapacity(stripeWorkers, importMaxMemoryBytes);
    logConsumer.accept(
      "Resolution " + resolution + ": workers=" + stripeWorkers +
        ", stripeBatchSize=" + batchSize +
        ", writeQueueCapacity=" + queueCapacity +
        ", nonzeroPixels=" + nonzeroPixelCount
    );
    final var queue = new java.util.concurrent.ArrayBlockingQueue<StripeWriteTask>(queueCapacity);
    final var writerThread = new Thread(() -> {
      long written = 0L;
      try {
        while (true) {
          final var task = queue.take();
          if (task == StripeWriteTask.POISON) {
            break;
          }
          if (task.blocks() != EMPTY_BLOCKS) {
            saveStripeBlocks(
              dst,
              task.stripeIndex(),
              stripeCount,
              task.blocks(),
              stripeSparseBase[task.stripeIndex()],
              stripeDenseBase[task.stripeIndex()],
              blockRowsPath,
              blockColsPath,
              blockValsPath,
              blockOffsetPath,
              blockLengthPath,
              denseBlocksPath,
              floatingPointSignal
            );
          }
          written++;
          writeProgress.report((int) written);

          final int percent = (int) (written * 100L / stripeCount);
          final var elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
          final var etaMillis = estimateEtaMillis(written, stripeCount, elapsedMillis);
          logConsumer.accept(
            String.format(
              "Resolution %d write: %d%% (%d/%d stripes), elapsed=%s, eta=%s",
              resolution,
              percent,
              written,
              stripeCount,
              formatDuration(elapsedMillis),
              formatDuration(etaMillis)
            )
          );
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        errorRef.compareAndSet(null, e);
      }
    }, "hict-writer-" + resolution);
    writerThread.setDaemon(true);
    writerThread.start();

    final List<Future<?>> futures = new ArrayList<>();
    for (int batchStart = 0; batchStart < stripeCount; batchStart += batchSize) {
      final int start = batchStart;
      final int end = Math.min(stripeCount, batchStart + batchSize);
      futures.add(stripeExecutor.submit(() -> {
        try {
          final var batchBlocks = readStripeBatch(readerHolder.get(), resolution, start, end, allRowsStartIndices, floatingPointSignal);
          for (int i = 0; i < batchBlocks.length; i++) {
            checkInterrupted();
            final int stripeIdx = start + i;
            final var block = batchBlocks[i];
            final StripeBlocks blocks = block.length() > 0 ? buildStripeBlocks(block, stripeCount) : EMPTY_BLOCKS;
            queue.put(new StripeWriteTask(stripeIdx, blocks));
          }
        } catch (Throwable t) {
          errorRef.compareAndSet(null, t);
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
    } finally {
      try {
        queue.put(StripeWriteTask.POISON);
        writerThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        stripeExecutor.shutdown();
        synchronized (readers) {
          for (final var reader : readers) {
            try {
              reader.close();
            } catch (Exception ignored) {
            }
          }
        }
      }
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

  private static SaveBlockResult saveStripeBlocks(
    final @NotNull IHDF5Writer dst,
    final int rowStripe,
    final int stripeCount,
    final @NotNull StripeBlocks blocks,
    final long stripeSparseOffset,
    final long stripeDenseOffset,
    final @NotNull String blockRowsPath,
    final @NotNull String blockColsPath,
    final @NotNull String blockValsPath,
    final @NotNull String blockOffsetPath,
    final @NotNull String blockLengthPath,
    final @NotNull String denseBlocksPath,
    final boolean floatingPointSignal
  ) {
    long sparseOffset = stripeSparseOffset;
    long denseOffset = stripeDenseOffset;

    final var colStripes = blocks.colStripes();
    final var lengths = blocks.blockLengths();
    final var denseFlags = blocks.denseFlags();
    final var sparseRows = blocks.sparseRows();
    final var sparseCols = blocks.sparseCols();
    long stripeSparseLen = 0L;
    for (int i = 0; i < colStripes.length; i++) {
      if (!denseFlags[i]) {
        stripeSparseLen += lengths[i];
      }
    }

    final long[] offsetRow = new long[stripeCount];
    final long[] lengthRow = new long[stripeCount];
    final long[] stripeRows = stripeSparseLen > 0 ? new long[(int) stripeSparseLen] : new long[0];
    final long[] stripeCols = stripeSparseLen > 0 ? new long[(int) stripeSparseLen] : new long[0];
    int stripePos = 0;
    if (floatingPointSignal) {
      final var typedBlocks = (DoubleStripeBlocks) blocks;
      final var sparseVals = typedBlocks.sparseVals();
      final var denseFlats = typedBlocks.denseFlats();
      final double[] stripeVals = stripeSparseLen > 0 ? new double[(int) stripeSparseLen] : new double[0];
      for (int i = 0; i < colStripes.length; i++) {
        final int blockLen = lengths[i];
        if (blockLen <= 0) {
          continue;
        }
        final int colStripe = colStripes[i];

        if (denseFlags[i]) {
          offsetRow[colStripe] = -denseOffset - 1L;
          lengthRow[colStripe] = blockLen;
          final var denseMd = new MDDoubleArray(denseFlats[i], new int[]{1, 1, SUBMATRIX_SIZE, SUBMATRIX_SIZE});
          dst.float64().writeMDArrayBlockWithOffset(denseBlocksPath, denseMd, new long[]{denseOffset, 0L, 0L, 0L});
          denseOffset++;
        } else {
          offsetRow[colStripe] = sparseOffset;
          lengthRow[colStripe] = blockLen;
          final var blockRows = sparseRows[i];
          final var blockCols = sparseCols[i];
          final var blockVals = sparseVals[i];
          System.arraycopy(blockRows, 0, stripeRows, stripePos, blockLen);
          System.arraycopy(blockCols, 0, stripeCols, stripePos, blockLen);
          System.arraycopy(blockVals, 0, stripeVals, stripePos, blockLen);
          stripePos += blockLen;
          sparseOffset += blockLen;
        }
      }
      final long rowBase = (long) rowStripe * stripeCount;
      dst.int64().writeArrayBlockWithOffset(blockOffsetPath, offsetRow, stripeCount, rowBase);
      dst.int64().writeArrayBlockWithOffset(blockLengthPath, lengthRow, stripeCount, rowBase);
      if (stripeSparseLen > 0) {
        dst.int64().writeArrayBlockWithOffset(blockRowsPath, stripeRows, (int) stripeSparseLen, stripeSparseOffset);
        dst.int64().writeArrayBlockWithOffset(blockColsPath, stripeCols, (int) stripeSparseLen, stripeSparseOffset);
        dst.float64().writeArrayBlockWithOffset(blockValsPath, stripeVals, (int) stripeSparseLen, stripeSparseOffset);
      }
    } else {
      final var typedBlocks = (LongStripeBlocks) blocks;
      final var sparseVals = typedBlocks.sparseVals();
      final var denseFlats = typedBlocks.denseFlats();
      final long[] stripeVals = stripeSparseLen > 0 ? new long[(int) stripeSparseLen] : new long[0];
      for (int i = 0; i < colStripes.length; i++) {
        final int blockLen = lengths[i];
        if (blockLen <= 0) {
          continue;
        }
        final int colStripe = colStripes[i];

        if (denseFlags[i]) {
          offsetRow[colStripe] = -denseOffset - 1L;
          lengthRow[colStripe] = blockLen;
          dst.int64().writeMDArrayBlockWithOffset(
            denseBlocksPath,
            new MDLongArray(denseFlats[i], new int[]{1, 1, SUBMATRIX_SIZE, SUBMATRIX_SIZE}),
            new long[]{denseOffset, 0L, 0L, 0L}
          );
          denseOffset++;
        } else {
          offsetRow[colStripe] = sparseOffset;
          lengthRow[colStripe] = blockLen;
          final var blockRows = sparseRows[i];
          final var blockCols = sparseCols[i];
          final var blockVals = sparseVals[i];
          System.arraycopy(blockRows, 0, stripeRows, stripePos, blockLen);
          System.arraycopy(blockCols, 0, stripeCols, stripePos, blockLen);
          System.arraycopy(blockVals, 0, stripeVals, stripePos, blockLen);
          stripePos += blockLen;
          sparseOffset += blockLen;
        }
      }
      final long rowBase = (long) rowStripe * stripeCount;
      dst.int64().writeArrayBlockWithOffset(blockOffsetPath, offsetRow, stripeCount, rowBase);
      dst.int64().writeArrayBlockWithOffset(blockLengthPath, lengthRow, stripeCount, rowBase);
      if (stripeSparseLen > 0) {
        dst.int64().writeArrayBlockWithOffset(blockRowsPath, stripeRows, (int) stripeSparseLen, stripeSparseOffset);
        dst.int64().writeArrayBlockWithOffset(blockColsPath, stripeCols, (int) stripeSparseLen, stripeSparseOffset);
        dst.int64().writeArrayBlockWithOffset(blockValsPath, stripeVals, (int) stripeSparseLen, stripeSparseOffset);
      }
    }
    return new SaveBlockResult(sparseOffset, denseOffset);
  }

  private static @NotNull StripeCountDetails countDenseAndSparse(
    final @NotNull java.nio.file.Path inputPath,
    final long resolution,
    final int stripeCount,
    final long @NotNull [] allRowsStartIndices,
    final int stripeWorkers,
    final boolean floatingPointSignal,
    final long importMaxMemoryBytes,
    final @NotNull java.util.function.IntConsumer countingProgressReporter
  ) {
    if (stripeCount <= 0) {
      return new StripeCountDetails(new long[0], new long[0], 0L, 0L);
    }

    final ExecutorService stripeExecutor = stripeWorkers > 1 ? Executors.newFixedThreadPool(stripeWorkers) : null;
    final var readers = java.util.Collections.synchronizedList(new ArrayList<IHDF5Reader>());
    final ThreadLocal<IHDF5Reader> readerHolder = ThreadLocal.withInitial(() -> {
      HDF5LibraryInitializer.initializeHDF5Library();
      final IHDF5Reader reader = HDF5Factory.openForReading(inputPath.toFile());
      readers.add(reader);
      return reader;
    });
    try {
      final long[] sparseCounts = new long[stripeCount];
      final long[] denseCounts = new long[stripeCount];
      final long nonzeroPixelCount = allRowsStartIndices.length == 0 ? 0L : allRowsStartIndices[allRowsStartIndices.length - 1];
      final int batchSize = resolveBatchSize(stripeWorkers, stripeCount, nonzeroPixelCount, importMaxMemoryBytes);
      if (stripeExecutor == null) {
        for (int batchStart = 0; batchStart < stripeCount; batchStart += batchSize) {
          checkInterrupted();
          final int end = Math.min(stripeCount, batchStart + batchSize);
          final var blocks = readStripeBatch(readerHolder.get(), resolution, batchStart, end, allRowsStartIndices, floatingPointSignal);
          for (int i = 0; i < blocks.length; i++) {
            final int stripeIdx = batchStart + i;
            final var block = blocks[i];
            if (block.length() > 0) {
              final var counts = countStripeBlocks(block, stripeCount);
              sparseCounts[stripeIdx] = counts.sparseElementCount();
              denseCounts[stripeIdx] = counts.denseBlockCount();
            }
            countingProgressReporter.accept(stripeIdx + 1);
          }
        }
      } else {
        final List<Future<?>> futures = new ArrayList<>();
        for (int batchStart = 0; batchStart < stripeCount; batchStart += batchSize) {
          final int start = batchStart;
          final int end = Math.min(stripeCount, batchStart + batchSize);
          futures.add(stripeExecutor.submit(() -> {
            checkInterrupted();
            final var blocks = readStripeBatch(readerHolder.get(), resolution, start, end, allRowsStartIndices, floatingPointSignal);
            for (int i = 0; i < blocks.length; i++) {
              final int stripeIdx = start + i;
              final var block = blocks[i];
              if (block.length() > 0) {
                final var counts = countStripeBlocks(block, stripeCount);
                sparseCounts[stripeIdx] = counts.sparseElementCount();
                denseCounts[stripeIdx] = counts.denseBlockCount();
              }
              countingProgressReporter.accept(stripeIdx + 1);
            }
          }));
        }

        try {
          for (final var future : futures) {
            future.get();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          throw new RuntimeException(e.getCause());
        }
      }

      long sparseTotal = 0L;
      long denseTotal = 0L;
      for (int i = 0; i < stripeCount; i++) {
        sparseTotal += sparseCounts[i];
        denseTotal += denseCounts[i];
      }
      return new StripeCountDetails(sparseCounts, denseCounts, sparseTotal, denseTotal);
    } finally {
      if (stripeExecutor != null) {
        stripeExecutor.shutdownNow();
      }
      synchronized (readers) {
        for (final var reader : readers) {
          try {
            reader.close();
          } catch (Exception ignored) {
          }
        }
      }
    }
  }

  private static @NotNull StripeCounts countStripeBlocks(
    final @NotNull PixelBlock block,
    final int stripeCount
  ) {
    final var cols = block.cols();
    final int offset = block.offset();
    final int n = block.length();
    if (offset == 0 && n == cols.length) {
      final var nativeCounts = NativeProcessingService.getInstance().tryCountStripeBlocks(
        cols,
        stripeCount,
        SUBMATRIX_SIZE,
        DENSE_THRESHOLD
      );
      if (nativeCounts != null) {
        return new StripeCounts(nativeCounts[0], nativeCounts[1]);
      }
    }
    final int maxTouched = Math.min(stripeCount, n);
    final var buffer = acquireCountBuffer(stripeCount, maxTouched);
    final int[] counts = buffer.counts();
    final int[] touched = buffer.touched();
    int touchedCount = 0;

    for (int i = 0; i < n; i++) {
      final int colStripe = (int) (cols[offset + i] / SUBMATRIX_SIZE);
      if (counts[colStripe]++ == 0) {
        touched[touchedCount++] = colStripe;
      }
    }

    Arrays.sort(touched, 0, touchedCount);
    long sparse = 0L;
    long dense = 0L;
    for (int i = 0; i < touchedCount; i++) {
      final int colStripe = touched[i];
      final int count = counts[colStripe];
      counts[colStripe] = 0;
      if (count >= DENSE_THRESHOLD) {
        dense++;
      } else {
        sparse += count;
      }
    }
    return new StripeCounts(sparse, dense);
  }

  private static @NotNull PixelBlock[] readStripeBatch(
    final @NotNull IHDF5Reader src,
    final long resolution,
    final int startStripe,
    final int endStripe,
    final long @NotNull [] allRowsStartIndices,
    final boolean floatingPointSignal
  ) {
    final int actualEnd = Math.max(startStripe, endStripe);
    final int count = actualEnd - startStripe;
    final PixelBlock[] blocks = new PixelBlock[count];
    if (count == 0) {
      return blocks;
    }
    final int startBin = startStripe * SUBMATRIX_SIZE;
    final int endBin = Math.min(actualEnd * SUBMATRIX_SIZE, allRowsStartIndices.length - 1);
    final long startOffset = allRowsStartIndices[startBin];
    final long endOffset = allRowsStartIndices[endBin];
    final int length = (int) (endOffset - startOffset);
    if (length <= 0) {
      for (int i = 0; i < count; i++) {
        blocks[i] = floatingPointSignal
          ? new DoublePixelBlock(new long[0], new long[0], new double[0])
          : new LongPixelBlock(new long[0], new long[0], new long[0]);
      }
      return blocks;
    }

    final var base = "/resolutions/" + resolution + "/pixels/";
    final var rows = src.int64().readArrayBlockWithOffset(base + "bin1_id", length, startOffset);
    final var cols = src.int64().readArrayBlockWithOffset(base + "bin2_id", length, startOffset);
    final var doubleVals = floatingPointSignal ? src.float64().readArrayBlockWithOffset(base + "count", length, startOffset) : null;
    final var longVals = floatingPointSignal ? null : src.int64().readArrayBlockWithOffset(base + "count", length, startOffset);

    for (int i = 0; i < count; i++) {
      final int stripeIdx = startStripe + i;
      final int stripeStartBin = stripeIdx * SUBMATRIX_SIZE;
      final int stripeEndBin = Math.min((stripeIdx + 1) * SUBMATRIX_SIZE, allRowsStartIndices.length - 1);
      final int stripeStart = (int) (allRowsStartIndices[stripeStartBin] - startOffset);
      final int stripeEnd = (int) (allRowsStartIndices[stripeEndBin] - startOffset);
      final int stripeLen = Math.max(0, stripeEnd - stripeStart);
      if (stripeLen <= 0) {
        blocks[i] = floatingPointSignal
          ? new DoublePixelBlock(new long[0], new long[0], new double[0])
          : new LongPixelBlock(new long[0], new long[0], new long[0]);
      } else {
        blocks[i] = floatingPointSignal
          ? new DoublePixelBlock(rows, cols, doubleVals, stripeStart, stripeLen)
          : new LongPixelBlock(rows, cols, longVals, stripeStart, stripeLen);
      }
    }
    return blocks;
  }

  private static int resolveBatchSize(
    final int stripeWorkers,
    final int stripeCount,
    final long nonzeroPixelCount,
    final long importMaxMemoryBytes
  ) {
    if (stripeCount <= 1) {
      return 1;
    }
    final long avgPixelsPerStripe = Math.max(1L, (nonzeroPixelCount + stripeCount - 1L) / stripeCount);
    final long workerCount = Math.max(1L, stripeWorkers);
    final long usableBytesPerWorker = Math.max(MIN_IMPORT_MAX_MEMORY_BYTES / 4L, importMaxMemoryBytes / Math.max(2L, workerCount + 1L));
    final long conservativeBytesPerPixel = 96L;
    final long targetPixelsPerBatch = Math.max(1L, usableBytesPerWorker / conservativeBytesPerPixel);
    final long memoryBoundBatch = Math.max(1L, targetPixelsPerBatch / avgPixelsPerStripe);
    final int workerFloor = stripeWorkers <= 1 ? 1 : 4;
    final int adaptive = (int) Math.min(256L, memoryBoundBatch);
    return Math.max(1, Math.min(stripeCount, Math.max(workerFloor, adaptive)));
  }

  private static int resolveWriteQueueCapacity(final int stripeWorkers, final long importMaxMemoryBytes) {
    final long gib = 1024L * 1024L * 1024L;
    final int memoryBound = importMaxMemoryBytes < 2L * gib ? 2 : importMaxMemoryBytes < 8L * gib ? 4 : 8;
    return Math.max(2, Math.min(Math.max(2, stripeWorkers * 2), memoryBound));
  }

  private static @NotNull StripeBlocks buildStripeBlocks(
    final @NotNull PixelBlock block,
    final int stripeCount
  ) {
    if (block instanceof DoublePixelBlock doubleBlock) {
      return buildStripeBlocks(doubleBlock, stripeCount);
    }
    if (block instanceof LongPixelBlock longBlock) {
      return buildStripeBlocks(longBlock, stripeCount);
    }
    throw new IllegalStateException("Unsupported pixel block type: " + block.getClass().getName());
  }

  private static @NotNull StripeBlocks buildStripeBlocks(
    final @NotNull DoublePixelBlock block,
    final int stripeCount
  ) {
    final var rows = block.rows();
    final var cols = block.cols();
    final var values = block.values();
    final int offset = block.offset();
    final int n = block.length();

    final int maxTouched = Math.min(stripeCount, n);
    final var buffer = acquireCountBuffer(stripeCount, maxTouched);
    final int[] counts = buffer.counts();
    final int[] touched = buffer.touched();
    int touchedCount = 0;

    for (int i = 0; i < n; i++) {
      final int sourceIndex = offset + i;
      final int colStripe = (int) (cols[sourceIndex] / SUBMATRIX_SIZE);
      if (counts[colStripe]++ == 0) {
        touched[touchedCount++] = colStripe;
      }
    }

    Arrays.sort(touched, 0, touchedCount);
    final int nonEmptyBlocks = touchedCount;
    final int[] blockColStripes = new int[nonEmptyBlocks];
    final boolean[] denseFlags = new boolean[nonEmptyBlocks];
    final int[] blockLengths = new int[nonEmptyBlocks];

    for (int i = 0; i < touchedCount; i++) {
      final int colStripe = touched[i];
      final int count = counts[colStripe];
      counts[colStripe] = 0;
      blockColStripes[i] = colStripe;
      blockLengths[i] = count;
      denseFlags[i] = count >= DENSE_THRESHOLD;
    }

    final long[][] sparseRows = new long[nonEmptyBlocks][];
    final long[][] sparseCols = new long[nonEmptyBlocks][];
    final double[][] sparseVals = new double[nonEmptyBlocks][];
    final double[][] denseFlats = new double[nonEmptyBlocks][];

    for (int i = 0; i < nonEmptyBlocks; i++) {
      if (denseFlags[i]) {
        denseFlats[i] = new double[SUBMATRIX_SIZE * SUBMATRIX_SIZE];
      } else {
        final int len = blockLengths[i];
        sparseRows[i] = new long[len];
        sparseCols[i] = new long[len];
        sparseVals[i] = new double[len];
      }
    }

    final int[] colStripeToIndex = new int[stripeCount];
    Arrays.fill(colStripeToIndex, -1);
    for (int i = 0; i < nonEmptyBlocks; i++) {
      colStripeToIndex[blockColStripes[i]] = i;
    }

    final int[] sparsePositions = new int[nonEmptyBlocks];
    for (int i = 0; i < n; i++) {
      final int sourceIndex = offset + i;
      final int colStripe = (int) (cols[sourceIndex] / SUBMATRIX_SIZE);
      final int idx = colStripeToIndex[colStripe];
      final int intraRow = (int) (rows[sourceIndex] % SUBMATRIX_SIZE);
      final int intraCol = (int) (cols[sourceIndex] % SUBMATRIX_SIZE);
      if (denseFlags[idx]) {
        denseFlats[idx][intraRow * SUBMATRIX_SIZE + intraCol] += values[sourceIndex];
      } else {
        final int pos = sparsePositions[idx]++;
        sparseRows[idx][pos] = intraRow;
        sparseCols[idx][pos] = intraCol;
        sparseVals[idx][pos] = values[sourceIndex];
      }
    }

    for (int i = 0; i < nonEmptyBlocks; i++) {
      if (!denseFlags[i] && blockLengths[i] > 1) {
        sortSparseBlockRowMajor(sparseRows[i], sparseCols[i], sparseVals[i]);
      }
    }

    return new DoubleStripeBlocks(blockColStripes, blockLengths, denseFlags, sparseRows, sparseCols, sparseVals, denseFlats);
  }

  private static @NotNull StripeBlocks buildStripeBlocks(
    final @NotNull LongPixelBlock block,
    final int stripeCount
  ) {
    final var rows = block.rows();
    final var cols = block.cols();
    final var values = block.values();
    final int offset = block.offset();
    final int n = block.length();

    final int maxTouched = Math.min(stripeCount, n);
    final var buffer = acquireCountBuffer(stripeCount, maxTouched);
    final int[] counts = buffer.counts();
    final int[] touched = buffer.touched();
    int touchedCount = 0;

    for (int i = 0; i < n; i++) {
      final int sourceIndex = offset + i;
      final int colStripe = (int) (cols[sourceIndex] / SUBMATRIX_SIZE);
      if (counts[colStripe]++ == 0) {
        touched[touchedCount++] = colStripe;
      }
    }

    Arrays.sort(touched, 0, touchedCount);
    final int nonEmptyBlocks = touchedCount;
    final int[] blockColStripes = new int[nonEmptyBlocks];
    final boolean[] denseFlags = new boolean[nonEmptyBlocks];
    final int[] blockLengths = new int[nonEmptyBlocks];

    for (int i = 0; i < touchedCount; i++) {
      final int colStripe = touched[i];
      final int count = counts[colStripe];
      counts[colStripe] = 0;
      blockColStripes[i] = colStripe;
      blockLengths[i] = count;
      denseFlags[i] = count >= DENSE_THRESHOLD;
    }

    final long[][] sparseRows = new long[nonEmptyBlocks][];
    final long[][] sparseCols = new long[nonEmptyBlocks][];
    final long[][] sparseVals = new long[nonEmptyBlocks][];
    final long[][] denseFlats = new long[nonEmptyBlocks][];

    for (int i = 0; i < nonEmptyBlocks; i++) {
      if (denseFlags[i]) {
        denseFlats[i] = new long[SUBMATRIX_SIZE * SUBMATRIX_SIZE];
      } else {
        final int len = blockLengths[i];
        sparseRows[i] = new long[len];
        sparseCols[i] = new long[len];
        sparseVals[i] = new long[len];
      }
    }

    final int[] colStripeToIndex = new int[stripeCount];
    Arrays.fill(colStripeToIndex, -1);
    for (int i = 0; i < nonEmptyBlocks; i++) {
      colStripeToIndex[blockColStripes[i]] = i;
    }

    final int[] sparsePositions = new int[nonEmptyBlocks];
    for (int i = 0; i < n; i++) {
      final int sourceIndex = offset + i;
      final int colStripe = (int) (cols[sourceIndex] / SUBMATRIX_SIZE);
      final int idx = colStripeToIndex[colStripe];
      final int intraRow = (int) (rows[sourceIndex] % SUBMATRIX_SIZE);
      final int intraCol = (int) (cols[sourceIndex] % SUBMATRIX_SIZE);
      if (denseFlags[idx]) {
        denseFlats[idx][intraRow * SUBMATRIX_SIZE + intraCol] += values[sourceIndex];
      } else {
        final int pos = sparsePositions[idx]++;
        sparseRows[idx][pos] = intraRow;
        sparseCols[idx][pos] = intraCol;
        sparseVals[idx][pos] = values[sourceIndex];
      }
    }

    for (int i = 0; i < nonEmptyBlocks; i++) {
      if (!denseFlags[i] && blockLengths[i] > 1) {
        sortSparseBlockRowMajor(sparseRows[i], sparseCols[i], sparseVals[i]);
      }
    }

    return new LongStripeBlocks(blockColStripes, blockLengths, denseFlags, sparseRows, sparseCols, sparseVals, denseFlats);
  }

  private static void sortSparseBlockRowMajor(
    final long @NotNull [] rows,
    final long @NotNull [] cols,
    final double @NotNull [] vals
  ) {
    final int n = rows.length;
    if (n <= 1) {
      return;
    }
    if (NativeProcessingService.getInstance().trySortSparseBlockRowMajor(rows, cols, vals, SUBMATRIX_SIZE)) {
      return;
    }
    final int bucketSize = SUBMATRIX_SIZE * SUBMATRIX_SIZE;
    final int[] counts = new int[bucketSize];
    final int[] keys = new int[n];
    for (int i = 0; i < n; i++) {
      final int key = (int) (rows[i] * SUBMATRIX_SIZE + cols[i]);
      keys[i] = key;
      counts[key]++;
    }
    int sum = 0;
    for (int i = 0; i < bucketSize; i++) {
      final int c = counts[i];
      counts[i] = sum;
      sum += c;
    }
    final long[] sortedRows = new long[n];
    final long[] sortedCols = new long[n];
    final double[] sortedVals = new double[n];
    for (int i = 0; i < n; i++) {
      final int key = keys[i];
      final int pos = counts[key]++;
      sortedRows[pos] = rows[i];
      sortedCols[pos] = cols[i];
      sortedVals[pos] = vals[i];
    }
    System.arraycopy(sortedRows, 0, rows, 0, n);
    System.arraycopy(sortedCols, 0, cols, 0, n);
    System.arraycopy(sortedVals, 0, vals, 0, n);
  }

  private static void sortSparseBlockRowMajor(
    final long @NotNull [] rows,
    final long @NotNull [] cols,
    final long @NotNull [] vals
  ) {
    final int n = rows.length;
    if (n <= 1) {
      return;
    }
    if (NativeProcessingService.getInstance().trySortSparseBlockRowMajor(rows, cols, vals, SUBMATRIX_SIZE)) {
      return;
    }
    final int bucketSize = SUBMATRIX_SIZE * SUBMATRIX_SIZE;
    final int[] counts = new int[bucketSize];
    final int[] keys = new int[n];
    for (int i = 0; i < n; i++) {
      final int key = (int) (rows[i] * SUBMATRIX_SIZE + cols[i]);
      keys[i] = key;
      counts[key]++;
    }
    int sum = 0;
    for (int i = 0; i < bucketSize; i++) {
      final int c = counts[i];
      counts[i] = sum;
      sum += c;
    }
    final long[] sortedRows = new long[n];
    final long[] sortedCols = new long[n];
    final long[] sortedVals = new long[n];
    for (int i = 0; i < n; i++) {
      final int key = keys[i];
      final int pos = counts[key]++;
      sortedRows[pos] = rows[i];
      sortedCols[pos] = cols[i];
      sortedVals[pos] = vals[i];
    }
    System.arraycopy(sortedRows, 0, rows, 0, n);
    System.arraycopy(sortedCols, 0, cols, 0, n);
    System.arraycopy(sortedVals, 0, vals, 0, n);
  }

  private static final ThreadLocal<CountBuffer> COUNT_BUFFER =
    ThreadLocal.withInitial(() -> new CountBuffer(new int[0], new int[0]));

  private static @NotNull CountBuffer acquireCountBuffer(final int stripeCount, final int touchedCapacity) {
    final var buffer = COUNT_BUFFER.get();
    if (buffer.counts().length < stripeCount) {
      buffer.counts = new int[stripeCount];
    }
    if (buffer.touched().length < touchedCapacity) {
      buffer.touched = new int[touchedCapacity];
    }
    return buffer;
  }

  private static @NotNull SortedStripePixels[] sortStripeBatch(
    final PixelBlock @NotNull [] blocks,
    final ExecutorService stripeExecutor
  ) {
    final var sortedBatch = new SortedStripePixels[blocks.length];
    if (stripeExecutor == null) {
      for (int i = 0; i < blocks.length; i++) {
        checkInterrupted();
        final var block = blocks[i];
        if (block.length() > 0) {
          sortedBatch[i] = block instanceof DoublePixelBlock doubleBlock
            ? sortStripePixels(slice(doubleBlock.rows(), doubleBlock.offset(), doubleBlock.length()), slice(doubleBlock.cols(), doubleBlock.offset(), doubleBlock.length()), slice(doubleBlock.values(), doubleBlock.offset(), doubleBlock.length()))
            : sortStripePixels(slice(((LongPixelBlock) block).rows(), block.offset(), block.length()), slice(((LongPixelBlock) block).cols(), block.offset(), block.length()), toDoubleArray(slice(((LongPixelBlock) block).values(), block.offset(), block.length())));
        }
      }
      return sortedBatch;
    }

    final List<Future<?>> futures = new ArrayList<>(blocks.length);
    for (int i = 0; i < blocks.length; i++) {
      final int idx = i;
      futures.add(stripeExecutor.submit(() -> {
        checkInterrupted();
        final var block = blocks[idx];
        if (block.length() > 0) {
          sortedBatch[idx] = block instanceof DoublePixelBlock doubleBlock
            ? sortStripePixels(slice(doubleBlock.rows(), doubleBlock.offset(), doubleBlock.length()), slice(doubleBlock.cols(), doubleBlock.offset(), doubleBlock.length()), slice(doubleBlock.values(), doubleBlock.offset(), doubleBlock.length()))
            : sortStripePixels(slice(((LongPixelBlock) block).rows(), block.offset(), block.length()), slice(((LongPixelBlock) block).cols(), block.offset(), block.length()), toDoubleArray(slice(((LongPixelBlock) block).values(), block.offset(), block.length())));
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

  private static long resolveImportMaxMemoryBytes() {
    final var configured = firstNonBlank(
      System.getProperty("hict.import.maxMemoryBytes"),
      System.getenv("HICT_IMPORT_MAX_MEMORY_BYTES"),
      System.getenv("HICT_CONVERSION_MAX_MEMORY_BYTES")
    );
    if (configured == null) {
      return DEFAULT_IMPORT_MAX_MEMORY_BYTES;
    }
    try {
      return Math.max(MIN_IMPORT_MAX_MEMORY_BYTES, parseByteSize(configured));
    } catch (IllegalArgumentException ignored) {
      return DEFAULT_IMPORT_MAX_MEMORY_BYTES;
    }
  }

  private static long parseByteSize(final @NotNull String rawValue) {
    final var value = rawValue.trim().toLowerCase();
    if (value.isBlank()) {
      throw new IllegalArgumentException("Blank byte size");
    }
    int suffixStart = value.length();
    while (suffixStart > 0 && Character.isLetter(value.charAt(suffixStart - 1))) {
      suffixStart--;
    }
    final var numberPart = value.substring(0, suffixStart).trim();
    final var suffix = value.substring(suffixStart).trim();
    final double number = Double.parseDouble(numberPart);
    final long multiplier = switch (suffix) {
      case "", "b", "bytes" -> 1L;
      case "k", "kb", "kib" -> 1024L;
      case "m", "mb", "mib" -> 1024L * 1024L;
      case "g", "gb", "gib" -> 1024L * 1024L * 1024L;
      case "t", "tb", "tib" -> 1024L * 1024L * 1024L * 1024L;
      default -> throw new IllegalArgumentException("Unknown byte-size suffix: " + suffix);
    };
    if (!Double.isFinite(number) || number <= 0.0d) {
      throw new IllegalArgumentException("Invalid byte size: " + rawValue);
    }
    return Math.max(1L, (long) Math.floor(number * multiplier));
  }

  private static String firstNonBlank(final String... values) {
    for (final var value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  private static @NotNull String formatByteSize(final long bytes) {
    final double gib = bytes / (1024.0d * 1024.0d * 1024.0d);
    if (gib >= 1.0d) {
      return String.format(java.util.Locale.ROOT, "%.1f GiB", gib);
    }
    final double mib = bytes / (1024.0d * 1024.0d);
    return String.format(java.util.Locale.ROOT, "%.1f MiB", mib);
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

  private static void checkInterrupted() {
    if (Thread.currentThread().isInterrupted()) {
      throw new RuntimeException("Conversion interrupted");
    }
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
    final var base = "/resolutions/" + resolution + "/pixels/";
    final var floatingPointSignal = isFloatingPointDataset(src, base + "count");

    if (length <= 0) {
      return floatingPointSignal
        ? new DoublePixelBlock(new long[0], new long[0], new double[0])
        : new LongPixelBlock(new long[0], new long[0], new long[0]);
    }

    final var rows = src.int64().readArrayBlockWithOffset(base + "bin1_id", length, startOffset);
    final var cols = src.int64().readArrayBlockWithOffset(base + "bin2_id", length, startOffset);
    return floatingPointSignal
      ? new DoublePixelBlock(rows, cols, src.float64().readArrayBlockWithOffset(base + "count", length, startOffset))
      : new LongPixelBlock(rows, cols, src.int64().readArrayBlockWithOffset(base + "count", length, startOffset));
  }

  private static @NotNull SortedStripePixels sortStripePixels(
    final long @NotNull [] rows,
    final long @NotNull [] cols,
    final double @NotNull [] values
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
    final var sortedValues = new double[n];

    for (int i = 0; i < n; i++) {
      final int srcIdx = order[i];
      sortedColStripes[i] = cols[srcIdx] / SUBMATRIX_SIZE;
      sortedIntraRows[i] = (int) (rows[srcIdx] % SUBMATRIX_SIZE);
      sortedIntraCols[i] = (int) (cols[srcIdx] % SUBMATRIX_SIZE);
      sortedValues[i] = values[srcIdx];
    }

    return new SortedStripePixels(sortedColStripes, sortedIntraRows, sortedIntraCols, sortedValues);
  }

  private static long @NotNull [] slice(final long @NotNull [] values, final int offset, final int length) {
    return offset == 0 && length == values.length ? values : Arrays.copyOfRange(values, offset, offset + length);
  }

  private static double @NotNull [] slice(final double @NotNull [] values, final int offset, final int length) {
    return offset == 0 && length == values.length ? values : Arrays.copyOfRange(values, offset, offset + length);
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

    final var hictMetadata = readHictAssemblyMetadata(src, contigNames, contigLengthBp);
    final long[] contigDirections = hictMetadata
      .map(HictAssemblyMetadata::directions)
      .orElseGet(() -> defaultDirections(contigCount));
    dst.int64().writeArray(getContigDirectionDatasetPath(), contigDirections, intStorageFeatures);

    final long[] orderedContigIds = hictMetadata
      .map(HictAssemblyMetadata::orderedContigIds)
      .orElseGet(() -> defaultOrderedContigIds(contigCount));
    final long[] contigScaffoldIds = hictMetadata
      .map(HictAssemblyMetadata::scaffoldIds)
      .orElseGet(() -> defaultScaffoldIds(contigCount));
    dst.int64().writeArray(getContigOrderDatasetPath(), orderedContigIds, intStorageFeatures);
    dst.int64().writeArray("/contig_info/contig_scaffold_id", contigScaffoldIds, intStorageFeatures);

    final Map<Long, long[]> contigStartBinsByResolution = new HashMap<>();
    final Map<Long, long[]> contigLengthBinsByResolution = new HashMap<>();
    final Map<Long, List<StripeDescriptor>> stripesByResolution = new HashMap<>();

    for (final var resolution : resolutions) {
      final var chromOffsets = src.int64().readArray("/resolutions/" + resolution + "/indexes/chrom_offset");
      final var lengthBins = new long[chromOffsets.length - 1];
      for (int i = 0; i < lengthBins.length; i++) {
        lengthBins[i] = chromOffsets[i + 1] - chromOffsets[i];
        if (lengthBins[i] < 0) {
          throw new IllegalStateException("Negative-length contig found at resolution " + resolution + " contig=" + i);
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
        hideTypes[i] = autoHideType(contigLengthBins[i], contigLengthBp[i], resolution);
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

  private static @NotNull Optional<HictAssemblyMetadata> readHictAssemblyMetadata(
    final @NotNull IHDF5Reader src,
    final String @NotNull [] contigNames,
    final long @NotNull [] contigLengthBp
  ) {
    if (!src.object().isDataSet(HictToMcoolConverter.HICT_METADATA_CONTIG_NAME_PATH)
      || !src.object().isDataSet(HictToMcoolConverter.HICT_METADATA_CONTIG_LENGTH_BP_PATH)
      || !src.object().isDataSet(HictToMcoolConverter.HICT_METADATA_CONTIG_DIRECTION_PATH)) {
      return Optional.empty();
    }

    final var metadataNames = src.string().readArray(HictToMcoolConverter.HICT_METADATA_CONTIG_NAME_PATH);
    final var metadataLengths = src.int64().readArray(HictToMcoolConverter.HICT_METADATA_CONTIG_LENGTH_BP_PATH);
    if (!Arrays.equals(metadataNames, contigNames) || !Arrays.equals(metadataLengths, contigLengthBp)) {
      return Optional.empty();
    }

    final var directions = src.int64().readArray(HictToMcoolConverter.HICT_METADATA_CONTIG_DIRECTION_PATH);
    if (directions.length != contigNames.length) {
      return Optional.empty();
    }
    for (final long direction : directions) {
      if (direction < 0L || direction >= ContigDirection.values().length) {
        return Optional.empty();
      }
    }

    final long[] orderedContigIds;
    if (src.object().isDataSet(HictToMcoolConverter.HICT_METADATA_CONTIG_ORDER_PATH)) {
      final var candidate = src.int64().readArray(HictToMcoolConverter.HICT_METADATA_CONTIG_ORDER_PATH);
      orderedContigIds = isValidContigOrder(candidate, contigNames.length)
        ? candidate
        : defaultOrderedContigIds(contigNames.length);
    } else {
      orderedContigIds = defaultOrderedContigIds(contigNames.length);
    }

    final long[] scaffoldIds;
    if (src.object().isDataSet(HictToMcoolConverter.HICT_METADATA_CONTIG_SCAFFOLD_ID_PATH)) {
      final var candidate = src.int64().readArray(HictToMcoolConverter.HICT_METADATA_CONTIG_SCAFFOLD_ID_PATH);
      scaffoldIds = candidate.length == contigNames.length
        ? candidate
        : defaultScaffoldIds(contigNames.length);
    } else {
      scaffoldIds = defaultScaffoldIds(contigNames.length);
    }

    return Optional.of(new HictAssemblyMetadata(directions, orderedContigIds, scaffoldIds));
  }

  private static long @NotNull [] defaultDirections(final int contigCount) {
    final var directions = new long[contigCount];
    Arrays.fill(directions, ContigDirection.FORWARD.ordinal());
    return directions;
  }

  private static long @NotNull [] defaultOrderedContigIds(final int contigCount) {
    final var orderedContigIds = new long[contigCount];
    for (int i = 0; i < contigCount; i++) {
      orderedContigIds[i] = i;
    }
    return orderedContigIds;
  }

  private static long @NotNull [] defaultScaffoldIds(final int contigCount) {
    final var scaffoldIds = new long[contigCount];
    for (int i = 0; i < contigCount; i++) {
      scaffoldIds[i] = i;
    }
    return scaffoldIds;
  }

  private static boolean isValidContigOrder(final long @NotNull [] orderedContigIds, final int contigCount) {
    if (orderedContigIds.length != contigCount) {
      return false;
    }
    final var seen = new boolean[contigCount];
    for (final long orderedContigId : orderedContigIds) {
      if (orderedContigId < 0L || orderedContigId >= contigCount || seen[(int) orderedContigId]) {
        return false;
      }
      seen[(int) orderedContigId] = true;
    }
    return true;
  }

  private static byte autoHideType(final long lengthBins, final long lengthBp, final long resolution) {
    return (byte) ((lengthBins > 0L && lengthBp >= resolution)
      ? ContigHideType.SHOWN.ordinal()
      : ContigHideType.HIDDEN.ordinal());
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
    if (endBin <= startBin) {
      return List.of();
    }
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

  private static boolean isFloatingPointDataset(final @NotNull IHDF5Reader reader, final @NotNull String path) {
    return reader.object().getDataSetInformation(path).getTypeInformation().getDataClass() == HDF5DataClass.FLOAT;
  }

  private static double @NotNull [] readNumericArrayBlockWithOffset(
    final @NotNull IHDF5Reader reader,
    final @NotNull String path,
    final int length,
    final long offset
  ) {
    if (isFloatingPointDataset(reader, path)) {
      return reader.float64().readArrayBlockWithOffset(path, length, offset);
    }
    return toDoubleArray(reader.int64().readArrayBlockWithOffset(path, length, offset));
  }

  private static void createNumericArray(
    final @NotNull IHDF5Writer writer,
    final @NotNull String path,
    final long length,
    final int chunkSize,
    final boolean floatingPoint,
    final @NotNull HDF5IntStorageFeatures intStorageFeatures,
    final @NotNull HDF5FloatStorageFeatures floatStorageFeatures
  ) {
    if (floatingPoint) {
      writer.float64().createArray(path, length, safeChunkLen(length, chunkSize), floatStorageFeatures);
    } else {
      writer.int64().createArray(path, length, safeChunkLen(length, chunkSize), intStorageFeatures);
    }
  }

  private static void createNumericMDArray(
    final @NotNull IHDF5Writer writer,
    final @NotNull String path,
    final long @NotNull [] dimensions,
    final int @NotNull [] blockDimensions,
    final boolean floatingPoint,
    final @NotNull HDF5IntStorageFeatures intStorageFeatures,
    final @NotNull HDF5FloatStorageFeatures floatStorageFeatures
  ) {
    if (floatingPoint) {
      writer.float64().createMDArray(path, dimensions, blockDimensions, floatStorageFeatures);
    } else {
      writer.int64().createMDArray(path, dimensions, blockDimensions, intStorageFeatures);
    }
  }

  private static double @NotNull [] toDoubleArray(final long @NotNull [] input) {
    final var result = new double[input.length];
    for (int i = 0; i < input.length; i++) {
      result[i] = input[i];
    }
    return result;
  }

  private static long @NotNull [] toLongArray(final double @NotNull [] input) {
    final var result = new long[input.length];
    for (int i = 0; i < input.length; i++) {
      result[i] = Math.round(input[i]);
    }
    return result;
  }

  private static @NotNull MDLongArray toLongMdArray(final double @NotNull [] input) {
    return new MDLongArray(toLongArray(input), new int[]{1, 1, SUBMATRIX_SIZE, SUBMATRIX_SIZE});
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

  private static final StripeBlocks EMPTY_BLOCKS =
    new LongStripeBlocks(new int[0], new int[0], new boolean[0], new long[0][], new long[0][], new long[0][], new long[0][]);

  private sealed interface PixelBlock permits LongPixelBlock, DoublePixelBlock {
    long @NotNull [] rows();

    long @NotNull [] cols();

    int offset();

    int length();
  }

  private record LongPixelBlock(
    long @NotNull [] rows,
    long @NotNull [] cols,
    long @NotNull [] values,
    int offset,
    int length
  ) implements PixelBlock {
    private LongPixelBlock(final long @NotNull [] rows, final long @NotNull [] cols, final long @NotNull [] values) {
      this(rows, cols, values, 0, rows.length);
    }

    private LongPixelBlock {
      if (offset < 0 || length < 0 || offset + length > rows.length || offset + length > cols.length || offset + length > values.length) {
        throw new IllegalArgumentException("Invalid long pixel block slice");
      }
    }
  }

  private record DoublePixelBlock(
    long @NotNull [] rows,
    long @NotNull [] cols,
    double @NotNull [] values,
    int offset,
    int length
  ) implements PixelBlock {
    private DoublePixelBlock(final long @NotNull [] rows, final long @NotNull [] cols, final double @NotNull [] values) {
      this(rows, cols, values, 0, rows.length);
    }

    private DoublePixelBlock {
      if (offset < 0 || length < 0 || offset + length > rows.length || offset + length > cols.length || offset + length > values.length) {
        throw new IllegalArgumentException("Invalid double pixel block slice");
      }
    }
  }

  private record SortedStripePixels(
    long @NotNull [] colStripes,
    int @NotNull [] intraRows,
    int @NotNull [] intraCols,
    double @NotNull [] values
  ) {
  }

  private sealed interface StripeBlocks permits LongStripeBlocks, DoubleStripeBlocks {
    int @NotNull [] colStripes();

    int @NotNull [] blockLengths();

    boolean @NotNull [] denseFlags();

    long @NotNull [] @NotNull [] sparseRows();

    long @NotNull [] @NotNull [] sparseCols();
  }

  private record LongStripeBlocks(
    int @NotNull [] colStripes,
    int @NotNull [] blockLengths,
    boolean @NotNull [] denseFlags,
    long @NotNull [] @NotNull [] sparseRows,
    long @NotNull [] @NotNull [] sparseCols,
    long @NotNull [] @NotNull [] sparseVals,
    long @NotNull [] @NotNull [] denseFlats
  ) implements StripeBlocks {
  }

  private record DoubleStripeBlocks(
    int @NotNull [] colStripes,
    int @NotNull [] blockLengths,
    boolean @NotNull [] denseFlags,
    long @NotNull [] @NotNull [] sparseRows,
    long @NotNull [] @NotNull [] sparseCols,
    double @NotNull [] @NotNull [] sparseVals,
    double @NotNull [] @NotNull [] denseFlats
  ) implements StripeBlocks {
  }

  private record StripeCounts(long sparseElementCount, long denseBlockCount) {
  }

  private record StripeCountDetails(
    long @NotNull [] sparseCounts,
    long @NotNull [] denseCounts,
    long sparseTotal,
    long denseTotal
  ) {
  }

  private record StripeWriteTask(int stripeIndex, @NotNull StripeBlocks blocks) {
    static final StripeWriteTask POISON = new StripeWriteTask(-1, EMPTY_BLOCKS);
  }

  private static final class CountBuffer {
    private int[] counts;
    private int[] touched;

    private CountBuffer(final int[] counts, final int[] touched) {
      this.counts = counts;
      this.touched = touched;
    }

    private int[] counts() {
      return counts;
    }

    private int[] touched() {
      return touched;
    }
  }

  private record SaveBlockResult(long sparseOffset, long denseOffset) {
  }

  private record HictAssemblyMetadata(
    long @NotNull [] directions,
    long @NotNull [] orderedContigIds,
    long @NotNull [] scaffoldIds
  ) {
  }
}
