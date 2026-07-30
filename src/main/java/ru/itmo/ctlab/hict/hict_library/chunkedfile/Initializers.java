/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

@Slf4j
public class Initializers {
  static {
    HDF5LibraryInitializer.initializeHDF5Library();
  }

  public interface ProgressReporter {
    void report(@NotNull String stage, double progress);
  }

  private static final ThreadLocal<ProgressReporter> PROGRESS = new ThreadLocal<>();

  public static <T> T withProgressReporter(final @NotNull ProgressReporter reporter, final @NotNull java.util.function.Supplier<T> supplier) {
    PROGRESS.set(reporter);
    try {
      return supplier.get();
    } finally {
      PROGRESS.remove();
    }
  }

  private static void reportProgress(final @NotNull String stage, final double progress) {
    final var reporter = PROGRESS.get();
    if (reporter != null) {
      reporter.report(stage, progress);
    }
  }

  private static long syntheticLengthBins(final long lengthBp, final long resolution) {
    if (lengthBp <= 0L || resolution <= 0L) {
      return 0L;
    }
    return Math.max(1L, (lengthBp + resolution - 1L) / resolution);
  }

  private static @NotNull ContigHideType inferHideType(final long lengthBins,
                                                       final long lengthBp,
                                                       final long resolution) {
    return lengthBins > 0L && lengthBp >= resolution
      ? ContigHideType.SHOWN
      : ContigHideType.HIDDEN;
  }

  public static @NotNull List<@NotNull StripeDescriptor> readStripeDescriptors(final long resolution, final @NotNull IHDF5Reader reader) {
    final List<StripeDescriptor> result = new ArrayList<>();
    final long[] stripeLengthBins;
    try (final var stripeLengthsBinsDataset = reader.object().openDataSet(getStripeLengthsBinsDatasetPath(resolution))) {
      stripeLengthBins = reader.int64().readArray(stripeLengthsBinsDataset.getDataSetPath());
    }

    try (final var stripeBinWeightsDataset = reader.object().openDataSet(getStripeBinWeightsDatasetPath(resolution))) {
      reportProgress("Reading stripe weights", 0.0);
      final long[] dims = reader.object().getDataSetInformation(stripeBinWeightsDataset.getDataSetPath()).getDimensions();
      final int rowLen = dims.length > 1 ? (int) dims[1] : 0;
      final int stripeCount = stripeLengthBins.length;
      final int bytesPerRow = Math.max(1, rowLen) * Double.BYTES;
      final int targetBlockBytes = 16 * 1024 * 1024;
      final int maxRowsPerBlock = Math.max(1, targetBlockBytes / bytesPerRow);
      final int blockSize = Math.min(256, Math.min(maxRowsPerBlock, stripeCount));

      for (int start = 0; start < stripeCount; start += blockSize) {
        final int count = Math.min(blockSize, stripeCount - start);
        final var block = reader.float64().readMDArrayBlockWithOffset(
          stripeBinWeightsDataset.getDataSetPath(),
          new int[]{count, rowLen},
          new long[]{start, 0}
        );
        final double[] flat = block.getAsFlatArray();
        for (int i = 0; i < count; i++) {
          final int stripeId = start + i;
          final int len = (int) stripeLengthBins[stripeId];
          final int rowOffset = i * rowLen;
          final int copyLen = Math.min(len, Math.max(0, rowLen));
          final double[] weights = copyLen > 0
            ? Arrays.copyOfRange(flat, rowOffset, rowOffset + copyLen)
            : new double[0];
          result.add(new StripeDescriptor(stripeId, stripeLengthBins[stripeId], weights));
        }
        reportProgress("Reading stripe weights", (double) Math.min(stripeCount, start + count) / Math.max(1, stripeCount));
      }
      reportProgress("Reading stripe weights", 1.0);
    }

    return result;
  }

  public static @NotNull List<@NotNull ATUDescriptor> readATL(final long resolution, final @NotNull IHDF5Reader reader, final List<StripeDescriptor> stripeDescriptors) {
    final List<ATUDescriptor> result;
    final long[][] basisAtuArray;

    log.debug("Reading ATL for resolution " + resolution);

    try (final var basisATUDataset = reader.object().openDataSet(getBasisATUDatasetPath(resolution))) {
      basisAtuArray = reader.int64().readMatrix(basisATUDataset.getDataSetPath());
    }

    result = Arrays.stream(basisAtuArray).map(row -> {
        final var atu = new ATUDescriptor(
          stripeDescriptors.get((int) row[0]),
          (int) row[1],
          (int) row[2],
          ATUDirection.values()[(int) row[3]]
        );

//        log.debug("Built ATU: StripeID=" + atu.getStripeDescriptor().stripeId() + " start=" + atu.getStartIndexInStripeIncl() + " end=" + atu.getEndIndexInStripeExcl() + " direction=" + atu.getDirection());

        return atu;
      }
    ).collect(Collectors.toList());


    return result;
  }

  private static long countNonFiniteWeights(final @NotNull List<@NotNull StripeDescriptor> stripes) {
    long count = 0L;
    for (final var stripe : stripes) {
      for (final var weight : stripe.bin_weights()) {
        if (!Double.isFinite(weight)) {
          count++;
        }
      }
    }
    return count;
  }

  public static @NotNull List<ContigTree.@NotNull ContigTuple> buildContigDescriptors(final ChunkedFile chunkedFile) {
    final var resolutions = chunkedFile.getResolutions();
    final List<List<StripeDescriptor>> resolutionOrderToStripes = new ArrayList<>(resolutions.length);
    IntStream.range(0, resolutions.length).forEach(idx -> resolutionOrderToStripes.add(null));
    final List<List<ATUDescriptor>> resolutionOrderToBasisATUs = new ArrayList<>(resolutions.length);
    IntStream.range(0, resolutions.length).forEach(idx -> resolutionOrderToBasisATUs.add(null));
    final List<List<ContigDescriptorDataBundle>> contigDescriptorDataBundles = new ArrayList<>(resolutions.length);
    contigDescriptorDataBundles.add(new ArrayList<>());
    IntStream.range(1, resolutions.length).forEach(idx -> contigDescriptorDataBundles.add(null));
    final List<ContigDirection> contigDirections;
    final String[] contigNames;
    final long[] contigLengthBp;
    try (final var reader = HDF5Factory.openForReading(chunkedFile.getHdfFilePath().toFile())) {
//      try (final ExecutorService executorService = Executors.newFixedThreadPool(8)) {
//        for (int i = 0; i < resolutions.length; ++i) {
//          final int finalI = i;
//          executorService.submit(() -> {
//            final var stripes = readStripeDescriptors(resolutions[finalI], reader);
//            resolutionOrderToStripes.set(finalI, stripes);
//            final var atus = readATL(resolutions[finalI], reader, stripes);
//            resolutionOrderToBasisATUs.set(finalI, atus);
//            final var dataBundles = readContigDataBundles(resolutions[finalI], reader, atus);
//            contigDescriptorDataBundles.set(finalI, dataBundles);
//          });
//        }
//      }
      reportProgress("Reading resolution metadata", 0.0);
      for (int i = 1; i < resolutions.length; ++i) {
        final var stripes = readStripeDescriptors(resolutions[i], reader);
        resolutionOrderToStripes.set(i, stripes);
        final var nonFiniteWeights = countNonFiniteWeights(stripes);
        if (nonFiniteWeights > 0L) {
          chunkedFile.addCoolerWeightsNaNCount(nonFiniteWeights);
          log.warn(
            "Detected {} non-finite Cooler weights while opening {} at resolution {}",
            nonFiniteWeights,
            chunkedFile.getHdfFilePath().getFileName(),
            resolutions[i]
          );
        }
        chunkedFile.getStripeCount()[i] = stripes.size();
        final var atus = readATL(resolutions[i], reader, stripes);
        resolutionOrderToBasisATUs.set(i, atus);
        final var dataBundles = readContigDataBundles(resolutions[i], reader, atus);
        contigDescriptorDataBundles.set(i, dataBundles);
        reportProgress("Reading resolution metadata", (double) i / Math.max(1, resolutions.length - 1));
      }
      reportProgress("Reading resolution metadata", 1.0);

      try (final var contigDirectionDataset = reader.object().openDataSet(getContigDirectionDatasetPath())) {
        contigDirections = Arrays.stream(reader.int64().readArray(contigDirectionDataset.getDataSetPath())).mapToInt(i -> (int) i).mapToObj(dir -> ContigDirection.values()[dir]).toList();
      }

      try (final var contigNamesDataset = reader.object().openDataSet(getContigNameDatasetPath())) {
        contigNames = reader.string().readArray(contigNamesDataset.getDataSetPath());
      }

      try (final var contigLengthBpDataset = reader.object().openDataSet(getContigLengthBpDatasetPath())) {
        contigLengthBp = reader.int64().readArray(contigLengthBpDataset.getDataSetPath());
      }
    }

    final int contigCount = contigNames.length;
    final boolean resolutionMetadataAligned = contigDescriptorDataBundles.stream()
      .skip(1L)
      .allMatch(bundle -> bundle != null && bundle.size() == contigCount);
    if (!resolutionMetadataAligned) {
      final int sampledResolutionContigCount = contigDescriptorDataBundles.stream()
        .skip(1L)
        .filter(Objects::nonNull)
        .mapToInt(List::size)
        .findFirst()
        .orElse(0);
      log.warn(
        "Contig metadata mismatch detected while opening {}: top-level contig count={}, resolution-level contig count={}. " +
          "Synthesizing contig descriptors from top-level assembly metadata so the file can still be opened.",
        chunkedFile.getHdfFilePath().getFileName(),
        contigCount,
        sampledResolutionContigCount
      );
    }

    reportProgress("Building contig descriptors", 0.0);
    final List<ContigTree.ContigTuple> result = new ArrayList<>(contigCount);
    for (int contigId = 0; contigId < contigCount; contigId++) {
      final int cid = contigId;
      final ContigDescriptor contigDescriptor;
      if (resolutionMetadataAligned) {
        contigDescriptor = new ContigDescriptor(
          cid,
          contigNames[cid],
          contigLengthBp[cid],
          contigDescriptorDataBundles.stream().skip(1L).mapToLong(bundlesAtResolution -> bundlesAtResolution.get(cid).lengthBins()).boxed().toList(),
          contigDescriptorDataBundles.stream().skip(1L).map(bundlesAtResolution -> bundlesAtResolution.get(cid).hideType()).toList(),
          contigDescriptorDataBundles.stream().skip(1L).map(bundlesAtResolution -> bundlesAtResolution.get(cid).atus()).toList(),
          contigNames[cid], 0
        );
      } else {
        final List<Long> syntheticLengthBins = new ArrayList<>(Math.max(0, resolutions.length - 1));
        final List<ContigHideType> syntheticPresence = new ArrayList<>(Math.max(0, resolutions.length - 1));
        final List<List<ATUDescriptor>> syntheticAtus = new ArrayList<>(Math.max(0, resolutions.length - 1));
        for (int resolutionIdx = 1; resolutionIdx < resolutions.length; resolutionIdx++) {
          final long resolution = resolutions[resolutionIdx];
          final long bins = syntheticLengthBins(contigLengthBp[cid], resolution);
          syntheticLengthBins.add(bins);
          syntheticPresence.add(inferHideType(bins, contigLengthBp[cid], resolution));
          syntheticAtus.add(List.of());
        }
        contigDescriptor = new ContigDescriptor(
          cid,
          contigNames[cid],
          contigLengthBp[cid],
          syntheticLengthBins,
          syntheticPresence,
          syntheticAtus,
          contigNames[cid],
          0
        );
      }
      final var contigDirection = contigId < contigDirections.size() ? contigDirections.get(contigDescriptor.getContigId()) : ContigDirection.FORWARD;
      result.add(new ContigTree.ContigTuple(contigDescriptor, contigDirection));
      if ((contigId & 1023) == 0) {
        reportProgress("Building contig descriptors", (double) contigId / Math.max(1, contigCount));
      }
    }
    reportProgress("Building contig descriptors", 1.0);

    return result;
  }

  public static @NotNull List<@NotNull ContigDescriptorDataBundle> readContigDataBundles(final long resolution, final @NotNull IHDF5Reader reader, final List<ATUDescriptor> basisATUs) {
    final List<ContigDescriptorDataBundle> result;
    final byte[] chtBytes;
    final long[] contigLengthBins;
    final long[][] contigATUMapping;


    try (final var basisATUDataset = reader.object().openDataSet(getContigHideTypeDatasetPath(resolution))) {
      chtBytes = reader.int8().readArray(basisATUDataset.getDataSetPath());
    }

    try (final var contigLengthBinsDataset = reader.object().openDataSet(getContigLengthBinsDatasetPath(resolution))) {
      contigLengthBins = reader.int64().readArray(contigLengthBinsDataset.getDataSetPath());
    }

    try (final var contigATLDataset = reader.object().openDataSet(getContigsATLDatasetPath(resolution))) {
      contigATUMapping = reader.int64().readMatrix(contigATLDataset.getDataSetPath());
    }

    final List<@NotNull List<ATUDescriptor>> contigIdToATUs = new ArrayList<>(contigLengthBins.length);
    IntStream.range(0, contigLengthBins.length).forEach(idx -> contigIdToATUs.add(new ArrayList<>()));

    for (final var row : contigATUMapping) {
      final var contigId = row[0];
      final var atuId = row[1];
      if (contigId < 0 || contigId >= contigIdToATUs.size()) {
        log.warn("Skipping ATU row with out-of-range contig id {} for resolution {}", contigId, resolution);
        continue;
      }
      if (atuId < 0 || atuId >= basisATUs.size()) {
        log.warn("Skipping ATU row with out-of-range ATU id {} for resolution {}", atuId, resolution);
        continue;
      }
      contigIdToATUs.get((int) contigId).add(basisATUs.get((int) atuId));
    }

    result = IntStream.range(0, contigLengthBins.length).mapToObj(i ->
      new ContigDescriptorDataBundle(
        contigIdToATUs.get(i),
        ContigHideType.values()[chtBytes[i]],
        contigLengthBins[i]
      )
    ).collect(Collectors.toList());

    return result;
  }

  public static void initializeContigTree(final ChunkedFile chunkedFile) {
    log.debug("Chunked file has " + chunkedFile.getResolutions().length + " resolutions");

    final var contigs = buildContigDescriptors(chunkedFile);

    final long[] contigOrder;
    try (final var reader = HDF5Factory.openForReading(chunkedFile.getHdfFilePath().toFile())) {
      try (final var contigOrderDataset = reader.object().openDataSet(getContigOrderDatasetPath())) {
        contigOrder = reader.int64().readArray(contigOrderDataset.getDataSetPath());
      }
    }

    final var contigTree = chunkedFile.getContigTree();

    for (final var orderLong : contigOrder) {
      final var order = (int) orderLong;
      if (order < 0 || order >= contigs.size()) {
        log.warn("Skipping contig order {} while initializing contig tree because the descriptor list has size {}", order, contigs.size());
        continue;
      }
      contigTree.appendContig(contigs.get(order).descriptor(), contigs.get(order).direction());
    }
  }

  public static void initializeScaffoldTree(final ChunkedFile chunkedFile) {
    final var scaffoldIdPath = "/contig_info/contig_scaffold_id";
    try (final var reader = HDF5Factory.openForReading(chunkedFile.getHdfFilePath().toFile())) {
      if (!reader.object().isDataSet(scaffoldIdPath)) {
        return;
      }
      final var contigScaffoldIds = reader.int64().readArray(scaffoldIdPath);
      final var scaffoldNames = reader.object().isDataSet("/contig_info/scaffold_name")
        ? reader.string().readArray("/contig_info/scaffold_name")
        : new String[0];
      final var scaffoldSpacerBp = reader.object().isDataSet("/contig_info/scaffold_spacer_bp")
        ? reader.int64().readArray("/contig_info/scaffold_spacer_bp")
        : new long[0];

      final var orderedContigs = chunkedFile.getContigTree().getOrderedContigList();
      if (orderedContigs.isEmpty()) {
        return;
      }

      final var tree = chunkedFile.getScaffoldTree();
      tree.unscaffold(0L, 1L + chunkedFile.getMatrixSizeBins()[0]);

      long positionBp = 0L;
      int groupStartIndex = 0;
      while (groupStartIndex < orderedContigs.size()) {
        final var firstDescriptor = orderedContigs.get(groupStartIndex).descriptor();
        final int firstContigId = firstDescriptor.getContigId();
        final long scaffoldId = firstContigId >= 0 && firstContigId < contigScaffoldIds.length
          ? contigScaffoldIds[firstContigId]
          : -1L;
        final long groupStartBp = positionBp;
        long groupLengthBp = 0L;
        int groupEndIndex = groupStartIndex;
        while (groupEndIndex < orderedContigs.size()) {
          final var descriptor = orderedContigs.get(groupEndIndex).descriptor();
          final int contigId = descriptor.getContigId();
          final long currentScaffoldId = contigId >= 0 && contigId < contigScaffoldIds.length
            ? contigScaffoldIds[contigId]
            : -1L;
          if (groupEndIndex > groupStartIndex && currentScaffoldId != scaffoldId) {
            break;
          }
          groupLengthBp += descriptor.getLengthBp();
          groupEndIndex++;
        }

        if (scaffoldId >= 0L) {
          final var scaffoldName = scaffoldId < scaffoldNames.length && scaffoldNames[(int) scaffoldId] != null && !scaffoldNames[(int) scaffoldId].isBlank()
            ? scaffoldNames[(int) scaffoldId]
            : "scaffold_" + scaffoldId;
          final long spacerBp = scaffoldId < scaffoldSpacerBp.length && scaffoldSpacerBp[(int) scaffoldId] > 0L
            ? scaffoldSpacerBp[(int) scaffoldId]
            : 1000L;
          final int groupSize = groupEndIndex - groupStartIndex;
          if (groupSize > 1 || !scaffoldName.startsWith("unscaffolded")) {
            tree.rescaffold(
              groupStartBp,
              groupStartBp + groupLengthBp,
              id -> new ScaffoldDescriptor(scaffoldId, scaffoldName, spacerBp)
            );
          }
        }

        positionBp += groupLengthBp;
        groupStartIndex = groupEndIndex;
      }
    } catch (Exception e) {
      log.warn("Failed to initialize scaffold tree from stored contig scaffold metadata in {}", chunkedFile.getHdfFilePath(), e);
    }
  }

  private record ContigDescriptorDataBundle(
    @NotNull List<@NotNull ATUDescriptor> atus,
    @NotNull ContigHideType hideType,
    long lengthBins
  ) {
  }
}
