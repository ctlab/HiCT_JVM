package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.PathGenerators.*;

@Slf4j
public class Initializers {
  public static @NotNull @NonNull List<@NotNull @NonNull StripeDescriptor> readStripeDescriptors(final long resolution, final @NotNull @NonNull IHDF5Reader reader) {
    final List<StripeDescriptor> result = new ArrayList<>();
    final long[] stripeLengthBins;
    try (final var stripeLengthsBinsDataset = reader.object().openDataSet(getStripeLengthsBinsDatasetPath(resolution))) {
      stripeLengthBins = reader.int64().readArray(stripeLengthsBinsDataset.getDataSetPath());
    }

    for (int stripeId = 0; stripeId < stripeLengthBins.length; ++stripeId) {
      try (final var stripeBinWeightsDataset = reader.object().openDataSet(getStripeBinWeightsDatasetPath(resolution))) {
        final var stripeBinWeights = reader.float64().readMDArraySlice(stripeBinWeightsDataset, new long[]{stripeId, -1L});
        final var newStripe = new StripeDescriptor(stripeId, stripeLengthBins[stripeId], stripeBinWeights.getAsFlatArray());
        result.add(newStripe);
      }
    }

    return result;
  }

  public static @NotNull @NonNull List<@NotNull @NonNull ATUDescriptor> readATL(final long resolution, final @NotNull @NonNull IHDF5Reader reader, final List<StripeDescriptor> stripeDescriptors) {
    final List<ATUDescriptor> result;
    final long[][] basisAtuArray;

    try (final var basisATUDataset = reader.object().openDataSet(getBasisATUDatasetPath(resolution))) {
      basisAtuArray = reader.int64().readMatrix(basisATUDataset.getDataSetPath());
    }

    result = Arrays.stream(basisAtuArray).map(row ->
      new ATUDescriptor(
        stripeDescriptors.get((int) row[0]),
        row[1],
        row[2],
        ATUDirection.values()[(int) row[3]]
      )
    ).collect(Collectors.toList());


    return result;
  }

  public static @NotNull @NonNull List<ContigTree.@NotNull @NonNull ContigTuple> buildContigDescriptors(final ChunkedFile chunkedFile) {
    final var resolutions = chunkedFile.getResolutions();
    final List<List<StripeDescriptor>> resolutionOrderToStripes = new ArrayList<>(resolutions.length);
    IntStream.range(0, resolutions.length).forEach(idx -> resolutionOrderToStripes.add(null));
    final List<List<ATUDescriptor>> resolutionOrderToBasisATUs = new ArrayList<>(resolutions.length);
    IntStream.range(0, resolutions.length).forEach(idx -> resolutionOrderToBasisATUs.add(null));
    final List<List<ContigDescriptorDataBundle>> contigDescriptorDataBundles = new ArrayList<>(resolutions.length);
    IntStream.range(0, resolutions.length).forEach(idx -> contigDescriptorDataBundles.add(null));
    final List<ContigDirection> contigDirections;
    final String[] contigNames;
    final long[] contigLengthBp;

    final int contigCount;
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
      for (int i = 1; i < resolutions.length; ++i) {
        final var stripes = readStripeDescriptors(resolutions[i], reader);
        resolutionOrderToStripes.set(i, stripes);
        chunkedFile.getResolutions()[i] = stripes.size();
        final var atus = readATL(resolutions[i], reader, stripes);
        resolutionOrderToBasisATUs.set(i, atus);
        final var dataBundles = readContigDataBundles(resolutions[i], reader, atus);
        contigDescriptorDataBundles.set(i, dataBundles);
      }

      contigCount = contigDescriptorDataBundles.get(0).size();

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


    final var contigDescriptors = IntStream.range(0, contigCount).parallel().mapToObj(contigId -> new ContigDescriptor(
        contigId,
        contigNames[contigId],
        contigLengthBp[contigId],
        contigDescriptorDataBundles.stream().mapToLong(bundlesAtResolution -> bundlesAtResolution.get(contigId).lengthBins()).boxed().toList(),
        contigDescriptorDataBundles.stream().map(bundlesAtResolution -> bundlesAtResolution.get(contigId).hideType()).toList(),
        contigDescriptorDataBundles.stream().map(bundlesAtResolution -> bundlesAtResolution.get(contigId).atus()).toList()
      )
    );

    return contigDescriptors.map(contigDescriptor -> new ContigTree.ContigTuple(contigDescriptor, contigDirections.get(contigDescriptor.getContigId()))).toList();
  }

  public static @NotNull @NonNull List<@NotNull @NonNull ContigDescriptorDataBundle> readContigDataBundles(final long resolution, final @NotNull @NonNull IHDF5Reader reader, final List<ATUDescriptor> basisATUs) {
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
      contigTree.appendContig(contigs.get(order).descriptor(), contigs.get(order).direction());
    }
  }

  public static void initializeScaffoldTree(final ChunkedFile chunkedFile) {
  }

  private record ContigDescriptorDataBundle(
    @NotNull @NonNull List<@NotNull @NonNull ATUDescriptor> atus,
    @NotNull @NonNull ContigHideType hideType,
    long lengthBins
  ) {
  }
}
