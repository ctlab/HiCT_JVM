package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.domain.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.PathGenerators.*;

public class Initializers {
  public static @NotNull @NonNull List<@NotNull @NonNull StripeDescriptor> readStripeDescriptors(final long resolution, final @NotNull @NonNull IHDF5Reader reader) {
    final List<StripeDescriptor> result = new ArrayList<>();
    final long[] stripeLengthBins;
    try (final var stripeLengthsBinsDataset = reader.object().openDataSet(getStripeLengthsBinsDatasetPath(resolution))) {
      stripeLengthBins = reader.int64().readArray(stripeLengthsBinsDataset.getDataSetPath());
    }

    for (int stripeId = 0; stripeId < stripeLengthBins.length; ++stripeId) {
      try (final var stripeBinWeightsDataset = reader.object().openDataSet(getStripeBinWeightsDatasetPath(resolution))) {
        final var stripeBinWeights = reader.float64().readMDArraySlice(stripeBinWeightsDataset, new long[]{-1L, stripeId});
        final var newStripe = new StripeDescriptor(stripeId, stripeLengthBins[stripeId], stripeBinWeights.getAsFlatArray());
        result.add(newStripe);
      }
    }

    return result;
  }

  public static @NotNull @NonNull List<@NotNull @NonNull ATUDescriptor> readATL(final long resolution, final @NotNull @NonNull IHDF5Reader reader, final List<StripeDescriptor> stripeDescriptors) {
    final List<ATUDescriptor> result;
    final long[][] basisAtuArray;

    try (final var basisATUDataset = reader.object().openDataSet(getBasisATUDataset(resolution))) {
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

  public static @NotNull @NonNull List<@NotNull @NonNull ContigTuple> buildContigDescriptors(List<List<ContigDescriptorDataBundle>> contigDescriptorDataBundles) {
    final var contigCount = contigDescriptorDataBundles.get(0).size();


    final var contigDescriptors = IntStream.range(0, contigCount).parallel().mapToObj(contigId -> new ContigDescriptor(
        contigId,
        "contigName", // TODO: Read names
        0L, //TODO: Read length bp
        contigDescriptorDataBundles.stream().mapToLong(bundlesAtResolution -> bundlesAtResolution.get(contigId).lengthBins()).boxed().toList(),
        contigDescriptorDataBundles.stream().map(bundlesAtResolution -> bundlesAtResolution.get(contigId).hideType()).toList(),
        contigDescriptorDataBundles.stream().map(bundlesAtResolution -> bundlesAtResolution.get(contigId).atus()).toList()
      )
    );

    return contigDescriptors.map(contigDescriptor -> new ContigTuple(contigDescriptor, ContigDirection.FORWARD)).toList();
  }

  public static @NotNull @NonNull List<@NotNull @NonNull ContigDescriptorDataBundle> readContigDataBundles(final long resolution, final @NotNull @NonNull IHDF5Reader reader, final List<ATUDescriptor> basisATUs) {
    final List<ContigDescriptorDataBundle> result;
    final byte[] chtBytes;
    final long[] contigLengthBins;
    final long[][] contigATUMapping;


    try (final var basisATUDataset = reader.object().openDataSet(getContigHideTypeDataset(resolution))) {
      chtBytes = reader.int8().readArray(basisATUDataset.getDataSetPath());
    }

    try (final var contigLengthBinsDataset = reader.object().openDataSet(getContigLengthBinsDataset(resolution))) {
      contigLengthBins = reader.int64().readArray(contigLengthBinsDataset.getDataSetPath());
    }

    try (final var contigATLDataset = reader.object().openDataSet(getContigsATLDataset(resolution))) {
      contigATUMapping = reader.int64().readMatrix(contigATLDataset.getDataSetPath());
    }

    final List<List<ATUDescriptor>> contigIdToATUs = new ArrayList<>(contigLengthBins.length);

    for (final var row : contigATUMapping) {
      final var contigId = row[0];
      final var atuId = row[1];
      if (contigIdToATUs.get((int) contigId) == null) {
        contigIdToATUs.set((int) contigId, new ArrayList<>());
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
    final var resolutions = chunkedFile.getResolutions();
    final List<List<StripeDescriptor>> resolutionOrderToStripes = new ArrayList<>(resolutions.length);
    final List<List<ATUDescriptor>> resolutionOrderToBasisATUs = new ArrayList<>(resolutions.length);
    final List<List<ContigDescriptorDataBundle>> contigDescriptorDataBundles = new ArrayList<>(resolutions.length);
    try (final var reader = HDF5Factory.openForReading(chunkedFile.getHdfFilePath().toFile())) {
      try (final ExecutorService executorService = Executors.newFixedThreadPool(8)) {
        for (int i = 0; i < resolutions.length; ++i) {
          final int finalI = i;
          executorService.submit(() -> {
            final var stripes = readStripeDescriptors(resolutions[finalI], reader);
            resolutionOrderToStripes.set(finalI, stripes);
            final var atus = readATL(resolutions[finalI], reader, stripes);
            resolutionOrderToBasisATUs.set(finalI, atus);
            final var dataBundles = readContigDataBundles(resolutions[finalI], reader, atus);
            contigDescriptorDataBundles.set(finalI, dataBundles);
          });
        }
      }
    }

    final var contigs = buildContigDescriptors(contigDescriptorDataBundles);


  }

  public static void initializeScaffoldTree(final ChunkedFile chunkedFile) {
  }

  public record ContigTuple(ContigDescriptor descriptor, ContigDirection direction) {
  }

  private static record ContigDescriptorDataBundle(
    @NotNull @NonNull List<@NotNull @NonNull ATUDescriptor> atus,
    @NotNull @NonNull ContigHideType hideType,
    long lengthBins
  ) {
  }
}
