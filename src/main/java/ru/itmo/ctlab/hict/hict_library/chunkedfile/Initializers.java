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

  public static @NotNull @NonNull List<@NotNull @NonNull ContigTuple> readContigDescriptors(final @NotNull @NonNull IHDF5Reader reader) {

  }

  public static void initializeContigTree(final ChunkedFile chunkedFile) {
    final var resolutions = chunkedFile.getResolutions();
    final List<List<StripeDescriptor>> resolutionOrderToStripes = new ArrayList<>(resolutions.length);
    final List<List<StripeDescriptor>> resolutionOrderToBasisATUs = new ArrayList<>(resolutions.length);
    try (final var reader = HDF5Factory.openForReading(chunkedFile.getHdfFilePath().toFile())) {
      try (final ExecutorService executorService = Executors.newFixedThreadPool(8)) {
        for (int i = 0; i < resolutions.length; ++i) {
          executorService.submit(() -> {
            final var stripes = readStripeDescriptors(resolutions[i], reader);
            resolutionOrderToStripes.set(i, stripes);
            final var atus = readATL(resolutions[i], reader, stripes);
            resolutionOrderToBasisATUs.set(i, atus);
          });
        }
      }
    }
  }

  public static void initializeScaffoldTree(final ChunkedFile chunkedFile) {
  }

  public record ContigTuple(ContigDescriptor descriptor, ContigDirection direction) {
  }
}
