package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.StripeDescriptor;

import java.util.List;

public class Initializers {
  public static @NotNull @NonNull List<@NotNull @NonNull StripeDescriptor> readStripeDescriptors(final @NotNull @NonNull IHDF5Reader reader) {
    //reader.int64().

    try (final var stripeLengthsBinsDataset = reader.object().openDataSet(ChunkedFile.g) {
      blockRows = reader.int64().readArrayBlockWithOffset(blockRowsDataset, (int) blockLength, blockOffset);
    }
    try (final var blockColsDataset = reader.object().openDataSet(getBlockColsDatasetPath(resolution))) {
      blockCols = reader.int64().readArrayBlockWithOffset(blockColsDataset, (int) blockLength, blockOffset);
    }
  }

  public static @NotNull @NonNull List<@NotNull @NonNull ContigTuple> readContigDescriptors(final @NotNull @NonNull IHDF5Reader reader) {

  }

  public record ContigTuple(ContigDescriptor descriptor, ContigDirection direction) {
  }
}
