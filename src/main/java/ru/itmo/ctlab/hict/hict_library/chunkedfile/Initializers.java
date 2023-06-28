package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;

import java.util.List;

public class Initializers {
  public static @NotNull @NonNull List<@NotNull @NonNull ContigTuple> readContigDescriptors(final @NotNull @NonNull IHDF5Reader reader){
    
  }

  public record ContigTuple(ContigDescriptor descriptor, ContigDirection direction){}
}
