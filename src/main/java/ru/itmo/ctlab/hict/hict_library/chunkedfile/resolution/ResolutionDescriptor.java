package ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Range;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.util.BinarySearch;

public abstract class ResolutionDescriptor {

  public static ResolutionDescriptor fromResolutionOrder(final int resolutionOrder) {
    return new ResolutionDescriptor() {
      @Override
      public @Range(from = 0, to = Integer.MAX_VALUE) int getResolutionOrderInArray() {
        return resolutionOrder;
      }
    };
  }

  public static ResolutionDescriptor fromBpResolution(final long bpResolution, final @NotNull ChunkedFile chunkedFile) {
    final int resolutionOrder;

    if (bpResolution < 1) {
      resolutionOrder = 0;
    } else {
      resolutionOrder = 1 + BinarySearch.leftBinarySearch(chunkedFile.getResolutions(), bpResolution);
    }

    return new ResolutionDescriptor() {
      @Override
      public @Range(from = 0, to = Integer.MAX_VALUE) int getResolutionOrderInArray() {
        return resolutionOrder;
      }
    };
  }

  /**
   * @return An order of resolution stored in node's arrays.
   */
  @Range(from = 0, to = Integer.MAX_VALUE)
  public abstract int getResolutionOrderInArray();
}
