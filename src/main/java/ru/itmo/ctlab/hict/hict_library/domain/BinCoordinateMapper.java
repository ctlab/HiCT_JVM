package ru.itmo.ctlab.hict.hict_library.domain;

/** Coordinates of retained source bins within one component; no tree state or I/O. */
public final class BinCoordinateMapper {
  private BinCoordinateMapper() {}

  public static long bpToBin(long bp, long length, long bins, long resolution,
                             long firstSourceBinOffset, boolean reversed) {
    validate(length, bins, resolution);
    if (bins == 0) return 0;
    long local = Math.max(0, Math.min(bp, length - 1));
    long source = reversed ? length - 1 - local : local;
    long ordinal = Math.max(0, Math.min(bins - 1, Math.floorDiv(Math.subtractExact(source, firstSourceBinOffset), resolution)));
    return reversed ? bins - 1 - ordinal : ordinal;
  }

  public static long binToBp(long bin, long length, long bins, long resolution,
                             long firstSourceBinOffset, boolean reversed) {
    validate(length, bins, resolution);
    if (bins == 0) return 0;
    long local = Math.max(0, Math.min(bin, bins - 1));
    long ordinal = reversed ? bins - 1 - local : local;
    long start = Math.addExact(firstSourceBinOffset, Math.multiplyExact(ordinal, resolution));
    long offset = reversed ? length - Math.min(length, Math.addExact(start, resolution)) : start;
    return Math.max(0, Math.min(length - 1, offset));
  }

  private static void validate(long length, long bins, long resolution) {
    if (length <= 0 || bins < 0 || resolution <= 0)
      throw new IllegalArgumentException("Invalid component bin geometry");
  }
}
