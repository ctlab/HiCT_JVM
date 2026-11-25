package ru.itmo.ctlab.hict.hict_library.visualization;

/** A display-only exclusion around the diagonal in the current assembly. */
public record DiagonalMask(boolean enabled, long distanceBp) {
  public static final long MAX_DISTANCE_BP = 1_000_000_000L;
  public static final DiagonalMask DEFAULT = new DiagonalMask(false, 100_000);

  public DiagonalMask {
    if (distanceBp < 1 || distanceBp > MAX_DISTANCE_BP) {
      throw new IllegalArgumentException("Diagonal distance must be between 1 and 1,000,000,000 bp");
    }
  }

  public long maskedOffsetBins(long resolutionBp) {
    if (resolutionBp < 1) throw new IllegalArgumentException("Resolution must be positive");
    // Equivalent to ceil(distanceBp / resolutionBp) - 1 without floating-point rounding.
    return enabled ? (distanceBp - 1) / resolutionBp : -1;
  }
}
