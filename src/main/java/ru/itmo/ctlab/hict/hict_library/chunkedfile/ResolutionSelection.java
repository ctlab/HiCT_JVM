package ru.itmo.ctlab.hict.hict_library.chunkedfile;

/** Select a stored resolution by log distance, preferring the finer level on a tie. */
public final class ResolutionSelection {
  private ResolutionSelection() { }

  public static int nearestOrder(long[] resolutions, long target) {
    if (target <= 0) throw new IllegalArgumentException("Resolution must be positive");
    int finer = -1;
    int coarser = -1;
    for (int i = 0; i < resolutions.length; i++) {
      final long bp = resolutions[i];
      if (bp <= 0) continue;
      if (bp == target) return i;
      if (bp < target && (finer < 0 || bp > resolutions[finer])) finer = i;
      if (bp > target && (coarser < 0 || bp < resolutions[coarser])) coarser = i;
    }
    if (finer < 0) return coarser;
    if (coarser < 0) return finer;
    final double crossover = Math.sqrt((double) resolutions[finer] * resolutions[coarser]);
    return target <= crossover ? finer : coarser;
  }
}
