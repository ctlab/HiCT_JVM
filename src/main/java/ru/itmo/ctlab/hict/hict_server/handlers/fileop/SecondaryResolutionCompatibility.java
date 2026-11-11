package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

/** Compare matrix dimensions by base-pair resolution, never by array position. */
final class SecondaryResolutionCompatibility {
  private SecondaryResolutionCompatibility() { }

  static boolean matchingSharedLevels(long[] primaryResolutions, long[] primaryBins,
                                long[] secondaryResolutions, long[] secondaryBins) {
    if (primaryResolutions.length != primaryBins.length
        || secondaryResolutions.length != secondaryBins.length
        || secondaryResolutions.length == 0) return false;
    for (int s = 0; s < secondaryResolutions.length; s++) {
      if (secondaryResolutions[s] <= 0 || secondaryBins[s] < 0) return false;
      for (int p = 0; p < primaryResolutions.length; p++) {
        if (primaryResolutions[p] == secondaryResolutions[s]) {
          if (primaryBins[p] != secondaryBins[s]) return false;
          break;
        }
      }
    }
    return true;
  }
}
