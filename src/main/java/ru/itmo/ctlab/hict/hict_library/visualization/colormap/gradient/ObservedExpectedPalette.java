package ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient;

import java.awt.Color;

/** Log-ratio palette centered at O/E=1. Undefined ratios are transparent. */
public final class ObservedExpectedPalette {
  private ObservedExpectedPalette() {}

  public static int argb(double ratio, double lower, double upper, Color depleted, Color enriched) {
    if (!Double.isFinite(ratio) || ratio < 0) return 0;
    // Preserve the legacy bounds fallback for existing sessions and dynamic graph limits.
    upper = upper > 1 && Double.isFinite(upper) ? upper : 2;
    lower = lower > 0 && lower < 1 ? lower : 1 / upper;
    double strength = ratio <= lower || ratio >= upper ? 1
      : Math.abs(Math.log(ratio) / Math.log(ratio < 1 ? lower : upper));
    strength = Double.isFinite(strength) ? Math.max(0, Math.min(1, strength)) : 0;
    final Color endpoint = ratio < 1 ? depleted : enriched;
    final int red = channel(255, endpoint.getRed(), strength);
    final int green = channel(255, endpoint.getGreen(), strength);
    final int blue = channel(255, endpoint.getBlue(), strength);
    final int alpha = channel(255, endpoint.getAlpha(), strength);
    return (alpha << 24) | (red << 16) | (green << 8) | blue;
  }

  private static int channel(int neutral, int endpoint, double strength) {
    return (int) Math.round(neutral + (endpoint - neutral) * strength);
  }
}
