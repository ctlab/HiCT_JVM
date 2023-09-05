package ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient;

import lombok.AccessLevel;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.DoubleColormap;

import java.awt.*;
import java.awt.color.ColorSpace;

@Getter(AccessLevel.PUBLIC)
public class SimpleLinearGradient extends DoubleColormap {
  private final Color startColor;
  private final Color endColor;
  private final double minSignal;
  private final double maxSignal;
  private final double signalRange;
  private final double[] deltaComponents;
  private final int componentCount;

  public SimpleLinearGradient(final int bitDepth, final @NotNull Color startColor, final @NotNull Color endColor, final double minSignal, final double maxSignal) {
    super(bitDepth);
    this.startColor = startColor;
    this.endColor = endColor;
    this.minSignal = minSignal;
    this.maxSignal = maxSignal;
    this.signalRange = maxSignal - minSignal;
    if (this.signalRange <= 0) {
      throw new IllegalArgumentException("Signal range must be positive: min < max");
    }
    final var startComp = this.startColor.getRGBComponents(null);
    final var endComp = this.endColor.getRGBComponents(null);
    assert (startComp.length == endComp.length) : "Different component counts in the same color space??";
    this.componentCount = startComp.length;
    this.deltaComponents = new double[this.componentCount];
    for (int i = 0; i < this.componentCount; i++) {
      this.deltaComponents[i] = (double) startComp[i] - (double) endComp[i];
    }
  }

  @Override
  public @NotNull Color mapSignal(final double value) {
    final var standardized = Math.max(0.0d, Math.min((value - this.minSignal) / this.signalRange, 1.0d));
    final var components = new float[componentCount];
    for (int i = 0; i < componentCount; i++) {
      components[i] = (float) (this.deltaComponents[i] * standardized);
    }
    return new Color(
      ColorSpace.getInstance(ColorSpace.CS_sRGB),
      components,
      (float) (this.deltaComponents[this.componentCount - 1] * standardized)
    );
  }
}
