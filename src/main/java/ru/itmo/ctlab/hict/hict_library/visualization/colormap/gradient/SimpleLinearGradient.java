/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient;

import lombok.AccessLevel;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.DoubleColormap;

import java.awt.*;
import java.awt.image.ComponentColorModel;

@Getter(AccessLevel.PUBLIC)
public class SimpleLinearGradient extends DoubleColormap {
  private final Color startColor;
  private final Color endColor;
  private final double minSignal;
  private final double maxSignal;
  private final double gamma;
  private final java.util.List<Double> curve;
  private final String curveMode;
  @Getter(AccessLevel.NONE)
  private final java.util.function.DoubleUnaryOperator colorTransfer;
  private final double signalRange;
  private final double[] deltaComponents;
  private final int componentCount;
  private final float[] startComponents, endComponents;

  public SimpleLinearGradient(final int bitDepth, final @NotNull Color startColor, final @NotNull Color endColor, final double minSignal, final double maxSignal) {
    this(bitDepth, startColor, endColor, minSignal, maxSignal, 1.0d);
  }

  public SimpleLinearGradient(final int bitDepth, final @NotNull Color startColor, final @NotNull Color endColor,
                              final double minSignal, final double maxSignal, final double gamma) {
    this(bitDepth, startColor, endColor, minSignal, maxSignal, gamma, null);
  }

  public SimpleLinearGradient(int bitDepth, Color startColor, Color endColor, double minSignal,
                              double maxSignal, double gamma, java.util.List<Double> curve) {
    this(bitDepth, startColor, endColor, minSignal, maxSignal, gamma, curve, null);
  }

  public SimpleLinearGradient(int bitDepth, Color startColor, Color endColor, double minSignal,
                              double maxSignal, double gamma, java.util.List<Double> curve, String curveMode) {
    super(bitDepth);
    validateGamma(gamma);
    this.gamma = gamma;
    this.curve = validateCurve(curve);
    this.curveMode = validateCurveMode(curveMode);
    this.colorTransfer = prepareTransfer(gamma, this.curve, this.curveMode);
    this.startColor = startColor;
    this.endColor = endColor;
    this.minSignal = minSignal;
    this.maxSignal = maxSignal;
    this.signalRange = maxSignal - minSignal;
    if (this.signalRange <= 0) {
      throw new IllegalArgumentException("Signal range must be positive: min < max");
    }
    this.startComponents = this.startColor.getRGBComponents(null);
    this.endComponents = this.endColor.getRGBComponents(null);
    assert (startComponents.length == endComponents.length) : "Different component counts in the same color space??";
    this.componentCount = startComponents.length;
    this.deltaComponents = new double[this.componentCount];
    for (int i = 0; i < this.componentCount; i++) {
      this.deltaComponents[i] = (double) endComponents[i] - (double) startComponents[i];
    }
  }

  /** Alternating normalized input and output coordinates. End pairs default to (0,0) and (1,1). */
  public static java.util.List<Double> validateCurve(java.util.List<Double> points) {
    if (points == null) return null;
    if (points.size() % 2 != 0 || points.size() > 128) throw new IllegalArgumentException("Invalid color curve length");
    double previous = -1;
    for (int i = 0; i < points.size(); i += 2) {
      final Double x = points.get(i), y = points.get(i + 1);
      if (x == null || y == null || !(x >= 0 && x <= 1 && x > previous && y >= 0 && y <= 1))
        throw new IllegalArgumentException("Invalid color curve point");
      previous = x;
    }
    return java.util.List.copyOf(points);
  }

  public static double transfer(double fraction, double gamma, java.util.List<Double> points) {
    final double x = Math.max(0.0d, Math.min(1.0d, fraction));
    if (points == null) return gamma == 1 ? x : Math.pow(x, gamma);
    double left = 0, lower = 0;
    final boolean hasStart = !points.isEmpty() && points.getFirst() == 0.0d;
    if (hasStart) {
      lower = points.get(1);
      if (x == 0) return lower;
    }
    if (x == 1 && !points.isEmpty() && points.get(points.size() - 2) == 1) return points.getLast();
    for (int i = hasStart ? 2 : 0; i < points.size(); i += 2) {
      final double right = points.get(i), upper = points.get(i + 1);
      if (x <= right) return lower + (upper-lower)*(x-left)/(right-left);
      left = right;
      lower = upper;
    }
    return lower + (1-lower)*(x-left)/(1-left);
  }

  public static String validateCurveMode(Object mode) {
    if (mode == null) return null;
    if ("smooth-value".equals(mode) || "smooth-log".equals(mode)) return (String) mode;
    throw new IllegalArgumentException("Invalid color curve mode");
  }

  public static java.util.function.DoubleUnaryOperator prepareTransfer(double gamma, java.util.List<Double> points, String mode) {
    return mode == null ? fraction -> transfer(fraction, gamma, points) : SmoothColorCurve.prepare(points, validateCurveMode(mode));
  }

  public double transferFraction(double fraction) {
    return colorTransfer.applyAsDouble(fraction);
  }

  public static void validateGamma(double gamma) {
    if (!Double.isFinite(gamma) || gamma < 0.05d || gamma > 100.0d)
      throw new IllegalArgumentException("Color gamma must be finite and between 0.05 and 100");
  }

  @Override
  public @NotNull Color mapSignal(final double value) {
    final var linear = Math.max(0.0d, Math.min((value - this.minSignal) / this.signalRange, 1.0d));
    final var standardized = transferFraction(linear);
    final var components = new float[componentCount];
    for (int i = 0; i < componentCount; i++) {
      components[i] = (float) (Double.max(0, ((double) startComponents[i] + this.deltaComponents[i] * standardized)));
    }
    final var floatAlpha = (float) Double.min(1.0d, Double.max(0.0d, ((double) startComponents[this.componentCount - 1] + this.deltaComponents[this.componentCount - 1] * standardized)));
    final var color = new Color(
      ComponentColorModel.getRGBdefault().getColorSpace(),
      components,
      floatAlpha
    );
    return color;
  }
}
