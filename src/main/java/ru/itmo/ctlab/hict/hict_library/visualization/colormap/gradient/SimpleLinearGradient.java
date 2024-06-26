/*
 * MIT License
 *
 * Copyright (c) 2021-2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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
  private final double signalRange;
  private final double[] deltaComponents;
  private final int componentCount;
  private final float[] startComponents, endComponents;

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
    this.startComponents = this.startColor.getRGBComponents(null);
    this.endComponents = this.endColor.getRGBComponents(null);
    assert (startComponents.length == endComponents.length) : "Different component counts in the same color space??";
    this.componentCount = startComponents.length;
    this.deltaComponents = new double[this.componentCount];
    for (int i = 0; i < this.componentCount; i++) {
      this.deltaComponents[i] = (double) endComponents[i] - (double) startComponents[i];
    }
  }

  @Override
  public @NotNull Color mapSignal(final double value) {
    final var standardized = Math.max(0.0d, Math.min((value - this.minSignal) / this.signalRange, 1.0d));
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
