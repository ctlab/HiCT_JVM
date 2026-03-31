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

package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;

import java.awt.*;

import static org.junit.jupiter.api.Assertions.*;

class RenderPipelineConfigNormalizationSyncTest {
  private static SimpleVisualizationOptions buildOptions(
    final double preLogBase,
    final double postLogBase,
    final boolean applyCoolerWeights,
    final boolean resolutionScaling,
    final boolean resolutionLinearScaling
  ) {
    return new SimpleVisualizationOptions(
      preLogBase,
      postLogBase,
      applyCoolerWeights,
      resolutionScaling,
      resolutionLinearScaling,
      new SimpleLinearGradient(
        32,
        new Color(0, 0, 0, 0),
        new Color(255, 0, 0, 255),
        0.0d,
        1.0d
      )
    );
  }

  @Test
  void fromVisualizationOptions_buildsEquivalentExpression() {
    final var options = buildOptions(10.0d, 2.0d, true, true, true);
    final var config = RenderPipelineConfig.fromVisualizationOptions(options, true, false);
    assertTrue(config.enabled());
    assertFalse(config.swapUpperLower());

    final var ctx = new RenderPipelineConfig.MutablePixelContext();
    ctx.primaryValue = 100.0d;
    ctx.secondaryValue = 100.0d;
    ctx.rowWeight = 2.0d;
    ctx.colWeight = 3.0d;
    ctx.resolutionScalingCoeff = 0.25d;
    ctx.resolutionLinearScalingCoeff = 0.5d;

    final var preLog = Math.log1p(ctx.primaryValue) / Math.log(10.0d);
    final var weighted = preLog
      * ctx.resolutionScalingCoeff
      * ctx.resolutionLinearScalingCoeff
      * ctx.rowWeight
      * ctx.colWeight;
    final var expected = Math.log1p(weighted) / Math.log(2.0d);
    final var actualUpper = config.evaluate(true, ctx);
    final var actualLower = config.evaluate(false, ctx);
    assertEquals(expected, actualUpper, 1e-12);
    assertEquals(expected, actualLower, 1e-12);
  }

  @Test
  void fromVisualizationOptions_disabledStagesKeepPrimaryValue() {
    final var options = buildOptions(-1.0d, -1.0d, false, false, false);
    final var config = RenderPipelineConfig.fromVisualizationOptions(options, false, true);
    assertFalse(config.enabled());
    assertTrue(config.swapUpperLower());

    final var ctx = new RenderPipelineConfig.MutablePixelContext();
    ctx.primaryValue = 42.75d;
    ctx.secondaryValue = 11.0d;
    ctx.rowWeight = 7.0d;
    ctx.colWeight = 9.0d;
    ctx.resolutionScalingCoeff = 0.123d;
    ctx.resolutionLinearScalingCoeff = 0.456d;
    assertEquals(42.75d, config.evaluate(true, ctx), 1e-12);
    assertEquals(42.75d, config.evaluate(false, ctx), 1e-12);
  }
}

