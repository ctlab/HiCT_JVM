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

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;

import java.awt.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RenderPipelineConfigColorNodeCompatibilityTest {
  private static SimpleVisualizationOptions dummyOptions() {
    return new SimpleVisualizationOptions(
      -1.0d,
      -1.0d,
      false,
      false,
      false,
      new SimpleLinearGradient(
        32,
        new Color(0, 0, 0, 0),
        new Color(255, 255, 255, 255),
        0.0d,
        1.0d
      )
    );
  }

  @Test
  void rgbNode_acceptsFrontendC1C2C3AlphaShape() {
    final var rgb = new JsonObject()
      .put("type", "rgb")
      .put("c1", new JsonObject().put("type", "constant").put("value", 100))
      .put("c2", new JsonObject().put("type", "constant").put("value", 150))
      .put("c3", new JsonObject().put("type", "constant").put("value", 200))
      .put("alpha", new JsonObject().put("type", "constant").put("value", 0.5));

    final var config = RenderPipelineConfig.fromJson(
      new JsonObject()
        .put("enabled", true)
        .put("upperExpression", rgb)
        .put("lowerExpression", rgb.copy())
    );

    final var ctx = new RenderPipelineConfig.MutablePixelContext();
    final int argb = config.evaluateArgb(true, ctx, dummyOptions());
    final int expectedArgb = (128 << 24) | (100 << 16) | (150 << 8) | 200;
    assertEquals(expectedArgb, argb);
  }

  @Test
  void hslNode_acceptsFrontendC1C2C3AlphaShape() {
    final var hsl = new JsonObject()
      .put("type", "hsl")
      .put("c1", new JsonObject().put("type", "constant").put("value", 0))
      .put("c2", new JsonObject().put("type", "constant").put("value", 1))
      .put("c3", new JsonObject().put("type", "constant").put("value", 0.5))
      .put("alpha", new JsonObject().put("type", "constant").put("value", 1));

    final var config = RenderPipelineConfig.fromJson(
      new JsonObject()
        .put("enabled", true)
        .put("upperExpression", hsl)
        .put("lowerExpression", hsl.copy())
    );

    final var ctx = new RenderPipelineConfig.MutablePixelContext();
    final int argb = config.evaluateArgb(true, ctx, dummyOptions());
    final int expectedArgb = (255 << 24) | (255 << 16);
    assertEquals(expectedArgb, argb);
  }
}
