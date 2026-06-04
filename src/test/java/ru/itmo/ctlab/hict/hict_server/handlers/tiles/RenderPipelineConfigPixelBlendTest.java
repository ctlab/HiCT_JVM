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

class RenderPipelineConfigPixelBlendTest {
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
  void pixelBlendNode_addModeCombinesOpaqueColorLayers() {
    final var blend = new JsonObject()
      .put("type", "pixel_blend")
      .put("mode", "ADD")
      .put("topOpacity", 1.0d)
      .put("bottomOpacity", 1.0d)
      .put(
        "top",
        new JsonObject()
          .put("type", "rgb")
          .put("c1", new JsonObject().put("type", "constant").put("value", 120))
          .put("c2", new JsonObject().put("type", "constant").put("value", 30))
          .put("c3", new JsonObject().put("type", "constant").put("value", 0))
          .put("alpha", new JsonObject().put("type", "constant").put("value", 255))
      )
      .put(
        "bottom",
        new JsonObject()
          .put("type", "rgb")
          .put("c1", new JsonObject().put("type", "constant").put("value", 20))
          .put("c2", new JsonObject().put("type", "constant").put("value", 40))
          .put("c3", new JsonObject().put("type", "constant").put("value", 80))
          .put("alpha", new JsonObject().put("type", "constant").put("value", 255))
      );

    final var config = RenderPipelineConfig.fromJson(
      new JsonObject()
        .put("enabled", true)
        .put("upperExpression", blend)
        .put("lowerExpression", blend.copy())
    );

    final int argb = config.evaluateArgb(true, new RenderPipelineConfig.MutablePixelContext(), dummyOptions());
    final int expectedArgb = (255 << 24) | (140 << 16) | (70 << 8) | 80;
    assertEquals(expectedArgb, argb);
  }

  @Test
  void pixelBlendNode_usesTopColorSignalAsRepresentativePipelineSignal() {
    final var blend = new JsonObject()
      .put("type", "pixel_blend")
      .put(
        "top",
        new JsonObject()
          .put("type", "colormap")
          .put("input", new JsonObject().put("type", "source").put("source", "SECONDARY"))
          .put("startColor", "#00000000")
          .put("endColor", "#00ff00ff")
          .put("minSignal", 1.0d)
          .put("maxSignal", 5.0d)
      )
      .put(
        "bottom",
        new JsonObject()
          .put("type", "rgb")
          .put("c1", new JsonObject().put("type", "constant").put("value", 0))
          .put("c2", new JsonObject().put("type", "constant").put("value", 0))
          .put("c3", new JsonObject().put("type", "constant").put("value", 0))
          .put("alpha", new JsonObject().put("type", "constant").put("value", 0))
      );

    final var config = RenderPipelineConfig.fromJson(
      new JsonObject()
        .put("enabled", true)
        .put("upperExpression", blend)
        .put("lowerExpression", blend.copy())
    );

    final var ctx = new RenderPipelineConfig.MutablePixelContext();
    ctx.secondaryValue = 7.5d;
    final double evaluatedSignal = config.evaluate(true, ctx);
    assertEquals(5.0d, evaluatedSignal, 1e-12);
  }

  @Test
  void sourceSpecificRenderingKeepsPrimaryAndSecondaryColorBranchesSeparate() {
    final var blend = new JsonObject()
      .put("type", "pixel_blend")
      .put("topOpacity", 1.0d)
      .put("bottomOpacity", 1.0d)
      .put(
        "top",
        new JsonObject()
          .put("type", "colormap")
          .put("input", new JsonObject().put("type", "source").put("source", "SECONDARY"))
          .put("startColor", "#00000000")
          .put("endColor", "#00ff00ff")
          .put("minSignal", 0.0d)
          .put("maxSignal", 1.0d)
      )
      .put(
        "bottom",
        new JsonObject()
          .put("type", "colormap")
          .put("input", new JsonObject().put("type", "source").put("source", "PRIMARY"))
          .put("startColor", "#00000000")
          .put("endColor", "#ff0000ff")
          .put("minSignal", 0.0d)
          .put("maxSignal", 1.0d)
      );

    final var config = RenderPipelineConfig.fromJson(
      new JsonObject()
        .put("enabled", true)
        .put("upperExpression", blend)
        .put("lowerExpression", blend.copy())
    );

    final var ctx = new RenderPipelineConfig.MutablePixelContext();
    ctx.primaryValue = 1.0d;
    ctx.secondaryValue = 1.0d;
    assertEquals(0xffff0000, config.evaluateSourceArgb("PRIMARY", ctx, dummyOptions()));
    assertEquals(0xff00ff00, config.evaluateSourceArgb("SECONDARY", ctx, dummyOptions()));
  }
}
