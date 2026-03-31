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

import static org.junit.jupiter.api.Assertions.*;

class RenderPipelineConfigBuiltinCoolerTrackTest {
  @Test
  void builtinCoolerWeightsTrackUsesContextWeightsAndDoesNotRequireTrackSampling() {
    final var upper = new JsonObject()
      .put("type", "colormap")
      .put("input", new JsonObject()
        .put("type", "track1d")
        .put("trackId", RenderPipelineConfig.BUILTIN_COOLER_WEIGHTS_TRACK_ID)
        .put("axis", "ROW"))
      .put("minSignal", 0.0d)
      .put("maxSignal", 10.0d)
      .put("startColor", "#00000000")
      .put("endColor", "#ffffffff");

    final var lower = new JsonObject()
      .put("type", "colormap")
      .put("input", new JsonObject()
        .put("type", "track1d")
        .put("trackId", RenderPipelineConfig.BUILTIN_COOLER_WEIGHTS_TRACK_ID)
        .put("axis", "COL"))
      .put("minSignal", 0.0d)
      .put("maxSignal", 10.0d)
      .put("startColor", "#00000000")
      .put("endColor", "#ffffffff");

    final var config = RenderPipelineConfig.fromJson(new JsonObject()
      .put("enabled", true)
      .put("swapUpperLower", false)
      .put("upperExpression", upper)
      .put("lowerExpression", lower));

    final var context = new RenderPipelineConfig.MutablePixelContext();
    context.rowWeight = 2.5d;
    context.colWeight = 7.0d;

    assertEquals(2.5d, config.evaluate(true, context), 1e-12);
    assertEquals(7.0d, config.evaluate(false, context), 1e-12);
    assertTrue(config.requiredTrackBindings().isEmpty());
  }

  @Test
  void logInputNode_usesDynamicBaseExpression() {
    final var expression = new JsonObject()
      .put("type", "colormap")
      .put(
        "input",
        new JsonObject()
          .put("type", "log_input")
          .put("input", new JsonObject().put("type", "source").put("source", "PRIMARY"))
          .put("base", new JsonObject().put("type", "dynamic").put("field", "BP_RESOLUTION"))
      )
      .put("minSignal", 0.0d)
      .put("maxSignal", 10.0d)
      .put("startColor", "#00000000")
      .put("endColor", "#ffffffff");

    final var config = RenderPipelineConfig.fromJson(new JsonObject()
      .put("enabled", true)
      .put("swapUpperLower", false)
      .put("upperExpression", expression)
      .put("lowerExpression", expression));

    final var context = new RenderPipelineConfig.MutablePixelContext();
    context.primaryValue = 99.0d;
    context.bpResolution = 10L;
    final var expected = Math.log1p(99.0d) / Math.log(10.0d);
    assertEquals(expected, config.evaluate(true, context), 1e-12);
  }
}
