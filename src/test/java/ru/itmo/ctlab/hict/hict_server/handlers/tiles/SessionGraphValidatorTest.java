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
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SessionGraphValidatorTest {
  private static JsonObject source() {
    return new JsonObject().put("type", "source").put("source", "PRIMARY");
  }

  private static JsonObject graph(JsonObject root) {
    return new JsonObject().put("enabled", true).put("swapUpperLower", false)
      .put("upperExpression", root).put("lowerExpression", source());
  }

  private static void invalid(JsonObject node, String field) {
    var error = assertThrows(IllegalArgumentException.class,
      () -> SessionGraphValidator.validate(graph(node), Set.of("trk_7")));
    assertTrue(error.getMessage().contains("renderPipeline.upperExpression" + field), error.getMessage());
  }

  @Test void preservesValidExpressionsAndAliases() {
    var color = new JsonObject().put("type", "rgb").put("c1", source())
      .put("c2", 100).put("c3", 0).put("alpha", 0.5);
    var root = new JsonObject().put("type", "pixel_blend").put("mode", "SCREEN")
      .put("foreground", color).put("background", new JsonObject().put("type", "colormap")
        .put("input", new JsonObject().put("type", "track1d").put("trackId", "trk_7").put("axis", "COLUMN")))
      .put("topOpacity", new JsonObject().put("type", "constant").put("value", 0.7));
    var json = graph(root);
    var before = json.encode();
    SessionGraphValidator.validate(json, Set.of("trk_7"));
    assertEquals(before, json.encode());
    assertEquals(json, RenderPipelineConfig.fromJson(json).toJson());
    SessionGraphValidator.validate(RenderPipelineConfig.disabled().toJson(), Set.of());
  }

  @Test void rejectsRendererFallbacksWithFieldPaths() {
    invalid(new JsonObject().put("type", "track1d").put("trackId", "trk_7").put("axis", "SIDEWAYS"), ".axis");
    invalid(new JsonObject().put("type", "track1d").put("trackId", "missing"), ".trackId");
    invalid(new JsonObject().put("type", "log").put("input", source()).put("base", 1), ".base");
    invalid(new JsonObject().put("type", "log_input").put("input", source())
      .put("base", new JsonObject().put("type", "constant").put("value", -2)), ".base.value");
    invalid(new JsonObject().put("type", "colormap").put("input", source()).put("startColor", "red"), ".startColor");
    invalid(new JsonObject().put("type", "pixel_blend").put("mode", "INVALID"), ".mode");
    invalid(new JsonObject().put("type", "pixel_blend").put("top", source()), ".top");
    invalid(new JsonObject().put("type", "rgb").put("c1", "bad"), ".c1");
    invalid(new JsonObject().put("type", "unary").put("op", "ABS"), ".input");
    invalid(new JsonObject().put("type", "constant").put("value", Double.NaN), ".value");
    invalid(new JsonObject().put("type", "binary").put("op", "TYPO"), ".op");
    invalid(new JsonObject().put("type", "unknown"), ".type");
  }

  @Test void boundsDepthAndNodeCountBeforeCompilation() {
    var deep = source();
    for (int i = 0; i < 34; i++) deep = new JsonObject().put("type", "unary").put("op", "ABS").put("input", deep);
    invalid(deep, ".input");
    var wide = source();
    for (int i = 0; i < 14; i++) wide = new JsonObject().put("type", "binary").put("op", "ADD").put("left", wide).put("right", wide);
    invalid(wide, ".");
  }

  @Test void validatesFlagsAndNumericChildTypes() {
    var json = graph(source()).put("enabled", "true");
    assertTrue(assertThrows(IllegalArgumentException.class,
      () -> SessionGraphValidator.validate(json, Set.of())).getMessage().contains("renderPipeline.enabled"));
    invalid(new JsonObject().put("type", "log").put("input", new JsonObject().put("type", "rgb")), ".input");
    invalid(new JsonObject().put("type", "clamp").put("input", source()).put("minValue", Double.POSITIVE_INFINITY), ".minValue");
  }
}
