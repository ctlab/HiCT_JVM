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

import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;

class RenderPipelineConfigDefaultStateTest {
  @Test
  void disabled_usesColdHotLikeDefaultExpressionWithRowAndColumnWeights() {
    final var config = RenderPipelineConfig.disabled();
    assertFalse(config.enabled());
    assertFalse(config.swapUpperLower());

    final var ctx = new RenderPipelineConfig.MutablePixelContext();
    ctx.primaryValue = 100.0d;
    ctx.secondaryValue = 100.0d;
    ctx.rowWeight = 2.0d;
    ctx.colWeight = 3.0d;

    final var preLog = Math.log1p(ctx.primaryValue) / Math.log(10.0d);
    final var weighted = preLog * ctx.rowWeight * ctx.colWeight;
    final var postLog = Math.log1p(weighted) / Math.log(5.0d);
    final var expectedSignal = Math.max(0.0d, Math.min(0.75d, postLog));

    assertEquals(expectedSignal, config.evaluate(true, ctx), 1e-12);
    assertEquals(expectedSignal, config.evaluate(false, ctx), 1e-12);

    final var upper = config.toJson().getJsonObject("upperExpression");
    assertNotNull(upper);
    assertTrue(containsNodeType(upper, "colormap"));
    assertFalse(containsNodeType(upper, "clamp"));
    assertTrue(containsTrackAxis(upper, "ROW"));
    assertTrue(containsTrackAxis(upper, "COL"));
    assertTrue(containsSource(upper, "PRIMARY"));
    assertEquals("#0013e300", findFirstValueByType(upper, "colormap", "startColor"));
    assertEquals("#e80000ff", findFirstValueByType(upper, "colormap", "endColor"));
    assertEquals(0.75d, Double.parseDouble(findFirstValueByType(upper, "colormap", "maxSignal")), 1e-12);
  }

  private static boolean containsNodeType(final JsonObject root, final String expectedType) {
    final Deque<JsonObject> queue = new ArrayDeque<>();
    queue.add(root);
    while (!queue.isEmpty()) {
      final var current = queue.removeFirst();
      if (expectedType.equalsIgnoreCase(current.getString("type", ""))) {
        return true;
      }
      for (final var key : current.fieldNames()) {
        final var value = current.getValue(key);
        if (value instanceof JsonObject jsonObject) {
          queue.addLast(jsonObject);
        }
      }
    }
    return false;
  }

  private static boolean containsDynamicField(final JsonObject root, final String expectedField) {
    final Deque<JsonObject> queue = new ArrayDeque<>();
    queue.add(root);
    while (!queue.isEmpty()) {
      final var current = queue.removeFirst();
      if ("dynamic".equalsIgnoreCase(current.getString("type", ""))
        && expectedField.equalsIgnoreCase(current.getString("field", ""))) {
        return true;
      }
      for (final var key : current.fieldNames()) {
        final var value = current.getValue(key);
        if (value instanceof JsonObject jsonObject) {
          queue.addLast(jsonObject);
        }
      }
    }
    return false;
  }

  private static boolean containsTrackAxis(final JsonObject root, final String expectedAxis) {
    final Deque<JsonObject> queue = new ArrayDeque<>();
    queue.add(root);
    while (!queue.isEmpty()) {
      final var current = queue.removeFirst();
      if ("track1d".equalsIgnoreCase(current.getString("type", ""))
        && RenderPipelineConfig.BUILTIN_COOLER_WEIGHTS_TRACK_ID.equals(current.getString("trackId", ""))
        && expectedAxis.equalsIgnoreCase(current.getString("axis", ""))) {
        return true;
      }
      for (final var key : current.fieldNames()) {
        final var value = current.getValue(key);
        if (value instanceof JsonObject jsonObject) {
          queue.addLast(jsonObject);
        }
      }
    }
    return false;
  }

  private static boolean containsSource(final JsonObject root, final String expectedSource) {
    final Deque<JsonObject> queue = new ArrayDeque<>();
    queue.add(root);
    while (!queue.isEmpty()) {
      final var current = queue.removeFirst();
      if ("source".equalsIgnoreCase(current.getString("type", ""))
        && expectedSource.equalsIgnoreCase(current.getString("source", ""))) {
        return true;
      }
      for (final var key : current.fieldNames()) {
        final var value = current.getValue(key);
        if (value instanceof JsonObject jsonObject) {
          queue.addLast(jsonObject);
        }
      }
    }
    return false;
  }

  private static String findFirstValueByType(
    final JsonObject root,
    final String type,
    final String key
  ) {
    final Deque<JsonObject> queue = new ArrayDeque<>();
    queue.add(root);
    while (!queue.isEmpty()) {
      final var current = queue.removeFirst();
      if (type.equalsIgnoreCase(current.getString("type", "")) && current.containsKey(key)) {
        return String.valueOf(current.getValue(key));
      }
      for (final var field : current.fieldNames()) {
        final var value = current.getValue(field);
        if (value instanceof JsonObject jsonObject) {
          queue.addLast(jsonObject);
        }
      }
    }
    fail("Node type " + type + " with key " + key + " was not found");
    return "";
  }
}
