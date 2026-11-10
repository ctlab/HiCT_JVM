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
import java.util.Locale;
import java.util.Set;

/** Validates session expressions before compilation, without rewriting their JSON.
 * Optional fields retain the renderer's documented defaults. Explicit invalid values
 * must never be silently repaired when claiming a restorable snapshot.
 */
public final class SessionGraphValidator {
  private static final Set<String> COLORS = Set.of("COLORMAP", "RGB", "HSL", "HSV", "PIXEL_BLEND");
  private int nodes;

  private SessionGraphValidator() {}

  public static void validate(JsonObject graph, Set<String> trackIds) {
    if (graph == null) fail("renderPipeline", "expected an object");
    for (String key : new String[]{"enabled", "swapUpperLower"}) {
      if (!(graph.getValue(key) instanceof Boolean)) fail("renderPipeline." + key, "expected a boolean");
    }
    var validator = new SessionGraphValidator();
    for (String key : new String[]{"upperExpression", "lowerExpression"}) {
      validator.node(graph.getValue(key), "renderPipeline." + key, 0, false, trackIds);
    }
  }

  private void node(Object value, String path, int depth, boolean numeric, Set<String> ids) {
    if (depth > 32 || ++nodes > 10000) fail(path, "expression exceeds session complexity limit");
    if (!(value instanceof JsonObject)) fail(path, "expected an expression object");
    var n = (JsonObject) value;
    String type = text(n.getValue("type"), path + ".type").toUpperCase(Locale.ROOT);
    if (numeric && COLORS.contains(type)) fail(path, "expected a numeric expression");
    switch (type) {
      case "SOURCE" -> choice(n, "source", path, "PRIMARY", "SECONDARY");
      case "CONSTANT" -> number(n.getValue("value"), path + ".value");
      case "DYNAMIC" -> choice(n, "field", path, "ROW_BP", "COL_BP", "ROW_BIN", "COL_BIN",
        "ROW_PX", "COL_PX", "ROW_WEIGHT", "COL_WEIGHT", "RESOLUTION_SCALING_COEFF",
        "RESOLUTION_LINEAR_SCALING_COEFF", "DIAG_BP_DISTANCE", "DIAG_BIN_DISTANCE",
        "DIAG_PX_DISTANCE", "BP_RESOLUTION");
      case "TRACK1D" -> {
        String id = text(n.getValue("trackId"), path + ".trackId");
        if (!ids.contains(id) && !id.equalsIgnoreCase("__builtin_cooler_weights__")
            && !id.equalsIgnoreCase("__builtin_cooler_weights"))
          fail(path + ".trackId", "references missing track " + id);
        optionalChoice(n, "axis", path, "ROW", "COL", "COLUMN");
      }
      case "UNARY" -> {
        choice(n, "op", path, "ABS", "LOG", "LOG1P", "EXP", "NEG");
        child(n, "input", path, depth, ids);
      }
      case "BINARY" -> {
        choice(n, "op", path, "ADD", "SUB", "MUL", "DIV", "MAX", "MIN");
        child(n, "left", path, depth, ids);
        child(n, "right", path, depth, ids);
      }
      case "LOG", "LOG_INPUT" -> {
        child(n, "input", path, depth, ids);
        if (type.equals("LOG")) {
          if (n.containsKey("base")) logBase(number(n.getValue("base"), path + ".base"), path + ".base");
        } else {
          scalarOrNode(n, "base", path, depth, ids);
          optionalNumber(n, "baseValue", path);
          Object base = n.getValue("base");
          if (base instanceof Number) logBase(number(base, path + ".base"), path + ".base");
          if (base instanceof JsonObject b && "constant".equalsIgnoreCase(b.getString("type")))
            logBase(number(b.getValue("value"), path + ".base.value"), path + ".base.value");
          if (n.containsKey("baseValue")) logBase(number(n.getValue("baseValue"), path + ".baseValue"), path + ".baseValue");
        }
      }
      case "CLAMP", "COLORMAP" -> {
        child(n, "input", path, depth, ids);
        for (String key : new String[]{"min", "max"}) scalarOrNode(n, key, path, depth, ids);
        for (String key : new String[]{"minValue", "maxValue", "minSignal", "maxSignal"}) optionalNumber(n, key, path);
        if (type.equals("COLORMAP")) {
          optionalChoice(n, "mode", path, "LINEAR", "MODDOTPLOT_IDENTITY");
          for (String key : new String[]{"startColor", "endColor"}) if (n.containsKey(key)) color(n.getValue(key), path + "." + key);
        }
      }
      case "RGB", "HSL", "HSV" -> {
        String[] channels = switch (type) {
          case "RGB" -> new String[]{"r", "g", "b"};
          case "HSL" -> new String[]{"h", "s", "l"};
          default -> new String[]{"h", "s", "v"};
        };
        for (String key : channels) {
          scalarOrNode(n, key, path, depth, ids);
          optionalNumber(n, key + "Value", path);
        }
        for (String key : new String[]{"c1", "c2", "c3", "a", "alpha"}) scalarOrNode(n, key, path, depth, ids);
        optionalNumber(n, "aValue", path);
        optionalNumber(n, "alphaValue", path);
      }
      case "PIXEL_BLEND" -> {
        optionalChoice(n, "mode", path, "OVER", "ADD", "SUBTRACT", "MULTIPLY", "SCREEN",
          "DIFFERENCE", "LIGHTEN", "DARKEN", "XOR");
        for (String key : new String[]{"top", "foreground", "upper", "bottom", "background", "lower"}) {
          if (n.containsKey(key)) {
            node(n.getValue(key), path + "." + key, depth + 1, false, ids);
            if (!COLORS.contains(text(((JsonObject)n.getValue(key)).getValue("type"), path + "." + key + ".type").toUpperCase(Locale.ROOT)))
              fail(path + "." + key, "expected a color expression");
          }
        }
        for (String key : new String[]{"topOpacity", "topAlpha", "bottomOpacity", "bottomAlpha"}) {
          scalarOrNode(n, key, path, depth, ids);
          optionalNumber(n, key + "Value", path);
        }
      }
      default -> fail(path + ".type", "unsupported expression type " + type);
    }
  }

  private void child(JsonObject n, String key, String path, int depth, Set<String> ids) {
    node(n.getValue(key), path + "." + key, depth + 1, true, ids);
  }

  private void scalarOrNode(JsonObject n, String key, String path, int depth, Set<String> ids) {
    if (!n.containsKey(key)) return;
    if (n.getValue(key) instanceof JsonObject) child(n, key, path, depth, ids);
    else number(n.getValue(key), path + "." + key);
  }

  private static void optionalNumber(JsonObject n, String key, String path) {
    if (n.containsKey(key)) number(n.getValue(key), path + "." + key);
  }

  private static double number(Object value, String path) {
    if (!(value instanceof Number) || !Double.isFinite(((Number)value).doubleValue()))
      fail(path, "expected a finite number");
    return ((Number)value).doubleValue();
  }

  private static String text(Object value, String path) {
    if (!(value instanceof String) || ((String)value).isBlank()) fail(path, "expected a nonempty string");
    return ((String)value).trim();
  }

  private static void optionalChoice(JsonObject n, String key, String path, String... values) {
    if (n.containsKey(key)) choice(n, key, path, values);
  }

  private static void choice(JsonObject n, String key, String path, String... values) {
    String value = text(n.getValue(key), path + "." + key).toUpperCase(Locale.ROOT);
    if (!Set.of(values).contains(value)) fail(path + "." + key, "unsupported value " + value);
  }

  private static void logBase(double base, String path) {
    if (base <= 0 || Math.abs(base - 1) < 1e-9) fail(path, "log base must be positive and different from one");
  }

  private static void color(Object value, String path) {
    String color = text(value, path);
    if (color.matches("#[0-9a-fA-F]{6}([0-9a-fA-F]{2})?")) return;
    String lower = color.toLowerCase(Locale.ROOT);
    boolean rgba = lower.startsWith("rgba(");
    if ((rgba || lower.startsWith("rgb(")) && lower.endsWith(")")) {
      String[] components = lower.substring(rgba ? 5 : 4, lower.length() - 1).split(",", -1);
      if (components.length == (rgba ? 4 : 3)) {
        try {
          for (String component : components) {
            double v = Double.parseDouble(component.trim());
            if (!Double.isFinite(v) || v < 0 || v > 255) fail(path, "color components must be in [0,255]");
          }
          return;
        } catch (NumberFormatException ignored) { /* report field below */ }
      }
    }
    fail(path, "expected #RRGGBB, #RRGGBBAA, rgb(...) or rgba(...)");
  }

  private static void fail(String path, String message) {
    throw new IllegalArgumentException(path + ": " + message);
  }
}
