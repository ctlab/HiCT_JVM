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
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;

import java.awt.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class RenderPipelineConfig {
  public static final String LOCAL_MAP_KEY = "renderPipelineConfig";
  public static final String BUILTIN_COOLER_WEIGHTS_TRACK_ID = "__builtin_cooler_weights__";

  private final boolean enabled;
  private final boolean swapUpperLower;
  private final @NotNull JsonObject upperExpressionJson;
  private final @NotNull JsonObject lowerExpressionJson;
  private final @NotNull CompiledRootExpression upperExpression;
  private final @NotNull CompiledRootExpression lowerExpression;
  private final @NotNull Set<TrackBinding> requiredTrackBindings;

  private RenderPipelineConfig(final boolean enabled,
                               final boolean swapUpperLower,
                               final @NotNull JsonObject upperExpressionJson,
                               final @NotNull JsonObject lowerExpressionJson) {
    this.enabled = enabled;
    this.swapUpperLower = swapUpperLower;
    this.upperExpressionJson = upperExpressionJson.copy();
    this.lowerExpressionJson = lowerExpressionJson.copy();
    final var required = new HashSet<TrackBinding>();
    this.upperExpression = compileRootExpression(this.upperExpressionJson, required);
    this.lowerExpression = compileRootExpression(this.lowerExpressionJson, required);
    this.requiredTrackBindings = Collections.unmodifiableSet(required);
  }

  public static @NotNull RenderPipelineConfig disabled() {
    final var defaultExpression = defaultColdHotExpression();
    return new RenderPipelineConfig(false, false, defaultExpression, defaultExpression.copy());
  }

  public static @NotNull RenderPipelineConfig fromVisualizationOptions(final @NotNull SimpleVisualizationOptions options,
                                                                       final boolean enabled,
                                                                       final boolean swapUpperLower) {
    final var expression = buildExpressionFromVisualizationOptions(options);
    return new RenderPipelineConfig(
      enabled,
      swapUpperLower,
      expression,
      expression.copy()
    );
  }

  public static @NotNull RenderPipelineConfig fromJson(final JsonObject json) {
    if (json == null) {
      return disabled();
    }
    final var enabled = json.getBoolean("enabled", false);
    final var swapUpperLower = json.getBoolean("swapUpperLower", false);
    final var upper = json.getJsonObject("upperExpression", json.getJsonObject("upper"));
    final var lower = json.getJsonObject("lowerExpression", json.getJsonObject("lower"));
    return new RenderPipelineConfig(
      enabled,
      swapUpperLower,
      upper != null ? upper : defaultColdHotExpression(),
      lower != null ? lower : defaultColdHotExpression()
    );
  }

  public @NotNull JsonObject toJson() {
    return new JsonObject()
      .put("enabled", this.enabled)
      .put("swapUpperLower", this.swapUpperLower)
      .put("upperExpression", this.upperExpressionJson.copy())
      .put("lowerExpression", this.lowerExpressionJson.copy());
  }

  public boolean enabled() {
    return this.enabled;
  }

  public boolean swapUpperLower() {
    return this.swapUpperLower;
  }

  public @NotNull Set<TrackBinding> requiredTrackBindings() {
    return this.requiredTrackBindings;
  }

  public double evaluate(final boolean upperTriangle, final @NotNull MutablePixelContext context) {
    final var root = selectRootExpression(upperTriangle);
    final var raw = root.evalSignal(context);
    if (Double.isFinite(raw)) {
      return raw;
    }
    return 0.0d;
  }

  public int evaluateArgb(final boolean upperTriangle,
                          final @NotNull MutablePixelContext context,
                          final @NotNull SimpleVisualizationOptions options) {
    final var root = selectRootExpression(upperTriangle);
    return root.evalArgb(context, options);
  }

  private @NotNull CompiledRootExpression selectRootExpression(final boolean upperTriangle) {
    final var useUpper = this.swapUpperLower ? !upperTriangle : upperTriangle;
    return useUpper ? this.upperExpression : this.lowerExpression;
  }

  private static @NotNull JsonObject defaultSourceNode(final @NotNull String source) {
    return new JsonObject()
      .put("type", "source")
      .put("source", source);
  }

  private static @NotNull JsonObject constantNode(final double value) {
    return new JsonObject()
      .put("type", "constant")
      .put("value", value);
  }

  private static @NotNull JsonObject dynamicNode(final @NotNull String field) {
    return new JsonObject()
      .put("type", "dynamic")
      .put("field", field);
  }

  private static @NotNull JsonObject trackNode(final @NotNull String trackId,
                                               final @NotNull String axis) {
    return new JsonObject()
      .put("type", "track1d")
      .put("trackId", trackId)
      .put("axis", axis);
  }

  private static @NotNull JsonObject unaryNode(final @NotNull String op,
                                               final @NotNull JsonObject input) {
    return new JsonObject()
      .put("type", "unary")
      .put("op", op)
      .put("input", input.copy());
  }

  private static @NotNull JsonObject logNode(final @NotNull JsonObject input,
                                             final double base) {
    return new JsonObject()
      .put("type", "log")
      .put("input", input.copy())
      .put("base", base);
  }

  private static @NotNull JsonObject logInputNode(final @NotNull JsonObject input,
                                                  final @NotNull JsonObject baseExpression) {
    return new JsonObject()
      .put("type", "log_input")
      .put("input", input.copy())
      .put("base", baseExpression.copy());
  }

  private static @NotNull JsonObject binaryNode(final @NotNull String op,
                                                final @NotNull JsonObject left,
                                                final @NotNull JsonObject right) {
    return new JsonObject()
      .put("type", "binary")
      .put("op", op)
      .put("left", left.copy())
      .put("right", right.copy());
  }

  private static @NotNull JsonObject clampNode(final @NotNull JsonObject input,
                                               final double minValue,
                                               final double maxValue) {
    return new JsonObject()
      .put("type", "clamp")
      .put("input", input.copy())
      .put("minValue", minValue)
      .put("maxValue", maxValue);
  }

  private static @NotNull JsonObject colormapNode(final @NotNull JsonObject input,
                                                  final @NotNull String startColor,
                                                  final @NotNull String endColor,
                                                  final double minSignal,
                                                  final double maxSignal) {
    return new JsonObject()
      .put("type", "colormap")
      .put("input", input.copy())
      .put("mode", "LINEAR")
      .put("startColor", startColor)
      .put("endColor", endColor)
      .put("minSignal", minSignal)
      .put("maxSignal", maxSignal);
  }

  private static @NotNull JsonObject applyLogByBase(final @NotNull JsonObject input,
                                                     final double base) {
    if (!Double.isFinite(base) || base <= 0.0d || Math.abs(base - 1.0d) < 1e-9d) {
      return input;
    }
    return logNode(input, base);
  }

  private static @NotNull String colorToHex(final @NotNull Color color) {
    if (color.getAlpha() >= 255) {
      return String.format("#%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue());
    }
    return String.format("#%02x%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue(), color.getAlpha());
  }

  private static @NotNull JsonObject buildExpressionFromVisualizationOptions(final @NotNull SimpleVisualizationOptions options) {
    JsonObject signalExpression = defaultSourceNode("PRIMARY");

    signalExpression = applyLogByBase(signalExpression, options.getPreLogBase());

    if (options.isResolutionScaling()) {
      signalExpression = binaryNode(
        "MUL",
        signalExpression,
        dynamicNode("RESOLUTION_SCALING_COEFF")
      );
    }

    if (options.isResolutionLinearScaling()) {
      signalExpression = binaryNode(
        "MUL",
        signalExpression,
        dynamicNode("RESOLUTION_LINEAR_SCALING_COEFF")
      );
    }

    if (options.isApplyCoolerWeights()) {
      final var coolerWeights = binaryNode(
        "MUL",
        trackNode(BUILTIN_COOLER_WEIGHTS_TRACK_ID, "ROW"),
        trackNode(BUILTIN_COOLER_WEIGHTS_TRACK_ID, "COL")
      );
      signalExpression = binaryNode(
        "MUL",
        signalExpression,
        coolerWeights
      );
    }

    signalExpression = applyLogByBase(signalExpression, options.getPostLogBase());

    Color startColor = new Color(255, 255, 255, 0);
    Color endColor = new Color(0, 96, 0, 255);
    double minSignal = 0.0d;
    double maxSignal = 1.0d;
    if (options.getColormap() instanceof SimpleLinearGradient gradient) {
      startColor = gradient.getStartColor();
      endColor = gradient.getEndColor();
      minSignal = gradient.getMinSignal();
      maxSignal = gradient.getMaxSignal();
    }

    return colormapNode(
      signalExpression,
      colorToHex(startColor),
      colorToHex(endColor),
      minSignal,
      maxSignal
    );
  }

  private static @NotNull JsonObject defaultColdHotExpression() {
    var signalExpression = defaultSourceNode("PRIMARY");
    signalExpression = applyLogByBase(signalExpression, 10.0d);
    final var coolerWeights = binaryNode(
      "MUL",
      trackNode(BUILTIN_COOLER_WEIGHTS_TRACK_ID, "ROW"),
      trackNode(BUILTIN_COOLER_WEIGHTS_TRACK_ID, "COL")
    );
    signalExpression = binaryNode(
      "MUL",
      signalExpression,
      coolerWeights
    );
    signalExpression = applyLogByBase(signalExpression, 5.0d);
    return colormapNode(
      signalExpression,
      "#0013e300",
      "#e80000ff",
      0.0d,
      0.75d
    );
  }

  private static @NotNull CompiledRootExpression compileRootExpression(final @NotNull JsonObject node,
                                                                        final @NotNull Set<TrackBinding> requiredTrackBindings) {
    final var nodeType = node.getString("type", "source").trim().toUpperCase(Locale.ROOT);
    if (isColorNode(nodeType)) {
      final var colorExpression = compileColorExpression(node, requiredTrackBindings);
      final var fallbackSignalExpression =
        compileRepresentativeSignalExpressionForColorNode(node, requiredTrackBindings);
      return new CompiledRootExpression(true, fallbackSignalExpression, colorExpression);
    }
    final var signalExpression = compileNumericExpression(node, requiredTrackBindings);
    return new CompiledRootExpression(false, signalExpression, null);
  }

  private static boolean isColorNode(final @NotNull String nodeType) {
    return switch (nodeType) {
      case "COLORMAP", "RGB", "HSL", "HSV", "PIXEL_BLEND" -> true;
      default -> false;
    };
  }

  private static @NotNull CompiledNumericExpression compileRepresentativeSignalExpressionForColorNode(
    final @NotNull JsonObject node,
    final @NotNull Set<TrackBinding> requiredTrackBindings
  ) {
    final var nodeType = node.getString("type", "colormap").trim().toUpperCase(Locale.ROOT);
    return switch (nodeType) {
      case "COLORMAP" -> compileImplicitColormapSignalExpression(node, requiredTrackBindings);
      case "PIXEL_BLEND" -> {
        final var topNode = firstChildObject(node, "top", "foreground", "upper");
        if (topNode != null) {
          yield compileRootExpression(topNode, requiredTrackBindings).signalExpression();
        }
        final var bottomNode = firstChildObject(node, "bottom", "background", "lower");
        if (bottomNode != null) {
          yield compileRootExpression(bottomNode, requiredTrackBindings).signalExpression();
        }
        yield context -> 0.0d;
      }
      default -> context -> 0.0d;
    };
  }

  private static @NotNull CompiledNumericExpression compileNumericExpression(final @NotNull JsonObject node,
                                                                              final @NotNull Set<TrackBinding> requiredTrackBindings) {
    final var nodeType = node.getString("type", "source").trim().toUpperCase(Locale.ROOT);
    return switch (nodeType) {
      case "SOURCE" -> {
        final var sourceName = node.getString("source", "PRIMARY").trim().toUpperCase(Locale.ROOT);
        yield switch (sourceName) {
          case "SECONDARY" -> context -> context.secondaryValue;
          case "PRIMARY" -> context -> context.primaryValue;
          default -> throw new IllegalArgumentException("Unsupported source: " + sourceName);
        };
      }
      case "CONSTANT" -> {
        final var value = node.getDouble("value", 0.0d);
        yield context -> value;
      }
      case "DYNAMIC" -> {
        final var field = node.getString("field", "PRIMARY").trim().toUpperCase(Locale.ROOT);
        yield switch (field) {
          case "ROW_BP" -> context -> context.rowBp;
          case "COL_BP" -> context -> context.colBp;
          case "ROW_BIN" -> context -> context.rowBin;
          case "COL_BIN" -> context -> context.colBin;
          case "ROW_PX" -> context -> context.rowPx;
          case "COL_PX" -> context -> context.colPx;
          case "ROW_WEIGHT" -> context -> context.rowWeight;
          case "COL_WEIGHT" -> context -> context.colWeight;
          case "RESOLUTION_SCALING_COEFF" -> context -> context.resolutionScalingCoeff;
          case "RESOLUTION_LINEAR_SCALING_COEFF" -> context -> context.resolutionLinearScalingCoeff;
          case "DIAG_BP_DISTANCE" -> context -> Math.abs(context.rowBp - context.colBp);
          case "DIAG_BIN_DISTANCE" -> context -> Math.abs(context.rowBin - context.colBin);
          case "DIAG_PX_DISTANCE" -> context -> Math.abs(context.rowPx - context.colPx);
          case "BP_RESOLUTION" -> context -> context.bpResolution;
          default -> throw new IllegalArgumentException("Unsupported dynamic field: " + field);
        };
      }
      case "TRACK1D" -> {
        final var trackId = node.getString("trackId", "").trim();
        final var axis = parseTrackAxis(node.getString("axis", "ROW"));
        if (isBuiltinCoolerWeightsTrackId(trackId)) {
          yield axis == TrackAxis.ROW ? context -> context.rowWeight : context -> context.colWeight;
        }
        if (!trackId.isBlank()) {
          requiredTrackBindings.add(new TrackBinding(trackId, axis));
        }
        yield context -> context.sampleTrackValue(trackId, axis);
      }
      case "UNARY" -> {
        final var op = node.getString("op", "ABS").trim().toUpperCase(Locale.ROOT);
        final var inputNode = node.getJsonObject("input", defaultSourceNode("PRIMARY"));
        final var inputExpression = compileNumericExpression(inputNode, requiredTrackBindings);
        yield switch (op) {
          case "ABS" -> context -> Math.abs(inputExpression.eval(context));
          case "LOG", "LOG1P" -> context -> Math.log1p(Math.max(0.0d, inputExpression.eval(context)));
          case "EXP" -> context -> {
            final var value = inputExpression.eval(context);
            final var bounded = Math.max(-60.0d, Math.min(60.0d, value));
            return Math.exp(bounded);
          };
          case "NEG" -> context -> -inputExpression.eval(context);
          default -> throw new IllegalArgumentException("Unsupported unary operation: " + op);
        };
      }
      case "LOG" -> {
        final var inputExpression = compileNumericExpression(
          node.getJsonObject("input", defaultSourceNode("PRIMARY")),
          requiredTrackBindings
        );
        final var base = node.getDouble("base", Math.E);
        yield context -> evalLogByBase(inputExpression.eval(context), base);
      }
      case "LOG_INPUT" -> {
        final var inputExpression = compileNumericExpression(
          node.getJsonObject("input", defaultSourceNode("PRIMARY")),
          requiredTrackBindings
        );
        final var baseExpression = compileNumericChildExpression(
          node,
          "base",
          "baseValue",
          Math.E,
          requiredTrackBindings
        );
        yield context -> evalLogByBase(inputExpression.eval(context), baseExpression.eval(context));
      }
      case "BINARY" -> {
        final var op = node.getString("op", "ADD").trim().toUpperCase(Locale.ROOT);
        final var leftNode = node.getJsonObject("left", defaultSourceNode("PRIMARY"));
        final var rightNode = node.getJsonObject("right", constantNode(0.0d));
        final var leftExpression = compileNumericExpression(leftNode, requiredTrackBindings);
        final var rightExpression = compileNumericExpression(rightNode, requiredTrackBindings);
        yield switch (op) {
          case "ADD" -> context -> leftExpression.eval(context) + rightExpression.eval(context);
          case "SUB" -> context -> leftExpression.eval(context) - rightExpression.eval(context);
          case "MUL" -> context -> leftExpression.eval(context) * rightExpression.eval(context);
          case "DIV" -> context -> {
            final var denominator = rightExpression.eval(context);
            if (!Double.isFinite(denominator) || Math.abs(denominator) < 1e-12) {
              return 0.0d;
            }
            return leftExpression.eval(context) / denominator;
          };
          case "MAX" -> context -> Math.max(leftExpression.eval(context), rightExpression.eval(context));
          case "MIN" -> context -> Math.min(leftExpression.eval(context), rightExpression.eval(context));
          default -> throw new IllegalArgumentException("Unsupported binary operation: " + op);
        };
      }
      case "CLAMP" -> {
        final var inputExpression = compileNumericExpression(
          node.getJsonObject("input", defaultSourceNode("PRIMARY")),
          requiredTrackBindings
        );
        final var minExpression = compileNumericChildExpression(node, "min", "minValue", 0.0d, requiredTrackBindings);
        final var maxExpression = compileNumericChildExpression(node, "max", "maxValue", 1.0d, requiredTrackBindings);
        yield context -> {
          final var value = inputExpression.eval(context);
          var minValue = minExpression.eval(context);
          var maxValue = maxExpression.eval(context);
          if (!Double.isFinite(minValue)) {
            minValue = 0.0d;
          }
          if (!Double.isFinite(maxValue)) {
            maxValue = 1.0d;
          }
          if (maxValue < minValue) {
            final var tmp = maxValue;
            maxValue = minValue;
            minValue = tmp;
          }
          return Math.max(minValue, Math.min(maxValue, value));
        };
      }
      case "COLORMAP" -> compileNumericExpression(node.getJsonObject("input", defaultSourceNode("PRIMARY")), requiredTrackBindings);
      default -> throw new IllegalArgumentException("Unsupported expression type: " + nodeType);
    };
  }

  private static double evalLogByBase(final double rawValue,
                                      final double rawBase) {
    final var value = Math.max(0.0d, rawValue);
    final var base = rawBase;
    if (!Double.isFinite(base) || base <= 0.0d || Math.abs(base - 1.0d) < 1e-9d) {
      return Math.log1p(value);
    }
    final var denominator = Math.log(base);
    if (!Double.isFinite(denominator) || Math.abs(denominator) < 1e-12d) {
      return Math.log1p(value);
    }
    return Math.log1p(value) / denominator;
  }

  private static boolean isBuiltinCoolerWeightsTrackId(final @NotNull String trackId) {
    final var normalized = trackId.trim().toLowerCase(Locale.ROOT);
    return BUILTIN_COOLER_WEIGHTS_TRACK_ID.equals(normalized) || "__builtin_cooler_weights".equals(normalized);
  }

  private static @NotNull CompiledNumericExpression compileImplicitColormapSignalExpression(
    final @NotNull JsonObject colormapNode,
    final @NotNull Set<TrackBinding> requiredTrackBindings
  ) {
    final var clampNode = new JsonObject()
      .put("type", "clamp")
      .put("input", colormapNode.getJsonObject("input", defaultSourceNode("PRIMARY")));

    final var minNode = colormapNode.getValue("min");
    final var maxNode = colormapNode.getValue("max");
    if (minNode instanceof JsonObject minExpression) {
      clampNode.put("min", minExpression.copy());
    } else if (colormapNode.containsKey("minValue")) {
      clampNode.put("minValue", colormapNode.getValue("minValue"));
    } else {
      clampNode.put("minValue", colormapNode.getDouble("minSignal", 0.0d));
    }

    if (maxNode instanceof JsonObject maxExpression) {
      clampNode.put("max", maxExpression.copy());
    } else if (colormapNode.containsKey("maxValue")) {
      clampNode.put("maxValue", colormapNode.getValue("maxValue"));
    } else {
      clampNode.put("maxValue", colormapNode.getDouble("maxSignal", 1.0d));
    }

    return compileNumericExpression(clampNode, requiredTrackBindings);
  }

  private static @NotNull CompiledNumericExpression compileNumericChildExpression(final @NotNull JsonObject node,
                                                                                   final @NotNull String objectKey,
                                                                                   final @NotNull String valueKey,
                                                                                   final double fallbackValue,
                                                                                   final @NotNull Set<TrackBinding> requiredTrackBindings) {
    final var objectValue = node.getValue(objectKey);
    if (objectValue instanceof JsonObject objectNode) {
      return compileNumericExpression(objectNode, requiredTrackBindings);
    }
    final var scalarFallback = node.containsKey(valueKey)
      ? node.getDouble(valueKey, fallbackValue)
      : node.getDouble(objectKey, fallbackValue);
    return context -> scalarFallback;
  }

  private static @NotNull CompiledNumericExpression compileNumericChildExpressionMulti(final @NotNull JsonObject node,
                                                                                        final @NotNull String[] objectKeys,
                                                                                        final @NotNull String[] valueKeys,
                                                                                        final double fallbackValue,
                                                                                        final @NotNull Set<TrackBinding> requiredTrackBindings) {
    for (final var objectKey : objectKeys) {
      final var objectValue = node.getValue(objectKey);
      if (objectValue instanceof JsonObject objectNode) {
        return compileNumericExpression(objectNode, requiredTrackBindings);
      }
    }

    for (final var valueKey : valueKeys) {
      final var scalar = node.getValue(valueKey);
      if (scalar instanceof Number number) {
        return context -> number.doubleValue();
      }
    }

    for (final var objectKey : objectKeys) {
      final var scalar = node.getValue(objectKey);
      if (scalar instanceof Number number) {
        return context -> number.doubleValue();
      }
    }

    return context -> fallbackValue;
  }

  private static JsonObject firstChildObject(final @NotNull JsonObject node,
                                             final @NotNull String... keys) {
    for (final var key : keys) {
      final var value = node.getValue(key);
      if (value instanceof JsonObject objectNode) {
        return objectNode;
      }
    }
    return null;
  }

  private static @NotNull CompiledColorExpression compileColorChildExpression(final @NotNull JsonObject node,
                                                                               final @NotNull String[] keys,
                                                                               final int fallbackArgb,
                                                                               final @NotNull Set<TrackBinding> requiredTrackBindings) {
    final var childNode = firstChildObject(node, keys);
    if (childNode != null) {
      final var childType = childNode.getString("type", "colormap").trim().toUpperCase(Locale.ROOT);
      if (isColorNode(childType)) {
        return compileColorExpression(childNode, requiredTrackBindings);
      }
    }
    return context -> fallbackArgb;
  }

  private static @NotNull CompiledColorExpression compileColorExpression(final @NotNull JsonObject node,
                                                                          final @NotNull Set<TrackBinding> requiredTrackBindings) {
    final var nodeType = node.getString("type", "colormap").trim().toUpperCase(Locale.ROOT);
    return switch (nodeType) {
      case "COLORMAP" -> {
        final var inputExpression = compileNumericExpression(
          node.getJsonObject("input", defaultSourceNode("PRIMARY")),
          requiredTrackBindings
        );
        final var minExpression = compileNumericChildExpression(
          node,
          "min",
          "minValue",
          node.getDouble("minSignal", 0.0d),
          requiredTrackBindings
        );
        final var maxExpression = compileNumericChildExpression(
          node,
          "max",
          "maxValue",
          node.getDouble("maxSignal", 1.0d),
          requiredTrackBindings
        );
        final var defaultStart = new Color(255, 255, 255, 0);
        final var defaultEnd = new Color(0, 96, 0, 255);
        final var startColor = parseColor(node.getString("startColor"), defaultStart);
        final var endColor = parseColor(node.getString("endColor"), defaultEnd);
        yield context -> {
          final var value = inputExpression.eval(context);
          final var minValue = minExpression.eval(context);
          final var maxValue = maxExpression.eval(context);
          return mapLinearColor(value, minValue, maxValue, startColor, endColor);
        };
      }
      case "RGB" -> {
        final var rExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"r", "c1"},
          new String[]{"rValue"},
          0.0d,
          requiredTrackBindings
        );
        final var gExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"g", "c2"},
          new String[]{"gValue"},
          0.0d,
          requiredTrackBindings
        );
        final var bExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"b", "c3"},
          new String[]{"bValue"},
          0.0d,
          requiredTrackBindings
        );
        final var aExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"a", "alpha"},
          new String[]{"aValue", "alphaValue"},
          255.0d,
          requiredTrackBindings
        );
        yield context -> toArgb(
          clampColorChannel(rExpression.eval(context)),
          clampColorChannel(gExpression.eval(context)),
          clampColorChannel(bExpression.eval(context)),
          clampAlphaChannel(aExpression.eval(context))
        );
      }
      case "HSL" -> {
        final var hExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"h", "c1"},
          new String[]{"hValue"},
          0.0d,
          requiredTrackBindings
        );
        final var sExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"s", "c2"},
          new String[]{"sValue"},
          1.0d,
          requiredTrackBindings
        );
        final var lExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"l", "c3"},
          new String[]{"lValue"},
          0.5d,
          requiredTrackBindings
        );
        final var aExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"a", "alpha"},
          new String[]{"aValue", "alphaValue"},
          255.0d,
          requiredTrackBindings
        );
        yield context -> {
          final var rgb = hslToRgb(hExpression.eval(context), sExpression.eval(context), lExpression.eval(context));
          return toArgb(rgb[0], rgb[1], rgb[2], clampAlphaChannel(aExpression.eval(context)));
        };
      }
      case "HSV" -> {
        final var hExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"h", "c1"},
          new String[]{"hValue"},
          0.0d,
          requiredTrackBindings
        );
        final var sExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"s", "c2"},
          new String[]{"sValue"},
          1.0d,
          requiredTrackBindings
        );
        final var vExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"v", "c3"},
          new String[]{"vValue"},
          1.0d,
          requiredTrackBindings
        );
        final var aExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"a", "alpha"},
          new String[]{"aValue", "alphaValue"},
          255.0d,
          requiredTrackBindings
        );
        yield context -> {
          final var hue = normalizeHue(hExpression.eval(context));
          final var sat = normalizeUnitInterval(sExpression.eval(context));
          final var val = normalizeUnitInterval(vExpression.eval(context));
          final var rgbInt = Color.HSBtoRGB((float) (hue / 360.0d), (float) sat, (float) val);
          final var red = (rgbInt >> 16) & 0xFF;
          final var green = (rgbInt >> 8) & 0xFF;
          final var blue = rgbInt & 0xFF;
          return toArgb(red, green, blue, clampAlphaChannel(aExpression.eval(context)));
        };
      }
      case "PIXEL_BLEND" -> {
        final var topExpression = compileColorChildExpression(
          node,
          new String[]{"top", "foreground", "upper"},
          0x00000000,
          requiredTrackBindings
        );
        final var bottomExpression = compileColorChildExpression(
          node,
          new String[]{"bottom", "background", "lower"},
          0x00000000,
          requiredTrackBindings
        );
        final var topOpacityExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"topOpacity", "topAlpha"},
          new String[]{"topOpacityValue", "topAlphaValue"},
          1.0d,
          requiredTrackBindings
        );
        final var bottomOpacityExpression = compileNumericChildExpressionMulti(
          node,
          new String[]{"bottomOpacity", "bottomAlpha"},
          new String[]{"bottomOpacityValue", "bottomAlphaValue"},
          1.0d,
          requiredTrackBindings
        );
        final var blendMode = parsePixelBlendMode(node.getString("mode", "OVER"));
        yield context -> blendArgb(
          topExpression.evalArgb(context),
          bottomExpression.evalArgb(context),
          topOpacityExpression.eval(context),
          bottomOpacityExpression.eval(context),
          blendMode
        );
      }
      default -> throw new IllegalArgumentException("Unsupported color expression type: " + nodeType);
    };
  }

  private static @NotNull TrackAxis parseTrackAxis(final String axisRaw) {
    if (axisRaw == null) {
      return TrackAxis.ROW;
    }
    final var normalized = axisRaw.trim().toUpperCase(Locale.ROOT);
    return switch (normalized) {
      case "COL", "COLUMN" -> TrackAxis.COL;
      default -> TrackAxis.ROW;
    };
  }

  private static int mapLinearColor(final double value,
                                    final double minValue,
                                    final double maxValue,
                                    final @NotNull Color startColor,
                                    final @NotNull Color endColor) {
    final var safeMin = Double.isFinite(minValue) ? minValue : 0.0d;
    final var safeMaxCandidate = Double.isFinite(maxValue) ? maxValue : 1.0d;
    final var safeMax = safeMaxCandidate > safeMin ? safeMaxCandidate : safeMin + 1.0d;
    final var standardized = Math.max(0.0d, Math.min((value - safeMin) / (safeMax - safeMin), 1.0d));
    final var red = interpolateColor(startColor.getRed(), endColor.getRed(), standardized);
    final var green = interpolateColor(startColor.getGreen(), endColor.getGreen(), standardized);
    final var blue = interpolateColor(startColor.getBlue(), endColor.getBlue(), standardized);
    final var alpha = interpolateColor(startColor.getAlpha(), endColor.getAlpha(), standardized);
    return toArgb(red, green, blue, alpha);
  }

  private static int interpolateColor(final int start,
                                      final int end,
                                      final double factor) {
    return (int) Math.round(start + (end - start) * factor);
  }

  private static int clampColorChannel(final double value) {
    final var safeValue = Double.isFinite(value) ? value : 0.0d;
    return (int) Math.max(0, Math.min(255, Math.round(safeValue)));
  }

  private static int clampAlphaChannel(final double value) {
    final var safeValue = Double.isFinite(value) ? value : 255.0d;
    if (safeValue >= 0.0d && safeValue <= 1.0d) {
      return (int) Math.max(0, Math.min(255, Math.round(safeValue * 255.0d)));
    }
    return (int) Math.max(0, Math.min(255, Math.round(safeValue)));
  }

  private static @NotNull Color parseColor(final String rawValue,
                                           final @NotNull Color fallback) {
    if (rawValue == null || rawValue.isBlank()) {
      return fallback;
    }
    final var value = rawValue.trim();
    try {
      if (value.startsWith("#")) {
        final var hex = value.substring(1);
        if (hex.length() == 6) {
          final var rgb = Integer.parseInt(hex, 16);
          return new Color((rgb >> 16) & 0xFF, (rgb >> 8) & 0xFF, rgb & 0xFF, 255);
        }
        if (hex.length() == 8) {
          final var rgba = Long.parseLong(hex, 16);
          return new Color(
            (int) ((rgba >> 24) & 0xFF),
            (int) ((rgba >> 16) & 0xFF),
            (int) ((rgba >> 8) & 0xFF),
            (int) (rgba & 0xFF)
          );
        }
      }
      final var normalized = value.toLowerCase(Locale.ROOT);
      if (normalized.startsWith("rgba(")) {
        final var components = normalized.substring(5, normalized.length() - 1).split(",");
        if (components.length == 4) {
          final var r = clampColorChannel(Double.parseDouble(components[0].trim()));
          final var g = clampColorChannel(Double.parseDouble(components[1].trim()));
          final var b = clampColorChannel(Double.parseDouble(components[2].trim()));
          final var a = clampAlphaChannel(Double.parseDouble(components[3].trim()));
          return new Color(r, g, b, a);
        }
      }
      if (normalized.startsWith("rgb(")) {
        final var components = normalized.substring(4, normalized.length() - 1).split(",");
        if (components.length == 3) {
          final var r = clampColorChannel(Double.parseDouble(components[0].trim()));
          final var g = clampColorChannel(Double.parseDouble(components[1].trim()));
          final var b = clampColorChannel(Double.parseDouble(components[2].trim()));
          return new Color(r, g, b, 255);
        }
      }
    } catch (final RuntimeException ignored) {
      // Fall through to fallback.
    }
    return fallback;
  }

  private static int[] hslToRgb(final double hRaw,
                                final double sRaw,
                                final double lRaw) {
    final var h = normalizeHue(hRaw) / 360.0d;
    final var s = normalizeUnitInterval(sRaw);
    final var l = normalizeUnitInterval(lRaw);

    if (s <= 0.0d) {
      final var gray = clampColorChannel(l * 255.0d);
      return new int[]{gray, gray, gray};
    }

    final var q = l < 0.5d ? l * (1.0d + s) : (l + s - l * s);
    final var p = 2.0d * l - q;
    final var r = hueToRgb(p, q, h + 1.0d / 3.0d);
    final var g = hueToRgb(p, q, h);
    final var b = hueToRgb(p, q, h - 1.0d / 3.0d);
    return new int[]{
      clampColorChannel(r * 255.0d),
      clampColorChannel(g * 255.0d),
      clampColorChannel(b * 255.0d)
    };
  }

  private static double hueToRgb(final double p,
                                 final double q,
                                 double t) {
    if (t < 0.0d) {
      t += 1.0d;
    }
    if (t > 1.0d) {
      t -= 1.0d;
    }
    if (t < 1.0d / 6.0d) {
      return p + (q - p) * 6.0d * t;
    }
    if (t < 1.0d / 2.0d) {
      return q;
    }
    if (t < 2.0d / 3.0d) {
      return p + (q - p) * (2.0d / 3.0d - t) * 6.0d;
    }
    return p;
  }

  private static double normalizeHue(final double rawHue) {
    final var safeHue = Double.isFinite(rawHue) ? rawHue : 0.0d;
    final var wrapped = safeHue % 360.0d;
    return wrapped < 0.0d ? wrapped + 360.0d : wrapped;
  }

  private static double normalizeUnitInterval(final double value) {
    final var safeValue = Double.isFinite(value) ? value : 0.0d;
    final var normalized = safeValue > 1.0d ? (safeValue / 100.0d) : safeValue;
    return Math.max(0.0d, Math.min(1.0d, normalized));
  }

  private static int toArgb(final int red,
                            final int green,
                            final int blue,
                            final int alpha) {
    return ((alpha & 0xFF) << 24)
      | ((red & 0xFF) << 16)
      | ((green & 0xFF) << 8)
      | (blue & 0xFF);
  }

  private static @NotNull PixelBlendMode parsePixelBlendMode(final String rawMode) {
    if (rawMode == null || rawMode.isBlank()) {
      return PixelBlendMode.OVER;
    }
    try {
      return PixelBlendMode.valueOf(rawMode.trim().toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException ignored) {
      return PixelBlendMode.OVER;
    }
  }

  private static double normalizeOpacity(final double rawOpacity) {
    final var safeOpacity = Double.isFinite(rawOpacity) ? rawOpacity : 1.0d;
    if (safeOpacity <= 0.0d) {
      return 0.0d;
    }
    if (safeOpacity <= 1.0d) {
      return safeOpacity;
    }
    if (safeOpacity <= 100.0d) {
      return safeOpacity / 100.0d;
    }
    if (safeOpacity <= 255.0d) {
      return safeOpacity / 255.0d;
    }
    return 1.0d;
  }

  private static int blendArgb(final int topArgb,
                               final int bottomArgb,
                               final double rawTopOpacity,
                               final double rawBottomOpacity,
                               final @NotNull PixelBlendMode mode) {
    final var top = toNormalizedRgba(topArgb, normalizeOpacity(rawTopOpacity));
    final var bottom = toNormalizedRgba(bottomArgb, normalizeOpacity(rawBottomOpacity));
    final var overlapAlpha = top[3] * bottom[3];
    final var topOnlyAlpha = top[3] * (1.0d - bottom[3]);
    final var bottomOnlyAlpha = bottom[3] * (1.0d - top[3]);
    final var outAlpha = overlapAlpha + topOnlyAlpha + bottomOnlyAlpha;
    if (outAlpha <= 1e-12d) {
      return 0x00000000;
    }

    final var blendedRed = applyBlendMode(top[0], bottom[0], mode);
    final var blendedGreen = applyBlendMode(top[1], bottom[1], mode);
    final var blendedBlue = applyBlendMode(top[2], bottom[2], mode);

    final var outRed = (
      blendedRed * overlapAlpha
        + top[0] * topOnlyAlpha
        + bottom[0] * bottomOnlyAlpha
      ) / outAlpha;
    final var outGreen = (
      blendedGreen * overlapAlpha
        + top[1] * topOnlyAlpha
        + bottom[1] * bottomOnlyAlpha
      ) / outAlpha;
    final var outBlue = (
      blendedBlue * overlapAlpha
        + top[2] * topOnlyAlpha
        + bottom[2] * bottomOnlyAlpha
      ) / outAlpha;

    return toArgb(
      clampColorChannel(outRed * 255.0d),
      clampColorChannel(outGreen * 255.0d),
      clampColorChannel(outBlue * 255.0d),
      clampAlphaChannel(outAlpha * 255.0d)
    );
  }

  private static double applyBlendMode(final double top,
                                       final double bottom,
                                       final @NotNull PixelBlendMode mode) {
    final var safeTop = Math.max(0.0d, Math.min(1.0d, top));
    final var safeBottom = Math.max(0.0d, Math.min(1.0d, bottom));
    return switch (mode) {
      case OVER -> safeTop;
      case ADD -> Math.min(1.0d, safeTop + safeBottom);
      case SUBTRACT -> Math.max(0.0d, safeTop - safeBottom);
      case MULTIPLY -> safeTop * safeBottom;
      case SCREEN -> 1.0d - ((1.0d - safeTop) * (1.0d - safeBottom));
      case DIFFERENCE -> Math.abs(safeTop - safeBottom);
      case LIGHTEN -> Math.max(safeTop, safeBottom);
      case DARKEN -> Math.min(safeTop, safeBottom);
      case XOR -> {
        final var topByte = clampColorChannel(safeTop * 255.0d);
        final var bottomByte = clampColorChannel(safeBottom * 255.0d);
        yield (topByte ^ bottomByte) / 255.0d;
      }
    };
  }

  private static double[] toNormalizedRgba(final int argb,
                                           final double layerOpacity) {
    final var alpha = (((argb >> 24) & 0xFF) / 255.0d) * Math.max(0.0d, Math.min(1.0d, layerOpacity));
    return new double[]{
      ((argb >> 16) & 0xFF) / 255.0d,
      ((argb >> 8) & 0xFF) / 255.0d,
      (argb & 0xFF) / 255.0d,
      alpha
    };
  }

  @FunctionalInterface
  private interface CompiledNumericExpression {
    double eval(@NotNull MutablePixelContext context);
  }

  @FunctionalInterface
  private interface CompiledColorExpression {
    int evalArgb(@NotNull MutablePixelContext context);
  }

  private record CompiledRootExpression(boolean color,
                                        @NotNull CompiledNumericExpression signalExpression,
                                        CompiledColorExpression colorExpression) {
    double evalSignal(final @NotNull MutablePixelContext context) {
      final var value = this.signalExpression.eval(context);
      return Double.isFinite(value) ? value : 0.0d;
    }

    int evalArgb(final @NotNull MutablePixelContext context,
                 final @NotNull SimpleVisualizationOptions options) {
      if (this.color && this.colorExpression != null) {
        return this.colorExpression.evalArgb(context);
      }
      final var value = evalSignal(context);
      final var color = options.getColormap().mapSignal(value);
      return toArgb(color.getRed(), color.getGreen(), color.getBlue(), color.getAlpha());
    }
  }

  public enum TrackAxis {
    ROW,
    COL
  }

  private enum PixelBlendMode {
    OVER,
    ADD,
    SUBTRACT,
    MULTIPLY,
    SCREEN,
    DIFFERENCE,
    LIGHTEN,
    DARKEN,
    XOR
  }

  public record TrackBinding(@NotNull String trackId,
                             @NotNull TrackAxis axis) {
  }

  public static final class MutablePixelContext {
    public double primaryValue;
    public double secondaryValue;
    public double rowWeight;
    public double colWeight;
    public double resolutionScalingCoeff;
    public double resolutionLinearScalingCoeff;
    public long rowPx;
    public long colPx;
    public long rowBin;
    public long colBin;
    public long rowBp;
    public long colBp;
    public long bpResolution;
    public int rowLocalIndex;
    public int colLocalIndex;
    public @NotNull Map<String, double[]> rowTrackValuesByTrackId = Map.of();
    public @NotNull Map<String, double[]> colTrackValuesByTrackId = Map.of();

    public double sampleTrackValue(final @NotNull String trackId,
                                   final @NotNull TrackAxis axis) {
      final var source = axis == TrackAxis.ROW ? this.rowTrackValuesByTrackId : this.colTrackValuesByTrackId;
      if (source == null || source.isEmpty() || trackId.isBlank()) {
        return 0.0d;
      }
      final var values = source.get(trackId);
      if (values == null || values.length == 0) {
        return 0.0d;
      }
      final var index = axis == TrackAxis.ROW ? this.rowLocalIndex : this.colLocalIndex;
      if (index < 0 || index >= values.length) {
        return 0.0d;
      }
      final var value = values[index];
      return Double.isFinite(value) ? value : 0.0d;
    }
  }
}
