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

public final class RenderPipelineConfig {
  public static final String LOCAL_MAP_KEY = "renderPipelineConfig";

  private final boolean enabled;
  private final boolean swapUpperLower;
  private final @NotNull JsonObject upperExpressionJson;
  private final @NotNull JsonObject lowerExpressionJson;
  private final @NotNull CompiledExpression upperExpression;
  private final @NotNull CompiledExpression lowerExpression;

  private RenderPipelineConfig(final boolean enabled,
                               final boolean swapUpperLower,
                               final @NotNull JsonObject upperExpressionJson,
                               final @NotNull JsonObject lowerExpressionJson) {
    this.enabled = enabled;
    this.swapUpperLower = swapUpperLower;
    this.upperExpressionJson = upperExpressionJson.copy();
    this.lowerExpressionJson = lowerExpressionJson.copy();
    this.upperExpression = compileExpression(this.upperExpressionJson);
    this.lowerExpression = compileExpression(this.lowerExpressionJson);
  }

  public static @NotNull RenderPipelineConfig disabled() {
    final var defaultNode = defaultSourceNode("PRIMARY");
    return new RenderPipelineConfig(false, false, defaultNode, defaultNode);
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
      upper != null ? upper : defaultSourceNode("PRIMARY"),
      lower != null ? lower : defaultSourceNode("PRIMARY")
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

  public double evaluate(final boolean upperTriangle, final @NotNull MutablePixelContext context) {
    final var useUpper = this.swapUpperLower ? !upperTriangle : upperTriangle;
    final var raw = useUpper ? this.upperExpression.eval(context) : this.lowerExpression.eval(context);
    if (Double.isFinite(raw)) {
      return raw;
    }
    return 0.0d;
  }

  private static @NotNull JsonObject defaultSourceNode(final @NotNull String source) {
    return new JsonObject()
      .put("type", "source")
      .put("source", source);
  }

  private static @NotNull CompiledExpression compileExpression(final @NotNull JsonObject node) {
    final var nodeType = node.getString("type", "source").trim().toUpperCase();
    return switch (nodeType) {
      case "SOURCE" -> {
        final var sourceName = node.getString("source", "PRIMARY").trim().toUpperCase();
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
        final var field = node.getString("field", "PRIMARY").trim().toUpperCase();
        yield switch (field) {
          case "ROW_BP" -> context -> context.rowBp;
          case "COL_BP" -> context -> context.colBp;
          case "ROW_BIN" -> context -> context.rowBin;
          case "COL_BIN" -> context -> context.colBin;
          case "ROW_PX" -> context -> context.rowPx;
          case "COL_PX" -> context -> context.colPx;
          case "ROW_WEIGHT" -> context -> context.rowWeight;
          case "COL_WEIGHT" -> context -> context.colWeight;
          case "DIAG_BP_DISTANCE" -> context -> Math.abs(context.rowBp - context.colBp);
          case "DIAG_BIN_DISTANCE" -> context -> Math.abs(context.rowBin - context.colBin);
          case "DIAG_PX_DISTANCE" -> context -> Math.abs(context.rowPx - context.colPx);
          case "BP_RESOLUTION" -> context -> context.bpResolution;
          default -> throw new IllegalArgumentException("Unsupported dynamic field: " + field);
        };
      }
      case "UNARY" -> {
        final var op = node.getString("op", "ABS").trim().toUpperCase();
        final var inputNode = node.getJsonObject("input", defaultSourceNode("PRIMARY"));
        final var inputExpression = compileExpression(inputNode);
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
      case "BINARY" -> {
        final var op = node.getString("op", "ADD").trim().toUpperCase();
        final var leftNode = node.getJsonObject("left", defaultSourceNode("PRIMARY"));
        final var rightNode = node.getJsonObject("right", new JsonObject().put("type", "constant").put("value", 0.0d));
        final var leftExpression = compileExpression(leftNode);
        final var rightExpression = compileExpression(rightNode);
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
      default -> throw new IllegalArgumentException("Unsupported expression type: " + nodeType);
    };
  }

  @FunctionalInterface
  private interface CompiledExpression {
    double eval(@NotNull MutablePixelContext context);
  }

  public static final class MutablePixelContext {
    public double primaryValue;
    public double secondaryValue;
    public double rowWeight;
    public double colWeight;
    public long rowPx;
    public long colPx;
    public long rowBin;
    public long colBin;
    public long rowBp;
    public long colBp;
    public long bpResolution;
  }
}
