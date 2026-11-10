package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.SignalDisplayMode;
import ru.itmo.ctlab.hict.hict_library.visualization.CoolerWeightsNaNPolicy;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import java.awt.Color;

/** Lossless session representation, separate from the legacy UI color-string DTO. */
public final class SessionVisualizationCodec {
  private SessionVisualizationCodec() {}

  public static JsonObject encode(SimpleVisualizationOptions options) {
    if (!(options.getColormap() instanceof SimpleLinearGradient gradient))
      throw new IllegalArgumentException("visualizationOptions.colormap: unsupported colormap");
    return new JsonObject().put("preLogBase", options.getPreLogBase()).put("postLogBase", options.getPostLogBase())
      .put("applyCoolerWeights", options.isApplyCoolerWeights()).put("resolutionScaling", options.isResolutionScaling())
      .put("resolutionLinearScaling", options.isResolutionLinearScaling()).put("autoThresholdEnabled", options.isAutoThresholdEnabled())
      .put("autoThresholdQuantile", options.getAutoThresholdQuantile()).put("signalDisplayMode", options.getSignalDisplayMode().name())
      .put("coolerWeightsNaNPolicy", options.getCoolerWeightsNaNPolicy().name())
      .put("colormap", new JsonObject().put("type", "SimpleLinearGradient").put("bitDepth", gradient.getBitDepth())
        .put("startColor", color(gradient.getStartColor())).put("endColor", color(gradient.getEndColor()))
        .put("minSignal", gradient.getMinSignal()).put("maxSignal", gradient.getMaxSignal()));
  }

  public static SimpleVisualizationOptions decode(JsonObject json) {
    if (json == null) throw invalid("", "expected an object");
    double pre = number(json, "preLogBase"), post = number(json, "postLogBase");
    if (pre == 1) throw invalid("preLogBase", "log base cannot be one");
    if (post == 1) throw invalid("postLogBase", "log base cannot be one");
    double quantile = number(json, "autoThresholdQuantile");
    if (quantile <= 0 || quantile > 1) throw invalid("autoThresholdQuantile", "must be in (0,1]");
    SignalDisplayMode mode = enumeration(json, "signalDisplayMode", SignalDisplayMode.class);
    CoolerWeightsNaNPolicy policy = enumeration(json, "coolerWeightsNaNPolicy", CoolerWeightsNaNPolicy.class);
    if (!(json.getValue("colormap") instanceof JsonObject gradient)) throw invalid("colormap", "expected an object");
    if (!"SimpleLinearGradient".equals(gradient.getValue("type"))) throw invalid("colormap.type", "unsupported colormap");
    double bits = number(gradient, "bitDepth", "colormap.");
    if (bits != Math.rint(bits) || bits < 1 || bits > 64) throw invalid("colormap.bitDepth", "invalid bit depth");
    double min = number(gradient, "minSignal", "colormap."), max = number(gradient, "maxSignal", "colormap.");
    if (max <= min || !Double.isFinite(max - min)) throw invalid("colormap.maxSignal", "must define a finite positive signal range");
    return new SimpleVisualizationOptions(pre, post, bool(json, "applyCoolerWeights"), bool(json, "resolutionScaling"),
      bool(json, "resolutionLinearScaling"), bool(json, "autoThresholdEnabled"), quantile, mode, policy,
      new SimpleLinearGradient((int)bits, color(gradient, "startColor"), color(gradient, "endColor"), min, max));
  }

  private static JsonArray color(Color color) {
    if (!color.getColorSpace().isCS_sRGB()) throw invalid("colormap", "only sRGB colors can be restored");
    var array = new JsonArray();
    for (float component : color.getRGBComponents(null)) array.add(component);
    return array;
  }
  private static Color color(JsonObject json, String key) {
    if (!(json.getValue(key) instanceof JsonArray array) || array.size() != 4)
      throw invalid("colormap." + key, "expected four RGBA components");
    float[] rgba = new float[4];
    for (int i = 0; i < 4; i++) {
      if (!(array.getValue(i) instanceof Number value) || !Double.isFinite(value.doubleValue())
          || value.doubleValue() < 0 || value.doubleValue() > 1)
        throw invalid("colormap." + key + "[" + i + "]", "expected a component in [0,1]");
      rgba[i] = ((Number)array.getValue(i)).floatValue();
    }
    return new Color(rgba[0], rgba[1], rgba[2], rgba[3]);
  }
  private static boolean bool(JsonObject json, String key) {
    if (!(json.getValue(key) instanceof Boolean value)) throw invalid(key, "expected a boolean");
    return value;
  }
  private static double number(JsonObject json, String key) { return number(json, key, ""); }
  private static double number(JsonObject json, String key, String prefix) {
    if (!(json.getValue(key) instanceof Number value) || !Double.isFinite(value.doubleValue()))
      throw invalid(prefix + key, "expected a finite number");
    return value.doubleValue();
  }
  private static <E extends Enum<E>> E enumeration(JsonObject json, String key, Class<E> type) {
    if (!(json.getValue(key) instanceof String)) throw invalid(key, "expected an enum value");
    try { return Enum.valueOf(type, json.getString(key)); }
    catch (IllegalArgumentException invalid) { throw invalid(key, "unsupported enum value"); }
  }
  private static IllegalArgumentException invalid(String path, String message) {
    return new IllegalArgumentException("visualizationOptions" + (path.isEmpty() ? "" : "." + path) + ": " + message);
  }
}
