package ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleUnaryOperator;

/** Smooth transfer in the normalized value or logarithmic coordinate. */
final class SmoothColorCurve {
  private static final double LOG_FLOOR = 1e-10;
  private static final double FLOOR_LOG = Math.log10(LOG_FLOOR);
  private record Point(double x, double y) { }

  private SmoothColorCurve() { }

  static DoubleUnaryOperator prepare(List<Double> points, String mode) {
    double start = 0, end = 1;
    var interior = new ArrayList<Point>();
    if (points != null) {
      for (int i = 0; i < points.size(); i += 2) {
        double x = points.get(i), y = points.get(i + 1);
        if (x == 0) start = y;
        else if (x == 1) end = y;
        else interior.add(new Point(x, y));
      }
    }
    var interpolation = interpolate(start, end, interior);
    return "smooth-log".equals(mode)
      ? x -> interpolation.applyAsDouble(clamp((Math.log10(Math.max(LOG_FLOOR, x)) - FLOOR_LOG) / -FLOOR_LOG))
      : x -> interpolation.applyAsDouble(clamp(x));
  }

  private static DoubleUnaryOperator interpolate(double start, double end, List<Point> interior) {
    if (interior.isEmpty()) return t -> start + (end - start) * t;
    var knots = new ArrayList<Point>(interior.size() + 2);
    knots.add(new Point(0, start));
    knots.addAll(interior);
    knots.add(new Point(1, end));
    var slopes = new double[knots.size() - 1];
    for (int i = 0; i < slopes.length; i++) {
      var left = knots.get(i);
      var right = knots.get(i + 1);
      slopes[i] = Math.max(-Double.MAX_VALUE, Math.min(Double.MAX_VALUE, (right.y() - left.y()) / (right.x() - left.x())));
    }
    if (interior.size() == 1) return interpolateSingle(start, interior.getFirst(), end, slopes[0], slopes[1]);

    var tangents = new double[knots.size()];
    for (int i = 1; i < tangents.length - 1; i++) {
      if (Math.signum(slopes[i - 1]) == Math.signum(slopes[i])) {
        tangents[i] = slopes[i - 1] / 2 + slopes[i] / 2;
      }
    }
    for (int i = 0; i < slopes.length; i++) {
      if (slopes[i] == 0) {
        tangents[i] = tangents[i + 1] = 0;
      } else {
        double norm = Math.hypot(tangents[i] / slopes[i], tangents[i + 1] / slopes[i]);
        if (norm > 3) {
          tangents[i] *= 3 / norm;
          tangents[i + 1] *= 3 / norm;
        }
      }
    }
    return t -> {
      int i = 0;
      while (i + 2 < knots.size() && t > knots.get(i + 1).x()) i++;
      var left = knots.get(i);
      var right = knots.get(i + 1);
      double span = right.x() - left.x();
      double u = (t - left.x()) / span;
      double transition = smoothstep(u);
      double result = (1 - transition) * left.y() + transition * right.y()
        + u * (1 - u) * ((1 - u) * span * tangents[i] - u * span * tangents[i + 1]);
      return Math.max(Math.min(left.y(), right.y()), Math.min(Math.max(left.y(), right.y()), result));
    };
  }

  private static DoubleUnaryOperator interpolateSingle(double start, Point point, double end, double left, double right) {
    double x = point.x(), y = point.y();
    if (left != 0 && right != 0 && Math.signum(left) != Math.signum(right)) {
      return t -> t <= x ? start + (y - start) * smoothstep(t / x)
        : y + (end - y) * smoothstep((t - x) / (1 - x));
    }
    double steepest = Math.max(Math.abs(left), Math.abs(right));
    double a = 1.5 * (steepest / Math.abs(left));
    double b = 1.5 * (steepest / Math.abs(right));
    return t -> {
      if (t <= x) return left == 0 ? start : start + (y - start) * Math.pow(t / x, a);
      return right == 0 ? end : end - (end - y) * Math.pow((1 - t) / (1 - x), b);
    };
  }

  private static double clamp(double x) { return Math.max(0, Math.min(1, x)); }
  private static double smoothstep(double t) { return t * t * (3 - 2 * t); }
}
