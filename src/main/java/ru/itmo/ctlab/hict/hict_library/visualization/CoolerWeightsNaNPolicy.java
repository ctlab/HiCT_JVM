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

package ru.itmo.ctlab.hict.hict_library.visualization;

import java.util.Locale;

public enum CoolerWeightsNaNPolicy {
  DISABLE_WEIGHTS,
  REPLACE_NANS_WITH_ONE,
  REPLACE_NANS_WITH_ZERO;

  public static CoolerWeightsNaNPolicy fromRaw(final String rawValue) {
    if (rawValue == null || rawValue.isBlank()) {
      return REPLACE_NANS_WITH_ONE;
    }
    final var normalized = rawValue.trim()
      .replace('-', '_')
      .replace(' ', '_')
      .toUpperCase(Locale.ROOT);
    return switch (normalized) {
      case "DISABLE", "DISABLE_WEIGHT", "DISABLE_WEIGHTS", "NO_WEIGHTS", "UNWEIGHTED" -> DISABLE_WEIGHTS;
      case "ZERO", "REPLACE_ZERO", "REPLACE_NAN_WITH_ZERO", "REPLACE_NANS_WITH_ZERO" -> REPLACE_NANS_WITH_ZERO;
      case "ONE", "REPLACE_ONE", "REPLACE_NAN_WITH_ONE", "REPLACE_NANS_WITH_ONE" -> REPLACE_NANS_WITH_ONE;
      default -> REPLACE_NANS_WITH_ONE;
    };
  }

  public double sanitize(final double weight) {
    if (this == DISABLE_WEIGHTS) {
      return 1.0d;
    }
    if (Double.isFinite(weight)) {
      return weight;
    }
    return this == REPLACE_NANS_WITH_ZERO ? 0.0d : 1.0d;
  }
}
