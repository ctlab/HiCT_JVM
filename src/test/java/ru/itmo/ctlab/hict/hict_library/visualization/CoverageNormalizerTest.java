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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parity with the pinned Juicebox VC_SQRT reference and the properties the recipe guarantees. */
class CoverageNormalizerTest {
  private static CoverageNormalizer.ContactSource source(final JsonArray records) {
    return visitor -> {
      for (int i = 0; i < records.size(); i++) {
        final var r = records.getJsonArray(i);
        visitor.accept(r.getLong(0), r.getLong(1), r.getDouble(2));
      }
    };
  }

  private static JsonObject golden() throws IOException {
    try (InputStream in = CoverageNormalizerTest.class.getResourceAsStream("/normalization/vc_sqrt_golden.json")) {
      return new JsonObject(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  @Test
  void matchesTheJuiceboxReferenceOnEveryGoldenCase() throws IOException {
    final var cases = golden().getJsonArray("cases");
    assertTrue(cases.size() >= 6);
    for (int c = 0; c < cases.size(); c++) {
      final var one = cases.getJsonObject(c);
      final var name = one.getString("name");
      final var vector = CoverageNormalizer.compute(one.getInteger("n"), source(one.getJsonArray("records")), () -> false);
      final var expected = one.getJsonObject("reference").getJsonArray("vector");
      assertEquals(expected.size(), vector.length(), name);
      for (int i = 0; i < expected.size(); i++) {
        final var e = expected.getDouble(i);
        if (e == null) {
          assertEquals(0.0d, vector.divisor()[i], 0.0d, name + " bin " + i + " has no reference value");
        } else {
          // The reference accumulates in 32-bit floats.
          assertEquals(e, vector.divisor()[i], Math.max(1e-6d, Math.abs(e) * 2e-6d), name + " bin " + i);
        }
      }
      final var factor = one.getJsonObject("reference").getDouble("factor");
      if (factor != null && Double.isFinite(factor)) {
        assertEquals(factor, vector.sumFactor(), Math.abs(factor) * 1e-6d, name + " factor");
      } else {
        assertTrue(Double.isNaN(vector.sumFactor()), name + " has no factor");
      }
    }
  }

  @Test
  void emptyBinsHaveNoWeightAndOthersHaveOne() {
    final var records = new JsonArray().add(new JsonArray().add(0).add(0).add(4)).add(new JsonArray().add(0).add(2).add(9));
    final var vector = CoverageNormalizer.compute(4, source(records), () -> false);
    assertTrue(Double.isNaN(vector.weight(1)));
    assertTrue(Double.isNaN(vector.weight(3)));
    assertFalse(Double.isNaN(vector.weight(0)));
    assertEquals(2L + 1L, vector.validBins() + 1L);
  }

  @Test
  void allZeroMatrixHasNoValidBins() {
    final var vector = CoverageNormalizer.compute(3, visitor -> { }, () -> false);
    assertEquals(0L, vector.validBins());
    assertTrue(Double.isNaN(vector.weight(0)));
  }

  @Test
  void normalizationPreservesTheTotal() {
    final var rnd = new java.util.Random(5);
    final List<double[]> records = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      for (int j = i; j < 30; j++) {
        if (rnd.nextDouble() < 0.4) {
          records.add(new double[]{i, j, 1 + rnd.nextInt(50)});
        }
      }
    }
    final CoverageNormalizer.ContactSource src = v -> records.forEach(r -> v.accept((long) r[0], (long) r[1], r[2]));
    final var vector = CoverageNormalizer.compute(30, src, () -> false);
    final double[] totals = new double[2];
    src.forEach((r, c, value) -> {
      final double w = r == c ? 1 : 2;
      totals[0] += w * value;
      totals[1] += w * value / (vector.divisor()[(int) r] * vector.divisor()[(int) c]);
    });
    assertEquals(totals[0], totals[1], totals[0] * 1e-9d);
  }

  @Test
  void rowOrderOfTheStreamDoesNotMatter() {
    final var forward = new JsonArray()
      .add(new JsonArray().add(0).add(1).add(3)).add(new JsonArray().add(1).add(2).add(5)).add(new JsonArray().add(2).add(2).add(7));
    final var backward = new JsonArray()
      .add(new JsonArray().add(2).add(2).add(7)).add(new JsonArray().add(1).add(2).add(5)).add(new JsonArray().add(0).add(1).add(3));
    final var a = CoverageNormalizer.compute(3, source(forward), () -> false);
    final var b = CoverageNormalizer.compute(3, source(backward), () -> false);
    for (int i = 0; i < 3; i++) {
      assertEquals(a.divisor()[i], b.divisor()[i], 1e-12);
    }
  }

  @Test
  void cancellationStopsTheScan() {
    final var records = new JsonArray().add(new JsonArray().add(0).add(1).add(3));
    assertThrows(CoverageNormalizer.CancelledException.class,
      () -> CoverageNormalizer.compute(3, source(records), () -> true));
  }

  @Test
  void invalidSizesAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> CoverageNormalizer.compute(-1, v -> { }, () -> false));
    assertThrows(IllegalArgumentException.class, () -> CoverageNormalizer.compute(Long.MAX_VALUE, v -> { }, () -> false));
  }
}
