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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistanceExpectedNormalizerTest {
  @Test
  void expectedModeUsesAbsoluteDiagonalMeans() {
    final var signal = new double[][]{
      {10.0d, 2.0d, 1.0d},
      {2.0d, 8.0d, 4.0d},
      {1.0d, 4.0d, 6.0d}
    };

    final var expected = DistanceExpectedNormalizer.transformSignal(
      signal,
      100L,
      100L,
      SignalDisplayMode.EXPECTED
    );

    assertArrayEquals(new double[]{8.0d, 3.0d, 1.0d}, expected[0], 1e-12);
    assertArrayEquals(new double[]{3.0d, 8.0d, 3.0d}, expected[1], 1e-12);
    assertArrayEquals(new double[]{1.0d, 3.0d, 8.0d}, expected[2], 1e-12);
  }

  @Test
  void observedOverExpectedUsesPositiveFiniteDiagonalMeans() {
    final var signal = new double[][]{
      {0.0d, 0.0d},
      {0.0d, 5.0d}
    };

    final var oe = DistanceExpectedNormalizer.transformSignal(
      signal,
      17L,
      19L,
      SignalDisplayMode.OBSERVED_OVER_EXPECTED
    );

    assertArrayEquals(new double[]{0.0d, 0.0d}, oe[0], 1e-12);
    assertArrayEquals(new double[]{0.0d, 1.0d}, oe[1], 1e-12);
  }

  @Test
  void cachedProfileKeepsExpectedConsistentAcrossTileBoundaries() {
    final var fullSignal = new double[][]{
      {10.0d, 6.0d, 3.0d, 1.0d},
      {6.0d, 9.0d, 5.0d, 2.0d},
      {3.0d, 5.0d, 8.0d, 4.0d},
      {1.0d, 2.0d, 4.0d, 7.0d}
    };
    final var profile = DistanceExpectedNormalizer.buildProfile(fullSignal, 20L, 20L, 3);
    final var topLeftTile = new double[][]{
      {10.0d, 6.0d},
      {6.0d, 9.0d}
    };
    final var bottomRightTile = new double[][]{
      {8.0d, 4.0d},
      {4.0d, 7.0d}
    };

    final var expectedTopLeft = DistanceExpectedNormalizer.transformSignal(
      topLeftTile,
      20L,
      20L,
      SignalDisplayMode.EXPECTED,
      profile
    );
    final var expectedBottomRight = DistanceExpectedNormalizer.transformSignal(
      bottomRightTile,
      22L,
      22L,
      SignalDisplayMode.EXPECTED,
      profile
    );

    assertEquals(8.5d, expectedTopLeft[0][0], 1e-12);
    assertEquals(5.0d, expectedTopLeft[0][1], 1e-12);
    assertEquals(8.5d, expectedBottomRight[0][0], 1e-12);
    assertEquals(5.0d, expectedBottomRight[0][1], 1e-12);
  }

  @Test
  void chunkAccumulatorMatchesWholeWindowProfile() {
    final var fullSignal = new double[][]{
      {10.0d, 6.0d, 3.0d, 1.0d},
      {6.0d, 9.0d, 5.0d, 2.0d},
      {3.0d, 5.0d, 8.0d, 4.0d},
      {1.0d, 2.0d, 4.0d, 7.0d}
    };
    final var wholeProfile = DistanceExpectedNormalizer.buildProfile(fullSignal, 50L, 80L, 4);
    final var accumulator = DistanceExpectedNormalizer.newAccumulator(4, 50L, 54L, 80L, 84L);

    accumulator.addSignal(
      new double[][]{
        {10.0d, 6.0d},
        {6.0d, 9.0d}
      },
      50L,
      80L
    );
    accumulator.addSignal(
      new double[][]{
        {3.0d, 1.0d},
        {5.0d, 2.0d}
      },
      50L,
      82L
    );
    accumulator.addSignal(
      new double[][]{
        {3.0d, 5.0d},
        {1.0d, 2.0d}
      },
      52L,
      80L
    );
    accumulator.addSignal(
      new double[][]{
        {8.0d, 4.0d},
        {4.0d, 7.0d}
      },
      52L,
      82L
    );

    final var chunkedProfile = accumulator.toProfile();

    assertEquals(wholeProfile.minDiagonal(), chunkedProfile.minDiagonal());
    assertEquals(wholeProfile.startRowPx(), chunkedProfile.startRowPx());
    assertEquals(wholeProfile.endRowPx(), chunkedProfile.endRowPx());
    assertEquals(wholeProfile.startColPx(), chunkedProfile.startColPx());
    assertEquals(wholeProfile.endColPx(), chunkedProfile.endColPx());
    assertArrayEquals(wholeProfile.means(), chunkedProfile.means(), 1e-12);
    assertTrue(chunkedProfile.matchesResolutionWindow(50L, 80L, 4, 4));
  }

  @Test
  void diagonalBoundsHandleWindowsThatCrossMainDiagonalAwayFromCorners() {
    final var signal = new double[][]{
      {1.0d, 1.0d, 1.0d},
      {1.0d, 1.0d, 1.0d},
      {1.0d, 1.0d, 1.0d}
    };

    final var expected = assertDoesNotThrow(() ->
      DistanceExpectedNormalizer.transformSignal(
        signal,
        10L,
        11L,
        SignalDisplayMode.EXPECTED
      )
    );

    assertArrayEquals(new double[]{1.0d, 1.0d, 1.0d}, expected[0], 1e-12);
    assertArrayEquals(new double[]{1.0d, 1.0d, 1.0d}, expected[1], 1e-12);
    assertArrayEquals(new double[]{1.0d, 1.0d, 1.0d}, expected[2], 1e-12);
  }
}
