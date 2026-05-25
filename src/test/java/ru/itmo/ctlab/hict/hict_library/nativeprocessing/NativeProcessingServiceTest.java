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

package ru.itmo.ctlab.hict.hict_library.nativeprocessing;

import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.visualization.DistanceExpectedNormalizer;
import ru.itmo.ctlab.hict.hict_library.visualization.SignalDisplayMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.*;

class NativeProcessingServiceTest {
  @Test
  void defaultJavaPathDoesNotRequireNativeLibrary() {
    final var service = NativeProcessingService.getInstance();
    service.setRequestedEnabled(false);

    final var matrix = new MatrixQueries.MatrixWithWeights(
      new MatrixQueries.DoubleMatrix(new double[][]{
        {1.0d, 2.0d},
        {3.0d, 4.0d}
      }),
      new double[]{1.0d, 0.5d},
      new double[]{2.0d, 1.0d},
      0L,
      0L,
      2L,
      2L,
      QueryLengthUnit.PIXELS,
      ResolutionDescriptor.fromResolutionOrder(0)
    );

    assertNull(service.tryPrepareBaseSignalMatrix(
      matrix,
      Math.log(10.0d),
      1.0d,
      1.0d,
      false,
      false,
      true
    ));
    final var status = service.status();
    assertFalse(status.requested());
    assertFalse(status.enabled());
    assertNotNull(status.reason());
  }

  @Test
  void requestedNativePathFallsBackWhenLibraryIsUnavailable() {
    final var service = NativeProcessingService.getInstance();
    try {
      final var status = service.setRequestedEnabled(true);
      assertEquals(status.available(), status.enabled());
      assertNotNull(status.version());
      assertNotNull(status.reason());
    } finally {
      service.setRequestedEnabled(false);
    }
  }

  @Test
  void compiledNativeLibraryMatchesJavaTileMathWhenProvided() {
    final var nativeLibraryPath = System.getProperty("hict.native.test.library", "");
    assumeTrue(!nativeLibraryPath.isBlank(), "Set -Dhict.native.test.library to run native parity checks");
    assumeTrue(Files.isRegularFile(Path.of(nativeLibraryPath)), "Native test library does not exist: " + nativeLibraryPath);

    final var previousPath = System.getProperty("hict.native.library.path");
    System.setProperty("hict.native.library.path", nativeLibraryPath);
    try {
      final var processor = new NativeTileProcessor();
      final var loadReport = processor.ensureLoaded();
      assertEquals(NativeTileProcessor.LoadState.LOADED, loadReport.state(), loadReport.reason());

      final double[] doubleInput = {
        0.0d, 3.0d, Double.NaN,
        -2.0d, Double.POSITIVE_INFINITY, 15.0d
      };
      final var rowWeights = new double[]{2.0d, 0.5d};
      final var columnWeights = new double[]{3.0d, 0.25d, 1.5d};
      final var output = new double[doubleInput.length];
      assertTrue(processor.computeBaseSignalDouble(
        doubleInput,
        rowWeights,
        columnWeights,
        2,
        3,
        Math.log(10.0d),
        1.5d,
        0.25d,
        true,
        true,
        true,
        output
      ));
      assertArrayEquals(
        expectedBaseSignal(doubleInput, rowWeights, columnWeights, Math.log(10.0d), 1.5d, 0.25d),
        output,
        1.0e-12
      );

      final long[] longInput = {
        0L, 3L, 7L,
        -2L, 0L, 15L
      };
      final var longOutput = new double[longInput.length];
      assertTrue(processor.computeBaseSignalLong(
        longInput,
        rowWeights,
        columnWeights,
        2,
        3,
        Math.log(10.0d),
        1.5d,
        0.25d,
        true,
        true,
        true,
        longOutput
      ));
      assertArrayEquals(
        expectedBaseSignal(longInput, rowWeights, columnWeights, Math.log(10.0d), 1.5d, 0.25d),
        longOutput,
        1.0e-12
      );

      final byte[] rgba = new byte[16];
      assertTrue(processor.mapLinearGradientRgba(
        new double[]{-1.0d, 0.0d, 0.5d, 2.0d},
        2,
        2,
        new float[]{1.0f, 1.0f, 1.0f, 0.0f},
        new float[]{1.0f, 0.0f, 0.0f, 1.0f},
        0.0d,
        1.0d,
        rgba
      ));
      assertUnsignedBytes(
        new int[]{
          255, 255, 255, 0,
          255, 255, 255, 0,
          255, 128, 128, 128,
          255, 0, 0, 255
        },
        rgba
      );

      final var sparseDenseCounts = new long[2];
      assertTrue(processor.countStripeBlocks(
        new long[]{0L, 1L, 9L, 10L, 11L, 30L},
        4,
        10,
        3,
        sparseDenseCounts
      ));
      assertArrayEquals(new long[]{3L, 1L}, sparseDenseCounts);

      final var postLogInput = new double[]{0.0d, 9.0d, -1.0d, Double.NaN, Double.POSITIVE_INFINITY};
      assertTrue(processor.applyPostLog(postLogInput, Math.log(10.0d)));
      assertArrayEquals(
        new double[]{0.0d, 1.0d, 0.0d, 0.0d, 0.0d},
        postLogInput,
        1.0e-12d
      );

      final var precomputedValues = new double[]{1.0d, 2.0d, 3.0d, 4.0d, 5.0d, 6.0d, 7.0d, 8.0d};
      final var precomputedSupport = new long[]{1L, 0L, 2L, 0L, 3L, 3L, 0L, 1L};
      final var aggregatedValues = new double[4];
      final var aggregatedSupport = new long[4];
      assertTrue(processor.aggregatePrecomputedSeries(
        precomputedValues,
        precomputedSupport,
        0L,
        8L,
        4,
        1,
        aggregatedValues,
        aggregatedSupport
      ));
      assertArrayEquals(new double[]{2.0d, 4.0d, 6.0d, 8.0d}, aggregatedValues, 1.0e-12d);
      assertArrayEquals(new long[]{1L, 2L, 6L, 1L}, aggregatedSupport);

      final var intervalStarts = new long[]{0L, 1L, 3L, 6L};
      final var intervalEnds = new long[]{2L, 5L, 7L, 8L};
      final var intervalValues = new double[]{1.0d, 2.0d, 4.0d, 8.0d};
      final var intervalOutput = new double[4];
      final var intervalCounts = new long[4];
      assertTrue(processor.aggregateIntervals(intervalStarts, intervalEnds, intervalValues, 0L, 8L, 4, 1, intervalOutput, intervalCounts));
      assertArrayEquals(new double[]{2.0d, 4.0d, 4.0d, 8.0d}, intervalOutput, 1.0e-12d);
      assertArrayEquals(new long[]{2L, 2L, 2L, 2L}, intervalCounts);

      assertTrue(processor.aggregateIntervals(intervalStarts, intervalEnds, intervalValues, 0L, 8L, 4, 2, intervalOutput, intervalCounts));
      assertArrayEquals(
        new double[]{4.0d / 3.0d, 8.0d / 3.0d, 10.0d / 3.0d, 20.0d / 3.0d},
        intervalOutput,
        1.0e-12d
      );
      assertArrayEquals(new long[]{2L, 2L, 2L, 2L}, intervalCounts);

      assertTrue(processor.aggregateIntervals(intervalStarts, intervalEnds, null, 0L, 8L, 4, 4, intervalOutput, intervalCounts));
      assertArrayEquals(new double[]{1.5d, 1.5d, 1.5d, 1.5d}, intervalOutput, 1.0e-12d);
      assertArrayEquals(new long[]{2L, 2L, 2L, 2L}, intervalCounts);

      final var sequence = "AaTtCcGgNnRYSWKMBDHVX";
      final var reverseComplementInput = sequence.getBytes(StandardCharsets.US_ASCII);
      final var reverseComplementOutput = new byte[reverseComplementInput.length];
      assertTrue(processor.reverseComplementAscii(reverseComplementInput, reverseComplementOutput));
      assertEquals(
        expectedReverseComplement(sequence),
        new String(reverseComplementOutput, StandardCharsets.US_ASCII)
      );

      final var sparseRowsDouble = new long[]{2L, 0L, 1L, 0L};
      final var sparseColumnsDouble = new long[]{0L, 2L, 1L, 1L};
      final var sparseDoubleValues = new double[]{20.0d, 2.0d, 11.0d, 1.0d};
      assertTrue(processor.sortSparseBlockDouble(sparseRowsDouble, sparseColumnsDouble, sparseDoubleValues, 4));
      assertArrayEquals(new long[]{0L, 0L, 1L, 2L}, sparseRowsDouble);
      assertArrayEquals(new long[]{1L, 2L, 1L, 0L}, sparseColumnsDouble);
      assertArrayEquals(new double[]{1.0d, 2.0d, 11.0d, 20.0d}, sparseDoubleValues, 1.0e-12d);

      final var sparseRowsLong = new long[]{2L, 0L, 1L, 0L};
      final var sparseColumnsLong = new long[]{0L, 2L, 1L, 1L};
      final var sparseLongValues = new long[]{20L, 2L, 11L, 1L};
      assertTrue(processor.sortSparseBlockLong(sparseRowsLong, sparseColumnsLong, sparseLongValues, 4));
      assertArrayEquals(new long[]{0L, 0L, 1L, 2L}, sparseRowsLong);
      assertArrayEquals(new long[]{1L, 2L, 1L, 0L}, sparseColumnsLong);
      assertArrayEquals(new long[]{1L, 2L, 11L, 20L}, sparseLongValues);

      final double[][] expectedTransformInput = {
        {0.0d, 2.0d, 4.0d},
        {-1.0d, Double.NaN, 10.0d}
      };
      final var diagonalProfile = DistanceExpectedNormalizer.buildProfile(
        expectedTransformInput,
        10L,
        12L,
        -1
      );
      final var expectedFlattened = new double[6];
      assertTrue(processor.transformExpectedSignal(
        flatten(expectedTransformInput),
        2,
        3,
        10L,
        12L,
        1,
        diagonalProfile.minDiagonal(),
        diagonalProfile.means(),
        expectedFlattened
      ));
      assertMatrixEquals(
        DistanceExpectedNormalizer.transformSignal(
          expectedTransformInput,
          10L,
          12L,
          SignalDisplayMode.EXPECTED,
          diagonalProfile
        ),
        inflate(expectedFlattened, 2, 3),
        1.0e-12
      );

      final var observedOverExpectedFlattened = new double[6];
      assertTrue(processor.transformExpectedSignal(
        flatten(expectedTransformInput),
        2,
        3,
        10L,
        12L,
        2,
        diagonalProfile.minDiagonal(),
        diagonalProfile.means(),
        observedOverExpectedFlattened
      ));
      assertMatrixEquals(
        DistanceExpectedNormalizer.transformSignal(
          expectedTransformInput,
          10L,
          12L,
          SignalDisplayMode.OBSERVED_OVER_EXPECTED,
          diagonalProfile
        ),
        inflate(observedOverExpectedFlattened, 2, 3),
        1.0e-12
      );
    } finally {
      if (previousPath == null) {
        System.clearProperty("hict.native.library.path");
      } else {
        System.setProperty("hict.native.library.path", previousPath);
      }
    }
  }

  private static double[] expectedBaseSignal(final double[] input,
                                             final double[] rowWeights,
                                             final double[] columnWeights,
                                             final double lnPreLogBase,
                                             final double resolutionScalingCoeff,
                                             final double resolutionLinearScalingCoeff) {
    final var result = new double[input.length];
    for (int row = 0; row < 2; row++) {
      for (int column = 0; column < 3; column++) {
        final var index = row * 3 + column;
        var signal = input[index];
        if (!Double.isFinite(signal) || signal < 0.0d) {
          signal = 0.0d;
        }
        signal = Math.log1p(signal) / lnPreLogBase;
        signal *= resolutionScalingCoeff;
        signal *= resolutionLinearScalingCoeff;
        signal *= rowWeights[row] * columnWeights[column];
        result[index] = Double.isFinite(signal) ? signal : 0.0d;
      }
    }
    return result;
  }

  private static double[] expectedBaseSignal(final long[] input,
                                             final double[] rowWeights,
                                             final double[] columnWeights,
                                             final double lnPreLogBase,
                                             final double resolutionScalingCoeff,
                                             final double resolutionLinearScalingCoeff) {
    final var asDouble = new double[input.length];
    for (int index = 0; index < input.length; index++) {
      asDouble[index] = input[index];
    }
    return expectedBaseSignal(
      asDouble,
      rowWeights,
      columnWeights,
      lnPreLogBase,
      resolutionScalingCoeff,
      resolutionLinearScalingCoeff
    );
  }

  private static void assertUnsignedBytes(final int[] expected,
                                          final byte[] actual) {
    assertEquals(expected.length, actual.length);
    for (int index = 0; index < expected.length; index++) {
      assertEquals(expected[index], Byte.toUnsignedInt(actual[index]), "byte index " + index);
    }
  }

  private static double[] flatten(final double[][] matrix) {
    final int rows = matrix.length;
    final int columns = rows == 0 ? 0 : matrix[0].length;
    final var result = new double[rows * columns];
    var dst = 0;
    for (final var row : matrix) {
      System.arraycopy(row, 0, result, dst, columns);
      dst += columns;
    }
    return result;
  }

  private static double[][] inflate(final double[] values,
                                    final int rows,
                                    final int columns) {
    final var result = new double[rows][columns];
    var src = 0;
    for (int row = 0; row < rows; row++) {
      System.arraycopy(values, src, result[row], 0, columns);
      src += columns;
    }
    return result;
  }

  private static void assertMatrixEquals(final double[][] expected,
                                         final double[][] actual,
                                         final double delta) {
    assertEquals(expected.length, actual.length);
    for (int row = 0; row < expected.length; row++) {
      assertArrayEquals(expected[row], actual[row], delta, "row " + row);
    }
  }

  private static String expectedReverseComplement(final String sequence) {
    final var result = new StringBuilder(sequence.length());
    for (int index = sequence.length() - 1; index >= 0; index--) {
      result.append(complement(sequence.charAt(index)));
    }
    return result.toString();
  }

  private static char complement(final char base) {
    return switch (base) {
      case 'A', 'a' -> 'T';
      case 'T', 't' -> 'A';
      case 'C', 'c' -> 'G';
      case 'G', 'g' -> 'C';
      case 'N', 'n' -> 'N';
      case 'R', 'r' -> 'Y';
      case 'Y', 'y' -> 'R';
      case 'S', 's' -> 'S';
      case 'W', 'w' -> 'W';
      case 'K', 'k' -> 'M';
      case 'M', 'm' -> 'K';
      case 'B', 'b' -> 'V';
      case 'D', 'd' -> 'H';
      case 'H', 'h' -> 'D';
      case 'V', 'v' -> 'B';
      default -> 'N';
    };
  }
}
