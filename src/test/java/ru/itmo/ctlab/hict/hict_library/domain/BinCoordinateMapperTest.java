package ru.itmo.ctlab.hict.hict_library.domain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BinCoordinateMapperTest {
  @Test void agreesWithSourceBinEnumerationIncludingOffsetPieces() {
    for (long resolution : new long[]{1, 7, 100, 500, 1000, 5000}) {
      for (long length = 1; length <= Math.min(3 * resolution + 3, 180); length++) {
        for (long anchor : new long[]{0, -resolution / 2, resolution / 2}) {
          long bins = Math.max(1, (length - anchor + resolution - 1) / resolution);
          for (boolean reversed : new boolean[]{false, true}) {
            for (long bp = 0; bp < length; bp++) {
              long source = reversed ? length - 1 - bp : bp;
              long expected = 0;
              for (long b = 0; b < bins; b++) if (anchor + b * resolution <= source) expected = b;
              if (reversed) expected = bins - 1 - expected;
              assertEquals(expected, BinCoordinateMapper.bpToBin(bp, length, bins, resolution, anchor, reversed));
            }
            for (long bin = 0; bin < bins; bin++) {
              long ordinal = reversed ? bins - 1 - bin : bin;
              long expected = reversed ? length - Math.min(length, anchor + (ordinal + 1) * resolution) : anchor + ordinal * resolution;
              expected = Math.max(0, Math.min(length - 1, expected));
              assertEquals(expected, BinCoordinateMapper.binToBp(bin, length, bins, resolution, anchor, reversed));
            }
          }
        }
      }
    }
  }

  @Test void validatesAndDoesNotWrapOverflow() {
    assertThrows(IllegalArgumentException.class, () -> BinCoordinateMapper.bpToBin(0, 1, 1, 0, 0, false));
    assertThrows(IllegalArgumentException.class, () -> BinCoordinateMapper.binToBp(0, 0, 1, 1, 0, false));
    assertThrows(ArithmeticException.class, () -> BinCoordinateMapper.binToBp(2, Long.MAX_VALUE, 3, Long.MAX_VALUE, 0, false));
    assertEquals(0, BinCoordinateMapper.bpToBin(0, 10, 0, 100, 0, true));
  }
}
