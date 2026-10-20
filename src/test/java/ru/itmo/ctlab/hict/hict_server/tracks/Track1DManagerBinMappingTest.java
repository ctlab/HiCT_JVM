package ru.itmo.ctlab.hict.hict_server.tracks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression tests for the base-pair <-> visible-bin fallback of 1D tracks.
 *
 * <p>Oracle: inside a component, bins are laid out from the component start in SOURCE orientation,
 * each of length r except possibly a shorter last bin; nb = ceil(len / r). A reversed component lists
 * the same bins in reverse order, so its shorter bin (if any) comes first. When len % r == 0 there is no
 * shorter bin and the reversed layout coincides with the forward one.</p>
 *
 * <p>Before the fix, reversed components with len % r == 0 were shifted by one bin
 * (first-bin length computed as len % r == 0 instead of r).</p>
 */
class Track1DManagerBinMappingTest {
  private static final long ASSEMBLY_START = 1_000L;
  private static final long VISIBLE_START = 7L;

  private static Track1DManager.AssemblySegment segment(final long length, final long resolution, final boolean reversed) {
    final long bins = (length + resolution - 1) / resolution;
    return new Track1DManager.AssemblySegment(0L, length, ASSEMBLY_START, ASSEMBLY_START + length,
      reversed, VISIBLE_START, VISIBLE_START + bins);
  }

  private static long expectedLocalBin(final long offset, final long length, final long resolution, final boolean reversed) {
    final long bins = (length + resolution - 1) / resolution;
    final long sourceOffset = reversed ? length - 1 - offset : offset;
    final long sourceBin = sourceOffset / resolution;
    return reversed ? bins - 1 - sourceBin : sourceBin;
  }

  private static long expectedFirstOffsetOfLocalBin(final long localBin, final long length, final long resolution, final boolean reversed) {
    final long bins = (length + resolution - 1) / resolution;
    if (!reversed) {
      return localBin * resolution;
    }
    final long sourceBin = bins - 1 - localBin;
    final long lastSourceOffsetInBin = Math.min(length - 1, (sourceBin + 1) * resolution - 1);
    return length - 1 - lastSourceOffsetInBin;
  }

  @Test
  void reversedComponentWithLengthDivisibleByResolutionIsNotShifted() {
    final var reversed = segment(1_000L, 100L, true);
    assertEquals(0L, Track1DManager.assemblyBpToLocalPx(reversed, ASSEMBLY_START, 100L));
    assertEquals(8L, Track1DManager.assemblyBpToLocalPx(reversed, ASSEMBLY_START + 850L, 100L));
    assertEquals(9L, Track1DManager.assemblyBpToLocalPx(reversed, ASSEMBLY_START + 999L, 100L));
    assertEquals(ASSEMBLY_START, Track1DManager.localPxToAssemblyBp(reversed, 0L, 100L));
    assertEquals(ASSEMBLY_START + 100L, Track1DManager.localPxToAssemblyBp(reversed, 1L, 100L));
    assertEquals(ASSEMBLY_START + 900L, Track1DManager.localPxToAssemblyBp(reversed, 9L, 100L));
  }

  @Test
  void reversedComponentWithRemainderPlacesShortBinFirst() {
    final var reversed = segment(1_050L, 100L, true);
    assertEquals(0L, Track1DManager.assemblyBpToLocalPx(reversed, ASSEMBLY_START + 49L, 100L));
    assertEquals(1L, Track1DManager.assemblyBpToLocalPx(reversed, ASSEMBLY_START + 50L, 100L));
    assertEquals(ASSEMBLY_START + 50L, Track1DManager.localPxToAssemblyBp(reversed, 1L, 100L));
  }

  @Test
  void exhaustiveAgreementWithBinLayoutOracle() {
    for (final long resolution : new long[]{100L, 500L, 1_000L}) {
      for (long length = 1L; length <= 4L * resolution + 3L; length++) {
        for (final boolean reversed : new boolean[]{false, true}) {
          final var seg = segment(length, resolution, reversed);
          final long bins = (length + resolution - 1) / resolution;
          for (long offset = 0L; offset < length; offset++) {
            assertEquals(expectedLocalBin(offset, length, resolution, reversed),
              Track1DManager.assemblyBpToLocalPx(seg, ASSEMBLY_START + offset, resolution),
              () -> "bp->bin mismatch");
          }
          for (long localBin = 0L; localBin < bins; localBin++) {
            assertEquals(ASSEMBLY_START + expectedFirstOffsetOfLocalBin(localBin, length, resolution, reversed),
              Track1DManager.localPxToAssemblyBp(seg, localBin, resolution),
              () -> "bin->bp mismatch");
          }
        }
      }
    }
  }
}
