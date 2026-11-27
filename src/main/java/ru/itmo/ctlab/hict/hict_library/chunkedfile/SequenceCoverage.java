package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Computed sequence rectangles in original stripe/bin coordinates, including zero-valued scores. */
public final class SequenceCoverage {
  private static final String ROOT = "/sequence_analysis";
  private static final String UPDATES = ROOT + "/updates";
  private final Map<Long, List<Region>> levels;

  private record Region(long[] rows, long[] columns) {}

  private SequenceCoverage(Map<Long, List<Region>> levels) {
    this.levels = levels;
  }

  static SequenceCoverage read(IHDF5Reader reader, List<Long> resolutions) {
    if (!reader.object().isGroup(UPDATES) || !reader.object().hasAttribute(ROOT, "explicit_coverage") ||
        !reader.bool().getAttr(ROOT, "explicit_coverage")) {
      return null;
    }
    var levels = new HashMap<Long, List<Region>>();
    for (String update : reader.object().getGroupMembers(UPDATES)) {
      for (long level : resolutions) {
        String path = UPDATES + "/" + update + "/" + level;
        if (reader.object().isGroup(path)) {
          levels.computeIfAbsent(level, ignored -> new ArrayList<>())
            .add(new Region(readAxis(reader, path + "/row_bins"), readAxis(reader, path + "/column_bins")));
        }
      }
    }
    return new SequenceCoverage(levels);
  }

  public static long[] readAxis(IHDF5Reader reader, String path) {
    long[] shape = reader.object().getDimensions(path);
    if (shape.length != 2 || shape[1] != 2) {
      throw new IllegalArgumentException("Invalid computed sequence coverage axis");
    }
    var result = new long[Math.toIntExact(shape[0])];
    for (int offset = 0; offset < result.length; ) {
      int length = Math.min(8192, result.length - offset);
      long[][] bins = reader.int64().readMatrixBlockWithOffset(path, length, 2, offset, 0);
      for (int i = 0; i < bins.length; i++) {
        result[offset + i] = binKey(Math.toIntExact(bins[i][0]), Math.toIntExact(bins[i][1]));
      }
      offset += length;
    }
    Arrays.sort(result);
    return result;
  }

  private static long binKey(int stripe, int bin) {
    if (stripe < 0 || bin < 0) throw new IllegalArgumentException("Invalid computed sequence coverage bin");
    return (long) stripe << 32 | bin;
  }

  private static long[] physicalBins(List<ATUDescriptor> atus) {
    var keys = new long[atus.stream().mapToInt(atu -> atu.getEndIndexInStripeExcl() - atu.getStartIndexInStripeIncl()).sum()];
    int position = 0;
    for (var atu : atus) {
      int start = atu.getStartIndexInStripeIncl();
      int end = atu.getEndIndexInStripeExcl();
      int stripe = atu.getStripeDescriptor().stripeId();
      if (atu.getDirection() == ATUDirection.FORWARD) {
        for (int i = start; i < end; i++) keys[position++] = binKey(stripe, i);
      } else {
        for (int i = end - 1; i >= start; i--) keys[position++] = binKey(stripe, i);
      }
    }
    return keys;
  }

  BitSet mask(long resolution, List<ATUDescriptor> rows, List<ATUDescriptor> columns,
              int rowOffset, int columnOffset, int columnCount) {
    var computed = new BitSet();
    var regions = levels.get(resolution);
    if (regions == null) return computed;
    var rowKeys = physicalBins(rows);
    var columnKeys = rows == columns ? rowKeys : physicalBins(columns);
    for (var region : regions) {
      var original = matchingColumns(columnKeys, region.columns);
      var reflected = matchingColumns(columnKeys, region.rows);
      for (int row = 0; row < rowKeys.length; row++) {
        int offset = (row + rowOffset) * columnCount + columnOffset;
        if (Arrays.binarySearch(region.rows, rowKeys[row]) >= 0) apply(computed, original, offset);
        if (Arrays.binarySearch(region.columns, rowKeys[row]) >= 0) apply(computed, reflected, offset);
      }
    }
    return computed;
  }

  private static BitSet matchingColumns(long[] columns, long[] axis) {
    var result = new BitSet(columns.length);
    for (int i = 0; i < columns.length; i++) {
      if (Arrays.binarySearch(axis, columns[i]) >= 0) result.set(i);
    }
    return result;
  }

  private static void apply(BitSet mask, BitSet columns, int offset) {
    for (int start = columns.nextSetBit(0); start >= 0; ) {
      int end = columns.nextClearBit(start);
      mask.set(offset + start, offset + end);
      start = columns.nextSetBit(end);
    }
  }
}
