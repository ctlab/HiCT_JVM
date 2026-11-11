package ru.itmo.ctlab.hict.hict_library.assembly;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;

/** Reduces a native matrix onto the primary grid in bounded source blocks. */
public final class ProjectedMatrix {
  private static final int READ_EDGE = 128;
  private static final int MAX_AXIS_CONTRIBUTIONS = 262_144;
  private static final long MAX_SOURCE_CELLS = 32L * 1024L * 1024L;

  public enum Reducer {
    COUNTS, IDENTITY, PRESENCE
  }

  private record Target(int index, long overlap, long binLength) { }

  private record AxisBlock(long start, long end, NavigableMap<Long, List<Target>> bins) {
    long length() {
      return end - start;
    }
  }

  private ProjectedMatrix() { }

  public static MatrixQueries.MatrixWithWeights query(
    AssemblyBinGrid primaryGrid, AssemblyBinGrid nativeGrid, ChunkedFile nativeFile,
    ResolutionDescriptor primaryResolution, ResolutionDescriptor nativeResolution,
    long rowStart, long colStart, long rowEnd, long colEnd, Reducer reducer
  ) {
    return query(primaryGrid, nativeGrid, nativeFile, primaryResolution, nativeResolution,
      rowStart, colStart, rowEnd, colEnd, 1, reducer);
  }

  public static MatrixQueries.MatrixWithWeights query(
    AssemblyBinGrid primaryGrid, AssemblyBinGrid nativeGrid, ChunkedFile nativeFile,
    ResolutionDescriptor primaryResolution, ResolutionDescriptor nativeResolution,
    long rowStart, long colStart, long rowEnd, long colEnd, int sampleFactor, Reducer reducer
  ) {
    final int rows = Math.toIntExact(rowEnd - rowStart);
    final int cols = Math.toIntExact(colEnd - colStart);
    final var rowBlocks = axis(primaryGrid, nativeGrid, rowStart, rows, sampleFactor);
    final var colBlocks = rowStart == colStart && rows == cols
      ? rowBlocks : axis(primaryGrid, nativeGrid, colStart, cols, sampleFactor);
    final long sourceRows = rowBlocks.stream().mapToLong(AxisBlock::length).sum();
    final long sourceCols = colBlocks.stream().mapToLong(AxisBlock::length).sum();
    if (sourceRows != 0 && sourceCols > MAX_SOURCE_CELLS / sourceRows) {
      throw new IllegalArgumentException("The overlay tile exceeds the native matrix read budget. Zoom in to load a smaller region.");
    }

    final var values = new double[rows][cols];
    final var scoreAreas = reducer == Reducer.IDENTITY ? new double[rows][cols] : null;
    final var rowWeights = new double[rows];
    final var colWeights = new double[cols];
    final var rowLengths = new long[rows];
    final var colLengths = new long[cols];
    for (int r = 0; r < rowBlocks.size(); r++) {
      final var rowBlock = rowBlocks.get(r);
      for (int c = 0; c < colBlocks.size(); c++) {
        checkCancelled();
        final var colBlock = colBlocks.get(c);
        final var nativeMatrix = nativeFile.matrixQueries().getSubmatrix(nativeResolution,
          rowBlock.start, colBlock.start, rowBlock.end, colBlock.end, true);
        if (c == 0) addWeights(rowBlock, nativeMatrix.rowWeights(), rowWeights, rowLengths);
        if (r == 0) addWeights(colBlock, nativeMatrix.colWeights(), colWeights, colLengths);
        accumulate(nativeMatrix.matrix(), rowBlock, colBlock, reducer, values, scoreAreas);
      }
    }
    if (scoreAreas != null) {
      for (int row = 0; row < rows; row++) {
        for (int col = 0; col < cols; col++) {
          if (scoreAreas[row][col] > 0) values[row][col] /= scoreAreas[row][col];
        }
      }
    }
    finishWeights(rowWeights, rowLengths);
    finishWeights(colWeights, colLengths);
    return new MatrixQueries.MatrixWithWeights(new MatrixQueries.DoubleMatrix(values), rowWeights, colWeights,
      rowStart, colStart, rowEnd, colEnd, QueryLengthUnit.PIXELS, primaryResolution);
  }

  private static List<AxisBlock> axis(AssemblyBinGrid primary, AssemblyBinGrid nativeGrid, long start, int size, int factor) {
    final var bins = new TreeMap<Long, List<Target>>();
    final int[] count = {0};
    for (int index = 0; index < size; index++) {
      checkCancelled();
      final var interval = primary.sampleAt(start + index, factor);
      if (interval == null) continue;
      final int target = index;
      nativeGrid.overlaps(interval, overlap -> {
        if (++count[0] > MAX_AXIS_CONTRIBUTIONS) {
          throw new IllegalArgumentException("The overlay tile spans too many native bins. Zoom in to load a smaller region.");
        }
        bins.computeIfAbsent(overlap.pixel(), key -> new ArrayList<>())
          .add(new Target(target, overlap.bp(), overlap.binLength()));
      });
    }
    final var blocks = new ArrayList<AxisBlock>();
    for (var first = bins.firstEntry(); first != null; ) {
      final long startPixel = first.getKey();
      final long lastPixel = startPixel + Math.min(READ_EDGE - 1L, Long.MAX_VALUE - startPixel);
      final var blockBins = bins.subMap(startPixel, true, lastPixel, true);
      blocks.add(new AxisBlock(startPixel, Math.addExact(blockBins.lastKey(), 1), blockBins));
      first = bins.higherEntry(lastPixel);
    }
    return blocks;
  }

  private static void accumulate(MatrixQueries.RawMatrix matrix, AxisBlock rows, AxisBlock cols,
                                 Reducer reducer, double[][] values, double[][] scoreAreas) {
    for (var row : rows.bins.entrySet()) {
      final int nativeRow = Math.toIntExact(row.getKey() - rows.start);
      for (var col : cols.bins.entrySet()) {
        final int nativeCol = Math.toIntExact(col.getKey() - cols.start);
        final double value = matrix.getAsDouble(nativeRow, nativeCol);
        if (!Double.isFinite(value) || value == 0) continue;
        for (var targetRow : row.getValue()) {
          for (var targetCol : col.getValue()) {
            final int r = targetRow.index;
            final int c = targetCol.index;
            switch (reducer) {
              case COUNTS -> values[r][c] += value * ((double) targetRow.overlap / targetRow.binLength)
                * ((double) targetCol.overlap / targetCol.binLength);
              case IDENTITY -> {
                if (value > 0) {
                  final double area = (double) targetRow.overlap * targetCol.overlap;
                  values[r][c] += value * area;
                  scoreAreas[r][c] += area;
                }
              }
              case PRESENCE -> {
                if (value > 0) values[r][c] = 1;
              }
            }
          }
        }
      }
    }
  }

  private static void addWeights(AxisBlock block, double[] nativeWeights, double[] weights, long[] lengths) {
    if (nativeWeights == null) return;
    for (var entry : block.bins.entrySet()) {
      final int index = Math.toIntExact(entry.getKey() - block.start);
      if (index >= nativeWeights.length) continue;
      for (var target : entry.getValue()) {
        weights[target.index] += nativeWeights[index] * target.overlap;
        lengths[target.index] = Math.addExact(lengths[target.index], target.overlap);
      }
    }
  }

  private static void finishWeights(double[] weights, long[] lengths) {
    for (int i = 0; i < weights.length; i++) {
      weights[i] = lengths[i] == 0 ? 1 : weights[i] / lengths[i];
    }
  }

  private static void checkCancelled() {
    if (Thread.currentThread().isInterrupted()) throw new CancellationException();
  }
}
