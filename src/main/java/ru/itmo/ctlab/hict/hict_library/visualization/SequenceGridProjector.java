package ru.itmo.ctlab.hict.hict_library.visualization;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.BinCoordinateMapper;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.awt.image.BufferedImage;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Draw native cell colors on a uniform one-base grid without interpolating contact values. */
public final class SequenceGridProjector {
  public static final int MAX_EDGE = 1024;
  private static final long MAX_NATIVE_CELLS = (long) MAX_EDGE * MAX_EDGE;
  private SequenceGridProjector() {}

  public static Projection prepare(ChunkedFile file, ResolutionDescriptor resolution,
                                   long rowStartBp, long rowEndBp, long colStartBp, long colEndBp,
                                   BooleanSupplier cancelled) {
    checkCancelled(cancelled);
    final int rows = edge(rowStartBp, rowEndBp), cols = edge(colStartBp, colEndBp);
    final var lock = file.getContigTree().getRootLock().readLock();
    lock.lock();
    try {
      final var rowAxis = axis(file, resolution, rowStartBp, rows, cancelled);
      final var colAxis = rowStartBp == colStartBp && rows == cols ? rowAxis
        : axis(file, resolution, colStartBp, cols, cancelled);
      if (rowAxis.first < 0 || colAxis.first < 0)
        return new Projection(null, new int[rows], new int[cols], rows, cols, rowStartBp, colStartBp, cancelled);
      final long nativeRows = Math.addExact(Math.subtractExact(rowAxis.last, rowAxis.first), 1);
      final long nativeCols = Math.addExact(Math.subtractExact(colAxis.last, colAxis.first), 1);
      if (nativeRows > MAX_NATIVE_CELLS || nativeCols > MAX_NATIVE_CELLS / nativeRows)
        throw new IllegalArgumentException("Sequence tile spans too many native bins; choose a coarser matrix resolution");
      checkCancelled(cancelled);
      final var matrix = file.matrixQueries().getSubmatrix(resolution, rowAxis.first, colAxis.first,
        Math.addExact(rowAxis.last, 1), Math.addExact(colAxis.last, 1), false);
      return new Projection(matrix, relative(rowAxis), relative(colAxis), rows, cols, rowStartBp, colStartBp, cancelled);
    } finally {
      lock.unlock();
    }
  }

  private static int edge(long start, long end) {
    final long length = Math.subtractExact(end, start);
    if (length < 1 || length > MAX_EDGE)
      throw new IllegalArgumentException("Sequence tile edges must be between 1 and " + MAX_EDGE + " bases");
    return (int) length;
  }

  private static Axis axis(ChunkedFile file, ResolutionDescriptor resolution, long startBp, int length,
                           BooleanSupplier cancelled) {
    final var bins = new long[length];
    Arrays.fill(bins, -1L);
    final var bp = ResolutionDescriptor.fromResolutionOrder(0);
    final var tree = file.getContigTree();
    final long total = tree.getLengthInUnits(QueryLengthUnit.BASE_PAIRS, bp);
    final long start = Math.max(0, startBp), end = Math.min(total, Math.addExact(startBp, length));
    if (end <= start) return new Axis(bins, -1, -1);
    final var exposed = tree.expose(bp, start, end, QueryLengthUnit.BASE_PAIRS);
    final long[] offset = {
      exposed.less() == null ? 0 : exposed.less().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, bp),
      exposed.less() == null ? 0 : exposed.less().getSubtreeLengthInUnits(QueryLengthUnit.BINS, resolution)
    };
    final int order = resolution.getResolutionOrderInArray();
    final long nativeBp = file.getResolutions()[order];
    ContigTree.Node.traverseNode(exposed.segment(), node -> {
      checkCancelled(cancelled);
      final var descriptor = node.getContigDescriptor();
      final long contigEnd = Math.addExact(offset[0], descriptor.getLengthBp());
      final long count = descriptor.getLengthInUnits(QueryLengthUnit.BINS, resolution);
      if (count > 0) {
        final long origin = file.firstOwnedBinOffset(descriptor, order);
        final boolean reversed = node.getTrueDirection() == ContigDirection.REVERSED;
        for (long position = Math.max(start, offset[0]); position < Math.min(end, contigEnd); position++) {
          if ((position & 63) == 0) checkCancelled(cancelled);
          final long local = BinCoordinateMapper.bpToBin(position - offset[0], descriptor.getLengthBp(),
            count, nativeBp, origin, reversed);
          bins[Math.toIntExact(position - startBp)] = Math.addExact(offset[1], local);
        }
      }
      offset[0] = contigEnd;
      offset[1] = Math.addExact(offset[1], count);
    });
    long first = Long.MAX_VALUE, last = -1;
    for (long bin : bins) if (bin >= 0) { first = Math.min(first, bin); last = Math.max(last, bin); }
    return new Axis(bins, last < 0 ? -1 : first, last);
  }

  private static int[] relative(Axis axis) {
    final var indices = new int[axis.bins.length];
    for (int i = 0; i < indices.length; i++)
      indices[i] = axis.bins[i] < 0 ? -1 : Math.toIntExact(axis.bins[i] - axis.first);
    return indices;
  }

  private record Axis(long[] bins, long first, long last) {}

  public static final class Projection {
    private final MatrixWithWeights nativeMatrix;
    private final int[] rows, cols;
    private final int height, width;
    private final long rowStartBp, colStartBp;
    private final BooleanSupplier cancelled;

    private Projection(MatrixWithWeights nativeMatrix, int[] rows, int[] cols, int height, int width,
                       long rowStartBp, long colStartBp, BooleanSupplier cancelled) {
      this.nativeMatrix = nativeMatrix;
      this.rows = rows;
      this.cols = cols;
      this.height = height;
      this.width = width;
      this.rowStartBp = rowStartBp;
      this.colStartBp = colStartBp;
      this.cancelled = cancelled;
    }

    /** The renderer receives real native bin coordinates and weights, never one-base pseudo counts. */
    public BufferedImage render(Function<MatrixWithWeights, BufferedImage> renderer) {
      return render((matrix, upper) -> renderer.apply(matrix), false);
    }

    /** Keep upper/lower source choices correct even inside a single native diagonal cell. */
    public BufferedImage renderTriangles(BiFunction<MatrixWithWeights, Boolean, BufferedImage> renderer) {
      return render(renderer, true);
    }

    private BufferedImage render(BiFunction<MatrixWithWeights, Boolean, BufferedImage> renderer, boolean splitTriangles) {
      checkCancelled(cancelled);
      final var result = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
      if (nativeMatrix == null) return result;
      final boolean hasUpper = rowStartBp <= Math.addExact(colStartBp, width - 1L);
      final boolean hasLower = Math.addExact(rowStartBp, height - 1L) > colStartBp;
      final var first = renderer.apply(nativeMatrix, !splitTriangles || hasUpper);
      checkCancelled(cancelled);
      final var second = splitTriangles && hasUpper && hasLower ? renderer.apply(nativeMatrix, false) : first;
      for (var image : new BufferedImage[]{first, second}) {
        if (image.getWidth() != nativeMatrix.matrix().cols() || image.getHeight() != nativeMatrix.matrix().rows())
          throw new IllegalArgumentException("Native renderer returned inconsistent tile dimensions");
      }
      final var output = new int[width * height];
      for (int row = 0; row < height; row++) {
        if ((row & 31) == 0) checkCancelled(cancelled);
        if (rows[row] < 0) continue;
        for (int col = 0; col < width; col++) if (cols[col] >= 0) {
          final boolean upper = Math.addExact(rowStartBp, row) <= Math.addExact(colStartBp, col);
          final var image = splitTriangles && hasUpper && !upper ? second : first;
          output[row * width + col] = image.getRGB(cols[col], rows[row]);
        }
      }
      checkCancelled(cancelled);
      result.setRGB(0, 0, width, height, output, 0, width);
      return result;
    }
  }

  private static void checkCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted())
      throw new CancellationException("Sequence tile is obsolete");
  }
}
