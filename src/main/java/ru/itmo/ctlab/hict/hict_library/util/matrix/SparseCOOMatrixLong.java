package ru.itmo.ctlab.hict.hict_library.util.matrix;

import lombok.AllArgsConstructor;

import java.util.Arrays;
import java.util.function.Function;

@AllArgsConstructor
public class SparseCOOMatrixLong {
  private final int[] rowIndices;
  private final int[] colIndices;
  private final long[] values;
  private final boolean transposed;
  private final boolean symmetric;
  private final boolean flipRows;
  private final boolean flipCols;

  public SparseCOOMatrixLong map(final Function<Long, Long> fun) {
    return new SparseCOOMatrixLong(
      rowIndices,
      colIndices,
      Arrays.stream(values).map(fun::apply).toArray(),
      transposed,
      symmetric,
      flipRows,
      flipCols
    );
  }

  public SparseCOOMatrixLong transpose() {
    return new SparseCOOMatrixLong(
      rowIndices,
      colIndices,
      values,
      !transposed,
      symmetric,
      flipRows,
      flipCols
    );
  }

  public long[][] toDense(final int rows, final int cols) {
    final var nonZeroCount = values.length;
    final var rowIdx = transposed ? colIndices : rowIndices;
    final var colIdx = transposed ? rowIndices : colIndices;
    assert (Arrays.stream(rowIdx).summaryStatistics().getMax() < rows) : "Embedding sparse array into smaller dense by rows?";
    assert (Arrays.stream(colIdx).summaryStatistics().getMax() < cols) : "Embedding sparse array into smaller dense by cols?";
    final var result2D = new long[rows][cols];

    final var dr = rows - 1;
    final var dc = cols - 1;

    if (!symmetric) {
      if (flipRows) {
        if (flipCols) {
          for (var i = 0; i < nonZeroCount; ++i) {
            result2D[dr - rowIdx[i]][dc - colIdx[i]] = values[i];
          }
        } else {
          for (var i = 0; i < nonZeroCount; ++i) {
            result2D[dr - rowIdx[i]][colIdx[i]] = values[i];
          }
        }
      } else {
        if (flipCols) {
          for (var i = 0; i < nonZeroCount; ++i) {
            result2D[rowIdx[i]][dc - colIdx[i]] = values[i];
          }
        } else {
          for (var i = 0; i < nonZeroCount; ++i) {
            result2D[rowIdx[i]][colIdx[i]] = values[i];
          }
        }
      }
    } else {
      if (flipRows) {
        if (flipCols) {
          for (var i = 0; i < nonZeroCount; ++i) {
            result2D[dr - rowIdx[i]][dc - colIdx[i]] = values[i];
            result2D[dc - colIdx[i]][dr - rowIdx[i]] = values[i];
          }
        } else {
          for (var i = 0; i < nonZeroCount; ++i) {
            result2D[dr - rowIdx[i]][colIdx[i]] = values[i];
            result2D[colIdx[i]][dr - rowIdx[i]] = values[i];
          }
        }
      } else {
        if (flipCols) {
          for (var i = 0; i < nonZeroCount; ++i) {
            result2D[rowIdx[i]][dc - colIdx[i]] = values[i];
            result2D[dc - colIdx[i]][rowIdx[i]] = values[i];
          }
        } else {
          for (var i = 0; i < nonZeroCount; ++i) {
            result2D[rowIdx[i]][colIdx[i]] = values[i];
            result2D[colIdx[i]][rowIdx[i]] = values[i];
          }
        }
      }
    }

    return result2D;
  }
}
