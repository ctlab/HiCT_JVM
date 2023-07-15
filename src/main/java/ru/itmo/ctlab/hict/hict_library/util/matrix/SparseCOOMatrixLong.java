package ru.itmo.ctlab.hict.hict_library.util.matrix;

import lombok.AllArgsConstructor;

import java.util.Arrays;
import java.util.function.Function;

@AllArgsConstructor
public class SparseCOOMatrixLong {
  private final int[] rowIndices;
  private final int[] colIndices;
  private final long[] values;
  private final boolean symmetric;

  public SparseCOOMatrixLong map(final Function<Long, Long> fun) {
    return new SparseCOOMatrixLong(
      rowIndices,
      colIndices,
      Arrays.stream(values).map(fun::apply).toArray(),
      symmetric
    );
  }


  public long[][] toDense(final int rows, final int cols) {
    final var nonZeroCount = values.length;
    final var rowIdx = rowIndices;
    final var colIdx = colIndices;
    assert (Arrays.stream(rowIdx).max().orElse(0) < rows) : "Embedding sparse array into smaller dense by rows?";
    assert (Arrays.stream(colIdx).max().orElse(0) < cols) : "Embedding sparse array into smaller dense by cols?";
    final var result2D = new long[rows][cols];

//    final var dr = rows - 1;
//    final var dc = cols - 1;

    if (!symmetric) {
      for (var i = 0; i < nonZeroCount; ++i) {
        result2D[rowIdx[i]][colIdx[i]] = values[i];
      }
    } else {
      for (var i = 0; i < nonZeroCount; ++i) {
        result2D[rowIdx[i]][colIdx[i]] = values[i];
        result2D[colIdx[i]][rowIdx[i]] = values[i];
      }
    }

    return result2D;
  }
}
