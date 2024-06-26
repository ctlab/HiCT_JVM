/*
 * MIT License
 *
 * Copyright (c) 2021-2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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
