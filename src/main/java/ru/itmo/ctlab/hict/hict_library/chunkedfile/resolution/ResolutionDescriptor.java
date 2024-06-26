/*
 * MIT License
 *
 * Copyright (c) 2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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

package ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Range;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.util.BinarySearch;

public abstract class ResolutionDescriptor {

  public static ResolutionDescriptor fromResolutionOrder(final int resolutionOrder) {
    return new ResolutionDescriptor() {
      @Override
      public @Range(from = 0, to = Integer.MAX_VALUE) int getResolutionOrderInArray() {
        return resolutionOrder;
      }
    };
  }

  public static ResolutionDescriptor fromBpResolution(final long bpResolution, final @NotNull ChunkedFile chunkedFile) {
    final int resolutionOrder;

    if (bpResolution < 1) {
      resolutionOrder = 0;
    } else {
      resolutionOrder = BinarySearch.leftBinarySearch(chunkedFile.getResolutions(), bpResolution);
    }

    return new ResolutionDescriptor() {
      @Override
      public @Range(from = 0, to = Integer.MAX_VALUE) int getResolutionOrderInArray() {
        return resolutionOrder;
      }
    };
  }

  /**
   * @return An order of resolution stored in node's arrays.
   */
  @Range(from = 0, to = Integer.MAX_VALUE)
  public abstract int getResolutionOrderInArray();
}
