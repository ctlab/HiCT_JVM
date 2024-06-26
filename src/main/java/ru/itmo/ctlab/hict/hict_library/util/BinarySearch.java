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

package ru.itmo.ctlab.hict.hict_library.util;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class BinarySearch {
  public static int leftBinarySearch(long @NotNull [] a, long key) {
    // TODO: Parallelize for large arrays?
    return lowerBound(a, key);
  }

  public static int rightBinarySearch(long @NotNull [] a, long key) {
    // TODO: Parallelize for large arrays?
    return upperBound(a, key);
  }

  private static int lowerBound(long[] a, long key) {
    var l = 0;
    var h = a.length;
    while (l < h) {
      int mid = l + (h - l) / 2;
      if (a[mid] < key) {
        l = mid + 1;
      } else {
        h = mid;
      }
    }
    return l;
  }

  private static int upperBound(long[] a, long key) {
    var l = 0;
    var h = a.length;
    while (l < h) {
      int mid = l + (h - l) / 2;
      if (a[mid] <= key) {
        l = mid + 1;
      } else {
        h = mid;
      }
    }
    return l;
  }

  private static int trivialLeftBinarySearch(long[] a, long key) {
    final var insertionPoint = Arrays.binarySearch(a, key);
    int index;
    if (insertionPoint >= 0) {
      index = insertionPoint;
      while (index > 0 && a[index - 1] == key) {
        --index;
      }
    } else {
      index = -insertionPoint - 1;
    }
    return index;
  }

  private static int primitiveRightBinarySearch(long[] a, long key) {
    final var insertionPoint = Arrays.binarySearch(a, 1 + key);
    int index;
    if (insertionPoint >= 0) {
      index = insertionPoint;
      while (index > 0 && a[index] > key) {
        --index;
      }
    } else {
      index = Integer.max(0, -insertionPoint - 2);
    }
    return index;
  }
}
