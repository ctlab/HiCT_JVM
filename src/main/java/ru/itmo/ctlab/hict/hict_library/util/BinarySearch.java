package ru.itmo.ctlab.hict.hict_library.util;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class BinarySearch {
  public static int leftBinarySearch(long @NotNull @NonNull [] a, long key) {
    // TODO: Parallelize for large arrays?
    return lowerBound(a, key);
  }

  public static int rightBinarySearch(long @NotNull @NonNull [] a, long key) {
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
