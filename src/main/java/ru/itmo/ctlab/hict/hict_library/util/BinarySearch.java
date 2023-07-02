package ru.itmo.ctlab.hict.hict_library.util;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class BinarySearch {
  public static int leftBinarySearch(long @NotNull @NonNull [] a, long key) {
    return primitiveLeftBinarySearch(a, key);
  }

  private static int primitiveLeftBinarySearch(long[] a, long key) {
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
