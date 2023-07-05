package ru.itmo.ctlab.hict.hict_library.util;

public class CommonUtils {
  public static long clamp(final long x, final long min, final long max) {
    return Long.max(min, Long.min(x, max));
  }
}
