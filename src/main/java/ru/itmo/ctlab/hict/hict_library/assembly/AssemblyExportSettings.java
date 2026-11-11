package ru.itmo.ctlab.hict.hict_library.assembly;

/** Shared output policy for AGP coordinates and the matching FASTA N runs. */
public final class AssemblyExportSettings {
  public static final String GAP_LENGTH_KEY = "HICT_EXPORT_GAP_LENGTH_BP";
  public static final long DEFAULT_GAP_LENGTH_BP = 1000L;

  private AssemblyExportSettings() {}

  public static long gapLengthBp() {
    return parseGapLength(System.getProperty(GAP_LENGTH_KEY, System.getenv(GAP_LENGTH_KEY)));
  }

  static long parseGapLength(final String value) {
    if (value == null || value.isBlank()) return DEFAULT_GAP_LENGTH_BP;
    try {
      return requireGapLength(Long.parseLong(value.trim()));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(GAP_LENGTH_KEY + " must be a non-negative integer in base pairs", e);
    }
  }

  public static long requireGapLength(final long value) {
    if (value < 0L) {
      throw new IllegalArgumentException(GAP_LENGTH_KEY + " must be non-negative (0 joins components without a gap row)");
    }
    return value;
  }
}
