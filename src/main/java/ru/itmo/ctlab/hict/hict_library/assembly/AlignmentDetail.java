package ru.itmo.ctlab.hict.hict_library.assembly;

import java.util.ArrayList;
import java.util.List;

/** Coordinates from one CIGAR-bearing PAF alignment in two displayed sequence windows. */
public final class AlignmentDetail {
  public enum Operation { MATCH, SUBSTITUTION, INSERTION, DELETION, AMBIGUOUS }

  public record Segment(long rowStart, long rowEnd, long columnStart, long columnEnd,
                        Operation operation, boolean reverse) { }

  private AlignmentDetail() { }

  /** The row sequence is the aligner's target. Output intervals are zero-based and half-open. */
  public static List<Segment> parsePaf(String line, CharSequence rows, CharSequence columns,
                                       long rowOffset, long columnOffset, int limit) {
    if (rowOffset < 0 || columnOffset < 0 || limit < 1) throw new IllegalArgumentException("Invalid alignment projection bounds");
    final var fields = line.split("\t", -1);
    if (fields.length < 13) throw malformed("Missing PAF fields");
    final int columnLength = positiveInt(fields[1]);
    final int columnStart = nonnegativeInt(fields[2]);
    final int columnEnd = nonnegativeInt(fields[3]);
    final boolean reverse = switch (fields[4]) {
      case "+" -> false;
      case "-" -> true;
      default -> throw malformed("Invalid PAF strand");
    };
    final int rowLength = positiveInt(fields[6]);
    final int rowStart = nonnegativeInt(fields[7]);
    final int rowEnd = nonnegativeInt(fields[8]);
    if (rowLength != rows.length() || columnLength != columns.length()
      || rowStart >= rowEnd || rowEnd > rowLength
      || columnStart >= columnEnd || columnEnd > columnLength) {
      throw malformed("Alignment lies outside its sequence windows");
    }
    String cigar = null;
    for (int i = 12; i < fields.length; i++) {
      if (fields[i].startsWith("cg:Z:")) {
        cigar = fields[i].substring(5);
        break;
      }
    }
    if (cigar == null || cigar.isEmpty()) throw malformed("Missing extended CIGAR");

    int row = rowStart;
    int col = reverse ? columnEnd : columnStart;
    final int step = reverse ? -1 : 1;
    final var segments = new ArrayList<Segment>();
    for (int i = 0; i < cigar.length(); ) {
      final int start = i;
      while (i < cigar.length() && cigar.charAt(i) >= '0' && cigar.charAt(i) <= '9') i++;
      if (i == start || i == cigar.length()) throw malformed("Invalid CIGAR operation length");
      final int length = positiveInt(cigar.substring(start, i));
      final char code = cigar.charAt(i++);
      final boolean consumesRows = code == '=' || code == 'X' || code == 'D';
      final boolean consumesCols = code == '=' || code == 'X' || code == 'I';
      if (!consumesRows && !consumesCols) throw malformed("Unsupported extended CIGAR operation " + code);
      if ((consumesRows && length > rowEnd - row)
        || (consumesCols && length > (reverse ? col - columnStart : columnEnd - col))) {
        throw malformed("CIGAR consumes more sequence than the PAF interval");
      }
      if (consumesRows && consumesCols) {
        appendAligned(segments, rows, columns, row, col, length, code, step, rowOffset, columnOffset, limit);
      } else {
        append(segments, row, row + (consumesRows ? length : 0), col, col + (consumesCols ? step * length : 0),
          code == 'I' ? Operation.INSERTION : Operation.DELETION, reverse, rowOffset, columnOffset, limit);
      }
      if (consumesRows) row += length;
      if (consumesCols) col += step * length;
    }
    if (row != rowEnd || col != (reverse ? columnStart : columnEnd)) throw malformed("CIGAR and PAF intervals disagree");
    return List.copyOf(segments);
  }

  private static void appendAligned(List<Segment> segments, CharSequence rows, CharSequence cols,
                                    int row, int col, int length, char code, int step,
                                    long rowOffset, long colOffset, int limit) {
    final var exact = code == '=' ? Operation.MATCH : Operation.SUBSTITUTION;
    int start = 0;
    var current = operationAt(rows, cols, row, col, step, exact);
    for (int i = 1; i < length; i++) {
      final var operation = operationAt(rows, cols, row + i, col + step * i, step, exact);
      if (operation == current) continue;
      append(segments, row + start, row + i, col + step * start, col + step * i,
        current, step < 0, rowOffset, colOffset, limit);
      start = i;
      current = operation;
    }
    append(segments, row + start, row + length, col + step * start, col + step * length,
      current, step < 0, rowOffset, colOffset, limit);
  }

  private static Operation operationAt(CharSequence rows, CharSequence cols, int row, int col, int step, Operation exact) {
    return canonical(rows.charAt(row)) && canonical(cols.charAt(step < 0 ? col - 1 : col))
      ? exact : Operation.AMBIGUOUS;
  }

  private static boolean canonical(char base) {
    return switch (Character.toUpperCase(base)) {
      case 'A', 'C', 'G', 'T' -> true;
      default -> false;
    };
  }

  private static void append(List<Segment> segments, int rowStart, int rowEnd, int colStart, int colEnd,
                             Operation op, boolean reverse, long rowOffset, long colOffset, int limit) {
    if (segments.size() >= limit) throw malformed("Alignment contains too many segments; choose a smaller region");
    segments.add(new Segment(Math.addExact(rowOffset, rowStart), Math.addExact(rowOffset, rowEnd),
      Math.addExact(colOffset, Math.min(colStart, colEnd)), Math.addExact(colOffset, Math.max(colStart, colEnd)), op, reverse));
  }

  private static int nonnegativeInt(String text) {
    try {
      final int value = Integer.parseInt(text);
      if (value >= 0) return value;
    } catch (NumberFormatException ignored) { }
    throw malformed("Invalid PAF or CIGAR integer");
  }

  private static int positiveInt(String text) {
    final int value = nonnegativeInt(text);
    if (value == 0) throw malformed("Zero-length CIGAR operation or sequence");
    return value;
  }

  private static IllegalArgumentException malformed(String message) {
    return new IllegalArgumentException(message);
  }
}
