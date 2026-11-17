package ru.itmo.ctlab.hict.hict_server.tracks;

import java.util.Arrays;
import java.util.Set;

/** Reference envelope of a VCF record, including its reference padding base.
 * INFO/END is already a zero-based exclusive endpoint after converting POS.
 * Symbolic insertions add no reference span. All arithmetic remains long-valued.
 * See https://samtools.github.io/hts-specs/VCFv4.5.pdf and VCFv4.3.pdf.
 */
final class VcfReferenceInterval {
  private static final Set<String> SPANNING_TYPES = Set.of("DEL", "DUP", "INV", "CNV");

  private VcfReferenceInterval() { }

  static long endExclusive(String[] fields, long position1Based, long line) {
    if (position1Based < 0) throw invalid(line, "POS must not be negative");
    try {
      long start = Math.max(0, position1Based - 1);
      long end = Math.addExact(start, Math.max(1, fields[3].length()));
      String explicitEnd = null;
      String lengths = null;
      if (fields.length > 7) {
        for (String item : fields[7].split(";", -1)) {
          if (item.startsWith("END=") || item.equals("END")) {
            if (explicitEnd != null) throw invalid(line, "duplicate INFO/END");
            explicitEnd = item.equals("END") ? "" : item.substring(4);
          } else if (item.startsWith("SVLEN=") || item.equals("SVLEN")) {
            if (lengths != null) throw invalid(line, "duplicate INFO/SVLEN");
            lengths = item.equals("SVLEN") ? "" : item.substring(6);
          }
        }
      }
      String[] alleles = fields[4].split(",", -1);
      if (lengths != null && !lengths.equals(".") && Arrays.stream(alleles).anyMatch(VcfReferenceInterval::spansReference)) {
        String[] values = lengths.split(",", -1);
        if (values.length != alleles.length) throw invalid(line, "INFO/SVLEN must have one value per ALT allele");
        for (int index = 0; index < alleles.length; index++) {
          if (!spansReference(alleles[index]) || values[index].equals(".")) continue;
          long length = number(values[index], "INFO/SVLEN", line);
          if (length < 0) length = Math.negateExact(length); // Legacy negative deletion lengths.
          if (length == 0) throw invalid(line, "INFO/SVLEN must be nonzero for a spanning symbolic allele");
          end = Math.max(end, Math.addExact(position1Based, length));
        }
      }
      boolean referenceBlock = Arrays.stream(alleles).anyMatch(a -> a.equals("<*>") || a.equals("<NON_REF>"));
      if (referenceBlock && fields.length > 9) {
        String[] format = fields[8].split(":", -1);
        for (int index = 0; index < format.length; index++) {
          if (!format[index].equals("LEN")) continue;
          for (int sample = 9; sample < fields.length; sample++) {
            String[] values = fields[sample].split(":", -1);
            if (index >= values.length || values[index].equals(".")) continue;
            long length = number(values[index], "FORMAT/LEN", line);
            if (length <= 0) throw invalid(line, "FORMAT/LEN must be positive");
            end = Math.max(end, Math.addExact(position1Based, length - 1));
          }
        }
      }
      if (explicitEnd != null && !explicitEnd.equals(".")) {
        long declared = number(explicitEnd, "INFO/END", line);
        if (declared < end) throw invalid(line, "INFO/END precedes the reference interval's last base");
        end = declared;
      }
      return end;
    } catch (ArithmeticException overflow) {
      throw invalid(line, "reference interval exceeds the supported coordinate range");
    }
  }

  private static boolean spansReference(String allele) {
    if (!allele.startsWith("<") || !allele.endsWith(">")) return false;
    String type = allele.substring(1, allele.length() - 1);
    int subtype = type.indexOf(':');
    return SPANNING_TYPES.contains(subtype < 0 ? type : type.substring(0, subtype));
  }

  private static long number(String value, String field, long line) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException invalidNumber) {
      throw invalid(line, "cannot parse " + field + ": " + value);
    }
  }

  private static IllegalArgumentException invalid(long line, String detail) {
    return new IllegalArgumentException("Invalid VCF reference interval at line " + line + ": " + detail);
  }
}
