package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.itmo.ctlab.hict.hict_library.assembly.AlignmentDetail.Operation.*;

class AlignmentDetailTest {
  @Test
  void projectsSubstitutionsAndGapsUsingHalfOpenIntervals() {
    assertEquals(List.of(
      new AlignmentDetail.Segment(100, 102, 200, 202, MATCH, false),
      new AlignmentDetail.Segment(102, 103, 202, 203, SUBSTITUTION, false),
      new AlignmentDetail.Segment(103, 103, 203, 204, INSERTION, false),
      new AlignmentDetail.Segment(103, 104, 204, 205, AMBIGUOUS, false),
      new AlignmentDetail.Segment(104, 105, 205, 205, DELETION, false),
      new AlignmentDetail.Segment(105, 106, 205, 206, MATCH, false)),
      AlignmentDetail.parsePaf(paf("+", "2=1X1I1=1D1="), "ACGNTC", "ACTAGC", 100, 200, 20));
  }

  @Test
  void reverseStrandConsumesColumnsFromTheirExclusiveEnd() {
    assertEquals(List.of(
      new AlignmentDetail.Segment(0, 2, 4, 6, MATCH, true),
      new AlignmentDetail.Segment(2, 3, 3, 4, AMBIGUOUS, true),
      new AlignmentDetail.Segment(3, 3, 2, 3, INSERTION, true),
      new AlignmentDetail.Segment(3, 4, 1, 2, MATCH, true),
      new AlignmentDetail.Segment(4, 5, 1, 1, DELETION, true),
      new AlignmentDetail.Segment(5, 6, 0, 1, MATCH, true)),
      AlignmentDetail.parsePaf(paf("-", "2=1X1I1=1D1="), "ACGATC", "GCRYGT", 0, 0, 20));
  }

  @Test
  void rejectsUnsupportedTruncatedOversizedAndInconsistentAlignments() {
    for (final var cigar : List.of("6M", "5=", "7=", "0=6=", "6")) {
      assertThrows(IllegalArgumentException.class,
        () -> AlignmentDetail.parsePaf(paf("+", cigar), "ACGTAC", "ACGTAC", 0, 0, 10));
    }
    assertThrows(IllegalArgumentException.class,
      () -> AlignmentDetail.parsePaf(paf("+", "6="), "ACGTAC", "ANGTAC", 0, 0, 2));
  }

  private static String paf(String strand, String cigar) {
    return "column\t6\t0\t6\t" + strand + "\trow\t6\t0\t6\t4\t7\t60\tcg:Z:" + cigar;
  }
}
