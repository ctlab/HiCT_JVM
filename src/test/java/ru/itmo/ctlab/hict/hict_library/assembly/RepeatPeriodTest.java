package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import java.util.concurrent.CancellationException;
import static org.junit.jupiter.api.Assertions.*;

class RepeatPeriodTest {
  private RepeatPeriod.Result estimate(String sequence) { return RepeatPeriod.estimate(sequence, () -> {}); }
  @Test void measuresShortestPeriodPartialCopiesHomopolymersAndSubstitutions() {
    assertEquals(2, estimate("CTCTCTCTCTCT").periodBp());
    assertEquals(2, estimate("CTCTCTCTCT").periodBp());
    assertEquals(3, estimate("ACGACGACGA").periodBp());
    assertEquals(1, estimate("AAAAAA").periodBp());
    var mutation = estimate("CTCTATCTCTCT");
    assertEquals(2, mutation.periodBp());
    assertEquals(8, mutation.matches());
    assertEquals(10, mutation.comparedPairs());
    assertEquals(.8, mutation.identity());
    assertEquals(6., mutation.copies());
    assertEquals(2, estimate("ctctctct").periodBp());
  }
  @Test void ambiguityIsNotRepeatEvidenceAndUnsupportedShortWindowsRemainUnknown() {
    for (String sequence : new String[]{"N", "NNNNNNNNNNNN", "RYRYRYRYRYRY", "ACGT", "A", "AC"})
      assertNull(estimate(sequence).periodBp(), sequence);
    assertEquals(12, estimate("NNNNNNNNNNNN").ambiguousBases());
    assertNull(estimate("AAAAAACAAAAA").periodBp()); // Composition-biased similarity alone is insufficient.
  }
  @Test void boundsAndCancellationAreEnforced() {
    assertThrows(IllegalArgumentException.class, () -> estimate(""));
    assertThrows(IllegalArgumentException.class, () -> estimate("A".repeat(4097)));
    assertThrows(CancellationException.class, () -> RepeatPeriod.estimate("ACGT".repeat(1024), () -> { throw new CancellationException(); }));
    assertEquals(4, estimate("ACGT".repeat(1024)).periodBp());
  }
  @Test void exactPeriodsMatchAnIndependentShiftOracle() {
    for (String motif : new String[]{"A", "CT", "ACG", "ACTG", "ACGTTGC", "CTCT"}) {
      String sequence = motif.repeat(5) + motif.substring(0, motif.length()/2);
      int expected = 0;
      for (int p=1;p<=sequence.length()/2;p++) {
        if (sequence.substring(p).equals(sequence.substring(0,sequence.length()-p))) { expected=p;break; }
      }
      assertEquals(expected, estimate(sequence).periodBp(), sequence);
    }
  }
}
