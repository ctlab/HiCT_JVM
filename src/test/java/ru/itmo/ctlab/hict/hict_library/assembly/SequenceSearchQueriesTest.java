package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SequenceSearchQueriesTest {
  @Test void acceptsMultilineFastaAndPreservesDuplicateFullDescriptions() {
    var queries = SequenceSearchQueries.parse("\n>motif repeat α\r\nacg\r\nta\n>motif repeat α\nNNN\n");
    assertEquals(2, queries.size());
    assertEquals("ACGTA", queries.getFirst().sequence());
    assertEquals("motif repeat α", queries.getFirst().description());
    assertNotEquals(queries.get(0).id(), queries.get(1).id());
  }
  @Test void assignsStableIdsToPlainSequences() {
    var queries = SequenceSearchQueries.parse(" acgt \n\nTT GG \n");
    assertEquals("query_2", queries.get(1).description());
    assertEquals("TTGG", queries.get(1).sequence());
  }
  @Test void rejectsMixedMalformedAndEmptyInputs() {
    for (String input : new String[]{"", "ACGT\n>other\nT", ">x\n>x\nA", ">\nA", ">x", "ACTU", "AC-GT", "123"})
      assertThrows(IllegalArgumentException.class, () -> SequenceSearchQueries.parse(input), input);
  }
}
