package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringReader;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class AGPProcessorTest {
  @ParameterizedTest
  @ValueSource(strings = {"A", "D", "F", "G", "O", "P", "W"})
  void sequenceTypesAndInclusiveSingleBaseIntervalsRoundTrip(String type) throws Exception {
    final var line = "obj\t1\t1\t1\t" + type + "\tcomponent\t25\t25\t-";
    assertEquals(line, AGPProcessor.parseRecordsFromReader(new StringReader(line)).get(0).toString());
  }

  @ParameterizedTest
  @ValueSource(strings = {"+", "-", "?", "0", "na"})
  void acceptsAllStandardOrientations(String orientation) throws Exception {
    final var records = AGPProcessor.parseRecordsFromReader(new StringReader(
      "obj\t1\t10\t1\tW\tcomponent\t11\t20\t" + orientation));
    assertEquals(1, records.size());
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 76, 100, 500, 1000, 2345})
  void acceptsSpecifiedGapsWithoutChangingTheirSize(long gap) throws Exception {
    final var text = "obj\t1\t10\t1\tW\ta\t1\t10\t+\n"
      + "obj\t11\t" + (10 + gap) + "\t2\tN\t" + gap + "\tscaffold\tyes\tproximity_ligation\n"
      + "obj\t" + (11 + gap) + "\t" + (20 + gap) + "\t3\tW\tb\t1\t10\t-";
    assertEquals(text, AGPProcessor.parseRecordsFromReader(new StringReader(text)).stream()
      .map(Object::toString).collect(Collectors.joining("\n")));
  }

  @Test
  void preservesUnknownGapTypeAndMultipleEvidenceWithStandardSpelling() throws Exception {
    final var text = "obj\t1\t100\t1\tU\t100\tscaffold\tyes\tpaired-ends;align_xgenus";
    final var previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      final var records = AGPProcessor.parseRecordsFromReader(new StringReader(text));
      assertEquals(text, records.get(0).toString());
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void acceptsBiologicalTerminalGapsAndHeaders() throws Exception {
    final var text = "##agp-version 2.1\n# header\n"
      + "chrY\t1\t10000\t1\tN\t10000\ttelomere\tno\tna\n"
      + "chrY\t10001\t13043\t2\tW\tscf1\t1\t3043\t?\n"
      + "chrY\t13044\t13143\t3\tU\t100\tcontig\tno\tna\n";
    assertEquals(3, AGPProcessor.parseRecordsFromReader(new StringReader(text)).size());
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "obj\t1\t101\t1\tU\t101\tscaffold\tyes\tmap",
    "obj\t1\t500\t1\tN\t500\tscaffold\tno\tna",
    "obj\t1\t500\t1\tN\t500\tcontig\tyes\tmap",
    "obj\t1\t500\t1\tN\t500\ttelomere\tyes\tmap",
    "obj\t1\t500\t1\tN\t500\trepeat\tno\tmap",
    "obj\t1\t500\t1\tN\t500\trepeat\tyes\tna",
    "obj\t1\t500\t1\tN\t499\trepeat\tyes\tmap",
    "obj\t1\t11\t1\tW\ta\t1\t10\t+",
    "obj\t1\t10\t1\tW\ta\t0\t9\t+",
    "obj\t1\t10\t1\tW\ta\t1\t10\t+\textra"
  })
  void rejectsInvalidStandardFields(String text) {
    assertThrows(Exception.class, () -> AGPProcessor.parseRecordsFromReader(new StringReader(text)));
  }

  @Test
  void outputGapConfigurationIsValidated() {
    assertEquals(1000, AssemblyExportSettings.parseGapLength(null));
    assertEquals(500, AssemblyExportSettings.parseGapLength("500"));
    assertEquals(2345, AssemblyExportSettings.parseGapLength("2345"));
    assertEquals(0, AssemblyExportSettings.parseGapLength("0"));
    assertThrows(IllegalArgumentException.class, () -> AssemblyExportSettings.parseGapLength("-1"));
    assertThrows(IllegalArgumentException.class, () -> AssemblyExportSettings.parseGapLength("NaN"));
  }
}
