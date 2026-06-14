package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssemblyLayoutConverterTest {
  @Test
  void parsesJuiceboxAssemblyIntoAgpRecords() throws Exception {
    final var assembly = """
      >ctgA 1 100
      >ctgB 2 200
      >ctgC 3 300
      1
      1
      2
      """;

    final var records = AssemblyLayoutConverter.parseJuiceboxAssembly(new StringReader(assembly));

    assertEquals(3, records.size());
    assertTrue(records.get(0) instanceof AGPProcessor.ContigAGPRecord);
    final var first = (AGPProcessor.ContigAGPRecord) records.get(0);
    assertEquals("1", first.getScaffoldName());
    assertEquals("ctgA", first.getContigName());
    assertEquals(100L, first.getIntraContigEndBpIncl());

    assertTrue(records.get(2) instanceof AGPProcessor.ContigAGPRecord);
    final var third = (AGPProcessor.ContigAGPRecord) records.get(2);
    assertEquals("2", third.getScaffoldName());
    assertEquals("ctgC", third.getContigName());
  }

  @Test
  void convertsJuiceboxAssemblyToAgpFile() throws Exception {
    final var source = Files.createTempFile("assembly-layout-", ".assembly");
    final var output = Files.createTempFile("assembly-layout-", ".agp");
    try {
      Files.writeString(
        source,
        """
        >ctgA 1 100
        >ctgB 2 200
        1
        2
        """
      );

      AssemblyLayoutConverter.convertToAgp(source, output);

      final var contents = Files.readString(output);
      assertTrue(contents.contains("1\t1\t100\t1\tW\tctgA\t1\t100\t+"));
      assertTrue(contents.contains("2\t1\t200\t1\tW\tctgB\t1\t200\t+"));
    } finally {
      Files.deleteIfExists(source);
      Files.deleteIfExists(output);
    }
  }

  @Test
  void parsesJuiceboxScaffoldLinesWithSignedContigIds() throws Exception {
    final var assembly = """
      >ctgA 1 100
      >ctgB 2 200
      >ctgC 3 300
      1 -2
      3
      """;

    final var records = AssemblyLayoutConverter.parseJuiceboxAssembly(new StringReader(assembly));

    assertEquals(3, records.size());
    final var first = (AGPProcessor.ContigAGPRecord) records.get(0);
    final var second = (AGPProcessor.ContigAGPRecord) records.get(1);
    final var third = (AGPProcessor.ContigAGPRecord) records.get(2);
    assertEquals("1", first.getScaffoldName());
    assertEquals("ctgA", first.getContigName());
    assertEquals(1L, first.getInterScaffoldStartIncl());
    assertEquals(100L, first.getInterScaffoldEndIncl());
    assertEquals(AGPProcessor.AGPContigOrientation.PLUS, first.getContigOrientation());
    assertEquals("1", second.getScaffoldName());
    assertEquals("ctgB", second.getContigName());
    assertEquals(101L, second.getInterScaffoldStartIncl());
    assertEquals(300L, second.getInterScaffoldEndIncl());
    assertEquals(AGPProcessor.AGPContigOrientation.MINUS, second.getContigOrientation());
    assertEquals("2", third.getScaffoldName());
    assertEquals("ctgC", third.getContigName());
  }
}
