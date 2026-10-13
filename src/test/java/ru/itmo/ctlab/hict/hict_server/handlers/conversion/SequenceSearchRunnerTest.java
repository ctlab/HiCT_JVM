package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.assembly.*;
import ru.itmo.ctlab.hict.hict_server.tracks.SequenceSearchMetadata;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class SequenceSearchRunnerTest {
  @TempDir Path dir;
  private static AssemblySequenceSnapshot snapshot() {
    return new AssemblySequenceSnapshot(List.of(new AssemblySequenceSnapshot.Fragment("A", 0, 8, true),
      new AssemblySequenceSnapshot.Fragment("B", 0, 8, false)));
  }

  @Test void ownershipRemovesChunkOverlapDuplicatesAndAnchorsCrossJoinHits() throws Exception {
    Path raw = dir.resolve("raw"), out = dir.resolve("out.bed");
    Files.writeString(raw, "hict_chunk_0_6\t6\t10\tquery_1\t0\t+\n"
      + "hict_chunk_6_12\t0\t4\tquery_1\t0\t+\n");
    var counts = SequenceSearchRunner.anchorHits(raw, out, snapshot(), SequenceSearchQueries.parse(">cross join motif\nTTGG"));
    assertArrayEquals(new long[]{1, 2}, counts);
    var rows = Files.readAllLines(out).stream().filter(s -> !s.startsWith("#")).toList();
    var a = rows.getFirst().split("\t"); var b = rows.getLast().split("\t");
    assertEquals("A", a[0]); assertEquals("0", a[1]); assertEquals("2", a[2]); assertEquals("-", a[5]);
    assertEquals("B", b[0]); assertEquals("0", b[1]); assertEquals("2", b[2]); assertEquals("+", b[5]);
    var metadata = SequenceSearchMetadata.decode(a[12]);
    assertEquals("cross join motif", metadata.description());
    assertEquals(metadata.encode(), a[12]);
    assertEquals(a[12], b[12]);
    assertEquals(6, snapshot().project(metadata.path()).pieces().getFirst().assemblyStart());
  }

  @Test void realSeqkitBothStrandsAndSubstitutionSearchMatchExhaustiveOracle() throws Exception {
    String executable = System.getenv("HICT_SEQKIT_BIN");
    assumeTrue(executable != null && Files.isExecutable(Path.of(executable)), "Set HICT_SEQKIT_BIN for native integration");
    String sequence = "CCAAGGTTGGAACCTT";
    var queries = SequenceSearchQueries.parse("TTGG\nAACC\n");
    for (int mismatches = 0; mismatches <= 1; mismatches++) {
      var workspace = dir.resolve("run" + mismatches); Files.createDirectory(workspace);
      var input = workspace.resolve("chunks.fasta");
      var chunks = new StringBuilder();
      for (int start = 0; start < sequence.length(); start += 6) {
        int ownedEnd = Math.min(start + 6, sequence.length());
        chunks.append(">hict_chunk_").append(start).append('_').append(ownedEnd).append('\n')
          .append(sequence, start, Math.min(ownedEnd + 3, sequence.length())).append('\n');
      }
      Files.writeString(input, chunks);
      var result = new SequenceSearchRunner().run(Path.of(executable), workspace, input, snapshot(), queries,
        new SequenceSearchRunner.Options(true, mismatches, false, 1), ignored -> {}, ignored -> {}, () -> false);
      var actual = new TreeSet<String>(); var ids = new HashSet<String>();
      for (var row : Files.readAllLines(result.bed())) {
        if (row.startsWith("#")) continue;
        var m = SequenceSearchMetadata.decode(row.split("\t")[12]);
        if (ids.add(m.hitId())) actual.add(m.queryId() + ":" + snapshot().project(m.path()).pieces().getFirst().assemblyStart() + ":" + m.strand());
      }
      var expected = new TreeSet<String>();
      for (var query : queries) for (String strand : List.of("+", "-")) {
        String motif = query.sequence();
        if (strand.equals("-")) {
          var reverse = new StringBuilder();
          for (int i = motif.length() - 1; i >= 0; i--) reverse.append("TGCA".charAt("ACGT".indexOf(motif.charAt(i))));
          motif = reverse.toString();
        }
        for (int i = 0; i <= sequence.length() - motif.length(); i++) {
          int differences = 0;
          for (int j = 0; j < motif.length(); j++) if (sequence.charAt(i + j) != motif.charAt(j)) differences++;
          if (differences <= mismatches) expected.add(query.id() + ":" + i + ":" + strand);
        }
      }
      assertEquals(expected, actual);
      assertEquals(expected.size(), result.hits());
    }
  }
}
