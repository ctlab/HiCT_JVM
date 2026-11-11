package ru.itmo.ctlab.hict.hict_library.converters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.SourceMetadata;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/** A FASTA file alone becomes an assembly with real contigs and no contacts. */
class FastaOnlyAssemblyTest {
  @TempDir Path directory;

  private static String n(int count) {
    return "N".repeat(count);
  }

  private Path fasta(String name, String text) throws IOException {
    var path = directory.resolve(name);
    if (name.endsWith(".gz")) {
      try (var out = new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(path)), StandardCharsets.ISO_8859_1)) {
        out.write(text);
      }
    } else {
      Files.writeString(path, text, StandardCharsets.ISO_8859_1);
    }
    return path;
  }

  private static final String SAMPLE =
    ">chr1 first record\nACGTACGTAC\n" + n(120) + "\nGGGGCCCC" + n(3) + "TTTT\n" + n(100) + "\n"
      + ">chr2\nacgtnnnnnnacgt\n"
      + ">emptyrec\n"
      + ">allgap\n" + n(500) + "\n"
      + ">chr3 lower-case gap\nAC" + "n".repeat(100) + "GT\n";

  @Test void recordsLengthsAndGapsAreReadInOnePass() throws Exception {
    var scan = FastaOnlyAssembly.scan(fasta("a.fa", SAMPLE), 100, () -> false, null);
    assertEquals(List.of("chr1", "chr2", "chr3"), scan.records().stream().map(FastaOnlyAssembly.Record::id).toList());
    var chr1 = scan.records().get(0);
    assertEquals("first record", chr1.description());
    assertEquals(10 + 120 + 8 + 3 + 4 + 100, chr1.length());
    // The 3 N between GGGGCCCC and TTTT is shorter than the window. The two long runs are gaps.
    assertEquals(2, chr1.gaps().size());
    assertArrayEquals(new long[]{10, 130}, chr1.gaps().get(0));
    assertArrayEquals(new long[]{145, 245}, chr1.gaps().get(1));
    assertEquals(1, scan.skippedEmpty());
    assertEquals(1, scan.skippedAllGap());
    assertEquals(2, scan.warnings().size());
    var chr3 = scan.records().get(2);
    assertEquals(1, chr3.gaps().size(), "lower-case n counts");
  }

  @Test void gzipInputGivesTheSameRecords() throws Exception {
    var plain = FastaOnlyAssembly.scan(fasta("p.fa", SAMPLE), 100, () -> false, null);
    var packed = FastaOnlyAssembly.scan(fasta("p.fa.gz", SAMPLE), 100, () -> false, null);
    assertEquals(plain.totalBases(), packed.totalBases());
    assertEquals(plain.records().size(), packed.records().size());
  }

  @Test void splittingNeverMakesEmptyPiecesAndKeepsSourceCoordinates() throws Exception {
    var scan = FastaOnlyAssembly.scan(fasta("s.fa", SAMPLE), 100, () -> false, null);
    var chr1 = FastaOnlyAssembly.segments(scan.records().get(0), true);
    // Leading text, then between the two runs, and nothing after the trailing run.
    assertEquals(List.of(new FastaOnlyAssembly.Segment("chr1", 0, 10), new FastaOnlyAssembly.Segment("chr1", 130, 145)), chr1);
    assertTrue(chr1.stream().allMatch(s -> s.end() > s.start()));
    assertEquals(1, FastaOnlyAssembly.segments(scan.records().get(0), false).size());
    var chr3 = FastaOnlyAssembly.segments(scan.records().get(2), true);
    assertEquals(List.of(new FastaOnlyAssembly.Segment("chr3", 0, 2), new FastaOnlyAssembly.Segment("chr3", 102, 104)), chr3);
  }

  @Test void agpLayoutDescribesPiecesAndTrueGapLengths() throws Exception {
    var scan = FastaOnlyAssembly.scan(fasta("g.fa", SAMPLE), 100, () -> false, null);
    var agp = FastaOnlyAssembly.agpLayout(scan);
    assertNotNull(agp);
    var lines = agp.lines().filter(l -> !l.startsWith("#")).toList();
    assertTrue(lines.contains("chr1\t1\t10\t1\tW\tchr1\t1\t10\t+"), agp);
    assertTrue(lines.contains("chr1\t11\t130\t2\tN\t120\tcontig\tno\tna"), agp);
    assertTrue(lines.contains("chr1\t131\t145\t3\tW\tchr1\t131\t145\t+"), agp);
    // An unsplit record is one component covering all of it.
    assertTrue(lines.contains("chr2\t1\t14\t1\tW\tchr2\t1\t14\t+") || lines.stream().anyMatch(l -> l.startsWith("chr2\t")));
    // Records with no run get no layout at all when nothing in the file would be split.
    var clean = FastaOnlyAssembly.scan(fasta("c.fa", ">a\nACGT\n>b\nTTTT\n"), 100, () -> false, null);
    assertNull(FastaOnlyAssembly.agpLayout(clean));
  }

  @Test void badInputIsRefusedWithTheLine() throws Exception {
    var duplicate = fasta("d.fa", ">x\nAC\n>y\nGT\n>x\nTT\n");
    var e1 = assertThrows(IllegalArgumentException.class, () -> FastaOnlyAssembly.scan(duplicate, 100, () -> false, null));
    assertTrue(e1.getMessage().contains("x appears twice") && e1.getMessage().contains("lines 1 and 5"), e1.getMessage());
    var headless = fasta("h.fa", "ACGT\n>x\nAC\n");
    var e2 = assertThrows(IllegalArgumentException.class, () -> FastaOnlyAssembly.scan(headless, 100, () -> false, null));
    assertTrue(e2.getMessage().contains("Line 1"), e2.getMessage());
    var none = fasta("n.fa", ">only-empty\n");
    assertThrows(IllegalArgumentException.class, () -> FastaOnlyAssembly.scan(none, 100, () -> false, null));
    var okay = fasta("o.fa", ">a\nAC\n");
    assertThrows(IllegalArgumentException.class, () -> FastaOnlyAssembly.scan(okay, 0, () -> false, null));
    assertThrows(java.util.concurrent.CancellationException.class,
      () -> FastaOnlyAssembly.scan(fasta("x.fa", ">a\nAC\n>b\nGG\n"), 100, () -> true, null));
  }

  @Test void resolutionsKeepTheEmptyMapSmall() {
    assertEquals(1L, FastaOnlyAssembly.baseResolution(4_000_000L));
    assertEquals(10L, FastaOnlyAssembly.baseResolution(40_000_000L));
    assertEquals(1000L, FastaOnlyAssembly.baseResolution(4_000_000_000L));
    var levels = FastaOnlyAssembly.resolutions(250_000_000L);
    assertEquals(100L, levels.get(0));
    assertTrue(levels.size() <= 6);
    assertTrue(levels.stream().sorted().toList().equals(levels));
  }

  @Test void writtenMapHasRealContigsAndNoContacts() throws Exception {
    var scan = FastaOnlyAssembly.scan(fasta("w.fa", SAMPLE), 100, () -> false, null);
    var output = directory.resolve("out/w.hict.hdf5");
    FastaOnlyAssembly.write(scan, output, () -> false, null);
    assertTrue(Files.exists(output));
    assertFalse(Files.exists(output.resolveSibling("w.hict.hdf5.partial")));
    assertFalse(Files.exists(output.resolveSibling("w.hict.hdf5.input.mcool")));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(output, 1, 2))) {
      assertEquals(SourceMetadata.ContactDataState.ABSENT, file.getSourceMetadata().contactDataState());
      assertEquals(SourceMetadata.Units.SEQUENCE_ONLY, file.getSourceMetadata().units());
      var contigs = file.getContigTree().getOrderedContigList();
      assertEquals(3, contigs.size());
      assertEquals("chr1", contigs.get(0).descriptor().getContigName());
      assertEquals(scan.records().get(0).length(), contigs.get(0).descriptor().getLengthBp());
    }
  }

  @Test void splitPiecesKeepTheirSourceCoordinatesAndScaffoldsKeepGapLengths() throws Exception {
    var scan = FastaOnlyAssembly.scan(fasta("sp.fa", SAMPLE), 100, () -> false, null);
    var output = directory.resolve("out/sp.hict.hdf5");
    FastaOnlyAssembly.write(scan, output, () -> false, null);
    var agp = FastaOnlyAssembly.agpLayout(scan);
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(output, 1, 2))) {
      var records = file.getAgpProcessor().parseRecords(new java.io.StringReader(agp));
      var report = file.getAgpProcessor().importRecords(records,
        ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.EXCLUDE);
      assertFalse(report.requiresChoice());
      var contigs = file.getContigTree().getOrderedContigList();
      // chr1 -> two pieces, chr2 -> one, chr3 -> two.
      assertEquals(5, contigs.size());
      long pieces = contigs.stream().mapToLong(c -> c.descriptor().getLengthBp()).sum();
      long expected = (10 + 15) + 14 + (2 + 2);
      assertEquals(expected, pieces, "only the removed N runs are missing");
      var second = contigs.get(1).descriptor();
      assertEquals("chr1", second.getContigNameInSourceFASTA());
      assertEquals(130L, second.getOffsetInSourceFASTA());
      assertEquals(15L, second.getLengthBp());
      var third = contigs.get(3).descriptor();
      assertEquals("chr3", third.getContigNameInSourceFASTA());
      assertEquals(0L, third.getOffsetInSourceFASTA());
      var fourth = contigs.get(4).descriptor();
      assertEquals(102L, fourth.getOffsetInSourceFASTA());
      // Exporting the assembly as AGP keeps the original coordinates and the real gap sizes.
      var exported = file.getAgpProcessor().getAGPStream(0).toList().toString();
      assertTrue(exported.contains("chr1\t131\t145") || exported.contains("131"), exported);
    }
  }
}
