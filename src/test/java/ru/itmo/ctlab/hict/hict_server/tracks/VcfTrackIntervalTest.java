package ru.itmo.ctlab.hict.hict_server.tracks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class VcfTrackIntervalTest {
  @TempDir Path directory;

  private static final String HEADER = """
    ##fileformat=VCFv4.3
    ##contig=<ID=tiny,length=17>
    ##INFO=<ID=END,Number=1,Type=Integer,Description="End reference position">
    #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
    """;

  @Test void symbolicVariantsAndReferenceBlocksRetainEveryCoveredBaseAfterExactCuts() throws Exception {
    checkProjectedIntervals(false);
  }

  @Test void gzipVariantsUseTheSameReferenceIntervals() throws Exception {
    checkProjectedIntervals(true);
  }

  private void checkProjectedIntervals(boolean gzip) throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "vcf-intervals", List.of(4L, 10L));
    var original = Files.readAllBytes(path);
    var text = HEADER + """
      tiny\t3\tdeletion\tN\t<DEL>\t.\tPASS\tEND=14
      tiny\t6\treference-block\tA\t<NON_REF>\t.\t.\tEND=11
      tiny\t2\tsnp\tC\tG\t.\t.\tOTHER_END=17
      tiny\t4\tsequence-deletion\tACGT\tA\t.\t.\t.
      """;
    var name = gzip ? "variants.vcf.gz" : "variants.vcf";
    if (gzip) {
      try (var output = new GZIPOutputStream(Files.newOutputStream(directory.resolve(name)))) {
        output.write(text.getBytes(StandardCharsets.UTF_8));
      }
    } else {
      Files.writeString(directory.resolve(name), text);
    }
    // These are reference intervals, including VCF's padding base, in
    // zero-based half-open coordinates. They are independent of map bins.
    var expected = Map.of("deletion", new int[]{2, 14}, "reference-block", new int[]{5, 11},
      "snp", new int[]{1, 2}, "sequence-deletion", new int[]{3, 7});
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      tracks.openTrack(name, "Variants", null);
      for (int state = 0; state < 3; state++) {
        if (state == 1) {
          file.scaffoldingOperations().splitContigAtBp(0, 5);
          int right = file.getAssemblyInfo().contigs().getLast().descriptor().getContigId();
          file.scaffoldingOperations().splitContigAtBp(right, 4);
        } else if (state == 2) {
          file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
        }
        var query = tracks.querySequenceTracks(file, null, 0, 17, 17, 4);
        var rendered = query.getTracks().getFirst();
        assertNull(rendered.getError());
        assertNull(rendered.getStatusMessage());
        for (var entry : expected.entrySet()) {
          for (int display = 0; display < 17; display++) {
            final int position = display;
            int source = state == 2 ? 16 - display : display;
            boolean shouldCover = source >= entry.getValue()[0] && source < entry.getValue()[1];
            boolean covers = rendered.getBins().stream().anyMatch(bin -> entry.getKey().equals(bin.getLabel())
              && bin.getStartBp() <= position && position < bin.getEndBp());
            assertEquals(shouldCover, covers, entry.getKey() + " source=" + source + " state=" + state);
          }
        }
      }
    }
    assertArrayEquals(original, Files.readAllBytes(path));
  }

  @Test void endCoordinatesBeyondTwoPointFiveGbStayLongValued() throws Exception {
    long start = 2_500_000_003L;
    long end = start + 9;
    var path = ReinforcementHdf5Fixture.create(directory, "long-vcf", List.of(1_000_000_000L),
      3_000_000_017L, new long[][]{{0, 0, 1}});
    Files.writeString(directory.resolve("long.vcf"), HEADER.replace("length=17>", "length=3000000017>") + "tiny\t" + (start + 1)
      + "\tlong-variant\tN\t<DEL>\t.\tPASS\tEND=" + end + "\n");
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      tracks.openTrack("long.vcf", "Long variant", null);
      var bins = tracks.querySequenceTracks(file, null, start, end, 9, 1_000_000_000L)
        .getTracks().getFirst().getBins();
      assertEquals(1, bins.size());
      assertEquals(start, bins.getFirst().getStartBp());
      assertEquals(end, bins.getFirst().getEndBp());
    }
  }

  @Test void modernSymbolicLengthsCoverReferenceBasesWithoutExpandingInsertions() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "modern-vcf", List.of(4L, 10L));
    Files.writeString(directory.resolve("modern.vcf"), """
      ##fileformat=VCFv4.5
      ##contig=<ID=tiny,length=17>
      ##INFO=<ID=SVLEN,Number=A,Type=Integer,Description="Structural variant length">
      ##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
      ##FORMAT=<ID=LEN,Number=1,Type=Integer,Description="Reference block length">
      #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tfirst\tsecond
      tiny\t3\tdeletion\tN\t<DEL>\t.\tPASS\tSVLEN=-11\tGT:LEN\t0/1:.\t0/0:.
      tiny\t5\tinversion\tN\t<INV>\t.\tPASS\tSVLEN=6\tGT:LEN\t0/1:.\t0/0:.
      tiny\t9\tcopy-number\tN\t<CNV>\t.\tPASS\tSVLEN=4\tGT:LEN\t0/1:.\t0/0:.
      tiny\t3\tmixed\tN\t<INS>,<DEL:ME>\t.\tPASS\tSVLEN=100000,-8\tGT:LEN\t1/2:.\t0/0:.
      tiny\t7\tblock\tA\t<*>\t.\tPASS\t.\tGT:LEN\t0/0:6\t0/0:3
      tiny\t2\tinsertion\tA\t<INS>\t.\tPASS\tSVLEN=100000\tGT:LEN\t0/1:.\t0/0:.
      """);
    var expected = Map.of("deletion", new int[]{2, 14}, "inversion", new int[]{4, 11},
      "copy-number", new int[]{8, 13}, "mixed", new int[]{2, 11}, "block", new int[]{6, 12},
      "insertion", new int[]{1, 2});
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      tracks.openTrack("modern.vcf", "Modern variants", null);
      file.scaffoldingOperations().splitContigAtBp(0, 5);
      file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
      var rendered = tracks.querySequenceTracks(file, null, 0, 17, 17, 4).getTracks().getFirst();
      assertNull(rendered.getError());
      for (var entry : expected.entrySet()) {
        for (int display = 0; display < 17; display++) {
          final int position = display;
          int source = 16 - display;
          boolean shouldCover = source >= entry.getValue()[0] && source < entry.getValue()[1];
          boolean covers = rendered.getBins().stream().anyMatch(bin -> entry.getKey().equals(bin.getLabel())
            && bin.getStartBp() <= position && position < bin.getEndBp());
          assertEquals(shouldCover, covers, entry.getKey() + " source=" + source);
        }
      }
    }
  }

  @Test void malformedEndsDoNotPublishAPartiallyParsedTrack() throws Exception {
    try (var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      Files.writeString(directory.resolve("existing.bed"), "tiny\t0\t1\tkeep\n");
      tracks.openTrack("existing.bed", "Keep", null);
      var before = tracks.listTracks().stream().map(Track1DManager.TrackSummary::getTrackId).toList();
      for (String info : List.of("END=2", "END=not-a-number", "END=9223372036854775808")) {
        Files.writeString(directory.resolve("invalid.vcf"), HEADER
          + "tiny\t3\tvalid\tA\tG\t.\t.\t.\n"
          + "tiny\t4\tinvalid\tN\t<DEL>\t.\t.\t" + info + "\n");
        var failure = assertThrows(IllegalArgumentException.class,
          () -> tracks.openTrack("invalid.vcf", "Invalid", null), info);
        assertTrue(failure.getMessage().contains("line"), failure.getMessage());
        assertEquals(before, tracks.listTracks().stream().map(Track1DManager.TrackSummary::getTrackId).toList());
        assertEquals("Keep", tracks.listTracks().getFirst().getName());
        assertEquals(1, tracks.listTracks().getFirst().getFeatureCount());
      }
    }
  }

  @Test void invalidLengthsAndCoordinateOverflowDoNotReplaceExistingTracks() throws Exception {
    var rows = List.of(
      "tiny\t3\tbad\tN\t<DEL>\t.\t.\tSVLEN=-9223372036854775808",
      "tiny\t3\tbad\tN\t<DEL>\t.\t.\tSVLEN=9223372036854775807",
      "tiny\t3\tbad\tN\t<DEL>\t.\t.\tSVLEN=0",
      "tiny\t3\tbad\tN\t<DEL>\t.\t.\tSVLEN=bad",
      "tiny\t3\tbad\tN\t<DEL>,<DUP>\t.\t.\tSVLEN=7",
      "tiny\t3\tbad\tN\t<DEL>\t.\t.\tSVLEN=7;SVLEN=8",
      "tiny\t3\tbad\tN\t<DEL>\t.\t.\tEND=10;END=11",
      "tiny\t3\tbad\tN\t<DEL>\t.\t.\tEND=",
      "tiny\t3\tbad\tACGT\tA\t.\t.\tEND=5",
      "tiny\t3\tbad\tN\t<*>\t.\t.\t.\tGT:LEN\t0/0:0",
      "tiny\t3\tbad\tN\t<*>\t.\t.\t.\tGT:LEN\t0/0:-1",
      "tiny\t3\tbad\tN\t<*>\t.\t.\t.\tGT:LEN\t0/0:bad",
      "tiny\t3\tbad\tN\t<*>\t.\t.\t.\tGT:LEN\t0/0:9223372036854775807",
      "tiny\t9223372036854775807\tbad\tAC\tA\t.\t.\t.",
      "tiny\t-1\tbad\tA\tG\t.\t.\t.");
    try (var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      Files.writeString(directory.resolve("keep.bed"), "tiny\t0\t1\tkeep\n");
      var retained = tracks.openTrack("keep.bed", "Keep", null).getTrackId();
      for (String row : rows) {
        Files.writeString(directory.resolve("invalid.vcf"), HEADER + row + "\n");
        var failure = assertThrows(IllegalArgumentException.class,
          () -> tracks.openTrack("invalid.vcf", "Invalid", null), row);
        assertTrue(failure.getMessage().contains("line"), failure.getMessage());
        assertEquals(List.of(retained), tracks.listTracks().stream().map(Track1DManager.TrackSummary::getTrackId).toList());
      }
    }
  }
}
