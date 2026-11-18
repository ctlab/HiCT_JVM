package ru.itmo.ctlab.hict.hict_server.tracks;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

class BamTrackSemanticsTest {
  @TempDir Path directory;

  private Path fixture() throws Exception {
    var file = ReinforcementHdf5Fixture.create(directory, "bam-semantics", List.of(1L, 4L, 10L));
    var header = new SAMFileHeader();
    header.setSortOrder(SAMFileHeader.SortOrder.coordinate);
    header.addSequence(new SAMSequenceRecord("tiny", 17));
    try (var writer = new SAMFileWriterFactory().setCreateIndex(true)
      .makeBAMWriter(header, true, directory.resolve("reads.bam").toFile())) {
      for (var spec : List.of(new String[]{"clipped-insertion", "1", "1S1=1I1X1H", "ACGT"},
        new String[]{"spliced", "2", "2M3N2M", "ACGT"},
        new String[]{"deletion", "10", "2M1D2M", "ACGT"},
        new String[]{"last-base", "17", "1M", "A"})) {
        var read = new SAMRecord(header);
        read.setReadName(spec[0]); read.setReferenceName("tiny");
        read.setAlignmentStart(Integer.parseInt(spec[1])); read.setCigarString(spec[2]);
        read.setReadString(spec[3]); read.setBaseQualityString("I".repeat(spec[3].length()));
        read.setMappingQuality(60); writer.addAlignment(read);
      }
    }
    return file;
  }

  private static double[] query(Track1DManager tracks, ChunkedFile file, boolean sequence,
                                int start, int end, int width) throws Exception {
    var render = ready(() -> (sequence ? tracks.querySequenceTracks(file, null, start, end, width, 4)
      : tracks.queryVisibleTracks(file, start, end, width, 1, QueryLengthUnit.BASE_PAIRS)).getTracks().getFirst());
    var result = new double[end - start];
    for (var bin : render.getBins()) {
      assertTrue(bin.getStartBp() >= start && bin.getEndBp() <= end);
      for (long p = bin.getStartBp(); p < bin.getEndBp(); p++) result[(int) p - start] = bin.getValue();
    }
    return result;
  }

  private static Track1DManager.TrackRender ready(Supplier<Track1DManager.TrackRender> query) throws Exception {
    var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    Track1DManager.TrackRender render;
    do {
      render = query.get();
      assertNull(render.getError());
      if (render.getStatusMessage() == null) break;
      assertTrue(render.getStatusMessage().startsWith("Preparing"), render.getStatusMessage());
      assertTrue(System.nanoTime() < deadline, render.getStatusMessage());
      Thread.sleep(10);
    } while (true);
    return render;
  }

  private static void cut(ChunkedFile file) {
    file.scaffoldingOperations().splitContigAtBp(0, 3);
    int right = file.getAssemblyInfo().contigs().getLast().descriptor().getContigId();
    file.scaffoldingOperations().splitContigAtBp(right, 3);
  }

  @Test void coverageCountsAlignedBasesWithoutIntronsDeletionsOrInsertedAndClippedBases() throws Exception {
    var path = fixture(); var original = Files.readAllBytes(path);
    // Independently enumerated M/= /X reference positions from the three CIGARs.
    double[] expected = {1, 2, 1, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, 1};
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      tracks.openTrack("reads.bam", "Reads", null);
      for (int state = 0; state < 3; state++) {
        if (state == 1) cut(file);
        if (state == 2) file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
        var oracle = expected.clone();
        if (state == 2) for (int i = 0; i < 17; i++) oracle[i] = expected[16 - i];
        for (boolean sequence : List.of(true, false)) {
          assertArrayEquals(oracle, query(tracks, file, sequence, 0, 17, 17), 0, "state=" + state + " sequence=" + sequence);
          double mean = Arrays.stream(oracle).sum() / 17;
          assertArrayEquals(new double[]{mean}, new double[]{query(tracks, file, sequence, 0, 17, 1)[0]}, 1e-12);
        }
      }
    }
    assertArrayEquals(original, Files.readAllBytes(path));
  }

  @Test void eachSourceReadCenterIsCountedOnceRegardlessOfCutsReversalOrViewportClipping() throws Exception {
    var path = fixture(); var original = Files.readAllBytes(path);
    // Midpoints of original spans [0,2), [1,8), [9,14), [16,17).
    double[] expected = new double[17]; expected[1] = 1; expected[4] = 1; expected[11] = 1; expected[16] = 1;
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      var id = tracks.openTrack("reads.bam", "Reads", null).getTrackId();
      tracks.updateTrack(id, null, null, null, "READ_DENSITY", null, null);
      for (int state = 0; state < 3; state++) {
        if (state == 1) cut(file);
        if (state == 2) file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
        var oracle = expected.clone();
        if (state == 2) for (int i = 0; i < 17; i++) oracle[i] = expected[16 - i];
        for (boolean sequence : List.of(true, false)) {
          assertArrayEquals(oracle, query(tracks, file, sequence, 0, 17, 17), 0, "state=" + state + " sequence=" + sequence);
          for (int start = 0; start < 17; start++) {
            assertArrayEquals(new double[]{oracle[start]}, query(tracks, file, sequence, start, start + 1, 1), 0,
              "viewport=" + start + " state=" + state + " sequence=" + sequence);
          }
          assertEquals(4, query(tracks, file, sequence, 0, 17, 1)[0], 0);
        }
      }
    }
    assertArrayEquals(original, Files.readAllBytes(path));
  }

  @Test void nativeCoarseLevelsAverageActualRepresentedBasesIncludingPartialBins() throws Exception {
    var path = fixture();
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      var id = tracks.openTrack("reads.bam", "Reads", null).getTrackId();
      for (int state = 0; state < 3; state++) {
        if (state == 1) cut(file);
        if (state == 2) file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
        for (long resolution : List.of(4L, 10L)) {
          int pixels = Math.toIntExact(file.getContigTree().getLengthInUnits(QueryLengthUnit.PIXELS,
            ResolutionDescriptor.fromBpResolution(resolution, file)));
          tracks.updateTrack(id, null, null, null, "COVERAGE", null, null);
          var coverage = ready(() -> tracks.queryVisibleTracks(file, 0, pixels, pixels, resolution, QueryLengthUnit.PIXELS)
            .getTracks().getFirst());
          assertNull(coverage.getError()); assertNull(coverage.getStatusMessage());
          if (state == 0) {
            double[] expected = resolution == 4 ? new double[]{1, .5, .5, .5, 1} : new double[]{.7, 4.0 / 7};
            double[] actual = new double[pixels];
            for (var bin : coverage.getBins()) actual[Math.toIntExact(bin.getStartPx())] = bin.getValue();
            assertArrayEquals(expected, actual, 1e-12, "native resolution=" + resolution);
          }
          var mean = ready(() -> tracks.queryVisibleTracks(file, 0, pixels, 1, resolution, QueryLengthUnit.PIXELS)
            .getTracks().getFirst());
          assertEquals(11.0 / 17, mean.getBins().getFirst().getValue(), 1e-12, "state=" + state + " resolution=" + resolution);
          tracks.updateTrack(id, null, null, null, "READ_DENSITY", null, null);
          var centers = ready(() -> tracks.queryVisibleTracks(file, 0, pixels, 1, resolution, QueryLengthUnit.PIXELS)
            .getTracks().getFirst());
          assertEquals(4, centers.getBins().getFirst().getValue(), 0, "state=" + state + " resolution=" + resolution);
        }
      }
    }
  }

  @Test void legacySignalCacheCannotReplaceCorrectedSourceValues() throws Exception {
    var path = fixture();
    var processed = directory.resolve("processed");
    Path hictCachePath;
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, processed)) {
      hictCachePath = file.getHdfFilePath().normalize().toAbsolutePath();
      tracks.openTrack("reads.bam", "Reads", null);
      assertEquals(1, query(tracks, file, false, 0, 17, 17)[0], 0);
    }
    Path current;
    try (var files = Files.list(processed.resolve("track_precompute"))) {
      current = files.filter(p -> p.toString().endsWith(".h5")).findFirst().orElseThrow();
    }
    String identity;
    try (var reader = HDF5Factory.openForReading(current.toFile())) {
      identity = reader.string().getAttr("/cache_meta", "sourceIdentity");
    }
    // Version 2 used this source/type/layout key and had no base-weighted
    // signal semantics. Seed a valid-shaped, fingerprint-matching old cache.
    String oldKey = "2|BAM|" + identity + "|" + hictCachePath;
    String oldName = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
      .digest(oldKey.getBytes(StandardCharsets.UTF_8))) + ".h5";
    Path legacy = current.resolveSibling(oldName);
    assertNotEquals(current, legacy);
    Files.copy(current, legacy);
    try (var writer = HDF5Factory.open(legacy.toFile())) {
      writer.string().setAttr("/cache_meta", "cacheVersion", "2");
      corruptValues(writer, "/");
    }
    var legacyBytes = Files.readAllBytes(legacy);
    Files.delete(current);
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, processed)) {
      tracks.openTrack("reads.bam", "Reads", null);
      assertEquals(1, query(tracks, file, false, 0, 17, 17)[0], 0);
    }
    assertTrue(Files.exists(current));
    assertArrayEquals(legacyBytes, Files.readAllBytes(legacy));
  }

  private static void corruptValues(ch.systemsx.cisd.hdf5.IHDF5Writer writer, String group) {
    for (String name : writer.object().getGroupMembers(group)) {
      String path = group.equals("/") ? "/" + name : group + "/" + name;
      if (writer.object().isGroup(path)) {
        if (writer.object().hasAttribute(path, "version")) writer.string().setAttr(path, "version", "2");
        corruptValues(writer, path);
      } else if (name.equals("values")) {
        double[] values = writer.float64().readArray(path);
        Arrays.fill(values, 999);
        writer.float64().writeArray(path, values);
      }
    }
  }
}
