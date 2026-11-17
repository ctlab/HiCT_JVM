package ru.itmo.ctlab.hict.hict_server.tracks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class BigWigTrackSemanticsTest {
  @TempDir Path directory;

  @Test void signedValuesAndNegativeMaximaSurviveNativeCachesCutsAndReversal() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "signed-bigwig", List.of(1L, 4L, 10L));
    try (var source = getClass().getResourceAsStream("/tracks/signed-signal.bw")) {
      assertNotNull(source); Files.copy(source, directory.resolve("signed.bw"));
    }
    var original = Files.readAllBytes(path);
    double[] sourceValues = {-7, Double.NaN, -9, -4, -4, Double.NaN, Double.NaN, 6, 6,
      Double.NaN, 0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN};
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      var id = tracks.openTrack("signed.bw", "Signed", null).getTrackId();
      for (int state = 0; state < 3; state++) {
        if (state == 1) {
          file.scaffoldingOperations().splitContigAtBp(0, 3);
          int right = file.getAssemblyInfo().contigs().getLast().descriptor().getContigId();
          file.scaffoldingOperations().splitContigAtBp(right, 3);
        }
        if (state == 2) file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
        for (String mode : List.of("MAX", "MEAN", "SUM")) {
          tracks.updateTrack(id, null, null, null, null, mode, null);
          double expected = mode.equals("MAX") ? 6 : mode.equals("MEAN") ? -12.0 / 7 : -12.0 / 17;
          var sequence = tracks.querySequenceTracks(file, null, 0, 17, 17, 4).getTracks().getFirst();
          assertNull(sequence.getError());
          double[] actual = new double[17]; Arrays.fill(actual, Double.NaN);
          for (var bin : sequence.getBins()) for (long p = bin.getStartBp(); p < bin.getEndBp(); p++) actual[(int) p] = bin.getValue();
          double[] oracle = sourceValues.clone();
          if (state == 2) for (int i = 0; i < 17; i++) oracle[i] = sourceValues[16 - i];
          assertArrayEquals(oracle, actual, 0, "state=" + state + " mode=" + mode);
          assertEquals(expected, tracks.querySequenceTracks(file, null, 0, 17, 1, 4)
            .getTracks().getFirst().getBins().getFirst().getValue(), 1e-12);
          if (state == 0) {
            double negative = mode.equals("MAX") ? -4 : mode.equals("MEAN") ? -6 : -24.0 / 5;
            assertEquals(negative, tracks.querySequenceTracks(file, null, 0, 5, 1, 4)
              .getTracks().getFirst().getBins().getFirst().getValue(), 1e-12);
          }
          for (long resolution : List.of(1L, 4L, 10L)) {
            int pixels = Math.toIntExact(file.getContigTree().getLengthInUnits(QueryLengthUnit.PIXELS,
              ResolutionDescriptor.fromBpResolution(resolution, file)));
            var nativeBins = ready(() -> tracks.queryVisibleTracks(file, 0, pixels, pixels, resolution, QueryLengthUnit.PIXELS)
              .getTracks().getFirst());
            assertNull(nativeBins.getError());
            if (state == 0 && resolution == 4) {
              double first = mode.equals("MAX") ? -4 : mode.equals("MEAN") ? -20.0 / 3 : -5;
              assertEquals(first, nativeBins.getBins().getFirst().getValue(), 1e-12);
            }
            assertEquals(expected, ready(() -> tracks.queryVisibleTracks(file, 0, pixels, 1, resolution, QueryLengthUnit.PIXELS)
              .getTracks().getFirst()).getBins().getFirst().getValue(), 1e-12,
              "state=" + state + " mode=" + mode + " resolution=" + resolution);
          }
        }
      }
    }
    assertArrayEquals(original, Files.readAllBytes(path));
    // Reopen the unedited assembly and prepare a persisted signed sidecar.
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      var id = tracks.openTrack("signed.bw", "Signed again", null).getTrackId();
      for (String mode : List.of("MAX", "MEAN", "SUM")) {
        tracks.updateTrack(id, null, null, null, null, mode, null);
        var render = ready(() -> tracks.queryVisibleTracks(file, 0, 17, 17, 1, QueryLengthUnit.PIXELS)
          .getTracks().getFirst());
        assertEquals(-7, render.getBins().getFirst().getValue(), 0, mode);
      }
      long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (tracks.getPrecomputeStatus().getRunningJobs() > 0) {
        assertTrue(System.nanoTime() < deadline); Thread.sleep(10);
      }
      assertTrue(tracks.getPrecomputeStatus().getTracks().stream().noneMatch(t -> t.getError() != null));
    }
    // Immediate responses with no scheduled jobs prove the disk cache was reused.
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      var id = tracks.openTrack("signed.bw", "Disk cache", null).getTrackId();
      for (String mode : List.of("MAX", "MEAN", "SUM")) {
        tracks.updateTrack(id, null, null, null, null, mode, null);
        var render = tracks.queryVisibleTracks(file, 0, 17, 17, 1, QueryLengthUnit.PIXELS).getTracks().getFirst();
        assertNull(render.getError()); assertNull(render.getStatusMessage());
        assertEquals(-7, render.getBins().getFirst().getValue(), 0, mode);
        assertTrue(tracks.getPrecomputeStatus().getTracks().isEmpty());
      }
    }
  }

  @Test void nonfiniteSignalsProduceExplicitErrorsInsteadOfInventedZeros() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "nonfinite-bigwig", List.of(1L, 4L));
    for (String name : List.of("nonfinite-nan", "nonfinite-infinity")) {
      try (var source = getClass().getResourceAsStream("/tracks/" + name + ".bw")) {
        assertNotNull(source); Files.copy(source, directory.resolve(name + ".bw"));
      }
      try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
           var tracks = new Track1DManager(directory, directory.resolve(name))) {
        var id = tracks.openTrack(name + ".bw", name, null).getTrackId();
        var render = tracks.querySequenceTracks(file, null, 0, 17, 17, 4).getTracks().getFirst();
        assertNotNull(render.getError(), name);
        assertTrue(render.getError().contains("non-finite"), render.getError());
        assertTrue(render.getBins().isEmpty());
        tracks.startPrecompute(file, id, true);
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (tracks.getPrecomputeStatus().getRunningJobs() > 0) {
          assertTrue(System.nanoTime() < deadline); Thread.sleep(10);
        }
        var job = tracks.getPrecomputeStatus().getTracks().getFirst();
        assertEquals("failed", job.getStatus());
        assertTrue(job.getError().contains("non-finite"), job.getError());
      }
    }
  }

  private static Track1DManager.TrackRender ready(Supplier<Track1DManager.TrackRender> query) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (true) {
      var render = query.get();
      assertNull(render.getError());
      if (render.getStatusMessage() == null) return render;
      assertTrue(render.getStatusMessage().startsWith("Preparing"), render.getStatusMessage());
      assertTrue(System.nanoTime() < deadline, render.getStatusMessage());
      Thread.sleep(10);
    }
  }

  @Test void coveredAndZeroFilledMeansUseReferenceBasesAcrossLevelsCutsAndReversal() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "bigwig-semantics", List.of(1L, 4L, 10L));
    try (var source = getClass().getResourceAsStream("/tracks/signal-with-gap.bw")) {
      assertNotNull(source); Files.copy(source, directory.resolve("signal.bw"));
    }
    var original = Files.readAllBytes(path);
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      var id = tracks.openTrack("signal.bw", "Signal", null).getTrackId();
      for (int state = 0; state < 3; state++) {
        if (state == 1) {
          file.scaffoldingOperations().splitContigAtBp(0, 3);
          int right = file.getAssemblyInfo().contigs().getLast().descriptor().getContigId();
          file.scaffoldingOperations().splitContigAtBp(right, 3);
        }
        if (state == 2) file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
        for (String mode : List.of("MAX", "MEAN", "SUM")) {
          tracks.updateTrack(id, null, null, null, null, mode, null);
          double expected = mode.equals("MAX") ? 9 : mode.equals("MEAN") ? 6 : 24.0 / 17;
          for (long resolution : List.of(1L, 4L, 10L)) {
            int pixels = Math.toIntExact(file.getContigTree().getLengthInUnits(QueryLengthUnit.PIXELS,
              ResolutionDescriptor.fromBpResolution(resolution, file)));
            var render = ready(() -> tracks.queryVisibleTracks(file, 0, pixels, pixels, resolution, QueryLengthUnit.PIXELS)
              .getTracks().getFirst());
            if (state == 0 && resolution == 4) {
              double[] actual = new double[pixels];
              for (var bin : render.getBins()) actual[Math.toIntExact(bin.getStartPx())] = bin.getValue();
              double[] nativeOracle = mode.equals("MAX") ? new double[]{9, 4, 0, 0, 0}
                : mode.equals("MEAN") ? new double[]{20.0 / 3, 4, 0, 0, 0} : new double[]{5, 1, 0, 0, 0};
              assertArrayEquals(nativeOracle, actual, 1e-12, "mode=" + mode);
            }
            var coarse = ready(() -> tracks.queryVisibleTracks(file, 0, pixels, 1, resolution, QueryLengthUnit.PIXELS)
              .getTracks().getFirst());
            assertEquals(expected, coarse.getBins().getFirst().getValue(), 1e-12,
              "state=" + state + " mode=" + mode + " resolution=" + resolution);
          }
          var sequence = tracks.querySequenceTracks(file, null, 0, 17, 17, 4).getTracks().getFirst();
          assertNull(sequence.getError()); assertNull(sequence.getStatusMessage());
          var perBase = new double[17]; Arrays.fill(perBase, Double.NaN);
          for (var bin : sequence.getBins()) for (long p = bin.getStartBp(); p < bin.getEndBp(); p++) perBase[(int) p] = bin.getValue();
          double[] sourceValues = {7, Double.NaN, 9, 4, 4, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
            Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN};
          var oracle = sourceValues.clone();
          if (state == 2) for (int i = 0; i < 17; i++) oracle[i] = sourceValues[16 - i];
          assertArrayEquals(oracle, perBase, 0, "state=" + state + " mode=" + mode);
          var coarseSequence = tracks.querySequenceTracks(file, null, 0, 17, 1, 4).getTracks().getFirst();
          assertEquals(expected, coarseSequence.getBins().getFirst().getValue(), 1e-12);
        }
      }
    }
    assertArrayEquals(original, Files.readAllBytes(path));
  }
}
