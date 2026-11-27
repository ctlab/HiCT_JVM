package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.assembly.AssemblySequenceSnapshot;
import ru.itmo.ctlab.hict.hict_server.dto.request.sequence.SequenceAnalysisBudgetRequestDTO;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SequenceAnalysisResourceBudgetTest {
  private static final List<Long> BORRELIA_LENGTHS = List.of(910_000L, 540_000L);
  private static final long BORRELIA_FASTA_BYTES = 1_470_000L;
  private static final long GIB = 1L << 30;

  @Test
  void sparseEstimatesStayWithinMeasuredBorreliaBounds() {
    checkEstimate(List.of(1_000L), List.of(4_000L, 16_000L, 64_000L), true, 596_000L);
    checkEstimate(List.of(250L, 1_000L, 10_000_000L), List.of(4_000L, 16_000L, 64_000L, 256_000L), true, 749_000L);
    checkEstimate(List.of(2_000L), List.of(8_000L, 32_000L, 128_000L), false, 598_000L);
  }

  private static void checkEstimate(List<Long> requested, List<Long> overviews, boolean alignment, long measured) {
    var estimate = SequenceAnalysisHandlersHolder.estimateFastaLengths(BORRELIA_LENGTHS, requested, overviews,
      BORRELIA_FASTA_BYTES, alignment, alignment ? 256 : 8256, 14 * GIB);
    var payload = estimate.toJson();
    assertEquals(requested, payload.getJsonArray("requestedLevels").getList());
    assertEquals(overviews, payload.getJsonArray("overviewLevels").getList());
    assertEquals(estimate.expectedBytes(), payload.getLong("estimatedBytes"));
    assertEquals(estimate.expectedBytes(), payload.getLong("expectedBytes"));
    assertEquals(estimate.worstCaseBytes(), payload.getLong("worstCaseBytes"));
    assertTrue(measured <= estimate.expectedBytes() && estimate.expectedBytes() <= 50 * measured);
  }

  @Test
  void softLimitRequiresOptInAndCannotOverrideHardLimits() {
    var soft = SequenceAnalysisHandlersHolder.estimateFastaLengths(BORRELIA_LENGTHS, List.of(100L), List.of(),
      BORRELIA_FASTA_BYTES, true, 256, 14 * GIB);
    assertEquals(100L, soft.warningLevel());
    assertNull(soft.limitingLevel());
    var defaultOptions = SequenceAnalysisBudgetRequestDTO.fromJson(new JsonObject());
    assertEquals(413, assertThrows(HttpException.class, () ->
      SequenceAnalysisHandlersHolder.requireOverlayBudget(soft, defaultOptions.acceptLargeOutput())).getStatusCode());
    var accepted = SequenceAnalysisBudgetRequestDTO.fromJson(new JsonObject().put("acceptLargeOutput", true));
    assertDoesNotThrow(() -> SequenceAnalysisHandlersHolder.requireOverlayBudget(soft, accepted.acceptLargeOutput()));

    var disk = SequenceAnalysisHandlersHolder.estimateFastaLengths(BORRELIA_LENGTHS, List.of(100L), List.of(),
      BORRELIA_FASTA_BYTES, true, 256, GIB);
    assertEquals(100L, disk.limitingLevel());
    var stripes = SequenceAnalysisHandlersHolder.estimateFastaLengths(List.of(300_000_000L), List.of(1L), List.of(),
      300_000_000L, true, 256, 14 * GIB);
    assertTrue(stripes.toJson().getString("hardLimitReason").contains("stripe limit"));
    for (var hard : List.of(disk, stripes)) {
      for (boolean accept : List.of(false, true))
        assertEquals(413, assertThrows(HttpException.class, () ->
          SequenceAnalysisHandlersHolder.requireOverlayBudget(hard, accept)).getStatusCode());
    }
    assertThrows(IllegalArgumentException.class, () ->
      SequenceAnalysisBudgetRequestDTO.fromJson(new JsonObject().put("acceptLargeOutput", "true")));
  }

  @Test
  void lowDiskFinishesAsFailedAndRemovesPendingOutput(@TempDir Path directory) throws Exception {
    var job = new SequenceAnalysisHandlersHolder.Job("low-disk", directory, directory,
      new AssemblySequenceSnapshot(List.of()), new JsonObject(), "", "dotplot");
    var partial = Files.writeString(directory.resolve("dotplot.hict.hdf5.pending"), "partial");
    var scratch = Files.writeString(directory.resolve("alignment.paf"), "partial");
    var fasta = Files.writeString(directory.resolve("input.fa"), ">a\nACGT\n");
    var stamp = SequenceAnalysisHandlersHolder.Stamp.read(fasta);
    try (var holder = new SequenceAnalysisHandlersHolder(null, path -> GIB - 1)) {
      holder.runOverlay(job, null, fasta, stamp, fasta, stamp, null, null, null, null, new JsonObject(), 0);
    }
    var status = job.statusJson();
    assertEquals("failed", status.getString("status"));
    assertEquals(SequenceAnalysisHandlersHolder.LOW_DISK_ERROR, status.getString("error"));
    assertNotNull(status.getLong("finishedAtMs"));
    assertFalse(Files.exists(partial));
    assertFalse(Files.exists(scratch));
    assertNull(status.getString("outputFile"));
  }
}
