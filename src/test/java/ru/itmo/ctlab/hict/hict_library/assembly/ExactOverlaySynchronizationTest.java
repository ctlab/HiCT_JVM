package ru.itmo.ctlab.hict.hict_library.assembly;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.*;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.*;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExactOverlaySynchronizationTest {
  @TempDir Path directory;

  @Test void lateAttachmentAndFurtherEditsRetainSharedBinsAcrossIndependentPyramidsAndSessions() throws Exception {
    final var primaryPath = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    final var secondaryPath = ReinforcementHdf5Fixture.create(directory, "secondary", List.of(3L, 7L));
    final var vertx = Vertx.vertx();
    try (var primary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(primaryPath, 1, 2));
         var secondary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(secondaryPath, 1, 2));
         var restored = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(secondaryPath, 1, 2))) {
      primary.scaffoldingOperations().splitContigAtBp(0, 5);
      final var map = vertx.sharedData().<String, Object>getLocalMap("hict_server");
      map.put("chunkedFile", new ShareableWrappers.ChunkedFileWrapper(primary));
      map.put("chunkedFileSecondary", new ShareableWrappers.ChunkedFileWrapper(secondary));
      map.put("assemblyInfoSource", "PRIMARY");
      FileOpHandlersHolder.synchronizeOverlayAssemblyForSharedState(map);
      assertIntersections(secondary);
      final int left = primary.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      primary.scaffoldingOperations().splitContigAtBp(left, 1);
      primary.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
      FileOpHandlersHolder.synchronizeOverlayAssemblyForSharedState(map);
      assertIntersections(secondary);
      assertTrue(secondary.getSplitEdits().isEmpty(), "Projected layout does not invent a target edit history");
      final var saved = SessionSnapshotDTO.source(secondary, "secondary", "");
      SessionAssemblyRestorer.restore(restored, saved);
      SessionAssemblyRestorer.verifyContactBinMapping(secondary, restored);
      assertEquals(saved, SessionSnapshotDTO.source(restored, "secondary", ""));
      // Switching authority to the projected source must keep the same policy.
      map.put("assemblyInfoSource", "SECONDARY");
      FileOpHandlersHolder.synchronizeOverlayAssemblyForSharedState(map);
      assertIntersections(primary);
    } finally { vertx.close().toCompletionStage().toCompletableFuture().get(); }
  }

  @Test void coveragePreparationUsesUniqueStoredContactsBeforeAndAfterExactCuts() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "coverage", List.of(4L, 10L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      final var before = CoverageWeightsBuilder.build(file, 1L << 20, () -> false, null);
      file.scaffoldingOperations().splitContigAtBp(0, 5);
      final int left = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.scaffoldingOperations().splitContigAtBp(left, 1);
      final var after = CoverageWeightsBuilder.build(file, 1L << 20, () -> false, null);
      assertArrayEquals(before.validBinsByResolution(), after.validBinsByResolution());
      for (int i = 1; i < file.getResolutions().length; i++)
        assertArrayEquals(before.weightsByResolution()[i], after.weightsByResolution()[i], 0);
    }
  }

  private static void assertIntersections(ChunkedFile file) {
    long length = 0;
    for (var tuple : file.getAssemblyInfo().contigs()) {
      final var d = tuple.descriptor();
      length += d.getLengthBp();
      assertTrue(d.isSharedBinSlice());
      for (int order = 1; order < file.getResolutions().length; order++) {
        final long resolution = file.getResolutions()[order];
        final long first = d.getOffsetInSourceFASTA() / resolution;
        final long last = Math.ceilDiv(d.getOffsetInSourceFASTA() + d.getLengthBp(), resolution);
        assertEquals(last - first, d.getLengthBinsAtResolution()[order]);
        assertEquals(first * resolution - d.getOffsetInSourceFASTA(), file.firstOwnedBinOffset(d, order));
        assertEquals(ContigHideType.SHOWN, d.getPresenceAtResolution().get(order));
      }
    }
    assertEquals(17, length);
  }
}
