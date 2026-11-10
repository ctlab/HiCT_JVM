package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class SessionAssemblyRestorerTest {
  @TempDir Path directory;

  @Test void restoresEditedLayoutNamesAndSpacerMetadataWithoutChangingSourceFile() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "map", List.of(4L, 10L));
    try (var live = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var candidate = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      live.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      live.setContigNameOverride(0, "renamed_tiny");
      var scaffold = live.getAssemblyInfo().scaffolds().getFirst().scaffoldDescriptor();
      live.setScaffoldNameOverride(scaffold.scaffoldId(), "renamed_scaffold");
      var saved = SessionSnapshotDTO.source(live, "map.hict.hdf5", "tiny.fa");
      SessionAssemblyRestorer.restore(candidate, saved);
      SessionAssemblyRestorer.verifyContactBinMapping(live, candidate);
      assertEquals(saved, SessionSnapshotDTO.source(candidate, "map.hict.hdf5", "tiny.fa"));
      var resolution = ResolutionDescriptor.fromBpResolution(4, candidate);
      assertEquals(11, candidate.getMatrixQueries().getSubmatrix(resolution, 0, 0, 1, 1, false).matrix().getAsDouble(0, 0));
      assertEquals(saved, SessionSnapshotDTO.source(live, "map.hict.hdf5", "tiny.fa"));
      candidate.getContigTree().mapDescriptors(d -> {
        var atus = new java.util.ArrayList<>(d.getAtus().subList(1, d.getAtus().size()));
        atus.set(0, atus.getFirst().stream().map(ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor::reversed).toList());
        return new ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor(d.getContigId(), d.getContigName(),
          d.getLengthBp(), java.util.Arrays.stream(d.getLengthBinsAtResolution()).skip(1).boxed().toList(),
          d.getPresenceAtResolution().subList(1, d.getPresenceAtResolution().size()), atus,
          d.getContigNameInSourceFASTA(), d.getOffsetInSourceFASTA());
      });
      assertThrows(IllegalArgumentException.class, () -> SessionAssemblyRestorer.verifyContactBinMapping(live, candidate));
    }
  }

  @Test void restoresSplitReverseComponentsAndAnnotatedGap() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "map", List.of(4L, 10L));
    try (var live = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var candidate = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      live.importAGP(new java.io.StringReader("scf\t1\t6\t1\tW\ttiny\t1\t6\t+\n"
        + "scf\t7\t11\t2\tN\t5\tscaffold\tyes\tpaired-ends\n"
        + "scf\t12\t22\t3\tW\ttiny\t7\t17\t-\n"));
      var saved = SessionSnapshotDTO.source(live, "map.hict.hdf5", "");
      SessionAssemblyRestorer.restore(candidate, saved);
      SessionAssemblyRestorer.verifyContactBinMapping(live, candidate);
      assertEquals(saved, SessionSnapshotDTO.source(candidate, "map.hict.hdf5", ""));
      for (long bp : List.of(4L, 10L)) {
        var liveResolution = ResolutionDescriptor.fromBpResolution(bp, live);
        var candidateResolution = ResolutionDescriptor.fromBpResolution(bp, candidate);
        int bins = Math.toIntExact(live.getContigTree().getLengthInUnits(
          ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit.BINS, liveResolution));
        var expected = live.getMatrixQueries().getSubmatrix(liveResolution, 0, 0, bins, bins, false).matrix();
        var actual = candidate.getMatrixQueries().getSubmatrix(candidateResolution, 0, 0, bins, bins, false).matrix();
        for (int row=0; row<bins; row++) for (int col=0; col<bins; col++)
          assertEquals(expected.getAsDouble(row,col), actual.getAsDouble(row,col));
      }
    }
  }

  @Test void rejectsTamperedHashAndLayoutAndRollsBackCandidate() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "map", List.of(4L, 10L));
    try (var live = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
         var candidate = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      var before = SessionSnapshotDTO.source(candidate, "", "");
      live.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      var saved = SessionSnapshotDTO.source(live, "", "");
      var badHash = saved.copy().put("agpSha256", "0".repeat(64));
      assertThrows(IllegalArgumentException.class, () -> SessionAssemblyRestorer.restore(candidate, badHash));
      assertEquals(before, SessionSnapshotDTO.source(candidate, "", ""));
      var badLayout = saved.copy();
      badLayout.getJsonObject("assemblyInfo").getJsonArray("contigDescriptors").getJsonObject(0).put("contigDirection", 1);
      assertThrows(IllegalArgumentException.class, () -> SessionAssemblyRestorer.restore(candidate, badLayout));
      assertEquals(before, SessionSnapshotDTO.source(candidate, "", ""));
      var badName = saved.copy();
      badName.getJsonObject("assemblyInfo").getJsonArray("contigDescriptors").getJsonObject(0).put("contigName", "different");
      assertThrows(IllegalArgumentException.class, () -> SessionAssemblyRestorer.restore(candidate, badName));
      assertEquals(before, SessionSnapshotDTO.source(candidate, "", ""));
    }
  }
}
