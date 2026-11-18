package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.CompactAssemblyInfoDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.SessionAssemblyRestorer;

import java.io.StringReader;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LongSourceOffsetTest {
  @TempDir Path directory;
  private static final long LENGTH = 3_000_000_017L, CUT = 2_500_000_001L;

  @Test void nestedExactCutsBeyondIntCoordinatesRetainIntervalsAndReplayWithoutTruncation() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "long", List.of(1_000_000_000L, 2_000_000_000L),
      LENGTH, new long[][]{{0, 0, 1}, {CUT, LENGTH - 1, 7}, {LENGTH - 1, LENGTH - 1, 11}});
    final var options = new ChunkedFile.ChunkedFileOptions(path, 1, 2);
    try (var file = new ChunkedFile(options); var restored = new ChunkedFile(options)) {
      file.scaffoldingOperations().splitContigAtBp(0, CUT);
      final int right = file.getAssemblyInfo().contigs().getLast().descriptor().getContigId();
      file.scaffoldingOperations().splitContigAtBp(right, 1);
      assertEquals(CUT, file.getSplitEdits().getLast().sourceOffsetBp());
      assertEquals(List.of(0L, CUT, CUT + 1), file.getAssemblyInfo().contigs().stream()
        .map(t -> t.descriptor().getOffsetInSourceFASTA()).toList());
      final var snapshot = AssemblySequenceSnapshot.capture(file);
      assertEquals(LENGTH, snapshot.length());
      assertEquals(new AssemblySequenceSnapshot.Fragment("tiny", CUT, CUT + 1, false), snapshot.slice(CUT, CUT + 1).fragments().getFirst());
      var compact = ((CompactAssemblyInfoDTO.Response) CompactAssemblyInfoDTO.response(file, 0, "compact-v1")).assemblyInfo();
      assertEquals(CUT + 1, ((Number) compact.contigs().getLast()[4]).longValue());
      file.scaffoldingOperations().reverseSelectionRangeBp(0, LENGTH);
      final var saved = SessionSnapshotDTO.source(file, "long", "");
      SessionAssemblyRestorer.restore(restored, saved);
      SessionAssemblyRestorer.verifyContactBinMapping(file, restored);
      assertEquals(saved, SessionSnapshotDTO.source(restored, "long", ""));
      assertEquals(CUT + 1, saved.getJsonObject("assemblyInfo").getJsonArray("contigDescriptors")
        .getJsonObject(0).getLong("contigOffsetInSource"));
    }
  }

  @Test void legacyAgpSourceBinOwnershipSupportsLongComponentOffsets() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "agp", List.of(1_000_000_000L),
      LENGTH, new long[][]{{LENGTH - 1, LENGTH - 1, 11}});
    final var options = new ChunkedFile.ChunkedFileOptions(path, 1, 2);
    try (var file = new ChunkedFile(options); var restored = new ChunkedFile(options)) {
      final long length = LENGTH - CUT;
      file.importAGP(new StringReader("scf\t1\t" + length + "\t1\tW\ttiny\t" + (CUT + 1) + "\t" + LENGTH + "\t+\n"),
        AGPImportPlan.MissingComponentsPolicy.EXCLUDE);
      assertEquals(CUT, file.getAssemblyInfo().contigs().getFirst().descriptor().getOffsetInSourceFASTA());
      final var saved = SessionSnapshotDTO.source(file, "agp", "");
      SessionAssemblyRestorer.restore(restored, saved);
      SessionAssemblyRestorer.verifyContactBinMapping(file, restored);
      assertEquals(saved, SessionSnapshotDTO.source(restored, "agp", ""));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void historicalBinRemovingCutsKeepLongSourceOffsetsAndReplay(boolean reversed) throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "legacy", List.of(1_000_000_000L),
      LENGTH, new long[][]{{LENGTH - 1, LENGTH - 1, 11}});
    final var options = new ChunkedFile.ChunkedFileOptions(path, 1, 2);
    try (var file = new ChunkedFile(options); var restored = new ChunkedFile(options)) {
      if (reversed) file.scaffoldingOperations().reverseSelectionRangeBp(0, LENGTH);
      file.scaffoldingOperations().splitContigAtBin(reversed ? 1_000_000_000L : 2_000_000_000L,
        ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      assertEquals(3_000_000_000L, file.getAssemblyInfo().contigs().stream()
        .mapToLong(t -> t.descriptor().getOffsetInSourceFASTA()).max().orElseThrow());
      assertEquals(LENGTH - 1_000_000_000L, AssemblySequenceSnapshot.capture(file).length());
      final var saved = SessionSnapshotDTO.source(file, "legacy", "");
      SessionAssemblyRestorer.restore(restored, saved);
      SessionAssemblyRestorer.verifyContactBinMapping(file, restored);
      assertEquals(saved, SessionSnapshotDTO.source(restored, "legacy", ""));
    }
  }
}
