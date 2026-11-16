package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.SessionAssemblyRestorer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ExactContigSplitTest {
  @TempDir Path directory;
  private static final String BASES = "ACGTRYMKSWBDHVNAC";

  @Test void allInteriorCutsPreserveSequenceAndIntersectingPhysicalBinsOnBothStrands() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "map", List.of(4L, 10L));
    final byte[] sourceBytes = Files.readAllBytes(path);
    final var fasta = directory.resolve("tiny.fa");
    Files.writeString(fasta, ">tiny\n" + BASES + "\n");
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      final var initial = file.snapshotAssembly();
      final var original = file.getAssemblyInfo().contigs().getFirst().descriptor();
      for (boolean reversed : new boolean[]{false, true}) {
        for (int cut = 1; cut < BASES.length(); cut++) {
          file.restoreAssembly(initial);
          if (reversed) file.scaffoldingOperations().reverseSelectionRangeBp(0, BASES.length());
          final String sequence = file.getFastaProcessor().readRegion(fasta, 0, BASES.length());
          file.scaffoldingOperations().splitContigAtBp(original.getContigId(), cut);
          final var children = file.getAssemblyInfo().contigs();
          assertEquals(2, children.size());
          assertEquals(cut, children.getFirst().descriptor().getLengthBp());
          assertEquals(BASES.length() - cut, children.getLast().descriptor().getLengthBp());
          assertEquals(sequence, file.getFastaProcessor().readRegion(fasta, 0, BASES.length()));
          assertEquals(BASES.length(), file.getContigTree().getLengthInUnits(QueryLengthUnit.BASE_PAIRS,
            ResolutionDescriptor.fromResolutionOrder(0)));
          for (int order = 1; order < file.getResolutions().length; order++) {
            final long resolution = file.getResolutions()[order];
            final var originalBins = expand(original.getAtus().get(order));
            final var retained = new HashSet<String>();
            for (var tuple : children) {
              final var d = tuple.descriptor();
              final int from = (int) (d.getOffsetInSourceFASTA() / resolution);
              final int to = (int) Math.ceilDiv(d.getOffsetInSourceFASTA() + d.getLengthBp(), resolution);
              final var actual = expand(d.getAtus().get(order));
              assertEquals(originalBins.subList(from, to), actual);
              assertEquals(actual.size(), d.getLengthBinsAtResolution()[order]);
              assertEquals(ContigHideType.SHOWN, d.getPresenceAtResolution().get(order));
              retained.addAll(actual);
              assertEquals(from * resolution - d.getOffsetInSourceFASTA(), file.firstOwnedBinOffset(d, order));
            }
            assertEquals(new HashSet<>(originalBins), retained, "Every source bin is still referenced");
            final int shared = reversed ? BASES.length() - cut : cut;
            final int references = children.stream().mapToInt(t -> expand(t.descriptor().getAtus().get(
              ResolutionDescriptor.fromBpResolution(resolution, file).getResolutionOrderInArray())).size()).sum();
            assertEquals(originalBins.size() + (shared % resolution == 0 ? 0 : 1), references);
          }
          assertTrue(file.getSplitEdits().getFirst().exact());
          // Both children are separate scaffolds: the cut adds no AGP N gap.
          assertTrue(file.getAgpProcessor().getAssemblyAGPRecords().stream()
            .allMatch(r -> r instanceof AGPProcessor.ContigAGPRecord));
        }
      }
    }
    assertArrayEquals(sourceBytes, Files.readAllBytes(path), "Editing must not rewrite contact blocks");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void repeatedCutsOfOneBinRestoreWithExactNativeLineageAndUndoRedo(boolean reversed) throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "map", List.of(4L, 10L));
    final var options = new ChunkedFile.ChunkedFileOptions(path, 1, 2);
    try (var file = new ChunkedFile(options); var restored = new ChunkedFile(options)) {
      final var before = file.snapshotAssembly();
      if (reversed) file.scaffoldingOperations().reverseSelectionRangeBp(0, BASES.length());
      file.scaffoldingOperations().splitContigAtBp(0, 5);
      final int left = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.scaffoldingOperations().splitContigAtBp(left, 3);
      file.scaffoldingOperations().reverseSelectionRangeBp(0, BASES.length());
      final var edited = file.snapshotAssembly();
      final var saved = SessionSnapshotDTO.source(file, "map", "");
      SessionAssemblyRestorer.restore(restored, saved);
      SessionAssemblyRestorer.verifyContactBinMapping(file, restored);
      assertEquals(saved, SessionSnapshotDTO.source(restored, "map", ""));
      file.restoreAssembly(before);
      assertTrue(file.getSplitEdits().isEmpty());
      file.restoreAssembly(edited);
      assertEquals(saved, SessionSnapshotDTO.source(file, "map", ""));
      for (long invalid : new long[]{-1, 0, Long.MAX_VALUE}) {
        assertThrows(IllegalArgumentException.class, () -> file.scaffoldingOperations().splitContigAtBp(left, invalid));
        assertEquals(saved, SessionSnapshotDTO.source(file, "map", ""));
      }
      final int current = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      final long end = file.getAssemblyInfo().contigs().getFirst().descriptor().getLengthBp();
      assertThrows(IllegalArgumentException.class, () -> file.scaffoldingOperations().splitContigAtBp(current, end));
      assertThrows(IllegalArgumentException.class, () -> file.scaffoldingOperations().splitContigAtBp(Integer.MAX_VALUE, 1));
      assertEquals(saved, SessionSnapshotDTO.source(file, "map", ""));
    }
  }

  private static List<String> expand(List<ATUDescriptor> atus) {
    final var result = new ArrayList<String>();
    for (var atu : atus) {
      for (int i = 0; i < atu.getLength(); i++) {
        final int bin = atu.getDirection() == ATUDirection.FORWARD
          ? atu.getStartIndexInStripeIncl() + i : atu.getEndIndexInStripeExcl() - 1 - i;
        result.add(atu.getStripeDescriptor().stripeId() + ":" + bin + ":" + atu.getDirection());
      }
    }
    return result;
  }
}
