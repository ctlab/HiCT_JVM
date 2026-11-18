package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.*;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SourceBinProvenanceTest {
  @TempDir Path directory;

  @Test void exactCutsAndReversalIdentifyRepeatedOriginalBinsWithoutInventingObservations() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "provenance", List.of(4L, 10L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      final var resolution = ResolutionDescriptor.fromBpResolution(4, file);
      final var queries = file.matrixQueries();
      final var original = queries.getSubmatrix(resolution, 0, 0, 5, 5, true).matrix();
      file.scaffoldingOperations().splitContigAtBp(0, 5);
      final int left = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.scaffoldingOperations().splitContigAtBp(left, 1);
      assertEquals(List.of(0, 0, 1, 1, 2, 3, 4), bins(queries.sourceBinProvenance(resolution, 0, 7, true)));
      file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
      final var provenance = queries.sourceBinProvenance(resolution, 0, 7, true);
      assertEquals(7, provenance.referencedBins());
      assertEquals(5, provenance.uniqueSourceBins());
      assertEquals(List.of(4, 3, 2, 1, 1, 0, 0), bins(provenance));
      final var displayed = queries.getSubmatrix(resolution, 0, 0, 7, 7, true).matrix();
      final var identities = bins(provenance);
      final var support = provenance.referenceMultiplicities();
      assertArrayEquals(new int[]{1,1,1,2,2,2,2}, support);
      double originalTotal = 0, weightedTotal = 0, supportMass = 0;
      for (int r = 0; r < 5; r++) for (int c = 0; c < 5; c++) originalTotal += original.getAsDouble(r,c);
      for (int r = 0; r < 7; r++) for (int c = 0; c < 7; c++)
        assertEquals(original.getAsDouble(identities.get(r), identities.get(c)), displayed.getAsDouble(r, c));
      for (int r = 0; r < 7; r++) for (int c = 0; c < 7; c++) {
        supportMass += 1.0 / support[r] / support[c];
        weightedTotal += displayed.getAsDouble(r,c) / support[r] / support[c];
      }
      assertEquals(25, supportMass, 1e-12, "Five unique bins produce 25 ordered physical pairs");
      assertEquals(originalTotal, weightedTotal, 1e-12, "Repeated references add no observations");
      assertEquals(List.of(2, 1, 1, 0), bins(queries.sourceBinProvenance(resolution, 2, 6, true)));
      final var padded = queries.sourceBinProvenance(resolution, -2, 9, true);
      assertEquals(11, padded.axisLength());
      assertEquals(2, padded.runs().getFirst().offset());
      assertEquals(5, padded.uniqueSourceBins());
      assertArrayEquals(new int[]{0,0,1,1,1,2,2,2,2,0,0}, padded.referenceMultiplicities());
      assertTrue(queries.sourceBinProvenance(resolution, -7, -2, true).runs().isEmpty());
      assertTrue(queries.sourceBinProvenance(resolution, 7, 9, true).runs().isEmpty());
      assertEquals(0, queries.sourceBinProvenance(resolution, 3, 3, true).axisLength());
      // Hidden fragments disappear only from PIXELS, not from BINS.
      final var d = file.getAssemblyInfo().contigs().getFirst().descriptor();
      file.getContigTree().mapDescriptors(cd -> cd.getContigId() != d.getContigId() ? cd : new ContigDescriptor(
        cd.getContigId(), cd.getContigName(), cd.getLengthBp(),
        Arrays.stream(cd.getLengthBinsAtResolution()).skip(1).boxed().toList(),
        List.of(ContigHideType.HIDDEN, ContigHideType.HIDDEN), cd.getAtus().subList(1, cd.getAtus().size()),
        cd.getContigNameInSourceFASTA(), cd.getOffsetInSourceFASTA(), cd.isSharedBinSlice()));
      assertEquals(List.of(1, 0, 0), bins(queries.sourceBinProvenance(resolution, 0, 3, true)));
      assertEquals(identities, bins(queries.sourceBinProvenance(resolution, 0, 7, false)));
    }
  }

  @Test void uniqueIdentityIncludesStripeAndCountsOverlappingIntervalsOnce() {
    final var first = new StripeDescriptor(0, 10, new double[10]);
    final var second = new StripeDescriptor(1, 10, new double[10]);
    final var provenance = SourceBinProvenance.fromATUs(List.of(
      new ATUDescriptor(first, 2, 6, ATUDirection.FORWARD),
      new ATUDescriptor(first, 4, 8, ATUDirection.REVERSED),
      new ATUDescriptor(second, 2, 6, ATUDirection.FORWARD)), 12, 0);
    assertEquals(12, provenance.referencedBins());
    assertEquals(10, provenance.uniqueSourceBins());
    assertArrayEquals(new int[]{1,1,2,2,1,1,2,2,1,1,1,1}, provenance.referenceMultiplicities());
    assertThrows(UnsupportedOperationException.class, () -> provenance.runs().clear());
    assertThrows(IllegalArgumentException.class, () -> SourceBinProvenance.fromATUs(
      List.of(new ATUDescriptor(first, 2, 6, ATUDirection.FORWARD)), 3, 0));
  }

  @Test void multiplicityDoesNotAllocateByPhysicalStripeSize() {
    final var stripe = new StripeDescriptor(4, 2_000_000_000, new double[0]);
    final var axis = SourceBinProvenance.fromATUs(List.of(
      new ATUDescriptor(stripe, 1_999_999_990, 1_999_999_995, ATUDirection.FORWARD),
      new ATUDescriptor(stripe, 1_999_999_992, 1_999_999_997, ATUDirection.REVERSED)), 10, 0);
    assertArrayEquals(new int[]{1,1,2,2,2,1,1,2,2,2}, axis.referenceMultiplicities());
    assertEquals(7, Arrays.stream(axis.referenceMultiplicities()).mapToDouble(n -> 1.0/n).sum(), 0);
  }

  private static List<Integer> bins(SourceBinProvenance provenance) {
    final var bins = new ArrayList<Integer>();
    for (var run : provenance.runs()) for (int i = 0; i < run.length(); i++) {
      assertEquals(0, run.stripeId(), "single-stripe fixture");
      bins.add(run.firstBinInStripe() + i * run.step());
    }
    return bins;
  }
}
