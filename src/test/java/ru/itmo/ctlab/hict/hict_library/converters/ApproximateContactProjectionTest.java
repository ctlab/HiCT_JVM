package ru.itmo.ctlab.hict.hict_library.converters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CancellationException;

import static org.junit.jupiter.api.Assertions.*;

class ApproximateContactProjectionTest {
  @TempDir Path directory;

  @Test void everyExactCutAndStrandMatchesIndependentBasePairEndpointOracleAndConservesContacts() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "cuts", List.of(4L, 10L));
    final var unchanged = Files.readAllBytes(path);
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      final var original = file.snapshotAssembly();
      final var contacts = originalContacts(file);
      for (boolean reversed : List.of(false, true)) for (int cut = 1; cut < 17; cut++) {
        file.restoreAssembly(original);
        file.scaffoldingOperations().splitContigAtBp(0, cut);
        if (reversed) file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
        final var records = file.getAgpProcessor().getAssemblyAGPRecords();
        final var before = SessionSnapshotDTO.source(file, "cuts", "");
        for (long resolution : List.of(4L, 10L)) {
          final var result = compareWithOracle(file, records, resolution, contacts.get(resolution));
          assertEquals(22, result, 1e-12, "Every original unordered contact is retained once");
        }
        assertEquals(before, SessionSnapshotDTO.source(file, "cuts", ""), "Projection must not alter active trees or lineage");
      }
    }
    assertArrayEquals(unchanged, Files.readAllBytes(path));
  }

  @Test void gapOffsetsReversalsRepeatedCutsAndOmittedIntervalsUseActualSequenceCoverage() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "gaps", List.of(4L, 10L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      final var contacts = originalContacts(file);
      final var records = parse("scf\t1\t5\t1\tW\ttiny\t1\t5\t+\n"
        + "scf\t6\t8\t2\tN\t3\tscaffold\tyes\tpaired-ends\n"
        + "scf\t9\t20\t3\tW\ttiny\t6\t17\t-\n");
      for (long resolution : List.of(4L, 10L))
        assertEquals(22, compareWithOracle(file, records, resolution, contacts.get(resolution)), 1e-12);
      final var oneBase = parse("one\t1\t1\t1\tW\ttiny\t6\t6\t+\n");
      final var partial = ApproximateContactProjection.prepare(file, oneBase, 10);
      // The first source bin covers ten bases; only one is retained. Its
      // self-contact keeps 1/100 of the count rather than being renormalized.
      assertEquals(0.1, partial.projectBin(0).retainedFraction(), 0);
      final double[] emitted = {0};
      assertEquals(1, partial.projectContact(0, 0, 100, (r, c, v) -> emitted[0] += v, () -> false), 1e-14);
      assertEquals(1, emitted[0], 1e-14);
      compareWithOracle(file, oneBase, 10, contacts.get(10L));
      assertTrue(partial.projectBin(1).contributions().isEmpty());
      // A short terminal bin uses its real seven-base width, not resolution 10.
      assertEquals(7, partial.projectBin(1).sourceLengthBp());
      file.scaffoldingOperations().splitContigAtBp(0, 5);
      file.scaffoldingOperations().splitContigAtBp(file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId(), 3);
      file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
      for (long resolution : List.of(4L, 10L))
        assertEquals(22, compareWithOracle(file, file.getAgpProcessor().getAssemblyAGPRecords(), resolution,
          contacts.get(resolution)), 1e-12);
    }
  }

  @Test void diagonalContactsAllocateBothExchangeableEndpointOrdersAndCancellationStopsDelivery() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "diagonal", List.of(4L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      final var records = parse("a\t1\t2\t1\tW\ttiny\t1\t2\t+\n"
        + "b\t1\t15\t1\tW\ttiny\t3\t17\t+\n");
      final var projection = ApproximateContactProjection.prepare(file, records, 4);
      final var values = new TreeMap<String, Double>();
      projection.projectContact(0, 0, 8, (r, c, v) -> values.merge(r + ":" + c, v, Double::sum), () -> false);
      assertEquals(Map.of("0:0", 2.0, "0:1", 4.0, "1:1", 2.0), values);
      assertThrows(CancellationException.class, () -> projection.projectContact(0, 0, 8,
        (r, c, v) -> fail("Cancelled projection must not deliver a value"), () -> true));
      assertThrows(IllegalArgumentException.class, () -> projection.projectContact(0, 0, Double.NaN,
        (r, c, v) -> fail(), () -> false));
      assertThrows(UnsupportedOperationException.class, () -> projection.projectBin(0).contributions().clear());
      assertThrows(IllegalArgumentException.class, () -> projection.projectBin(-1));
      assertThrows(IllegalArgumentException.class, () -> projection.projectBin(5));
    }
  }

  @Test void rejectsAmbiguousCopiedSourceIntervalsUnknownStrandsAndOutOfRangeComponents() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, "invalid", List.of(4L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      for (var agp : List.of(
        "a\t1\t5\t1\tW\ttiny\t1\t5\t+\nb\t1\t17\t1\tW\ttiny\t1\t17\t+\n",
        "a\t1\t17\t1\tW\ttiny\t1\t17\t?\n",
        "a\t1\t18\t1\tW\ttiny\t1\t18\t+\n"))
        assertThrows(IllegalArgumentException.class, () -> ApproximateContactProjection.prepare(file, parse(agp), 4));
      assertThrows(IllegalArgumentException.class, () -> ApproximateContactProjection.prepare(file,
        parse("a\t1\t17\t1\tW\ttiny\t1\t17\t+\n"), 3));
    }
  }

  @Test void physicalSourceIdentityAcrossStripesAndShortTerminalBinsMatchesBaseEndpointOracle() throws Exception {
    final int length = 2057;
    final var path = ReinforcementHdf5Fixture.create(directory, "stripes", List.of(4L), length,
      new long[][]{{1,1025,5},{1027,2056,7},{2056,2056,11}});
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      final var resolution = ResolutionDescriptor.fromBpResolution(4, file);
      assertTrue(file.getStripeCount()[resolution.getResolutionOrderInArray()] > 1);
      file.scaffoldingOperations().splitContigAtBp(0,1025);
      file.scaffoldingOperations().splitContigAtBp(file.getAssemblyInfo().contigs().getLast().descriptor().getContigId(),1);
      file.scaffoldingOperations().reverseSelectionRangeBp(0,length);
      final var records = file.getAgpProcessor().getAssemblyAGPRecords();
      final var projection = ApproximateContactProjection.prepare(file,records,4);
      final var target = new long[length];
      for (var record : records) if (record instanceof AGPProcessor.ContigAGPRecord c) {
        final long offset = projection.chromosomes().stream().filter(ch -> ch.name().equals(c.getScaffoldName())).findFirst().orElseThrow().firstBin();
        for (int bp = (int)c.getIntraContigStartBpIncl()-1; bp < c.getIntraContigEndBpIncl(); bp++)
          target[bp] = offset + (c.getInterScaffoldStartIncl()-1 + c.getIntraContigEndBpIncl()-bp-1)/4;
      }
      final var expected = new HashMap<String,Double>(); final var actual = new HashMap<String,Double>();
      file.matrixQueries().scanStoredContacts(resolution,(r,c,value)->{
        projection.projectContact(r,c,value,(a,b,v)->actual.merge(a+":"+b,v,Double::sum),()->false);
        int a=(int)r*4,b=Math.min(length,a+4),x=(int)c*4,y=Math.min(length,x+4);
        for(int i=a;i<b;i++)for(int j=x;j<y;j++) expected.merge(
          Math.min(target[i],target[j])+":"+Math.max(target[i],target[j]),value/((b-a)*(double)(y-x)),Double::sum);
      },()->false,null);
      assertEquals(expected.keySet(),actual.keySet());
      expected.forEach((key,value)->assertEquals(value,actual.get(key),1e-12,key));
      assertEquals(23,actual.values().stream().mapToDouble(Double::doubleValue).sum(),1e-12);
      assertEquals(1,projection.projectBin(514).sourceLengthBp());
    }
  }

  private static Map<Long, double[][]> originalContacts(ChunkedFile file) {
    final var result = new HashMap<Long, double[][]>();
    for (long resolution : file.getResolutions()) {
      if (resolution == 0) continue;
      final int bins = (int) Math.ceilDiv(17, resolution);
      final var matrix = file.matrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(resolution, file),
        0, 0, bins, bins, false).matrix();
      final var values = new double[bins][bins];
      for (int r = 0; r < bins; r++) for (int c = 0; c < bins; c++) values[r][c] = matrix.getAsDouble(r, c);
      result.put(resolution, values);
    }
    return result;
  }

  /** Enumerates all ordered base endpoint positions independently of ATUs or projection runs. */
  private static double compareWithOracle(ChunkedFile file, List<AGPProcessor.AGPFileRecord> records,
      long resolution, double[][] original) {
    final var projection = ApproximateContactProjection.prepare(file, records, resolution);
    final int bins = Math.toIntExact(projection.targetBinCount());
    final var actual = new double[bins][bins];
    final var expected = new double[bins][bins];
    final int[] targetAtBase = new int[17]; Arrays.fill(targetAtBase, -1);
    for (var record : records) if (record instanceof AGPProcessor.ContigAGPRecord component) {
      final long chromOffset = projection.chromosomes().stream().filter(c -> c.name().equals(component.getScaffoldName()))
        .findFirst().orElseThrow().firstBin();
      for (int bp = (int) component.getIntraContigStartBpIncl() - 1; bp < component.getIntraContigEndBpIncl(); bp++) {
        long local = component.getContigOrientation() == AGPProcessor.AGPContigOrientation.MINUS
          ? component.getIntraContigEndBpIncl() - bp - 1 : bp - component.getIntraContigStartBpIncl() + 1;
        targetAtBase[bp] = Math.toIntExact(chromOffset + (component.getInterScaffoldStartIncl() - 1 + local) / resolution);
      }
    }
    double retained = 0;
    for (int r = 0; r < original.length; r++) for (int c = r; c < original.length; c++) {
      retained += projection.projectContact(r, c, original[r][c], (row, col, value) -> actual[(int) row][(int) col] += value, () -> false);
      final int a = Math.toIntExact(r * resolution), b = (int) Math.min(17, (r + 1) * resolution);
      final int x = Math.toIntExact(c * resolution), y = (int) Math.min(17, (c + 1) * resolution);
      final double value = original[r][c] / ((b - a) * (double) (y - x));
      for (int i = a; i < b; i++) for (int j = x; j < y; j++) {
        final int ti = targetAtBase[i], tj = targetAtBase[j];
        if (ti >= 0 && tj >= 0) expected[Math.min(ti, tj)][Math.max(ti, tj)] += value;
      }
    }
    double sum = 0;
    for (int r = 0; r < bins; r++) for (int c = 0; c < bins; c++) {
      assertEquals(expected[r][c], actual[r][c], 1e-12, "target cell " + r + "," + c + " at " + resolution);
      sum += actual[r][c];
    }
    assertEquals(retained, sum, 1e-12);
    return sum;
  }

  private static List<AGPProcessor.AGPFileRecord> parse(String agp) throws Exception {
    return AGPProcessor.parseRecordsFromReader(new StringReader(agp));
  }
}
