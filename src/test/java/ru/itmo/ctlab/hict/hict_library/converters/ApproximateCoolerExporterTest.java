package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

import java.io.StringReader;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ApproximateCoolerExporterTest {
  @TempDir Path directory;
  private static final String AGP = "scf\t1\t5\t1\tW\ttiny\t1\t5\t+\n"
    + "scf\t6\t13\t2\tN\t8\tscaffold\tyes\tpaired-ends\n"
    + "scf\t14\t25\t3\tW\ttiny\t6\t17\t-\n";

  @Test void writesLabeledFractionalMcoolWithCorrectIndexesGapCoverageAndConservedContacts() throws Exception {
    final var input = ReinforcementHdf5Fixture.create(directory, "source", List.of(4L, 10L));
    final var untouched = Files.readAllBytes(input);
    final var output = directory.resolve("approximate.mcool");
    final var records = AGPProcessor.parseRecordsFromReader(new StringReader(AGP));
    new ApproximateCoolerExporter().export(options(input, output, true), records, ignored -> {}, () -> false);
    try (var reader = HDF5Factory.openForReading(output.toFile());
         var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(input, 1, 2))) {
      assertEquals("HDF5::MCOOL", reader.string().getAttr("/", "format"));
      assertTrue(reader.bool().getAttr("/", "hict-approximate-export"));
      for (long resolution : List.of(4L, 10L)) {
        final String root = "/resolutions/" + resolution;
        final var projection = ApproximateContactProjection.prepare(file, records, resolution);
        final int size = Math.toIntExact(projection.targetBinCount());
        final var expected = new double[size][size];
        file.matrixQueries().scanStoredContacts(ResolutionDescriptor.fromBpResolution(resolution, file),
          (r, c, v) -> projection.projectContact(r, c, v, (a, b, value) -> expected[(int) a][(int) b] += value, () -> false),
          () -> false, null);
        final var rows = reader.int64().readArray(root + "/pixels/bin1_id");
        final var cols = reader.int64().readArray(root + "/pixels/bin2_id");
        final var counts = reader.float64().readArray(root + "/pixels/count");
        final var offsets = reader.int64().readArray(root + "/indexes/bin1_offset");
        assertEquals(size + 1, offsets.length); assertEquals(rows.length, offsets[size]);
        final var actual = new double[size][size];
        for (int i = 0; i < rows.length; i++) {
          assertTrue(rows[i] <= cols[i]);
          if (i > 0) assertTrue(rows[i] > rows[i - 1] || rows[i] == rows[i - 1] && cols[i] > cols[i - 1]);
          actual[(int) rows[i]][(int) cols[i]] = counts[i];
          assertTrue(i >= offsets[(int) rows[i]] && i < offsets[(int) rows[i] + 1]);
        }
        for (int i = 0; i < size; i++) assertArrayEquals(expected[i], actual[i], 1e-12);
        assertEquals(22, Arrays.stream(counts).sum(), 1e-12);
        final var metadata = new JsonObject(reader.string().getAttr(root, "metadata"));
        assertTrue(metadata.getBoolean("hict_approximate_export"));
        assertEquals(ApproximateContactProjection.METHOD, metadata.getString("method"));
        assertFalse(metadata.getBoolean("native_contacts_modified"));
        assertEquals(22, metadata.getDouble("expected_retained_contact_total"), 1e-12);
        assertEquals(22, reader.float64().getAttr(root, "sum"), 1e-12);
        assertEquals(25, reader.int64().readArray(root + "/chroms/length")[0]);
        assertFalse(reader.object().exists(root + "/bins/weight"), "No fabricated balancing weights");
        if (resolution == 4) {
          final var coverage = reader.float64().readArray(root + "/bins/hict_sequence_fraction");
          assertArrayEquals(new double[]{1, .25, 0, .75, 1, 1, 1}, coverage, 0);
          assertEquals(0, offsets[3] - offsets[2], "A gap-only bin has no contact rows");
        }
      }
      assertEquals(AGP, reader.string().read("/hict/approximate_export/agp"));
    }
    assertArrayEquals(untouched, Files.readAllBytes(input));
    assertNoOwnedTemporaryFiles();
  }

  @Test void writesEmptySingleCoolerAndDoesNotInventContacts() throws Exception {
    final var input = ReinforcementHdf5Fixture.create(directory, "empty", List.of(4L), 17, new long[0][]);
    final var output = directory.resolve("empty.cool");
    new ApproximateCoolerExporter().export(options(input, output, false),
      AGPProcessor.parseRecordsFromReader(new StringReader(AGP)), ignored -> {}, () -> false);
    try (var reader = HDF5Factory.openForReading(output.toFile())) {
      assertEquals("HDF5::Cooler", reader.string().getAttr("/", "format"));
      assertEquals(0, reader.int64().getAttr("/", "nnz"));
      assertEquals(0, reader.float64().readArray("/pixels/count").length);
      assertArrayEquals(new long[8], reader.int64().readArray("/indexes/bin1_offset"));
    }
    assertNoOwnedTemporaryFiles();
  }

  @Test void failureAndCancellationPreserveExistingOutputAndCleanOwnedFiles() throws Exception {
    final var input = ReinforcementHdf5Fixture.create(directory, "cancel", List.of(4L, 10L));
    final var output = directory.resolve("owned.cool"); Files.writeString(output, "existing user output");
    final var records = AGPProcessor.parseRecordsFromReader(new StringReader(AGP));
    final var checks = new AtomicInteger();
    assertThrows(CancellationException.class, () -> new ApproximateCoolerExporter().export(options(input, output, false),
      records, ignored -> {}, () -> checks.incrementAndGet() > 9));
    assertEquals("existing user output", Files.readString(output));
    assertNoOwnedTemporaryFiles();
    final var invalid = AGPProcessor.parseRecordsFromReader(new StringReader("a\t1\t18\t1\tW\ttiny\t1\t18\t+\n"));
    assertThrows(IllegalArgumentException.class, () -> new ApproximateCoolerExporter().export(options(input, output, false),
      invalid, ignored -> {}, () -> false));
    assertEquals("existing user output", Files.readString(output));
    assertNoOwnedTemporaryFiles();
  }

  @Test void ordinaryConversionRequiresExplicitOptInAndRejectsIncompatibleBalancing() throws Exception {
    final var input = ReinforcementHdf5Fixture.create(directory, "opt-in", List.of(4L, 10L));
    final var agp = directory.resolve("cut.agp"); Files.writeString(agp, AGP);
    final var output = directory.resolve("opt-in-export.mcool");
    final var exactOptions = new ConversionOptions(input, output, List.of(), 2, 1, ConversionOptions.CompressionAlgorithm.DEFLATE,
      agp.toString(), true, 1, true, false, false, false, ConversionOptions.ExportMode.INTERNAL);
    assertFalse(exactOptions.approximateRebin());
    assertThrows(IllegalStateException.class, () -> new HictToMcoolConverter().convert(exactOptions, ignored -> {}));
    assertFalse(Files.exists(output));
    assertThrows(IllegalArgumentException.class, () -> new ConversionOptions(input, output, List.of(), 2, 1,
      ConversionOptions.CompressionAlgorithm.DEFLATE, agp.toString(), true, 1, true, false, false, true,
      ConversionOptions.ExportMode.AUTO, true));
    final var approximate = new ConversionOptions(input, output, List.of(), 2, 1, ConversionOptions.CompressionAlgorithm.DEFLATE,
      agp.toString(), true, 1, true, false, false, false, ConversionOptions.ExportMode.AUTO, true);
    new ru.itmo.ctlab.hict.hict_server.handlers.conversion.HictToMcoolExportPipeline(
      new ru.itmo.ctlab.hict.hict_server.handlers.conversion.ExternalToolchainManager()).convert(approximate, ignored -> {});
    assertTrue(Files.isRegularFile(output));
    assertNoOwnedTemporaryFiles();
  }

  private ConversionOptions options(Path input, Path output, boolean all) {
    return new ConversionOptions(input, output, List.of(), 2, 1, ConversionOptions.CompressionAlgorithm.DEFLATE,
      "", false, 1, all, false, false, false, ConversionOptions.ExportMode.INTERNAL);
  }
  private void assertNoOwnedTemporaryFiles() throws Exception {
    try (var paths = Files.list(directory)) {
      assertFalse(paths.anyMatch(p -> p.getFileName().toString().startsWith(".hict-")));
    }
  }
}
