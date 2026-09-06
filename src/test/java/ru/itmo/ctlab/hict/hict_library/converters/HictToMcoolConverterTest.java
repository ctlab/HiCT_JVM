package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import java.io.StringReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigNameDatasetPath;

class HictToMcoolConverterTest {
  @TempDir
  Path tempDir;

  @Test
  void javaCoolerSorterOrdersPrimitiveRecordsByRowColumnAndCount() {
    final var rows = new long[]{2L, 0L, 1L, 0L, 1L, 0L};
    final var cols = new long[]{2L, 3L, 1L, 1L, 1L, 1L};
    final var counts = new long[]{9L, 4L, 5L, 7L, 3L, 2L};

    HictToMcoolConverter.sortCoolerRecordsJava(rows, cols, counts);

    assertArrayEquals(new long[]{0L, 0L, 0L, 1L, 1L, 2L}, rows);
    assertArrayEquals(new long[]{1L, 1L, 3L, 1L, 1L, 2L}, cols);
    assertArrayEquals(new long[]{2L, 7L, 4L, 3L, 5L, 9L}, counts);
  }

  @Test
  void coolerSorterCompactsDuplicateRowColumnRecords() {
    final var rows = new long[]{3L, 0L, 1L, 0L, 1L, 0L, 3L};
    final var cols = new long[]{1L, 3L, 1L, 1L, 1L, 1L, 1L};
    final var counts = new long[]{4L, 9L, 5L, 7L, 3L, 2L, 6L};

    final int compactedLength = HictToMcoolConverter.sortAndCompactCoolerRecordsRowMajor(rows, cols, counts);

    assertEquals(4, compactedLength);
    assertArrayEquals(new long[]{0L, 0L, 1L, 3L}, Arrays.copyOf(rows, compactedLength));
    assertArrayEquals(new long[]{1L, 3L, 1L, 1L}, Arrays.copyOf(cols, compactedLength));
    assertArrayEquals(new long[]{9L, 9L, 8L, 10L}, Arrays.copyOf(counts, compactedLength));
  }

  @Test
  void coolOutputAutomaticallyUsesFinestSelectedResolution() {
    final var messages = new ArrayList<String>();

    final var selected = HictToMcoolConverter.normalizeSelectedResolutionsForOutput(
      Path.of("export.cool"),
      List.of(10_000L, 250L, 1_000L),
      messages::add
    );

    assertEquals(List.of(250L), selected);
    assertTrue(messages.stream().anyMatch(message -> message.contains(".cool files contain exactly one resolution")));
    assertEquals(
      List.of(10_000L, 250L, 1_000L),
      HictToMcoolConverter.normalizeSelectedResolutionsForOutput(
        Path.of("export.mcool"),
        List.of(10_000L, 250L, 1_000L),
        ignored -> {
        }
      )
    );
  }

  @Test
  void missingRequestedResolutionReportsAvailableValues() {
    final var exception = assertThrows(
      IllegalArgumentException.class,
      () -> HictToMcoolConverter.requireUsableSelectedResolutions(
        Path.of("example.hict.hdf5"),
        List.of(),
        new long[]{0L, 1_000L, 5_000L},
        List.of(250L),
        false
      )
    );

    assertTrue(exception.getMessage().contains("Available resolutions are [1000, 5000]"));
    assertTrue(exception.getMessage().contains("requested [250]"));
  }

  @Test
  void internalExporterRoundTripsCoolerArraysAndOverwritesExistingOutput() throws Exception {
    final var sourceMcool = tempDir.resolve("source.mcool");
    final var intermediateHict = tempDir.resolve("source.hict.hdf5");
    final var exportedMcool = tempDir.resolve("exported.mcool");

    writeSyntheticMcool(sourceMcool);
    new McoolToHictConverter().convert(
      new ConversionOptions(
        sourceMcool,
        intermediateHict,
        List.of(1_000L),
        64,
        0,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        ConversionOptions.NO_AGP,
        false,
        1,
        false,
        ConversionOptions.ExportMode.AUTO
      ),
      ignored -> {
      }
    );

    Files.writeString(exportedMcool, "stale");
    new HictToMcoolConverter().convert(
      new ConversionOptions(
        intermediateHict,
        exportedMcool,
        List.of(1_000L),
        64,
        0,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        ConversionOptions.NO_AGP,
        false,
        1,
        false,
        ConversionOptions.ExportMode.INTERNAL
      ),
      ignored -> {
      }
    );

    try (final var sourceReader = HDF5Factory.openForReading(sourceMcool.toFile());
         final var exportedReader = HDF5Factory.openForReading(exportedMcool.toFile())) {
      assertArrayEquals(
        sourceReader.int64().readArray("/resolutions/1000/pixels/bin1_id"),
        exportedReader.int64().readArray("/resolutions/1000/pixels/bin1_id")
      );
      assertArrayEquals(
        sourceReader.int64().readArray("/resolutions/1000/pixels/bin2_id"),
        exportedReader.int64().readArray("/resolutions/1000/pixels/bin2_id")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/pixels/count")),
        exportedReader.int32().readArray("/resolutions/1000/pixels/count")
      );
      assertArrayEquals(
        sourceReader.int64().readArray("/resolutions/1000/indexes/bin1_offset"),
        exportedReader.int64().readArray("/resolutions/1000/indexes/bin1_offset")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/bins/start")),
        exportedReader.int32().readArray("/resolutions/1000/bins/start")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/bins/end")),
        exportedReader.int32().readArray("/resolutions/1000/bins/end")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/bins/chrom")),
        exportedReader.int32().readArray("/resolutions/1000/bins/chrom")
      );
      assertArrayEquals(
        sourceReader.float64().readArray("/resolutions/1000/bins/weight"),
        exportedReader.float64().readArray("/resolutions/1000/bins/weight"),
        1.0e-12
      );
      assertArrayEquals(
        sourceReader.int64().readArray("/resolutions/1000/indexes/chrom_offset"),
        exportedReader.int64().readArray("/resolutions/1000/indexes/chrom_offset")
      );
      assertArrayEquals(
        sourceReader.string().readArray("/chroms/name"),
        exportedReader.string().readArray("/chroms/name")
      );
      assertArrayEquals(
        sourceReader.string().readArray("/resolutions/1000/chroms/name"),
        exportedReader.string().readArray("/resolutions/1000/chroms/name")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/chroms/length")),
        exportedReader.int32().readArray("/chroms/length")
      );
    }
  }

  @Test
  void internalExporterDoesNotSerializeHiddenAssemblyPlaceholdersAsCoolerChroms() throws Exception {
    final var sourceMcool = tempDir.resolve("partial-source.mcool");
    final var assembly = tempDir.resolve("partial-layout.assembly");
    final var intermediateHict = tempDir.resolve("partial-source.hict.hdf5");
    final var exportedMcool = tempDir.resolve("partial-exported.mcool");
    final var reimportedHict = tempDir.resolve("partial-reimported.hict.hdf5");

    writeSyntheticMcool(sourceMcool);
    Files.writeString(
      assembly,
      String.join(
        System.lineSeparator(),
        ">ctgA 1 2000",
        ">ctgB 2 2000",
        ">ctgC 3 1500",
        "1",
        "2",
        "3"
      ) + System.lineSeparator()
    );

    new McoolToHictConverter().convert(
      new ConversionOptions(
        sourceMcool,
        intermediateHict,
        List.of(1_000L),
        64,
        0,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        assembly.toString(),
        false,
        1,
        true,
        ConversionOptions.ExportMode.AUTO
      ),
      ignored -> {
      }
    );

    try (final var reader = HDF5Factory.openForReading(intermediateHict.toFile())) {
      assertArrayEquals(new String[]{"ctgA", "ctgB", "ctgC"}, reader.string().readArray(getContigNameDatasetPath()));
    }

    new HictToMcoolConverter().convert(
      new ConversionOptions(
        intermediateHict,
        exportedMcool,
        List.of(1_000L),
        64,
        0,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        ConversionOptions.NO_AGP,
        false,
        1,
        true,
        ConversionOptions.ExportMode.INTERNAL
      ),
      ignored -> {
      }
    );

    try (final var reader = HDF5Factory.openForReading(exportedMcool.toFile())) {
      assertArrayEquals(new String[]{"ctgA", "ctgB"}, reader.string().readArray("/chroms/name"));
      assertArrayEquals(new String[]{"ctgA", "ctgB"}, reader.string().readArray("/resolutions/1000/chroms/name"));
      assertArrayEquals(new String[]{"ctgA", "ctgB"}, reader.string().readArray(HictToMcoolConverter.HICT_METADATA_CONTIG_NAME_PATH));
    }

    new McoolToHictConverter().convert(
      new ConversionOptions(
        exportedMcool,
        reimportedHict,
        List.of(1_000L),
        64,
        0,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        ConversionOptions.NO_AGP,
        false,
        1,
        false,
        ConversionOptions.ExportMode.AUTO
      ),
      ignored -> {
      }
    );

    try (final var reader = HDF5Factory.openForReading(reimportedHict.toFile())) {
      assertArrayEquals(new String[]{"ctgA", "ctgB"}, reader.string().readArray(getContigNameDatasetPath()));
    }
  }

  @Test
  void internalExporterWritesSingleResolutionCoolerAtRoot() throws Exception {
    final var sourceMcool = tempDir.resolve("single-source.mcool");
    final var intermediateHict = tempDir.resolve("single-source.hict.hdf5");
    final var exportedCool = tempDir.resolve("exported.cool");

    writeSyntheticMcool(sourceMcool);
    new McoolToHictConverter().convert(
      new ConversionOptions(
        sourceMcool,
        intermediateHict,
        List.of(1_000L),
        64,
        0,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        ConversionOptions.NO_AGP,
        false,
        1,
        false,
        ConversionOptions.ExportMode.AUTO
      ),
      ignored -> {
      }
    );

    new HictToMcoolConverter().convert(
      new ConversionOptions(
        intermediateHict,
        exportedCool,
        List.of(1_000L),
        64,
        0,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        ConversionOptions.NO_AGP,
        false,
        1,
        false,
        ConversionOptions.ExportMode.INTERNAL
      ),
      ignored -> {
      }
    );

    try (final var sourceReader = HDF5Factory.openForReading(sourceMcool.toFile());
         final var exportedReader = HDF5Factory.openForReading(exportedCool.toFile())) {
      assertFalse(exportedReader.object().isGroup("/resolutions"));
      assertArrayEquals(
        sourceReader.string().readArray("/resolutions/1000/chroms/name"),
        exportedReader.string().readArray("/chroms/name")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/chroms/length")),
        exportedReader.int32().readArray("/chroms/length")
      );
      assertArrayEquals(
        sourceReader.int64().readArray("/resolutions/1000/pixels/bin1_id"),
        exportedReader.int64().readArray("/pixels/bin1_id")
      );
      assertArrayEquals(
        sourceReader.int64().readArray("/resolutions/1000/pixels/bin2_id"),
        exportedReader.int64().readArray("/pixels/bin2_id")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/pixels/count")),
        exportedReader.int32().readArray("/pixels/count")
      );
      assertArrayEquals(
        sourceReader.int64().readArray("/resolutions/1000/indexes/bin1_offset"),
        exportedReader.int64().readArray("/indexes/bin1_offset")
      );
      assertArrayEquals(
        sourceReader.int64().readArray("/resolutions/1000/indexes/chrom_offset"),
        exportedReader.int64().readArray("/indexes/chrom_offset")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/bins/start")),
        exportedReader.int32().readArray("/bins/start")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/bins/end")),
        exportedReader.int32().readArray("/bins/end")
      );
      assertArrayEquals(
        toIntArray(sourceReader.int64().readArray("/resolutions/1000/bins/chrom")),
        exportedReader.int32().readArray("/bins/chrom")
      );
      assertArrayEquals(
        sourceReader.float64().readArray("/resolutions/1000/bins/weight"),
        exportedReader.float64().readArray("/bins/weight"),
        1.0e-12
      );
    }
  }

  private static void writeSyntheticMcool(final Path path) {
    HDF5LibraryInitializer.initializeHDF5Library();
    try (final var writer = HDF5Factory.open(path.toFile())) {
      writer.object().createGroup("/chroms");
      writer.string().writeArray("/chroms/name", new String[]{"ctgA", "ctgB"});
      writer.int64().writeArray("/chroms/length", new long[]{2_000L, 2_000L});

      writer.object().createGroup("/resolutions");
      writer.object().createGroup("/resolutions/1000");
      writer.object().createGroup("/resolutions/1000/chroms");
      writer.object().createGroup("/resolutions/1000/indexes");
      writer.object().createGroup("/resolutions/1000/bins");
      writer.object().createGroup("/resolutions/1000/pixels");

      writer.string().writeArray("/resolutions/1000/chroms/name", new String[]{"ctgA", "ctgB"});
      writer.int64().writeArray("/resolutions/1000/chroms/length", new long[]{2_000L, 2_000L});
      writer.int64().writeArray("/resolutions/1000/indexes/chrom_offset", new long[]{0L, 2L, 4L});
      writer.int64().writeArray("/resolutions/1000/indexes/bin1_offset", new long[]{0L, 3L, 5L, 6L, 7L});
      writer.int64().writeArray("/resolutions/1000/bins/chrom", new long[]{0L, 0L, 1L, 1L});
      writer.int64().writeArray("/resolutions/1000/bins/start", new long[]{0L, 1_000L, 0L, 1_000L});
      writer.int64().writeArray("/resolutions/1000/bins/end", new long[]{1_000L, 2_000L, 1_000L, 2_000L});
      writer.float64().writeArray("/resolutions/1000/bins/weight", new double[]{0.25d, 2.0d, 1.5d, 0.75d});
      writer.int64().writeArray("/resolutions/1000/pixels/bin1_id", new long[]{0L, 0L, 0L, 1L, 1L, 2L, 3L});
      writer.int64().writeArray("/resolutions/1000/pixels/bin2_id", new long[]{0L, 1L, 3L, 1L, 2L, 3L, 3L});
      writer.int64().writeArray("/resolutions/1000/pixels/count", new long[]{11L, 5L, 2L, 9L, 4L, 3L, 7L});
    }
  }

  @Test
  void agpRoundTripPreservesScaffoldOrderReversalAndUnalignedBoundaryBins() throws Exception {
    final var source = tempDir.resolve("agp-source.mcool");
    final var hict = tempDir.resolve("agp-source.hict.hdf5");
    final var exported = tempDir.resolve("agp-export.mcool");
    final var reopened = tempDir.resolve("agp-reopened.hict.hdf5");
    final var agp = tempDir.resolve("layout.agp");
    writeSyntheticMcool(source);
    Files.writeString(agp, "scf\t1\t2000\t1\tW\tctgB\t1\t2000\t-\n"
      + "scf\t2001\t2250\t2\tN\t250\tscaffold\tyes\tproximity_ligation\n"
      + "scf\t2251\t4250\t3\tW\tctgA\t1\t2000\t+\n");
    new McoolToHictConverter().convert(roundTripOptions(source, hict, null), ignored -> {});
    new HictToMcoolConverter().convert(roundTripOptions(hict, exported, agp), ignored -> {});
    try (final var reader = HDF5Factory.openForReading(exported.toFile())) {
      assertArrayEquals(new String[]{"scf"}, reader.string().readArray("/resolutions/1000/chroms/name"));
      assertArrayEquals(new long[]{4250}, reader.int64().readArray("/resolutions/1000/chroms/length"));
      final var expected = new long[][]{{7,3,2,0,0}, {3,0,0,4,0}, {2,0,11,5,0}, {0,4,5,9,0}, {0,0,0,0,0}};
      final var observed = new long[5][5];
      final var rows = reader.int64().readArray("/resolutions/1000/pixels/bin1_id");
      final var cols = reader.int64().readArray("/resolutions/1000/pixels/bin2_id");
      final var counts = reader.int64().readArray("/resolutions/1000/pixels/count");
      for (int i = 0; i < rows.length; i++) {
        observed[(int) rows[i]][(int) cols[i]] = counts[i];
        observed[(int) cols[i]][(int) rows[i]] = counts[i];
      }
      assertArrayEquals(expected, observed);
    }
    // Simulate an external pyramid builder dropping non-standard metadata.
    try (final var writer = HDF5Factory.open(exported.toFile())) {
      writer.object().delete("/hict_metadata");
    }
    assertEquals(0, new picocli.CommandLine(new ru.itmo.ctlab.hict.hict_server.tools.HictCli()).execute(
      "convert", "annotate-agp", "--agp=" + agp, "--input=" + exported));
    new McoolToHictConverter().convert(roundTripOptions(exported, reopened, null), ignored -> {});
    try (final var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(reopened, 1, 2))) {
      final var contigs = file.getAssemblyInfo().contigs();
      assertEquals(List.of("ctgB", "ctgA"), contigs.stream().map(c -> c.descriptor().getContigName()).toList());
      assertEquals("scf", file.getAssemblyInfo().scaffolds().get(0).scaffoldDescriptor().scaffoldName());
      final var matrix = (MatrixQueries.LongMatrix) file.getMatrixQueries().getSubmatrix(
        ResolutionDescriptor.fromBpResolution(1000L, file), 0, 0, 4, 4, false).matrix();
      assertArrayEquals(new long[][]{{7,3,2,0}, {3,0,0,4}, {2,0,11,5}, {0,4,5,9}}, matrix.values());
    }
  }

  @Test
  void agpExportExcludesUnlistedContigs() throws Exception {
    final var source = tempDir.resolve("subset.mcool");
    final var hict = tempDir.resolve("subset.hict.hdf5");
    final var exported = tempDir.resolve("subset-export.mcool");
    final var agp = tempDir.resolve("subset.agp");
    writeSyntheticMcool(source);
    Files.writeString(agp, "onlyB\t1\t2000\t1\tW\tctgB\t1\t2000\t+\n");
    new McoolToHictConverter().convert(roundTripOptions(source, hict, null), ignored -> {});
    new HictToMcoolConverter().convert(roundTripOptions(hict, exported, agp), ignored -> {});
    try (final var reader = HDF5Factory.openForReading(exported.toFile())) {
      assertArrayEquals(new String[]{"onlyB"}, reader.string().readArray("/resolutions/1000/chroms/name"));
      assertArrayEquals(new String[]{"ctgB"}, reader.string().readArray(HictToMcoolConverter.HICT_METADATA_AGP_COMPONENT_NAME_PATH));
      assertArrayEquals(new long[]{3,7}, reader.int64().readArray("/resolutions/1000/pixels/count"));
    }
  }

  @Test
  void overlappingAgpFailsWithActionableCoordinates() {
    final var error = assertThrows(IllegalArgumentException.class, () -> AGPProcessor.parseRecordsFromReader(new StringReader(
      "debris\t1\t2000\t1\tW\tctgA\t1\t2000\t+\n"
        + "debris\t1\t2000\t1\tW\tctgB\t1\t2000\t+\n")));
    assertTrue(error.getMessage().contains("expected start 2001 and part 2"));
  }

  @Test
  void annotationRejectsUnreorderedCoolerWithoutChangingMetadata() throws Exception {
    final var source = tempDir.resolve("unreordered.mcool");
    final var agp = tempDir.resolve("different.agp");
    writeSyntheticMcool(source);
    Files.writeString(agp, "other\t1\t2000\t1\tW\tctgB\t1\t2000\t+\n");
    assertEquals(1, new picocli.CommandLine(new ru.itmo.ctlab.hict.hict_server.tools.HictCli()).execute(
      "convert", "annotate-agp", "--agp=" + agp, "--input=" + source));
    try (final var reader = HDF5Factory.openForReading(source.toFile())) {
      assertFalse(reader.object().isGroup("/hict_metadata"));
      assertArrayEquals(new String[]{"ctgA", "ctgB"}, reader.string().readArray("/chroms/name"));
    }
  }

  @Test
  void rawQueriesIncludeHiddenContigsBetweenVisibleContigs() throws Exception {
    final var source = tempDir.resolve("hidden-middle.mcool");
    final var hict = tempDir.resolve("hidden-middle.hict.hdf5");
    writeSyntheticMcool(source);
    try (final var writer = HDF5Factory.open(source.toFile())) {
      for (final var root : List.of("", "/resolutions/1000")) {
        writer.string().writeArray(root + "/chroms/name", new String[]{"a", "tiny", "b"});
        writer.int64().writeArray(root + "/chroms/length", new long[]{1000,500,2000});
      }
      writer.int64().writeArray("/resolutions/1000/indexes/chrom_offset", new long[]{0,1,2,4});
      writer.int64().writeArray("/resolutions/1000/bins/chrom", new long[]{0,1,2,2});
      writer.int64().writeArray("/resolutions/1000/bins/start", new long[]{0,0,0,1000});
      writer.int64().writeArray("/resolutions/1000/bins/end", new long[]{1000,500,1000,2000});
    }
    new McoolToHictConverter().convert(roundTripOptions(source, hict, null), ignored -> {});
    try (final var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hict, 1, 2))) {
      final var resolution = ResolutionDescriptor.fromBpResolution(1000, file);
      final var raw = (MatrixQueries.LongMatrix) file.getMatrixQueries().getSubmatrix(resolution, 0, 0, 4, 4, false).matrix();
      assertArrayEquals(new long[][]{{11,5,0,2},{5,9,4,0},{0,4,0,3},{2,0,3,7}}, raw.values());
      final var visible = (MatrixQueries.LongMatrix) file.getMatrixQueries().getSubmatrix(resolution, 0, 0, 3, 3, true).matrix();
      assertArrayEquals(new long[][]{{11,0,2},{0,0,3},{2,3,7}}, visible.values());
    }
  }

  private static ConversionOptions roundTripOptions(Path input, Path output, Path agp) {
    return new ConversionOptions(input, output, List.of(1000L), 64, 1,
      ConversionOptions.CompressionAlgorithm.DEFLATE, agp == null ? ConversionOptions.NO_AGP : agp.toString(),
      agp != null, 2, false, ConversionOptions.ExportMode.INTERNAL);
  }

  private static int[] toIntArray(final long[] values) {
    final var out = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = Math.toIntExact(values[i]);
    }
    return out;
  }
}
