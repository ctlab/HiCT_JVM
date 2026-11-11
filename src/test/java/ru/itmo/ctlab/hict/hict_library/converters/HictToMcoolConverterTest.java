package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
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
  void cooCursorRejectsTruncatedRecordsAndReadsSignedInt64Exactly() throws Exception {
    final var type = Class.forName(HictToMcoolConverter.class.getName() + "$ChunkCursor");
    final var constructor = type.getDeclaredConstructor(Path.class); constructor.setAccessible(true);
    final var advance = type.getDeclaredMethod("advance"); advance.setAccessible(true);
    final var path = tempDir.resolve("cursor.bin");
    try (var out = new java.io.DataOutputStream(Files.newOutputStream(path))) {
      out.writeLong(42L); out.writeLong(99L); out.writeLong((1L << 54) + 1L);
    }
    try (var cursor = (AutoCloseable) constructor.newInstance(path)) {
      assertEquals(true, advance.invoke(cursor));
      final var count = type.getDeclaredField("count"); count.setAccessible(true);
      assertEquals((1L << 54) + 1L, count.getLong(cursor));
      assertEquals(false, advance.invoke(cursor));
    }
    Files.write(path, new byte[]{1, 2, 3});
    try (var cursor = (AutoCloseable) constructor.newInstance(path)) {
      final var error = org.junit.jupiter.api.Assertions.assertThrows(java.lang.reflect.InvocationTargetException.class,
        () -> advance.invoke(cursor));
      org.junit.jupiter.api.Assertions.assertInstanceOf(java.io.EOFException.class, error.getCause());
    }
  }

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

  @Test
  void exportedAgpAndFastaUseIdenticalConfiguredGapsAndInclusiveEnds() throws Exception {
    final var source = tempDir.resolve("gap-source.mcool");
    final var hict = tempDir.resolve("gap-source.hict.hdf5");
    final var fasta = tempDir.resolve("gap-source.fa");
    writeSyntheticMcool(source);
    Files.writeString(fasta, ">ctgA\n" + "A".repeat(2000) + "\n>ctgB\n" + "C".repeat(2000) + "\n");
    new McoolToHictConverter().convert(roundTripOptions(source, hict, null), ignored -> {});
    try (final var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hict, 1, 2))) {
      file.importAGP(new StringReader("scf\t1\t2000\t1\tW\tctgB\t1\t2000\t-\n"
        + "scf\t2001\t2500\t2\tN\t500\tscaffold\tyes\tproximity_ligation\n"
        + "scf\t2501\t4500\t3\tW\tctgA\t1\t2000\t+\n"));
      assertEquals(500, file.getAssemblyInfo().scaffolds().get(0).scaffoldDescriptor().spacerLength());
      final var assemblyBefore = file.getAssemblyInfo().contigs();
      assertThrows(IllegalArgumentException.class, () -> file.importAGP(new StringReader(
        "scf\t1\t2000\t1\tW\tctgA\t1\t2000\t+\n"
          + "scf\t2001\t3000\t2\tW\tctgA\t1\t1000\t+\n")));
      assertEquals(assemblyBefore, file.getAssemblyInfo().contigs());
      assertThrows(IllegalArgumentException.class, () -> file.importAGP(new StringReader(
        "scf\t1\t2000\t1\tW\tctgA\t2\t2001\t+\n")));
      assertEquals(assemblyBefore, file.getAssemblyInfo().contigs());
      for (long gap : new long[]{0, 1, 500, 1000, 2345}) {
        final var records = file.getAgpProcessor().getAGPRecords(gap);
        assertEquals(gap == 0 ? 2 : 3, records.size());
        final var first = (AGPProcessor.ContigAGPRecord) records.get(0);
        final var last = (AGPProcessor.ContigAGPRecord) records.get(records.size() - 1);
        assertEquals(1, first.getInterScaffoldStartIncl());
        assertEquals(2000, first.getInterScaffoldEndIncl());
        assertEquals(2000, first.getIntraContigEndBpIncl());
        assertEquals(2001 + gap, last.getInterScaffoldStartIncl());
        assertEquals(4000 + gap, last.getInterScaffoldEndIncl());
        final var sequence = file.getFastaProcessor().exportAssembly(fasta, gap).lines()
          .filter(line -> !line.startsWith(">"))
          .collect(java.util.stream.Collectors.joining());
        assertEquals("G".repeat(2000) + "N".repeat((int) gap) + "A".repeat(2000), sequence);
        assertEquals(sequence.length(), last.getInterScaffoldEndIncl());
        final var text = records.stream().map(Object::toString).collect(java.util.stream.Collectors.joining("\n"));
        assertEquals(records, AGPProcessor.parseRecordsFromReader(new StringReader(text)));
      }
      assertEquals(500, ((AGPProcessor.GapAGPRecord) file.getAgpProcessor().getAssemblyAGPRecords().get(1)).getGapLength());
      final var key = ru.itmo.ctlab.hict.hict_library.assembly.AssemblyExportSettings.GAP_LENGTH_KEY;
      final var previous = System.getProperty(key);
      try {
        System.setProperty(key, "1234");
        final var text = file.getAgpProcessor().getAGPStream().collect(java.util.stream.Collectors.joining());
        assertTrue(text.contains("\tN\t1234\t"));
        assertTrue(file.getFastaProcessor().exportAssembly(fasta).lines()
          .filter(line -> !line.startsWith(">"))
          .collect(java.util.stream.Collectors.joining()).contains("N".repeat(1234)));
      } finally {
        if (previous == null) System.clearProperty(key); else System.setProperty(key, previous);
      }
    }
  }

  @Test
  void partialAgpImportPreservesContactsFastaGapsAndExplicitOmissionPolicy() throws Exception {
    final var source = tempDir.resolve("partial.mcool");
    final var hict = tempDir.resolve("partial.hict.hdf5");
    final var fasta = tempDir.resolve("partial.fa");
    writeSyntheticMcool(source);
    Files.writeString(fasta, ">ctgA\n" + "ACGT".repeat(500) + "\n>ctgB\n" + "C".repeat(2000) + "\n");
    new McoolToHictConverter().convert(roundTripOptions(source, hict, null), ignored -> {});
    final var text = "s1 1 550 1 W ctgA 1 550 +\n"
      + "s1 551 650 2 U 100 scaffold yes paired-ends\n"
      + "s1 651 2650 3 W ctgB 1 2000 -\n"
      + "s2 1 1450 1 W ctgA 551 2000 -\n";
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hict, 1, 2))) {
      var resolution = ResolutionDescriptor.fromBpResolution(1000, file);
      var raw = file.getMatrixQueries().getSubmatrix(resolution, 0, 0, 4, 4, false).matrix();
      var report = file.importAGP(new StringReader(text), ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.REQUIRE_CHOICE);
      assertEquals(1, report.splitSourceContigs()); assertFalse(report.requiresChoice());
      assertEquals(AGPProcessor.parseRecordsFromReader(new StringReader(text)), file.getAgpProcessor().getAssemblyAGPRecords());
      var changed = file.getMatrixQueries().getSubmatrix(resolution, 0, 0, 4, 4, false).matrix();
      int[] permutation = {0, 3, 2, 1};
      for(int i=0;i<4;i++) for(int j=0;j<4;j++) assertEquals(raw.getAsDouble(permutation[i],permutation[j]),changed.getAsDouble(i,j));
      var exported = file.getFastaProcessor().exportAssemblyPreservingGaps(fasta);
      var seq = exported.lines().filter(l->!l.startsWith(">")).collect(java.util.stream.Collectors.joining());
      var original = "ACGT".repeat(500);
      var reverse = new StringBuilder(original.substring(550)).reverse().toString().chars()
        .mapToObj(c->String.valueOf("TGCA".charAt("ACGT".indexOf(c)))).collect(java.util.stream.Collectors.joining());
      assertEquals(original.substring(0,550)+"N".repeat(100)+"G".repeat(2000)+reverse,seq);
      final var before = file.getAssemblyInfo().contigs();
      final var subset = "only 1 550 1 W ctgA 1 550 +";
      var preview = file.importAGP(new StringReader(subset), ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.REQUIRE_CHOICE);
      assertTrue(preview.requiresChoice()); assertEquals(before,file.getAssemblyInfo().contigs());
      for(var policy:List.of(ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.EXCLUDE,
          ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.APPEND_UNPLACED)) {
        file.importAGP(new StringReader(subset),policy);
        int expected = policy == ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.EXCLUDE ? 550 : 4000;
        assertEquals(expected,file.getAssemblyInfo().contigs().stream().mapToLong(c->c.descriptor().getLengthBp()).sum());
        assertEquals(expected,file.getFastaProcessor().exportAssemblyPreservingGaps(fasta).lines().filter(l->!l.startsWith(">")).mapToInt(String::length).sum());
      }
      var stable = file.getAssemblyInfo().contigs();
      assertThrows(IllegalArgumentException.class,()->file.importAGP(new StringReader("bad 1 2001 1 W ctgA 1 2001 +")));
      assertEquals(stable,file.getAssemblyInfo().contigs());
    }
  }

  @Test
  void persistentAssemblyCheckpointsRestoreSplitDebrisNamesAgpAndFastaExactly() throws Exception {
    var source=tempDir.resolve("history.mcool"); var hict=tempDir.resolve("history.hict.hdf5");
    var fasta=tempDir.resolve("history.fa");
    writeSyntheticMcool(source);
    // Three bins per component permit a genuine interior-bin removal leaving
    // sequence on both sides. The old 2-bin/500-bp cut misaligned sequence and ATUs.
    try (var writer=HDF5Factory.open(source.toFile())) {
      writer.int64().writeArray("/chroms/length",new long[]{3000,3000});
      writer.int64().writeArray("/resolutions/1000/chroms/length",new long[]{3000,3000});
      writer.int64().writeArray("/resolutions/1000/indexes/chrom_offset",new long[]{0,3,6});
      writer.int64().writeArray("/resolutions/1000/indexes/bin1_offset",new long[]{0,1,2,3,4,5,6});
      writer.int64().writeArray("/resolutions/1000/bins/chrom",new long[]{0,0,0,1,1,1});
      writer.int64().writeArray("/resolutions/1000/bins/start",new long[]{0,1000,2000,0,1000,2000});
      writer.int64().writeArray("/resolutions/1000/bins/end",new long[]{1000,2000,3000,1000,2000,3000});
      writer.float64().writeArray("/resolutions/1000/bins/weight",new double[]{1,1,1,1,1,1});
      writer.int64().writeArray("/resolutions/1000/pixels/bin1_id",new long[]{0,1,2,3,4,5});
      writer.int64().writeArray("/resolutions/1000/pixels/bin2_id",new long[]{0,1,2,3,4,5});
      writer.int64().writeArray("/resolutions/1000/pixels/count",new long[]{11,12,13,14,15,16});
    }
    Files.writeString(fasta, ">ctgA\n"+"ACGT".repeat(750)+"\n>ctgB\n"+"C".repeat(3000)+"\n");
    new McoolToHictConverter().convert(roundTripOptions(source,hict,null),ignored->{});
    try(var file=new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hict,1,2))) {
      var states=new java.util.ArrayList<ChunkedFile.AssemblySnapshot>();
      var layouts=new java.util.ArrayList<String>(); var fastas=new java.util.ArrayList<String>();
      var matrices=new java.util.ArrayList<long[]>();
      var names=new java.util.ArrayList<java.util.Map<Integer,String>>();
      var registry=new java.util.ArrayList<java.util.Map<Integer,ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor>>();
      var resolution=ResolutionDescriptor.fromBpResolution(1000,file);
      Runnable remember=()->{
        states.add(file.snapshotAssembly()); layouts.add(file.getAgpProcessor().getAssemblyAGPRecords().toString());
        fastas.add(file.getFastaProcessor().exportAssemblyPreservingGaps(fasta));
        var matrix=file.getMatrixQueries().getSubmatrix(resolution,0,0,6,6,false).matrix();
        var cells=new long[36];for(int i=0;i<6;i++)for(int j=0;j<6;j++)cells[i*6+j]=(long)matrix.getAsDouble(i,j);
        matrices.add(cells); names.add(java.util.Map.copyOf(file.getContigNameOverrides()));
        registry.add(java.util.Map.copyOf(file.getContigTree().getContigDescriptors()));
      };
      remember.run();
      file.scaffoldingOperations().reverseSelectionRangeBp(0,3000);remember.run();
      file.scaffoldingOperations().moveSelectionRangeBp(0,3000,6000);remember.run();
      file.scaffoldingOperations().splitContigAtBin(1500,ResolutionDescriptor.fromResolutionOrder(0),QueryLengthUnit.BASE_PAIRS);remember.run();
      file.restoreAssembly(states.get(0));
      file.importAGP(new StringReader("s1 1 550 1 W ctgA 1 550 +\ns1 551 650 2 U 100 scaffold yes paired-ends\ns1 651 2650 3 W ctgB 1 2000 -\ns2 1 1450 1 W ctgA 551 2000 -\n"));remember.run();
      int contigId=file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.setContigNameOverride(contigId,"renamed_piece");remember.run();
      file.scaffoldingOperations().moveRegionToDebris(0,550,ResolutionDescriptor.fromResolutionOrder(0),QueryLengthUnit.BASE_PAIRS);remember.run();
      for(int index:new int[]{0,3,1,5,2,6,4,0,6}) {
        file.restoreAssembly(states.get(index));
        assertEquals(layouts.get(index),file.getAgpProcessor().getAssemblyAGPRecords().toString());
        assertEquals(fastas.get(index),file.getFastaProcessor().exportAssemblyPreservingGaps(fasta));
        assertEquals(names.get(index),file.getContigNameOverrides());
        assertEquals(registry.get(index),file.getContigTree().getContigDescriptors());
        var matrix=file.getMatrixQueries().getSubmatrix(resolution,0,0,4,4,false).matrix();
        for(int i=0;i<4;i++)for(int j=0;j<4;j++)assertEquals(matrices.get(index)[i*6+j],(long)matrix.getAsDouble(i,j));
      }
      file.restoreAssembly(states.get(0));
      var groupingBefore=file.snapshotAssembly();
      file.scaffoldingOperations().scaffoldRegion(0,4000,ResolutionDescriptor.fromResolutionOrder(0),QueryLengthUnit.BASE_PAIRS,null);
      var group=file.getAssemblyInfo().scaffolds();file.restoreAssembly(groupingBefore);
      file.scaffoldingOperations().scaffoldRegion(0,4000,ResolutionDescriptor.fromResolutionOrder(0),QueryLengthUnit.BASE_PAIRS,null);
      assertEquals(group,file.getAssemblyInfo().scaffolds(),"Allocator must be restored with scaffold root");
    }
  }

  @Test
  void failingServerEditRollsBackBothRootsAndMetadataWithoutAddingHistory() throws Exception {
    var source=tempDir.resolve("rollback.mcool"); var hict=tempDir.resolve("rollback.hict.hdf5");
    writeSyntheticMcool(source);new McoolToHictConverter().convert(roundTripOptions(source,hict,null),ignored->{});
    var vertx=io.vertx.core.Vertx.vertx();
    var scheduler=new ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler(vertx,
      new ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.SchedulerConfig(10,8,30,java.util.Map.of()));
    try(var file=new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hict,1,2));var client=java.net.http.HttpClient.newHttpClient()) {
      var map=vertx.sharedData().getLocalMap("hict_server");
      map.put("chunkedFile",new ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers.ChunkedFileWrapper(file));
      var history=new ru.itmo.ctlab.hict.hict_server.history.AssemblyHistoryHandlersHolder(vertx,scheduler);
      var router=io.vertx.ext.web.Router.router(vertx);router.route().handler(io.vertx.ext.web.handler.BodyHandler.create());
      history.addHandlersToRouter(router);
      router.post("/reverse_selection_range").handler(ctx->scheduler.submit(ctx,
        ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.RequestPriority.ASSEMBLY,null,()->{
          file.scaffoldingOperations().moveRegionToDebris(0,2000,ResolutionDescriptor.fromResolutionOrder(0),QueryLengthUnit.BASE_PAIRS);
          file.setContigNameOverride(0,"changed");
          throw new IllegalStateException("Injected failure after mutation");
        },result->ctx.response().end("unexpected success")));
      var server=vertx.createHttpServer().requestHandler(router).listen(0).toCompletionStage().toCompletableFuture().get();
      var before=file.snapshotAssembly();var layout=file.getAssemblyInfo();
      var request=java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+server.actualPort()+"/reverse_selection_range"))
        .header("Content-Type","application/json").POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}")).build();
      assertEquals(500,client.send(request,java.net.http.HttpResponse.BodyHandlers.ofString()).statusCode());
      assertEquals(layout,file.getAssemblyInfo());assertEquals(before,file.snapshotAssembly());
      request=java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+server.actualPort()+"/assembly/history"))
        .header("Content-Type","application/json").POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}")).build();
      var response=new io.vertx.core.json.JsonObject(client.send(request,java.net.http.HttpResponse.BodyHandlers.ofString()).body());
      assertEquals(1,response.getJsonObject("summary").getInteger("size"));
      server.close().toCompletionStage().toCompletableFuture().get();
    } finally { scheduler.close();vertx.close().toCompletionStage().toCompletableFuture().get(); }
  }

  private static int[] toIntArray(final long[] values) {
    final var out = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = Math.toIntExact(values[i]);
    }
    return out;
  }
}
