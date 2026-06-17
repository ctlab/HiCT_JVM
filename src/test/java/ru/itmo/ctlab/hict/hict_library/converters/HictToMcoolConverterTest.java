package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

  private static int[] toIntArray(final long[] values) {
    final var out = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = Math.toIntExact(values[i]);
    }
    return out;
  }
}
