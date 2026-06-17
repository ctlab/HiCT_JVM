package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

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
