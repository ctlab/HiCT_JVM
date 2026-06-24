package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBasisATUDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockColsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockRowsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockValuesDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigDirectionDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigHideTypeDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigLengthBinsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigNameDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigOrderDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigsATLDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getDenseBlockDatasetPath;

class McoolToHictConverterTest {
  @TempDir
  Path tempDir;

  @Test
  void assemblyLayoutUsesExactSourceBinOffsetsInsteadOfProportionalRebinning() throws Exception {
    final var mcool = tempDir.resolve("source.mcool");
    final var assembly = tempDir.resolve("layout.agp");
    final var output = tempDir.resolve("output.hict.hdf5");

    writeSyntheticMcool(mcool);
    Files.writeString(
      assembly,
      String.join(
        System.lineSeparator(),
        "scaffold_1\t1\t3600\t1\tW\tctgB\t1\t3600\t+",
        "scaffold_2\t1\t2500\t1\tW\tctgA\t1\t2500\t+"
      ) + System.lineSeparator()
    );

    new McoolToHictConverter().convert(
      new ConversionOptions(
        mcool,
        output,
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

    try (final var reader = HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(new String[]{"ctgB", "ctgA"}, reader.string().readArray(getContigNameDatasetPath()));
      assertArrayEquals(new long[]{4L, 3L}, reader.int64().readArray(getContigLengthBinsDatasetPath(1_000L)));

      final long[][] contigsAtl = reader.int64().readMatrix(getContigsATLDatasetPath(1_000L));
      assertEquals(2, contigsAtl.length);
      assertArrayEquals(new long[]{0L, 0L}, contigsAtl[0]);
      assertArrayEquals(new long[]{1L, 1L}, contigsAtl[1]);

      final long[][] basisAtu = reader.int64().readMatrix(getBasisATUDatasetPath(1_000L));
      assertEquals(2, basisAtu.length);
      assertArrayEquals(new long[]{0L, 3L, 7L, 1L}, basisAtu[0]);
      assertArrayEquals(new long[]{0L, 0L, 3L, 1L}, basisAtu[1]);
    }
  }

  @Test
  void juiceboxAssemblyLayoutCanSliceSingleHictkAssemblyChromosome() throws Exception {
    final var mcool = tempDir.resolve("single-assembly-source.mcool");
    final var assembly = tempDir.resolve("single-assembly-layout.agp");
    final var output = tempDir.resolve("single-assembly-output.hict.hdf5");

    writeSyntheticSingleAssemblyChromosomeMcool(mcool);
    Files.writeString(
      assembly,
      String.join(
        System.lineSeparator(),
        "1\t1\t3600\t1\tW\tctgB\t1\t3600\t+",
        "2\t1\t2500\t1\tW\tctgA\t1\t2500\t+"
      ) + System.lineSeparator()
    );

    new McoolToHictConverter().convert(
      new ConversionOptions(
        mcool,
        output,
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

    try (final var reader = HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(new String[]{"ctgB", "ctgA"}, reader.string().readArray(getContigNameDatasetPath()));
      assertArrayEquals(new long[]{4L, 3L}, reader.int64().readArray(getContigLengthBinsDatasetPath(1_000L)));

      final long[][] basisAtu = reader.int64().readMatrix(getBasisATUDatasetPath(1_000L));
      assertEquals(2, basisAtu.length);
      assertArrayEquals(new long[]{0L, 0L, 4L, 1L}, basisAtu[0]);
      assertArrayEquals(new long[]{0L, 4L, 7L, 1L}, basisAtu[1]);
    }
  }

  @Test
  void reorderedJuiceboxAssemblyUsesHeaderOrderForSingleHictkSourceOffsets() throws Exception {
    final var mcool = tempDir.resolve("reordered-single-assembly-source.mcool");
    final var assembly = tempDir.resolve("reordered-single-assembly-layout.assembly");
    final var output = tempDir.resolve("reordered-single-assembly-output.hict.hdf5");

    writeSyntheticSingleAssemblyChromosomeMcool(mcool);
    Files.writeString(
      assembly,
      String.join(
        System.lineSeparator(),
        ">ctgA 1 2500",
        ">ctgB 2 3600",
        "2 1"
      ) + System.lineSeparator()
    );

    new McoolToHictConverter().convert(
      new ConversionOptions(
        mcool,
        output,
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

    try (final var reader = HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(new String[]{"ctgB", "ctgA"}, reader.string().readArray(getContigNameDatasetPath()));
      assertArrayEquals(new long[]{4L, 3L}, reader.int64().readArray(getContigLengthBinsDatasetPath(1_000L)));

      final long[][] basisAtu = reader.int64().readMatrix(getBasisATUDatasetPath(1_000L));
      assertEquals(2, basisAtu.length);
      assertArrayEquals(new long[]{0L, 3L, 7L, 1L}, basisAtu[0]);
      assertArrayEquals(new long[]{0L, 0L, 3L, 1L}, basisAtu[1]);
    }
  }

  @Test
  void juiceboxAssemblyKeepsContigsMissingFromMultiChromSourceHidden() throws Exception {
    final var mcool = tempDir.resolve("partial-source.mcool");
    final var assembly = tempDir.resolve("partial-layout.assembly");
    final var output = tempDir.resolve("partial-output.hict.hdf5");

    writeSyntheticMcool(mcool);
    Files.writeString(
      assembly,
      String.join(
        System.lineSeparator(),
        ">ctgA 1 2500",
        ">ctgB 2 3600",
        ">ctgC 3 1500",
        "1",
        "2",
        "3"
      ) + System.lineSeparator()
    );

    new McoolToHictConverter().convert(
      new ConversionOptions(
        mcool,
        output,
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

    try (final var reader = HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(new String[]{"ctgA", "ctgB", "ctgC"}, reader.string().readArray(getContigNameDatasetPath()));
      assertArrayEquals(new long[]{3L, 4L, 0L}, reader.int64().readArray(getContigLengthBinsDatasetPath(1_000L)));

      final long[][] basisAtu = reader.int64().readMatrix(getBasisATUDatasetPath(1_000L));
      assertEquals(2, basisAtu.length);
      assertArrayEquals(new long[]{0L, 0L, 3L, 1L}, basisAtu[0]);
      assertArrayEquals(new long[]{0L, 3L, 7L, 1L}, basisAtu[1]);
    }
  }

  @Test
  void noAssemblyImportRestoresHiCTAssemblyMetadataWhenPresent() throws Exception {
    final var mcool = tempDir.resolve("hict-origin-source.mcool");
    final var output = tempDir.resolve("hict-origin-output.hict.hdf5");

    writeSyntheticMcool(mcool);
    HDF5LibraryInitializer.initializeHDF5Library();
    try (final var writer = HDF5Factory.open(mcool.toFile())) {
      writer.object().createGroup(HictToMcoolConverter.HICT_METADATA_GROUP);
      writer.object().createGroup(HictToMcoolConverter.HICT_ASSEMBLY_METADATA_GROUP);
      writer.string().writeArray(HictToMcoolConverter.HICT_METADATA_CONTIG_NAME_PATH, new String[]{"ctgA", "ctgB"});
      writer.int64().writeArray(HictToMcoolConverter.HICT_METADATA_CONTIG_LENGTH_BP_PATH, new long[]{2_500L, 3_600L});
      writer.int64().writeArray(
        HictToMcoolConverter.HICT_METADATA_CONTIG_DIRECTION_PATH,
        new long[]{ContigDirection.REVERSED.ordinal(), ContigDirection.FORWARD.ordinal()}
      );
      writer.int64().writeArray(HictToMcoolConverter.HICT_METADATA_CONTIG_ORDER_PATH, new long[]{0L, 1L});
      writer.int64().writeArray(HictToMcoolConverter.HICT_METADATA_CONTIG_SCAFFOLD_ID_PATH, new long[]{42L, 42L});
    }

    new McoolToHictConverter().convert(
      new ConversionOptions(
        mcool,
        output,
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

    try (final var reader = HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(new String[]{"ctgA", "ctgB"}, reader.string().readArray(getContigNameDatasetPath()));
      assertArrayEquals(
        new long[]{ContigDirection.REVERSED.ordinal(), ContigDirection.FORWARD.ordinal()},
        reader.int64().readArray(getContigDirectionDatasetPath())
      );
      assertArrayEquals(new long[]{0L, 1L}, reader.int64().readArray(getContigOrderDatasetPath()));
      assertArrayEquals(new long[]{42L, 42L}, reader.int64().readArray("/contig_info/contig_scaffold_id"));
    }
  }

  @Test
  void noAssemblyImportCreatesIndividualScaffoldsForSourceChromosomes() throws Exception {
    final var mcool = tempDir.resolve("sealed-source.mcool");
    final var output = tempDir.resolve("sealed-output.hict.hdf5");

    writeSyntheticMcool(mcool);

    new McoolToHictConverter().convert(
      new ConversionOptions(
        mcool,
        output,
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

    try (final var reader = HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(new String[]{"ctgA", "ctgB"}, reader.string().readArray(getContigNameDatasetPath()));
      assertArrayEquals(new long[]{0L, 1L}, reader.int64().readArray("/contig_info/contig_scaffold_id"));
    }
  }

  @Test
  void subResolutionContigsAreHiddenAtCoarseResolution() throws Exception {
    final var mcool = tempDir.resolve("sub-resolution-source.mcool");
    final var output = tempDir.resolve("sub-resolution-output.hict.hdf5");

    writeSyntheticSubResolutionMcool(mcool);

    new McoolToHictConverter().convert(
      new ConversionOptions(
        mcool,
        output,
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

    try (final var reader = HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(new String[]{"tiny", "visible"}, reader.string().readArray(getContigNameDatasetPath()));
      assertArrayEquals(new long[]{1L, 2L}, reader.int64().readArray(getContigLengthBinsDatasetPath(1_000L)));
      assertArrayEquals(new byte[]{0, 1}, reader.int8().readArray(getContigHideTypeDatasetPath(1_000L)));
    }
  }

  @Test
  void denseBlocksDoNotAllocateSparseDatasetsForAllNonzeroPixels() throws Exception {
    final var mcool = tempDir.resolve("dense-block-source.mcool");
    final var output = tempDir.resolve("dense-block-output.hict.hdf5");

    writeDenseBlockMcool(mcool);

    new McoolToHictConverter().convert(
      new ConversionOptions(
        mcool,
        output,
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

    try (final var reader = HDF5Factory.openForReading(output.toFile())) {
      assertEquals(1L, reader.object().getDataSetInformation(getBlockRowsDatasetPath(1_000L)).getDimensions()[0]);
      assertEquals(1L, reader.object().getDataSetInformation(getBlockColsDatasetPath(1_000L)).getDimensions()[0]);
      assertEquals(1L, reader.object().getDataSetInformation(getBlockValuesDatasetPath(1_000L)).getDimensions()[0]);
      assertArrayEquals(
        new long[]{1L, 1L, 256L, 256L},
        reader.object().getDataSetInformation(getDenseBlockDatasetPath(1_000L)).getDimensions()
      );
    }
  }

  private static void writeSyntheticMcool(final Path path) {
    HDF5LibraryInitializer.initializeHDF5Library();
    try (final var writer = HDF5Factory.open(path.toFile())) {
      writer.object().createGroup("/chroms");
      writer.string().writeArray("/chroms/name", new String[]{"ctgA", "ctgB"});
      writer.int64().writeArray("/chroms/length", new long[]{2_500L, 3_600L});

      writer.object().createGroup("/resolutions");
      writer.object().createGroup("/resolutions/1000");
      writer.object().createGroup("/resolutions/1000/indexes");
      writer.object().createGroup("/resolutions/1000/bins");
      writer.object().createGroup("/resolutions/1000/pixels");

      writer.int64().writeArray("/resolutions/1000/indexes/chrom_offset", new long[]{0L, 3L, 7L});
      writer.int64().writeArray("/resolutions/1000/indexes/bin1_offset", new long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L});
      writer.int64().writeArray("/resolutions/1000/bins/chrom", new long[]{0L, 0L, 0L, 1L, 1L, 1L, 1L});
      writer.int64().writeArray("/resolutions/1000/bins/start", new long[]{0L, 1_000L, 2_000L, 0L, 1_000L, 2_000L, 3_000L});
      writer.int64().writeArray("/resolutions/1000/bins/end", new long[]{1_000L, 2_000L, 2_500L, 1_000L, 2_000L, 3_000L, 3_600L});
      writer.int64().writeArray("/resolutions/1000/pixels/bin1_id", new long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L});
      writer.int64().writeArray("/resolutions/1000/pixels/bin2_id", new long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L});
      writer.int64().writeArray("/resolutions/1000/pixels/count", new long[]{1L, 1L, 1L, 1L, 1L, 1L, 1L});
    }
  }

  private static void writeSyntheticSubResolutionMcool(final Path path) {
    HDF5LibraryInitializer.initializeHDF5Library();
    try (final var writer = HDF5Factory.open(path.toFile())) {
      writer.object().createGroup("/chroms");
      writer.string().writeArray("/chroms/name", new String[]{"tiny", "visible"});
      writer.int64().writeArray("/chroms/length", new long[]{500L, 1_500L});

      writer.object().createGroup("/resolutions");
      writer.object().createGroup("/resolutions/1000");
      writer.object().createGroup("/resolutions/1000/indexes");
      writer.object().createGroup("/resolutions/1000/bins");
      writer.object().createGroup("/resolutions/1000/pixels");

      writer.int64().writeArray("/resolutions/1000/indexes/chrom_offset", new long[]{0L, 1L, 3L});
      writer.int64().writeArray("/resolutions/1000/indexes/bin1_offset", new long[]{0L, 1L, 2L, 3L});
      writer.int64().writeArray("/resolutions/1000/bins/chrom", new long[]{0L, 1L, 1L});
      writer.int64().writeArray("/resolutions/1000/bins/start", new long[]{0L, 0L, 1_000L});
      writer.int64().writeArray("/resolutions/1000/bins/end", new long[]{500L, 1_000L, 1_500L});
      writer.int64().writeArray("/resolutions/1000/pixels/bin1_id", new long[]{0L, 1L, 2L});
      writer.int64().writeArray("/resolutions/1000/pixels/bin2_id", new long[]{0L, 1L, 2L});
      writer.int64().writeArray("/resolutions/1000/pixels/count", new long[]{1L, 1L, 1L});
    }
  }

  private static void writeSyntheticSingleAssemblyChromosomeMcool(final Path path) {
    HDF5LibraryInitializer.initializeHDF5Library();
    try (final var writer = HDF5Factory.open(path.toFile())) {
      writer.object().createGroup("/chroms");
      writer.string().writeArray("/chroms/name", new String[]{"assembly"});
      writer.int64().writeArray("/chroms/length", new long[]{6_100L});

      writer.object().createGroup("/resolutions");
      writer.object().createGroup("/resolutions/1000");
      writer.object().createGroup("/resolutions/1000/indexes");
      writer.object().createGroup("/resolutions/1000/bins");
      writer.object().createGroup("/resolutions/1000/pixels");

      writer.int64().writeArray("/resolutions/1000/indexes/chrom_offset", new long[]{0L, 7L});
      writer.int64().writeArray("/resolutions/1000/indexes/bin1_offset", new long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L});
      writer.int64().writeArray("/resolutions/1000/bins/chrom", new long[]{0L, 0L, 0L, 0L, 0L, 0L, 0L});
      writer.int64().writeArray("/resolutions/1000/bins/start", new long[]{0L, 1_000L, 2_000L, 3_000L, 4_000L, 5_000L, 6_000L});
      writer.int64().writeArray("/resolutions/1000/bins/end", new long[]{1_000L, 2_000L, 3_000L, 4_000L, 5_000L, 6_000L, 6_100L});
      writer.int64().writeArray("/resolutions/1000/pixels/bin1_id", new long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L});
      writer.int64().writeArray("/resolutions/1000/pixels/bin2_id", new long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L});
      writer.int64().writeArray("/resolutions/1000/pixels/count", new long[]{1L, 1L, 1L, 1L, 1L, 1L, 1L});
    }
  }

  private static void writeDenseBlockMcool(final Path path) {
    HDF5LibraryInitializer.initializeHDF5Library();
    try (final var writer = HDF5Factory.open(path.toFile())) {
      final int bins = 256;
      final int pixels = bins * bins;
      final var rows = new long[pixels];
      final var cols = new long[pixels];
      final var counts = new long[pixels];
      final var rowOffsets = new long[bins + 1];
      int cursor = 0;
      for (int row = 0; row < bins; row++) {
        rowOffsets[row] = cursor;
        for (int col = 0; col < bins; col++) {
          rows[cursor] = row;
          cols[cursor] = col;
          counts[cursor] = 1L;
          cursor++;
        }
      }
      rowOffsets[bins] = cursor;

      final var chrom = new long[bins];
      final var starts = new long[bins];
      final var ends = new long[bins];
      for (int i = 0; i < bins; i++) {
        starts[i] = i * 1_000L;
        ends[i] = (i + 1L) * 1_000L;
      }

      writer.object().createGroup("/chroms");
      writer.string().writeArray("/chroms/name", new String[]{"dense"});
      writer.int64().writeArray("/chroms/length", new long[]{bins * 1_000L});

      writer.object().createGroup("/resolutions");
      writer.object().createGroup("/resolutions/1000");
      writer.object().createGroup("/resolutions/1000/indexes");
      writer.object().createGroup("/resolutions/1000/bins");
      writer.object().createGroup("/resolutions/1000/pixels");

      writer.int64().writeArray("/resolutions/1000/indexes/chrom_offset", new long[]{0L, bins});
      writer.int64().writeArray("/resolutions/1000/indexes/bin1_offset", rowOffsets);
      writer.int64().writeArray("/resolutions/1000/bins/chrom", chrom);
      writer.int64().writeArray("/resolutions/1000/bins/start", starts);
      writer.int64().writeArray("/resolutions/1000/bins/end", ends);
      writer.int64().writeArray("/resolutions/1000/pixels/bin1_id", rows);
      writer.int64().writeArray("/resolutions/1000/pixels/bin2_id", cols);
      writer.int64().writeArray("/resolutions/1000/pixels/count", counts);
    }
  }
}
