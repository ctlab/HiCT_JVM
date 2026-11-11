package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_server.util.ProcessTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HictkConversionPipelineTest {
  @TempDir
  Path tempDir;

  @Test
  void importsMultiResolutionCoolerThroughItsSelectedGroup() throws Exception {
    final var manager = new ExternalToolchainManager();
    org.junit.jupiter.api.Assumptions.assumeTrue(manager.inspect().hictkAvailable(), "hictk payload unavailable on this platform");
    final var toolchain = manager.requireHictkToolchain();
    final var hictk = toolchain.hictkCommand();
    org.junit.jupiter.api.Assumptions.assumeTrue(hictk != null, "hictk payload unavailable on this platform");
    final var chroms = Files.writeString(tempDir.resolve("chrom.sizes"), "test\t12000\n");
    final var pixels = Files.writeString(tempDir.resolve("pixels.tsv"), "0\t0\t10\n0\t1\t5\n1\t1\t10\n2\t2\t10\n");
    final var single = tempDir.resolve("input.cool");
    final var multiple = tempDir.resolve("input.mcool");
    final var log = tempDir.resolve("hictk.log");
    for (var command : List.of(
      List.of(hictk.toString(), "load", pixels.toString(), single.toString(), "--chrom-sizes", chroms.toString(), "--bin-size", "1000", "--format", "coo"),
      List.of(hictk.toString(), "zoomify", single.toString(), multiple.toString(), "--resolutions", "1000", "2000")
    )) {
      final var process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
      try {
        assertTrue(process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS), "hictk fixture creation timed out");
        assertEquals(0, process.exitValue(), Files.readString(log));
      } finally { ProcessTree.stop(process); }
    }
    final var output = tempDir.resolve("output.hict.hdf5");
    new HictkConversionPipeline(manager).convertCoolerToHictWithPyramid(
      new ConversionOptions(
        multiple, output, List.of(1000L, 4000L), 4, 6,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        "", false, 2, true, true, false, false,
        ConversionOptions.ExportMode.AUTO),
      toolchain, ignored -> {}, ignored -> {}, () -> false);
    try (var file = new ChunkedFile(
      new ChunkedFile.ChunkedFileOptions(output, 1, 2))) {
      assertTrue(file.getResolutionsList().containsAll(List.of(1000L, 4000L)));
      assertEquals(12000L, file.getAssemblyInfo().contigs().getFirst().descriptor().getLengthBp());
    }
  }

  @Test
  void resolveTargetResolutionsKeepsAvailableOrderWhenNoFilterRequested() {
    assertEquals(
      List.of(50_000L, 100_000L, 250_000L),
      HictkConversionPipeline.resolveTargetResolutions(
        List.of(),
        List.of(50_000L, 100_000L, 250_000L)
      )
    );
  }

  @Test
  void resolveTargetResolutionsFiltersAndSortsRequestedValues() {
    assertEquals(
      List.of(50_000L, 250_000L),
      HictkConversionPipeline.resolveTargetResolutions(
        List.of(250_000L, 50_000L, 50_000L, 10_000L),
        List.of(50_000L, 100_000L, 250_000L)
      )
    );
  }

  @Test
  void resolveTargetResolutionsRejectsUnavailableExplicitFilter() {
    assertThrows(
      IllegalArgumentException.class,
      () -> HictkConversionPipeline.resolveTargetResolutions(
        List.of(10_000L),
        List.of(50_000L, 100_000L, 250_000L)
      )
    );
  }

  @Test
  void automaticZoomifyPyramidIsUsedUnlessExplicitResolutionsWereRequested() {
    assertTrue(HictkConversionPipeline.shouldUseAutomaticZoomifyPyramid(List.of(), List.of(50_000L)));
    assertTrue(HictkConversionPipeline.shouldUseAutomaticZoomifyPyramid(List.of(), List.of(50_000L, 100_000L)));
    assertTrue(
      HictkConversionPipeline.shouldUseAutomaticZoomifyPyramid(
        List.of(),
        List.of(50_000L, 100_000L, 250_000L, 500_000L)
      )
    );
    assertFalse(
      HictkConversionPipeline.shouldUseAutomaticZoomifyPyramid(
        List.of(50_000L),
        List.of(50_000L)
      )
    );
  }

  @Test
  void pyramidTargetResolutionsUseFinestInputWhenNoExplicitFilterRequested() {
    assertEquals(
      List.of(1_000L),
      HictkConversionPipeline.resolvePyramidTargetResolutions(
        List.of(),
        List.of(1_000L, 100_000L, 10_000_000L)
      )
    );
  }

  @Test
  void pyramidTargetResolutionsKeepFinestInputAndRequestedValues() {
    assertEquals(
      List.of(1_000L, 5_000L, 25_000L),
      HictkConversionPipeline.resolvePyramidTargetResolutions(
        List.of(25_000L, 5_000L, 5_000L),
        List.of(1_000L, 100_000L)
      )
    );
  }

  @Test
  void pyramidTargetResolutionsDropRequestsFinerThanInputBaseResolution() {
    assertEquals(
      List.of(1_000L, 2_000L),
      HictkConversionPipeline.resolvePyramidTargetResolutions(
        List.of(500L, 2_000L),
        List.of(1_000L, 5_000L)
      )
    );
  }

  @Test
  void requestedPyramidGuardRejectsSingleResolutionOutputForSingleResolutionInput() {
    assertThrows(
      IllegalStateException.class,
      () -> HictkConversionPipeline.ensureRequestedPyramidWasGenerated(
        List.of(1_000L),
        List.of(1_000L),
        Path.of("single.mcool")
      )
    );
  }

  @Test
  void requestedPyramidGuardAcceptsGeneratedMultiResolutionOutput() {
    HictkConversionPipeline.ensureRequestedPyramidWasGenerated(
      List.of(1_000L),
      List.of(1_000L, 2_000L, 5_000L),
      Path.of("single.mcool")
    );
  }

  @Test
  void syntheticCooChromSizesUseObservedZeroBasedBinRange() throws Exception {
    final var coo = tempDir.resolve("sample.coo");
    Files.writeString(
      coo,
      """
      # row col count
      0\t0\t1
      0\t2\t3
      2\t2\t4
      """
    );

    final var geometry = HictkConversionPipeline.createSyntheticChromSizes(coo, tempDir, 1L);

    assertEquals(
      List.of("assembly\t3"),
      Files.readAllLines(geometry.chromSizesPath())
    );
    assertEquals(1L, geometry.binSize());
  }

  @Test
  void singletonScaffoldAgpKeepsSealedHicChromosomesAsScaffolds() throws Exception {
    final var agp = HictkConversionPipeline.writeSingletonScaffoldAgp(
      tempDir.resolve("sealed-singleton.agp"),
      new String[]{"CM029348", "CM029349"},
      new long[]{273_000_000L, 42_000_000L}
    );

    assertEquals(
      List.of(
        "CM029348\t1\t273000000\t1\tW\tCM029348\t1\t273000000\t+",
        "CM029349\t1\t42000000\t1\tW\tCM029349\t1\t42000000\t+"
      ),
      Files.readAllLines(agp)
    );
  }
}
