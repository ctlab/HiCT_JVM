package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

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
  void automaticZoomifyPyramidIsUsedForSparseCoarseMetadataOnly() {
    assertTrue(HictkConversionPipeline.shouldUseAutomaticZoomifyPyramid(List.of(), List.of(50_000L)));
    assertTrue(HictkConversionPipeline.shouldUseAutomaticZoomifyPyramid(List.of(), List.of(50_000L, 100_000L)));
    assertFalse(
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
}
