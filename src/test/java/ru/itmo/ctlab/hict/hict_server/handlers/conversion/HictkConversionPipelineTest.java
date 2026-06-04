package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HictkConversionPipelineTest {

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
}
