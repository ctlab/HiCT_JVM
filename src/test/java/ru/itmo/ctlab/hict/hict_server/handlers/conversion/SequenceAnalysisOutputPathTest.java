package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SequenceAnalysisOutputPathTest {
  @TempDir Path temporary;

  @Test
  void confinesDurableResultsToDataAndProtectsManagedSidecars() throws Exception {
    final var data = Files.createDirectory(temporary.resolve("data")).toRealPath();
    assertDoesNotThrow(() -> SequenceAnalysisHandlersHolder.validateOutputRequest(
      new JsonObject().put("outputFilename", "results/search.bed"), data, ".bed"));
    assertThrows(IllegalArgumentException.class, () -> SequenceAnalysisHandlersHolder.validateOutputRequest(
      new JsonObject().put("outputFilename", "../outside.bed"), data, ".bed"));
    assertThrows(IllegalArgumentException.class, () -> SequenceAnalysisHandlersHolder.validateOutputRequest(
      new JsonObject().put("outputFilename", "processed/sequence-analysis/result.bed"), data, ".bed"));
    assertThrows(IllegalArgumentException.class, () -> SequenceAnalysisHandlersHolder.validateOutputRequest(
      new JsonObject().put("outputFilename", "results/wrong.txt"), data, ".bed"));
  }

  @Test
  void requiresExplicitOverwriteForExistingResults() throws Exception {
    final var data = Files.createDirectory(temporary.resolve("data-existing")).toRealPath();
    final var target = data.resolve("results/search.bed");
    Files.createDirectories(target.getParent());
    Files.writeString(target, "existing");
    assertThrows(FileAlreadyExistsException.class, () -> SequenceAnalysisHandlersHolder.validateOutputRequest(
      new JsonObject().put("outputFilename", "results/search.bed"), data, ".bed"));
    assertDoesNotThrow(() -> SequenceAnalysisHandlersHolder.validateOutputRequest(
      new JsonObject().put("outputFilename", "results/search.bed").put("overwrite", true), data, ".bed"));
  }
}
