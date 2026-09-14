package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.itmo.ctlab.hict.hict_library.util.ProcessingThreads;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class BundledToolThreadsTest {
  @ParameterizedTest @ValueSource(strings = {"load", "convert", "merge", "zoomify"})
  void fillsSupportedHictkDefault(String operation) {
    final var command = BundledToolThreads.toolbox(List.of("hictk", operation, "input", "output"), "hictk", s -> {});
    assertEquals("--threads", command.get(2));
    assertTrue(Integer.parseInt(command.get(3)) >= 1);
  }
  @ParameterizedTest @ValueSource(strings = {"ice", "scale"})
  void fillsBalancingDefaultAfterAlgorithm(String algorithm) {
    final var command = BundledToolThreads.toolbox(List.of("hictk", "balance", algorithm, "file"), "hictk", s -> {});
    assertEquals("--threads", command.get(3));
  }
  @ParameterizedTest @ValueSource(strings = {"dump", "metadata", "validate", "fix-mcool", "rename-chromosomes"})
  void leavesSerialCommandsAlone(String operation) {
    final var command = List.of("hictk", operation, "file");
    assertEquals(command, BundledToolThreads.toolbox(command, "hictk", s -> {}));
  }
  @Test void leavesVcAndHelpAlone() {
    for (var command : List.of(List.of("hictk", "balance", "vc", "file"), List.of("hictk", "load", "--help"))) {
      assertEquals(command, BundledToolThreads.toolbox(command, "hictk", s -> {}));
    }
  }
  @Test void preservesExplicitToolboxOverrideAndAppliesManagedSetting() {
    final var previous = System.getProperty("HICT_HICTK_THREADS");
    try {
      System.setProperty("HICT_HICTK_THREADS", "4");
      var command = List.of("hictk", "balance", "ice", "--threads=1", "file");
      assertEquals("1", BundledToolThreads.toolbox(command, "hictk", s -> {}).get(4));
      assertEquals(Integer.toString(Math.min(4, ProcessingThreads.available())), BundledToolThreads.hictk(command, s -> {}).get(4));
    } finally {
      if (previous == null) System.clearProperty("HICT_HICTK_THREADS"); else System.setProperty("HICT_HICTK_THREADS", previous);
    }
  }
  @Test void minimumTwoForLoadOnSingleThreadRequest() {
    assertEquals("2", BundledToolThreads.hictk(List.of("hictk", "load", "-t1", "in", "out"), s -> {}).get(3));
  }
  @Test void leavesVariadicResolutionAndPositionalOrderIntact() {
    final var command = List.of("hictk", "zoomify", "--threads", Integer.toString(Math.min(2, ProcessingThreads.available())), "--resolutions", "50", "100", "input.cool", "output.mcool");
    assertEquals(command, BundledToolThreads.toolbox(command, "hictk", s -> {}));
  }
  @ParameterizedTest @ValueSource(strings = {"minimap2", "mm2plus", "mm2plus-avx2", "mm2plus-avx512"})
  void alignsWithAutoThreadsAndPreservesExplicitShortFlag(String tool) {
    var result = BundledToolThreads.toolbox(List.of(tool, "-t1", "target.fa", "query.fa"), tool, s -> {});
    assertEquals(List.of(tool, "-t", "1", "target.fa", "query.fa"), result);
    result = BundledToolThreads.toolbox(List.of(tool, "target.fa", "query.fa"), tool, s -> {});
    assertEquals("-t", result.get(1));
  }
  @Test void preservesFilenamesAfterOptionTerminator() {
    final var command = List.of("minimap2", "-t1", "--", "-t2", "query.fa");
    assertEquals(List.of("minimap2", "-t", "1", "--", "-t2", "query.fa"), BundledToolThreads.toolbox(command, "minimap2", s -> {}));
  }
}
