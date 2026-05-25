package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExternalToolchainManagerTest {
  @TempDir
  Path tempDir;

  @AfterEach
  void clearToolchainOverrides() {
    System.clearProperty("HICT_TOOLCHAIN_DIR");
    System.clearProperty("HICT_HICTK_BIN");
    System.clearProperty("HICT_MINIMAP2_BIN");
    System.clearProperty("HICT_MM2PLUS_AVX2_BIN");
    System.clearProperty("HICT_MM2PLUS_AVX512_BIN");
    System.clearProperty("HICT_DOTPLOT_ALIGNER");
    System.clearProperty("HICT_COOLER_BIN");
    System.clearProperty("HICT_PYTHON_BIN");
    ExternalToolchainManager.setDotplotAlignerPreference("auto");
  }

  @Test
  void resolvesManifestBackedExternalHictkPayload() throws Exception {
    final var toolchainDir = tempDir.resolve("toolchain");
    final var binDir = toolchainDir.resolve("bin");
    final var docDir = toolchainDir.resolve("share/doc/hictk");
    Files.createDirectories(binDir);
    Files.createDirectories(docDir);

    final var hictk = binDir.resolve(System.getProperty("os.name", "").toLowerCase().contains("win") ? "hictk.exe" : "hictk");
    Files.writeString(hictk, "stub");
    hictk.toFile().setExecutable(true, true);
    Files.writeString(docDir.resolve("build-info.txt"), "build=test");

    Files.writeString(
      toolchainDir.resolve("manifest.json"),
      """
        {
          "id": "test-hictk",
          "commands": {
            "hictk": "%s"
          },
          "files": [
            "%s",
            "share/doc/hictk/build-info.txt"
          ],
          "notices": [
            "test notice"
          ],
          "citations": [
            "test citation"
          ]
        }
        """.formatted(
        toolchainDir.relativize(hictk).toString().replace('\\', '/'),
        toolchainDir.relativize(hictk).toString().replace('\\', '/')
      )
    );

    System.setProperty("HICT_TOOLCHAIN_DIR", toolchainDir.toString());

    final var status = new ExternalToolchainManager().inspect();

    assertTrue(status.hicConversionAvailable());
    assertEquals("external", status.source());
    assertEquals(hictk.toAbsolutePath().normalize().toString(), status.hictkCommand());
    assertTrue(status.notices().contains("test notice"));
    assertTrue(status.citations().contains("test citation"));
    assertTrue(
      status.limitations().stream().noneMatch(limit -> limit.toLowerCase().contains(".hic conversion is unavailable"))
    );
  }

  @Test
  void reportsHictkOnlyBundleWithoutCoolerFailure() throws Exception {
    final var toolchainDir = tempDir.resolve("toolchain-no-manifest");
    final var binDir = toolchainDir.resolve("bin");
    Files.createDirectories(binDir);

    final var hictk = binDir.resolve(System.getProperty("os.name", "").toLowerCase().contains("win") ? "hictk.exe" : "hictk");
    Files.writeString(hictk, "stub");
    hictk.toFile().setExecutable(true, true);

    System.setProperty("HICT_TOOLCHAIN_DIR", toolchainDir.toString());

    final var status = new ExternalToolchainManager().inspect();

    assertTrue(status.hicConversionAvailable());
    assertTrue(
      status.limitations().stream().noneMatch(limit -> limit.toLowerCase().contains(".hic conversion is unavailable"))
    );
    assertTrue(
      status.limitations().stream().anyMatch(limit -> limit.contains("hictk-backed .hic conversion workflow"))
    );
  }

  @Test
  void selectsManifestBackedMm2PlusOrMinimap2ForDotplots() throws Exception {
    final var toolchainDir = tempDir.resolve("toolchain-dotplot");
    final var binDir = toolchainDir.resolve("bin");
    Files.createDirectories(binDir);

    final var exeSuffix = System.getProperty("os.name", "").toLowerCase().contains("win") ? ".exe" : "";
    final var hictk = writeExecutable(binDir.resolve("hictk" + exeSuffix));
    final var minimap2 = writeExecutable(binDir.resolve("minimap2" + exeSuffix));
    final var mm2PlusAvx2 = writeExecutable(binDir.resolve("mm2plus-avx2" + exeSuffix));
    final var mm2PlusAvx512 = writeExecutable(binDir.resolve("mm2plus-avx512" + exeSuffix));

    Files.writeString(
      toolchainDir.resolve("manifest.json"),
      """
        {
          "id": "test-dotplot-toolchain",
          "commands": {
            "hictk": "%s",
            "minimap2": "%s",
            "mm2plus_avx2": "%s",
            "mm2plus_avx512": "%s"
          }
        }
        """.formatted(
        toolchainDir.relativize(hictk).toString().replace('\\', '/'),
        toolchainDir.relativize(minimap2).toString().replace('\\', '/'),
        toolchainDir.relativize(mm2PlusAvx2).toString().replace('\\', '/'),
        toolchainDir.relativize(mm2PlusAvx512).toString().replace('\\', '/')
      )
    );

    System.setProperty("HICT_TOOLCHAIN_DIR", toolchainDir.toString());

    ExternalToolchainManager.setDotplotAlignerPreference("mm2plus-avx2");
    final var mm2PlusStatus = new ExternalToolchainManager().inspect();
    assertEquals("mm2plus-avx2", mm2PlusStatus.dotplotAlignerPreference());
    assertEquals("mm2-plus AVX2", mm2PlusStatus.selectedDotplotAligner());
    assertEquals(mm2PlusAvx2.toAbsolutePath().normalize().toString(), mm2PlusStatus.selectedDotplotAlignerCommand());

    ExternalToolchainManager.setDotplotAlignerPreference("minimap2");
    final var minimap2Status = new ExternalToolchainManager().inspect();
    assertEquals("minimap2", minimap2Status.dotplotAlignerPreference());
    assertEquals("minimap2", minimap2Status.selectedDotplotAligner());
    assertEquals(minimap2.toAbsolutePath().normalize().toString(), minimap2Status.selectedDotplotAlignerCommand());
  }

  private static Path writeExecutable(final Path path) throws Exception {
    Files.writeString(path, "stub");
    path.toFile().setExecutable(true, true);
    return path;
  }
}
