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
    System.clearProperty("HICT_COOLER_BIN");
    System.clearProperty("HICT_PYTHON_BIN");
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
}
