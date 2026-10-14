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
    System.clearProperty("HICT_SEQKIT_BIN");
    System.clearProperty("HICT_MODDOTPLOT_CLI_BIN");
    System.clearProperty("HICT_MODDOTPLOT_BIN");
    ExternalToolchainManager.setDotplotAlignerPreference("auto");
  }

  @Test
  void resolvesDistinctSequenceToolOverrides() throws Exception {
    final var seqkit = writeExecutable(tempDir.resolve("seqkit"));
    final var upstream = writeExecutable(tempDir.resolve("moddotplot"));
    final var adapter = writeExecutable(tempDir.resolve("hict-moddotplot"));
    System.setProperty("HICT_SEQKIT_BIN", seqkit.toString());
    System.setProperty("HICT_MODDOTPLOT_CLI_BIN", upstream.toString());
    System.setProperty("HICT_MODDOTPLOT_BIN", adapter.toString());
    final var manager = new ExternalToolchainManager();
    assertEquals(seqkit.toAbsolutePath(), manager.requireSequenceCommand("seqkit"));
    assertEquals(upstream.toAbsolutePath(), manager.requireSequenceCommand("moddotplot"));
    assertEquals(adapter.toAbsolutePath(), manager.requireSequenceCommand("hict-moddotplot"));
  }

  @Test
  void nestedPayloadCommandsBecomeExecutableAndPathsCannotEscape() throws Exception {
    var oldTemp = System.getProperty("java.io.tmpdir");
    try {
      System.setProperty("java.io.tmpdir", tempDir.toString());
      var manifest = new io.vertx.core.json.JsonObject().put("id", "sequence-test")
        .put("commands", new io.vertx.core.json.JsonObject().put("seqkit", "sequence/hash/bin/seqkit"))
        .put("files", new io.vertx.core.json.JsonArray(java.util.List.of("sequence/hash/bin/seqkit")));
      var root = tempDir.resolve("hict-toolchains/test-platform/" + ExternalToolchainManager.bundledPayloadId(manifest));
      var executable = root.resolve("sequence/hash/bin/seqkit");
      Files.createDirectories(executable.getParent()); Files.writeString(executable, "fixture");
      executable.toFile().setExecutable(false, false);
      var method = ExternalToolchainManager.class.getDeclaredMethod("extractBundledPayload", String.class, io.vertx.core.json.JsonObject.class);
      method.setAccessible(true);
      assertEquals(root, method.invoke(new ExternalToolchainManager(), "test-platform", manifest));
      assertTrue(Files.isExecutable(executable));
      manifest.put("files", new io.vertx.core.json.JsonArray(java.util.List.of("../../../../escape")));
      var error = org.junit.jupiter.api.Assertions.assertThrows(java.lang.reflect.InvocationTargetException.class,
        () -> method.invoke(new ExternalToolchainManager(), "test-platform", manifest));
      assertTrue(error.getCause() instanceof java.io.IOException);
      assertTrue(error.getCause().getMessage().contains("escapes"));
    } finally { restoreSystemProperty("java.io.tmpdir", oldTemp); }
  }

  @Test
  void changedBuildWithSameReleaseLabelCannotReuseOldBinaryCache() {
    var old = new io.vertx.core.json.JsonObject().put("id", "same-release")
      .put("sha256", new io.vertx.core.json.JsonObject().put("bin/minimap2", "old-build"));
    var current = old.copy().put("sha256", new io.vertx.core.json.JsonObject().put("bin/minimap2", "portable-build"));
    org.junit.jupiter.api.Assertions.assertNotEquals(ExternalToolchainManager.bundledPayloadId(old), ExternalToolchainManager.bundledPayloadId(current));
    assertEquals(ExternalToolchainManager.bundledPayloadId(current), ExternalToolchainManager.bundledPayloadId(current.copy()));
  }

  @Test
  void detectsDarwinArm64PlatformWithoutCollapsingToX86() throws Exception {
    final var originalOsName = System.getProperty("os.name");
    final var originalOsArch = System.getProperty("os.arch");
    try {
      System.setProperty("os.name", "Mac OS X");
      System.setProperty("os.arch", "aarch64");

      final var detectPlatform = ExternalToolchainManager.class.getDeclaredMethod("detectPlatform");
      detectPlatform.setAccessible(true);

      assertEquals("darwin_arm64", detectPlatform.invoke(null));
    } finally {
      restoreSystemProperty("os.name", originalOsName);
      restoreSystemProperty("os.arch", originalOsArch);
    }
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

  private static void restoreSystemProperty(final String key, final String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }
}
