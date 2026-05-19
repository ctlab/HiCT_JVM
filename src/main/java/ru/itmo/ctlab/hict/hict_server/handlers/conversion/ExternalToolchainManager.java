package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public final class ExternalToolchainManager {
  private static final @NotNull String TOOLCHAIN_DIR_KEY = "HICT_TOOLCHAIN_DIR";
  private static final @NotNull String HICTK_BIN_KEY = "HICT_HICTK_BIN";
  private static final @NotNull String COOLER_BIN_KEY = "HICT_COOLER_BIN";
  private static final @NotNull String PYTHON_BIN_KEY = "HICT_PYTHON_BIN";
  private static final @NotNull String BUNDLED_ROOT = "/toolchains";
  private static final @NotNull List<String> HICTK_PATH_CANDIDATES = List.of("hictk", "hictk.exe");
  private static final @NotNull List<String> COOLER_PATH_CANDIDATES = List.of("cooler", "cooler.exe", "cooler.bat");
  private static final @NotNull List<String> PYTHON_PATH_CANDIDATES = List.of("python3", "python", "python.exe");

  public @NotNull ToolchainStatus inspect() {
    final var platform = detectPlatform();
    final var explicitToolchainDir = readSetting(TOOLCHAIN_DIR_KEY).map(Path::of).map(Path::toAbsolutePath).map(Path::normalize);

    if (explicitToolchainDir.isPresent()) {
      final var resolved = resolveFromDirectory(platform, explicitToolchainDir.get(), "external");
      if (resolved.hictkCommand() != null || resolved.coolerCommand() != null || resolved.pythonCommand() != null) {
        return ToolchainStatus.fromResolved(resolved);
      }
    }

    final var bundled = resolveBundled(platform);
    if (bundled.hictkCommand() != null || bundled.coolerCommand() != null || bundled.pythonCommand() != null) {
      return ToolchainStatus.fromResolved(bundled);
    }

    return ToolchainStatus.fromResolved(resolveFromSystemPath(platform));
  }

  public @NotNull ResolvedToolchain requireHictkToolchain() {
    final var status = inspect();
    if (!status.hicConversionAvailable()) {
      throw new IllegalStateException(status.summary());
    }
    return new ResolvedToolchain(
      status.platform(),
      status.source(),
      status.hictkCommand() == null || status.hictkCommand().isBlank() ? null : Path.of(status.hictkCommand()),
      status.coolerCommand() == null || status.coolerCommand().isBlank() ? null : Path.of(status.coolerCommand()),
      status.pythonCommand() == null || status.pythonCommand().isBlank() ? null : Path.of(status.pythonCommand()),
      status.notices(),
      status.citations(),
      status.limitations()
    );
  }

  private @NotNull ResolvedToolchain resolveBundled(final @NotNull String platform) {
    final var manifestPath = BUNDLED_ROOT + "/" + platform + "/manifest.json";
    try (final InputStream stream = ExternalToolchainManager.class.getResourceAsStream(manifestPath)) {
      if (stream == null) {
        return new ResolvedToolchain(
          platform,
          "bundled",
          null,
          null,
          null,
          List.of(
            "This build does not ship a bundled external conversion toolchain payload."
          ),
          defaultCitations(),
          List.of(
            "Add a toolchain manifest and payload files under " + BUNDLED_ROOT + "/" + platform + " to enable self-contained .hic conversion."
          )
        );
      }
      final var manifest = new JsonObject(new String(stream.readAllBytes()));
      final var extractionRoot = extractBundledPayload(platform, manifest);
      return resolveFromManifest(platform, extractionRoot, manifest, "bundled");
    } catch (final IOException e) {
      log.warn("Failed to inspect bundled conversion toolchain", e);
      return new ResolvedToolchain(
        platform,
        "bundled",
        null,
        null,
        null,
        List.of("Bundled conversion toolchain metadata could not be read."),
        defaultCitations(),
        List.of(e.getMessage() == null ? "Unknown bundled toolchain error." : e.getMessage())
      );
    }
  }

  private @NotNull Path extractBundledPayload(final @NotNull String platform,
                                              final @NotNull JsonObject manifest) throws IOException {
    final var manifestId = manifest.getString("id", "default");
    final var extractionRoot = Path.of(
      System.getProperty("java.io.tmpdir"),
      "hict-toolchains",
      platform,
      manifestId
    ).toAbsolutePath().normalize();
    Files.createDirectories(extractionRoot);
    final var files = manifest.getJsonArray("files", new JsonArray());
    for (int i = 0; i < files.size(); i++) {
      final var relative = files.getString(i);
      if (relative == null || relative.isBlank()) {
        continue;
      }
      final var target = extractionRoot.resolve(relative).normalize();
      Files.createDirectories(target.getParent());
      if (Files.isRegularFile(target) && Files.size(target) > 0L) {
        if (target.startsWith(extractionRoot.resolve("bin")) && !isWindows()) {
          target.toFile().setExecutable(true, true);
        }
        continue;
      }
      final var resourcePath = BUNDLED_ROOT + "/" + platform + "/" + relative;
      try (final InputStream fileStream = ExternalToolchainManager.class.getResourceAsStream(resourcePath)) {
        if (fileStream == null) {
          throw new IOException("Missing bundled resource " + resourcePath);
        }
        Files.copy(fileStream, target, StandardCopyOption.REPLACE_EXISTING);
      }
      if (target.startsWith(extractionRoot.resolve("bin")) && !isWindows()) {
        target.toFile().setExecutable(true, true);
      }
    }
    return extractionRoot;
  }

  private @NotNull ResolvedToolchain resolveFromDirectory(final @NotNull String platform,
                                                          final @NotNull Path directory,
                                                          final @NotNull String source) {
    final var manifestFile = directory.resolve("manifest.json");
    if (Files.isRegularFile(manifestFile)) {
      try {
        final var manifest = new JsonObject(Files.readString(manifestFile));
        return resolveFromManifest(platform, directory, manifest, source);
      } catch (final IOException e) {
        log.warn("Failed to read external toolchain manifest from {}", manifestFile, e);
      }
    }

    final var hictk = firstExisting(
      readSetting(HICTK_BIN_KEY).map(Path::of),
      directory.resolve(isWindows() ? "bin/hictk.exe" : "bin/hictk"),
      directory.resolve(isWindows() ? "hictk.exe" : "hictk")
    );
    final var cooler = firstExisting(
      readSetting(COOLER_BIN_KEY).map(Path::of),
      directory.resolve(isWindows() ? "bin/cooler.bat" : "bin/cooler"),
      directory.resolve(isWindows() ? "cooler.bat" : "cooler")
    );
    final var python = firstExisting(
      readSetting(PYTHON_BIN_KEY).map(Path::of),
      directory.resolve(isWindows() ? "python/python.exe" : "python/bin/python3"),
      directory.resolve(isWindows() ? "python.exe" : "python3")
    );
    return new ResolvedToolchain(
      platform,
      source,
      hictk,
      cooler,
      python,
      defaultNotices(source),
      defaultCitations(),
      buildLimitations(hictk, cooler, python)
    );
  }

  private @NotNull ResolvedToolchain resolveFromManifest(final @NotNull String platform,
                                                         final @NotNull Path root,
                                                         final @NotNull JsonObject manifest,
                                                         final @NotNull String source) {
    final var commands = manifest.getJsonObject("commands", new JsonObject());
    final var notices = stringList(manifest.getJsonArray("notices"));
    final var citations = stringList(manifest.getJsonArray("citations"));
    final var limitations = stringList(manifest.getJsonArray("limitations"));
    final var hictk = resolveManifestCommand(root, commands, "hictk", readSetting(HICTK_BIN_KEY));
    final var cooler = resolveManifestCommand(root, commands, "cooler", readSetting(COOLER_BIN_KEY));
    final var python = resolveManifestCommand(root, commands, "python", readSetting(PYTHON_BIN_KEY));
    return new ResolvedToolchain(
      platform,
      source,
      hictk,
      cooler,
      python,
      notices.isEmpty() ? defaultNotices(source) : notices,
      citations.isEmpty() ? defaultCitations() : citations,
      limitations.isEmpty() ? buildLimitations(hictk, cooler, python) : limitations
    );
  }

  private @NotNull ResolvedToolchain resolveFromSystemPath(final @NotNull String platform) {
    final var hictk = firstExisting(
      readSetting(HICTK_BIN_KEY).map(Path::of),
      findOnPath(HICTK_PATH_CANDIDATES)
    );
    final var cooler = firstExisting(
      readSetting(COOLER_BIN_KEY).map(Path::of),
      findOnPath(COOLER_PATH_CANDIDATES)
    );
    final var python = firstExisting(
      readSetting(PYTHON_BIN_KEY).map(Path::of),
      findOnPath(PYTHON_PATH_CANDIDATES)
    );
    return new ResolvedToolchain(
      platform,
      "system",
      hictk,
      cooler,
      python,
      defaultNotices("system"),
      defaultCitations(),
      buildLimitations(hictk, cooler, python)
    );
  }

  private static @Nullable Path resolveManifestCommand(final @NotNull Path root,
                                                       final @NotNull JsonObject commands,
                                                       final @NotNull String key,
                                                       final @NotNull Optional<String> explicitOverride) {
    if (explicitOverride.isPresent() && !explicitOverride.get().isBlank()) {
      final var explicit = Path.of(explicitOverride.get()).toAbsolutePath().normalize();
      return Files.exists(explicit) ? explicit : null;
    }
    final var relative = commands.getString(key);
    if (relative == null || relative.isBlank()) {
      return null;
    }
    final var resolved = root.resolve(relative).normalize();
    return isUsableCommand(resolved) ? resolved : null;
  }

  private static @NotNull Optional<String> readSetting(final @NotNull String key) {
    final var systemProperty = System.getProperty(key);
    if (systemProperty != null && !systemProperty.isBlank()) {
      return Optional.of(systemProperty.trim());
    }
    final var env = System.getenv(key);
    if (env != null && !env.isBlank()) {
      return Optional.of(env.trim());
    }
    return Optional.empty();
  }

  private static @Nullable Path findOnPath(final @NotNull List<String> candidates) {
    final var pathValue = System.getenv("PATH");
    if (pathValue == null || pathValue.isBlank()) {
      return null;
    }
    for (final var rawDir : pathValue.split(java.io.File.pathSeparator)) {
      final var dir = Path.of(rawDir);
      for (final var candidate : candidates) {
        final var maybe = dir.resolve(candidate);
        if (isUsableCommand(maybe)) {
          return maybe.toAbsolutePath().normalize();
        }
      }
    }
    return null;
  }

  @SafeVarargs
  private static @Nullable Path firstExisting(final @Nullable Optional<Path> explicit,
                                              final @Nullable Path... candidates) {
    if (explicit != null && explicit.isPresent()) {
      final var normalized = explicit.get().toAbsolutePath().normalize();
      if (isUsableCommand(normalized)) {
        return normalized;
      }
    }
    if (candidates == null) {
      return null;
    }
    for (final var candidate : candidates) {
      if (candidate != null && isUsableCommand(candidate)) {
        return candidate.toAbsolutePath().normalize();
      }
    }
    return null;
  }

  private static @NotNull String detectPlatform() {
    final var os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    final var arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
    final var archId = arch.contains("64") ? "x86_64" : arch;
    if (os.contains("win")) {
      return "windows_" + archId;
    }
    if (os.contains("linux")) {
      return "linux_" + archId;
    }
    return os.replaceAll("[^a-z0-9]+", "_") + "_" + archId;
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
  }

  private static boolean isUsableCommand(final @NotNull Path path) {
    return Files.isRegularFile(path) && (isWindows() || Files.isExecutable(path));
  }

  private static @NotNull List<String> stringList(final @Nullable JsonArray array) {
    if (array == null || array.isEmpty()) {
      return List.of();
    }
    final var result = new ArrayList<String>(array.size());
    for (int i = 0; i < array.size(); i++) {
      final var value = array.getString(i);
      if (value != null && !value.isBlank()) {
        result.add(value);
      }
    }
    return List.copyOf(result);
  }

  private static @NotNull List<String> defaultNotices(final @NotNull String source) {
    return List.of(
      "HiCT orchestrates third-party conversion tools instead of reimplementing .hic ingestion from scratch.",
      "This build currently resolves hictk from the " + source + " toolchain source when available.",
      "The bundled .hic conversion workflow is executed through hictk; a separate cooler or Python runtime is not required for that path.",
      "Bundled third-party payloads must be redistributed together with their license files and scientific citations."
    );
  }

  private static @NotNull List<String> defaultCitations() {
    return List.of(
      "hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408."
    );
  }

  private static @NotNull List<String> buildLimitations(final @Nullable Path hictk,
                                                        final @Nullable Path cooler,
                                                        final @Nullable Path python) {
    final var limitations = new ArrayList<String>();
    if (hictk == null) {
      limitations.add("No hictk executable was found. .hic conversion is unavailable in this build until hictk is bundled or installed.");
    }
    if (hictk != null && cooler == null && python == null) {
      limitations.add("Only the hictk-backed .hic conversion workflow is bundled in this build.");
    }
    return List.copyOf(limitations);
  }

  public record ResolvedToolchain(
    @NotNull String platform,
    @NotNull String source,
    @Nullable Path hictkCommand,
    @Nullable Path coolerCommand,
    @Nullable Path pythonCommand,
    @NotNull List<String> notices,
    @NotNull List<String> citations,
    @NotNull List<String> limitations
  ) {
  }

  public record ToolchainStatus(
    @NotNull String platform,
    @NotNull String source,
    boolean supportedPlatform,
    boolean hicConversionAvailable,
    boolean hictkAvailable,
    @Nullable String hictkCommand,
    boolean coolerAvailable,
    @Nullable String coolerCommand,
    boolean pythonAvailable,
    @Nullable String pythonCommand,
    @NotNull String summary,
    @NotNull List<String> notices,
    @NotNull List<String> citations,
    @NotNull List<String> limitations
  ) {
    private static @NotNull ToolchainStatus fromResolved(final @NotNull ResolvedToolchain toolchain) {
      final var supportedPlatform = toolchain.platform().startsWith("linux_") || toolchain.platform().startsWith("windows_");
      final var hictkAvailable = toolchain.hictkCommand() != null;
      final var coolerAvailable = toolchain.coolerCommand() != null;
      final var pythonAvailable = toolchain.pythonCommand() != null;
      final var summary = hictkAvailable
        ? "External .hic conversion is available through " + toolchain.source() + " hictk command " + toolchain.hictkCommand()
        : "External .hic conversion is unavailable because no hictk executable was found.";
      return new ToolchainStatus(
        toolchain.platform(),
        toolchain.source(),
        supportedPlatform,
        hictkAvailable,
        hictkAvailable,
        hictkAvailable ? Objects.requireNonNull(toolchain.hictkCommand()).toString() : null,
        coolerAvailable,
        coolerAvailable ? Objects.requireNonNull(toolchain.coolerCommand()).toString() : null,
        pythonAvailable,
        pythonAvailable ? Objects.requireNonNull(toolchain.pythonCommand()).toString() : null,
        summary,
        toolchain.notices(),
        toolchain.citations(),
        toolchain.limitations()
      );
    }
  }
}
