package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeCpuFeatures;

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
  private static final @NotNull String MINIMAP2_BIN_KEY = "HICT_MINIMAP2_BIN";
  private static final @NotNull String MM2PLUS_AVX2_BIN_KEY = "HICT_MM2PLUS_AVX2_BIN";
  private static final @NotNull String MM2PLUS_AVX512_BIN_KEY = "HICT_MM2PLUS_AVX512_BIN";
  private static final @NotNull String DOTPLOT_ALIGNER_KEY = "HICT_DOTPLOT_ALIGNER";
  private static final @NotNull String COOLER_BIN_KEY = "HICT_COOLER_BIN";
  private static final @NotNull String PYTHON_BIN_KEY = "HICT_PYTHON_BIN";
  private static final @NotNull String BUNDLED_ROOT = "/toolchains";
  private static final @NotNull List<String> HICTK_PATH_CANDIDATES = List.of("hictk", "hictk.exe");
  private static final @NotNull List<String> MINIMAP2_PATH_CANDIDATES = List.of("minimap2", "minimap2.exe");
  private static final @NotNull List<String> MM2PLUS_AVX2_PATH_CANDIDATES = List.of("mm2plus-avx2", "mm2plus-avx2.exe", "mm2plus", "mm2plus.exe");
  private static final @NotNull List<String> MM2PLUS_AVX512_PATH_CANDIDATES = List.of("mm2plus-avx512", "mm2plus-avx512.exe");
  private static final @NotNull List<String> COOLER_PATH_CANDIDATES = List.of("cooler", "cooler.exe", "cooler.bat");
  private static final @NotNull List<String> PYTHON_PATH_CANDIDATES = List.of("python3", "python", "python.exe");
  private static volatile @Nullable String dotplotAlignerPreferenceOverride = null;

  public @NotNull ToolchainStatus inspect() {
    return ToolchainStatus.fromResolved(resolveAnyToolchain(), dotplotAlignerPreference());
  }

  public static @NotNull String dotplotAlignerPreference() {
    final var override = dotplotAlignerPreferenceOverride;
    if (override != null && !override.isBlank()) {
      return override;
    }
    final var configuredPreference = readSetting(DOTPLOT_ALIGNER_KEY);
    if (configuredPreference.isPresent()) {
      return normalizeDotplotAlignerPreference(configuredPreference.get());
    }
    return switch (NativeCpuFeatures.requestedNativeVariantLimit()) {
      case "generic" -> "minimap2";
      case "avx2" -> "mm2plus-avx2";
      case "avx512" -> "mm2plus-avx512";
      default -> "auto";
    };
  }

  public static void setDotplotAlignerPreference(final @Nullable String value) {
    dotplotAlignerPreferenceOverride = normalizeDotplotAlignerPreference(value == null ? "auto" : value);
  }

  private @NotNull ResolvedToolchain resolveAnyToolchain() {
    final var platform = detectPlatform();
    final var explicitToolchainDir = readSetting(TOOLCHAIN_DIR_KEY).map(Path::of).map(Path::toAbsolutePath).map(Path::normalize);

    if (explicitToolchainDir.isPresent()) {
      final var resolved = resolveFromDirectory(platform, explicitToolchainDir.get(), "external");
      if (resolved.hasAnyCommand()) {
        return resolved;
      }
    }

    final var bundled = resolveBundled(platform);
    if (bundled.hasAnyCommand()) {
      return bundled;
    }

    return resolveFromSystemPath(platform);
  }

  public @NotNull ResolvedToolchain requireHictkToolchain() {
    final var toolchain = resolveAnyToolchain();
    if (toolchain.hictkCommand() == null) {
      throw new IllegalStateException(ToolchainStatus.fromResolved(toolchain, dotplotAlignerPreference()).summary());
    }
    return toolchain;
  }

  public @NotNull ResolvedToolchain requireDotplotToolchain() {
    return requireDotplotToolchain(dotplotAlignerPreference());
  }

  public @NotNull ResolvedToolchain requireDotplotToolchain(final @Nullable String requestedPreference) {
    final var preference = normalizeDotplotAlignerPreference(requestedPreference == null ? dotplotAlignerPreference() : requestedPreference);
    final var toolchain = resolveAnyToolchain();
    final var selectedAligner = toolchain.selectedDotplotAlignerCommand(preference);
    if (toolchain.hictkCommand() == null || selectedAligner == null) {
      final var limitations = new ArrayList<String>();
      if (toolchain.hictkCommand() == null) {
        limitations.add("hictk is unavailable");
      }
      if (selectedAligner == null) {
        limitations.add("no dotplot aligner matches preference '" + preference + "'");
      }
      throw new IllegalStateException("Dotplot generation is unavailable because " + String.join(" and ", limitations) + ".");
    }
    return toolchain;
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
    final var minimap2 = firstExisting(
      readSetting(MINIMAP2_BIN_KEY).map(Path::of),
      directory.resolve(isWindows() ? "bin/minimap2.exe" : "bin/minimap2"),
      directory.resolve(isWindows() ? "minimap2.exe" : "minimap2")
    );
    final var mm2PlusAvx2 = firstExisting(
      readSetting(MM2PLUS_AVX2_BIN_KEY).map(Path::of),
      directory.resolve(isWindows() ? "bin/mm2plus-avx2.exe" : "bin/mm2plus-avx2"),
      directory.resolve(isWindows() ? "mm2plus-avx2.exe" : "mm2plus-avx2"),
      directory.resolve(isWindows() ? "bin/mm2plus.exe" : "bin/mm2plus"),
      directory.resolve(isWindows() ? "mm2plus.exe" : "mm2plus")
    );
    final var mm2PlusAvx512 = firstExisting(
      readSetting(MM2PLUS_AVX512_BIN_KEY).map(Path::of),
      directory.resolve(isWindows() ? "bin/mm2plus-avx512.exe" : "bin/mm2plus-avx512"),
      directory.resolve(isWindows() ? "mm2plus-avx512.exe" : "mm2plus-avx512")
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
      minimap2,
      mm2PlusAvx2,
      mm2PlusAvx512,
      cooler,
      python,
      defaultNotices(source),
      defaultCitations(),
      buildLimitations(hictk, minimap2, mm2PlusAvx2, mm2PlusAvx512, cooler, python)
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
    final var minimap2 = firstExisting(
      Optional.<Path>empty(),
      resolveManifestCommand(root, commands, "minimap2", readSetting(MINIMAP2_BIN_KEY)),
      findOnPath(MINIMAP2_PATH_CANDIDATES)
    );
    final var mm2PlusAvx2 = firstExisting(
      Optional.<Path>empty(),
      resolveManifestCommand(root, commands, "mm2plus_avx2", readSetting(MM2PLUS_AVX2_BIN_KEY)),
      findOnPath(MM2PLUS_AVX2_PATH_CANDIDATES)
    );
    final var mm2PlusAvx512 = firstExisting(
      Optional.<Path>empty(),
      resolveManifestCommand(root, commands, "mm2plus_avx512", readSetting(MM2PLUS_AVX512_BIN_KEY)),
      findOnPath(MM2PLUS_AVX512_PATH_CANDIDATES)
    );
    final var cooler = resolveManifestCommand(root, commands, "cooler", readSetting(COOLER_BIN_KEY));
    final var python = resolveManifestCommand(root, commands, "python", readSetting(PYTHON_BIN_KEY));
    return new ResolvedToolchain(
      platform,
      source,
      hictk,
      minimap2,
      mm2PlusAvx2,
      mm2PlusAvx512,
      cooler,
      python,
      notices.isEmpty() ? defaultNotices(source) : notices,
      citations.isEmpty() ? defaultCitations() : citations,
      limitations.isEmpty() ? buildLimitations(hictk, minimap2, mm2PlusAvx2, mm2PlusAvx512, cooler, python) : limitations
    );
  }

  private @NotNull ResolvedToolchain resolveFromSystemPath(final @NotNull String platform) {
    final var hictk = firstExisting(
      readSetting(HICTK_BIN_KEY).map(Path::of),
      findOnPath(HICTK_PATH_CANDIDATES)
    );
    final var minimap2 = firstExisting(
      readSetting(MINIMAP2_BIN_KEY).map(Path::of),
      findOnPath(MINIMAP2_PATH_CANDIDATES)
    );
    final var mm2PlusAvx2 = firstExisting(
      readSetting(MM2PLUS_AVX2_BIN_KEY).map(Path::of),
      findOnPath(MM2PLUS_AVX2_PATH_CANDIDATES)
    );
    final var mm2PlusAvx512 = firstExisting(
      readSetting(MM2PLUS_AVX512_BIN_KEY).map(Path::of),
      findOnPath(MM2PLUS_AVX512_PATH_CANDIDATES)
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
      minimap2,
      mm2PlusAvx2,
      mm2PlusAvx512,
      cooler,
      python,
      defaultNotices("system"),
      defaultCitations(),
      buildLimitations(hictk, minimap2, mm2PlusAvx2, mm2PlusAvx512, cooler, python)
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
    final var archId = switch (arch) {
      case "amd64", "x86_64", "x64" -> "x86_64";
      case "aarch64", "arm64" -> "arm64";
      default -> arch.replaceAll("[^a-z0-9]+", "_");
    };
    if (os.contains("win")) {
      return "windows_" + archId;
    }
    if (os.contains("linux")) {
      return "linux_" + archId;
    }
    if (os.contains("mac") || os.contains("darwin")) {
      return "darwin_" + archId;
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
      "The bundled dotplot workflow can use mm2-plus or minimap2 for self-alignment, then HiCT performs Java/native post-processing.",
      "Bundled third-party payloads must be redistributed together with their license files and scientific citations."
    );
  }

  private static @NotNull List<String> defaultCitations() {
    return List.of(
      "minimap2: Li H. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics. 2018;34(18):3094-3100.",
      "mm2-plus: Ghanshyam Chandra, Md Vasimuddin, Sanchit Misra and Chirag Jain. Accelerating whole-genome alignment in the age of complete genome assemblies. bioRxiv 2024. doi:10.1101/2024.11.25.625328.",
      "hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408."
    );
  }

  private static @NotNull List<String> buildLimitations(final @Nullable Path hictk,
                                                        final @Nullable Path minimap2,
                                                        final @Nullable Path mm2PlusAvx2,
                                                        final @Nullable Path mm2PlusAvx512,
                                                        final @Nullable Path cooler,
                                                        final @Nullable Path python) {
    final var limitations = new ArrayList<String>();
    if (hictk == null) {
      limitations.add("No hictk executable was found. .hic conversion is unavailable in this build until hictk is bundled or installed.");
    }
    if (minimap2 == null && mm2PlusAvx2 == null && mm2PlusAvx512 == null) {
      limitations.add("No minimap2 or mm2-plus executable was found. FASTA self-dotplot generation is unavailable until an aligner is bundled or installed.");
    }
    if (mm2PlusAvx512 != null && !NativeCpuFeatures.supportsAvx512Core()) {
      limitations.add("An mm2-plus AVX-512 executable is present, but this CPU/JVM does not advertise AVX-512F/DQ/BW/VL; auto mode will not execute it.");
    }
    if (hictk != null && cooler == null && python == null) {
      limitations.add("Only the hictk-backed .hic conversion workflow is bundled in this build.");
    }
    return List.copyOf(limitations);
  }

  private static @NotNull String normalizeDotplotAlignerPreference(final @NotNull String raw) {
    final var normalized = raw.trim().toLowerCase(Locale.ROOT).replace('_', '-');
    return switch (normalized) {
      case "", "default", "auto" -> "auto";
      case "minimap2" -> "minimap2";
      case "mm2plus", "mm2-plus" -> "mm2plus";
      case "mm2plus-avx2", "mm2-plus-avx2", "avx2" -> "mm2plus-avx2";
      case "mm2plus-avx512", "mm2-plus-avx512", "avx512", "avx-512" -> "mm2plus-avx512";
      default -> "auto";
    };
  }

  public record ResolvedToolchain(
    @NotNull String platform,
    @NotNull String source,
    @Nullable Path hictkCommand,
    @Nullable Path minimap2Command,
    @Nullable Path mm2PlusAvx2Command,
    @Nullable Path mm2PlusAvx512Command,
    @Nullable Path coolerCommand,
    @Nullable Path pythonCommand,
    @NotNull List<String> notices,
    @NotNull List<String> citations,
    @NotNull List<String> limitations
  ) {
    public boolean hasAnyCommand() {
      return hictkCommand != null
        || minimap2Command != null
        || mm2PlusAvx2Command != null
        || mm2PlusAvx512Command != null
        || coolerCommand != null
        || pythonCommand != null;
    }

    public @Nullable Path selectedDotplotAlignerCommand(final @Nullable String requestedPreference) {
      final var preference = normalizeDotplotAlignerPreference(requestedPreference == null ? dotplotAlignerPreference() : requestedPreference);
      return switch (preference) {
        case "minimap2" -> minimap2Command;
        case "mm2plus-avx2" -> mm2PlusAvx2Command == null ? minimap2Command : mm2PlusAvx2Command;
        case "mm2plus-avx512" -> {
          final var mm2Plus = selectBestMm2Plus("avx512");
          yield mm2Plus == null ? minimap2Command : mm2Plus;
        }
        case "mm2plus" -> selectBestMm2Plus("auto");
        default -> {
          final var mm2Plus = selectBestMm2Plus(NativeCpuFeatures.requestedNativeVariantLimit());
          yield mm2Plus == null ? minimap2Command : mm2Plus;
        }
      };
    }

    public @NotNull String selectedDotplotAlignerName(final @Nullable String requestedPreference) {
      final var selected = selectedDotplotAlignerCommand(requestedPreference);
      if (selected == null) {
        return "none";
      }
      if (Objects.equals(selected, mm2PlusAvx512Command)) {
        return "mm2-plus AVX-512";
      }
      if (Objects.equals(selected, mm2PlusAvx2Command)) {
        return "mm2-plus AVX2";
      }
      if (Objects.equals(selected, minimap2Command)) {
        return "minimap2";
      }
      return selected.getFileName().toString();
    }

    private @Nullable Path selectBestMm2Plus(final @Nullable String rawLimit) {
      for (final var variant : NativeCpuFeatures.preferredNativeVariantOrder(rawLimit)) {
        if (variant.equals("avx512") && mm2PlusAvx512Command != null) {
          return mm2PlusAvx512Command;
        }
        if (variant.equals("avx2") && mm2PlusAvx2Command != null) {
          return mm2PlusAvx2Command;
        }
      }
      return null;
    }
  }

  public record ToolchainStatus(
    @NotNull String platform,
    @NotNull String source,
    boolean supportedPlatform,
    boolean hicConversionAvailable,
    boolean hictkAvailable,
    @Nullable String hictkCommand,
    boolean minimap2Available,
    @Nullable String minimap2Command,
    boolean mm2PlusAvx2Available,
    @Nullable String mm2PlusAvx2Command,
    boolean mm2PlusAvx512Available,
    @Nullable String mm2PlusAvx512Command,
    @NotNull String dotplotAlignerPreference,
    @NotNull String selectedDotplotAligner,
    @Nullable String selectedDotplotAlignerCommand,
    boolean coolerAvailable,
    @Nullable String coolerCommand,
    boolean pythonAvailable,
    @Nullable String pythonCommand,
    @NotNull String summary,
    @NotNull List<String> notices,
    @NotNull List<String> citations,
    @NotNull List<String> limitations
  ) {
    private static @NotNull ToolchainStatus fromResolved(final @NotNull ResolvedToolchain toolchain,
                                                         final @NotNull String dotplotAlignerPreference) {
      final var supportedPlatform = toolchain.platform().startsWith("linux_")
        || toolchain.platform().startsWith("windows_")
        || toolchain.platform().startsWith("darwin_");
      final var hictkAvailable = toolchain.hictkCommand() != null;
      final var minimap2Available = toolchain.minimap2Command() != null;
      final var mm2PlusAvx2Available = toolchain.mm2PlusAvx2Command() != null;
      final var mm2PlusAvx512Available = toolchain.mm2PlusAvx512Command() != null;
      final var selectedDotplotAlignerCommand = toolchain.selectedDotplotAlignerCommand(dotplotAlignerPreference);
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
        minimap2Available,
        minimap2Available ? Objects.requireNonNull(toolchain.minimap2Command()).toString() : null,
        mm2PlusAvx2Available,
        mm2PlusAvx2Available ? Objects.requireNonNull(toolchain.mm2PlusAvx2Command()).toString() : null,
        mm2PlusAvx512Available,
        mm2PlusAvx512Available ? Objects.requireNonNull(toolchain.mm2PlusAvx512Command()).toString() : null,
        dotplotAlignerPreference,
        toolchain.selectedDotplotAlignerName(dotplotAlignerPreference),
        selectedDotplotAlignerCommand == null ? null : selectedDotplotAlignerCommand.toString(),
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
