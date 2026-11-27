package ru.itmo.ctlab.hict.hict_server.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Build identity shared by the CLI and version endpoint, independent of the data runtime. */
public final class BuildVersion {
  private BuildVersion() {
  }

  public static String version() {
    final var override = System.getProperty("hict.version");
    if (override != null && !override.isBlank()) return override.trim();

    final var bundled = readResource("/version.txt");
    if (!bundled.isBlank()) return bundled;
    try (var reader = Files.newBufferedReader(Path.of("version.txt"))) {
      final var line = reader.readLine();
      return line == null || line.isBlank() ? "unknown" : line.trim();
    } catch (IOException | SecurityException unavailable) {
      return "unknown";
    }
  }

  public static String backendRevision() {
    return revision("/backend-revision.txt");
  }

  public static String webuiRevision() {
    return revision("/webui/webui-revision.txt");
  }

  private static String revision(String path) {
    final var value = readResource(path);
    return value.isBlank() ? "unknown" : value;
  }

  private static String readResource(String path) {
    try (var stream = BuildVersion.class.getResourceAsStream(path)) {
      if (stream == null) return "";
      try (var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        final var line = reader.readLine();
        return line == null ? "" : line.trim();
      }
    } catch (IOException | SecurityException unavailable) {
      return "";
    }
  }
}
