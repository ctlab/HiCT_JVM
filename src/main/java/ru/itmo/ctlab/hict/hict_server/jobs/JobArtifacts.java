package ru.itmo.ctlab.hict.hict_server.jobs;

import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.handler.HttpException;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;

/** Job-owned incomplete outputs stay separate until publication succeeds. */
public final class JobArtifacts {
  private static final String STAGING_PREFIX = ".hict-incomplete-";
  private static final java.util.regex.Pattern STAGING_NAME = java.util.regex.Pattern.compile(
    "\\.hict-incomplete-[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}-[0-9]+");

  private JobArtifacts() { }

  public static Path stage(Path directory, String id) throws IOException {
    Files.createDirectories(directory);
    return Files.createTempDirectory(directory, STAGING_PREFIX + id + "-");
  }

  public static boolean isStagingDirectoryName(String name) {
    return name.startsWith(STAGING_PREFIX) && STAGING_NAME.matcher(name).matches();
  }

  public static void requireClosedMap(LocalMap<String, Object> map, Path target) throws IOException {
    final var normalized = target.toAbsolutePath().normalize();
    for (var key : java.util.List.of("chunkedFile", "chunkedFileSecondary")) {
      if (map.get(key) instanceof ShareableWrappers.ChunkedFileWrapper wrapper) {
        final var opened = wrapper.getChunkedFile().getHdfFilePath().toAbsolutePath().normalize();
        if (opened.equals(normalized) || Files.exists(normalized) && Files.exists(opened) &&
          Files.isSameFile(normalized, opened)) {
          throw new HttpException(409, "This map is open. Close it or choose another output name.");
        }
      }
    }
  }

  public static void publish(LocalMap<String, Object> map, Path staged, Path target, boolean overwrite) throws IOException {
    synchronized (map) {
      if (overwrite) requireClosedMap(map, target);
      publish(staged, target, overwrite);
    }
  }

  public static void deleteClosedMap(LocalMap<String, Object> map, Path target) throws IOException {
    synchronized (map) {
      requireClosedMap(map, target);
      Files.deleteIfExists(target);
    }
  }

  public static synchronized void publish(Path staged, Path target, boolean overwrite) throws IOException {
    if (!overwrite) {
      Files.move(staged, target);
      return;
    }
    try {
      Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException unsupported) {
      Files.move(staged, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  public static void remove(Path directory) throws IOException {
    if (directory == null || !Files.exists(directory)) return;
    try (var paths = Files.walk(directory)) {
      for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
    }
  }
}
