package ru.itmo.ctlab.hict.hict_server.jobs;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;

/** Job-owned incomplete outputs stay separate until publication succeeds. */
public final class JobArtifacts {
  private JobArtifacts() { }

  public static Path stage(Path directory, String id) throws IOException {
    Files.createDirectories(directory);
    return Files.createTempDirectory(directory, ".hict-incomplete-" + id + "-");
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
