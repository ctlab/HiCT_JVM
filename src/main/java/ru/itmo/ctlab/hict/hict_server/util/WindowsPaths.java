package ru.itmo.ctlab.hict.hict_server.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class WindowsPaths {
  private static final Pattern INPUT_EXTENSION =
    Pattern.compile("\\.[A-Za-z0-9]+(?:\\.(?:gz|bz2|zst))?$", Pattern.CASE_INSENSITIVE);
  private static final String INPUT_ERROR = "Conversion could not start. The bundled hictk on Windows cannot open "
    + "file paths with non-ASCII characters, and HiCT could not create a short or linked ASCII path for this file. "
    + "Move the file to a folder whose name uses only English letters and digits, then try again.";

  private final boolean windows;
  private final Function<String, String> environment;
  private final CommandRunner cmd;
  private final HardLink hardLink;

  public WindowsPaths(Function<String, String> environment) {
    this(System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows"),
      environment, WindowsPaths::runCmd, Files::createLink);
  }

  WindowsPaths(boolean windows, Function<String, String> environment, CommandRunner cmd, HardLink hardLink) {
    this.windows = windows;
    this.environment = environment;
    this.cmd = cmd;
    this.hardLink = hardLink;
  }

  public static boolean isAscii(Path path) {
    return path.toString().chars().allMatch(unit -> unit <= 0x7F);
  }

  public Path runtimeTemp(Path requested, Predicate<Path> usable) {
    if (!windows || isAscii(requested)) {
      return usable.test(requested) ? requested : null;
    }
    final var shortPath = shortPath(requested);
    if (shortPath != null && usable.test(shortPath)) {
      return shortPath;
    }
    final var candidates = new ArrayList<Path>();
    final var programData = environmentPath("ProgramData");
    final var drive = systemDriveRoot();
    if (programData != null) candidates.add(programData.resolve("HiCT").resolve("tmp"));
    if (drive != null) candidates.add(drive.resolve("HiCT-tmp"));
    return candidates.stream().filter(WindowsPaths::isAscii).filter(usable).findFirst().orElse(null);
  }

  public Path executable(Path executable) throws IOException {
    if (!windows || executable == null) return executable;
    final var absolute = executable.toAbsolutePath().normalize();
    if (absolute.toString().length() <= 120) return executable;

    final var directory = absolute.getParent();
    final var local = environmentPath("LOCALAPPDATA");
    final var drive = systemDriveRoot();
    final var root = local != null && isAscii(local) ? local.resolve("HiCT").resolve("t")
      : drive == null ? null : drive.resolve("HiCT-t");
    if (root == null || !isAscii(root)) {
      throw new IOException("Could not find an ASCII directory for a short link to " + directory);
    }
    final var junction = root.resolve("%08x".formatted(directory.toString().toLowerCase(Locale.ROOT).hashCode()));
    Files.createDirectories(root);
    if (!Files.exists(junction)) {
      // Another job can create the same junction while this command runs.
      cmd.run("mklink /J \"%HICT_JUNCTION%\" \"%HICT_TARGET%\"",
        Map.of("HICT_JUNCTION", junction.toString(), "HICT_TARGET", directory.toString()));
    }
    if (!Files.isDirectory(junction) || !Files.isSameFile(junction, directory)) {
      throw new IOException("Could not create a short link to " + directory + " at " + junction);
    }
    return junction.resolve(absolute.getFileName());
  }

  public StagedInput hictkInput(Path input, Path processedDirectory) throws IOException {
    if (!windows || isAscii(input)) return new StagedInput(input, null);
    try {
      final var shortPath = shortPath(input);
      if (shortPath != null && Files.isRegularFile(shortPath)) return new StagedInput(shortPath, null);
      final var programData = environmentPath("ProgramData");
      final var root = isAscii(processedDirectory) ? processedDirectory.resolve(".hict-ascii-stage")
        : programData == null ? null : programData.resolve("HiCT").resolve("stage");
      if (root != null && isAscii(root)) {
        Files.createDirectories(root);
        final var directory = Files.createTempDirectory(root, "input-");
        final var extension = INPUT_EXTENSION.matcher(input.getFileName().toString());
        final var linked = directory.resolve(extension.find() ? "input" + extension.group() : "input");
        boolean linkedSuccessfully = false;
        try {
          hardLink.create(linked, input);
          linkedSuccessfully = true;
          return new StagedInput(linked, directory);
        } finally {
          if (!linkedSuccessfully) Files.deleteIfExists(directory);
        }
      }
    } catch (IOException ex) {
      throw new IOException(INPUT_ERROR, ex);
    }
    throw new IOException(INPUT_ERROR);
  }

  private Path shortPath(Path original) {
    if (!Files.exists(original)) return null;
    try {
      final var result = cmd.run("for %I in (\"%HICT_SHORT_PATH%\") do @echo %~sI",
        Map.of("HICT_SHORT_PATH", original.toAbsolutePath().toString())).strip();
      if (result.isEmpty() || result.indexOf('\n') >= 0) return null;
      final var shortPath = Path.of(result);
      return shortPath.isAbsolute() && isAscii(shortPath) && Files.exists(shortPath)
        && Files.isSameFile(shortPath, original) ? shortPath : null;
    } catch (IOException | java.nio.file.InvalidPathException ignored) {
      return null;
    }
  }

  private Path systemDriveRoot() {
    final var drive = environment.apply("SystemDrive");
    return drive != null && drive.matches("[A-Za-z]:") ? Path.of(drive + "\\") : environmentPath("SystemDrive");
  }

  private Path environmentPath(String key) {
    final var value = environment.apply(key);
    if (value == null || value.isBlank()) return null;
    final var path = Path.of(value);
    return path.isAbsolute() ? path : null;
  }

  private static String runCmd(String command, Map<String, String> environment) throws IOException {
    final var systemRoot = System.getenv("SystemRoot");
    final var cmd = systemRoot == null ? "cmd.exe" : Path.of(systemRoot, "System32", "cmd.exe").toString();
    final var builder = new ProcessBuilder(cmd, "/D", "/V:OFF", "/U", "/C", command)
      .redirectErrorStream(true);
    builder.environment().putAll(environment);
    final var process = builder.start();
    try (final var stream = process.getInputStream()) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_16LE);
    }
  }

  @FunctionalInterface
  interface CommandRunner {
    String run(String command, Map<String, String> environment) throws IOException;
  }

  @FunctionalInterface
  interface HardLink {
    Path create(Path link, Path input) throws IOException;
  }

  public record StagedInput(Path path, Path directory) implements AutoCloseable {
    @Override
    public void close() throws IOException {
      if (directory != null) {
        Files.deleteIfExists(path);
        Files.deleteIfExists(directory);
      }
    }
  }
}
