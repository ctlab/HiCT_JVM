package ru.itmo.ctlab.hict.fixtures;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Runs as a real child JVM on all supported test hosts. */
public final class ChildProcessFixture {
  public static List<String> command(String... args) throws Exception {
    var bin = Path.of(System.getProperty("java.home"), "bin");
    var java = bin.resolve(Files.isRegularFile(bin.resolve("java.exe")) ? "java.exe" : "java");
    var classes = Path.of(ChildProcessFixture.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    var command = new ArrayList<>(List.of(java.toString(), "-cp", classes.toString(), ChildProcessFixture.class.getName()));
    command.addAll(List.of(args));
    return command;
  }

  public static void main(String[] args) throws Exception {
    switch (args[0]) {
      case "spawn" -> new ProcessBuilder(command("wait")).inheritIO().start().waitFor();
      case "wait" -> Thread.sleep(120_000);
      case "copy-output" -> {
        var arguments = List.of(args);
        Files.copy(Path.of(args[1]), Path.of(arguments.get(arguments.indexOf("-out") + 1)));
      }
      case "fail" -> {
        var arguments = List.of(args);
        Files.writeString(Path.of(arguments.get(arguments.indexOf("-out") + 1)), "partial result");
        System.err.println("Controlled child process failure");
        System.exit(23);
      }
      default -> throw new IllegalArgumentException("Unknown child fixture mode: " + args[0]);
    }
  }
}
