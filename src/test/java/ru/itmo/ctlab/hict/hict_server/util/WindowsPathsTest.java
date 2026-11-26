package ru.itmo.ctlab.hict.hict_server.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WindowsPathsTest {
  @TempDir Path root;

  @Test
  @DisabledOnOs(value = OS.WINDOWS, disabledReason = "Simulating short paths requires symbolic links")
  void runtimePrefersAnAsciiAliasThenProgramDataThenSystemDrive() throws Exception {
    final var requested = Files.createDirectory(root.resolve("Δοκιμή"));
    final var alias = Files.createSymbolicLink(root.resolve("TEST~1"), requested);
    final var environment = Map.of("ProgramData", root.resolve("ProgramData").toString(),
      "SystemDrive", root.resolve("drive").toString());
    final var paths = new WindowsPaths(true, environment::get, (cmd, env) -> {
      assertTrue(cmd.contains("%~sI"));
      assertEquals(requested.toString(), env.get("HICT_SHORT_PATH"));
      return alias + "\r\n";
    }, Files::createLink);
    final var checked = new ArrayList<Path>();
    assertEquals(alias, paths.runtimeTemp(requested, path -> { checked.add(path); return true; }));
    assertEquals(java.util.List.of(alias), checked);

    checked.clear();
    final var driveTemp = root.resolve("drive/HiCT-tmp");
    assertEquals(driveTemp, paths.runtimeTemp(requested, path -> { checked.add(path); return path.equals(driveTemp); }));
    assertEquals(java.util.List.of(alias, root.resolve("ProgramData/HiCT/tmp"), driveTemp), checked);
  }

  @Test
  void nonWindowsLeavesUnicodePathsUnchanged() throws Exception {
    final var requested = Files.createDirectory(root.resolve("Δοκιμή"));
    final var input = Files.writeString(requested.resolve("data.mcool"), "test");
    final var paths = new WindowsPaths(false, key -> null, (command, environment) -> fail("Unexpected command"),
      (link, target) -> fail("Unexpected link"));
    assertEquals(requested, paths.runtimeTemp(requested, candidate -> candidate.equals(requested)));
    try (var staged = paths.hictkInput(input, root)) {
      assertEquals(input, staged.path());
    }
    assertEquals(input, paths.executable(input));
    assertTrue(Files.isRegularFile(input));
  }

  @Test
  void usesAnExistingAsciiAliasForAUnicodeInput() throws Exception {
    final var input = Files.writeString(root.resolve("Δ.mcool"), "test");
    final var alias = Files.createLink(root.resolve("TEST~1.MCO"), input);
    final var paths = new WindowsPaths(true, key -> null, (command, environment) -> alias.toString(),
      (link, target) -> fail("Unexpected hard link"));
    try (var staged = paths.hictkInput(input, root)) {
      assertEquals(alias, staged.path());
    }
    assertTrue(Files.isSameFile(input, alias));
  }

  @Test
  void stagesConcurrentInputsAndRemovesOnlyItsOwnLink() throws Exception {
    final var input = Files.writeString(root.resolve("Δ.mcool"), "test");
    final var processed = root.resolve("processed");
    final var paths = new WindowsPaths(true, key -> null, (command, environment) -> "", Files::createLink);
    Path first;
    try (var second = paths.hictkInput(input, processed)) {
      try (var staged = paths.hictkInput(input, processed)) {
        first = staged.path();
        assertEquals("input.mcool", first.getFileName().toString());
        assertTrue(first.startsWith(processed.resolve(".hict-ascii-stage")));
        assertNotEquals(second.path(), first);
        assertTrue(Files.isSameFile(input, first));
      }
      assertFalse(Files.exists(first));
      assertTrue(Files.isSameFile(input, second.path()));
    }
    assertTrue(Files.isRegularFile(input));
    try (var entries = Files.list(processed.resolve(".hict-ascii-stage"))) {
      assertEquals(0, entries.count());
    }
  }

  @Test
  void usesProgramDataIfTheProcessedDirectoryIsUnicodeAndExplainsLinkFailure() throws Exception {
    final var input = Files.writeString(root.resolve("漢.cool"), "test");
    final var processed = root.resolve("Δοκιμή");
    final var programData = root.resolve("ProgramData");
    final var environment = Map.of("ProgramData", programData.toString());
    final var paths = new WindowsPaths(true, environment::get,
      (command, variables) -> { throw new IOException("short name unavailable"); }, Files::createLink);
    try (var staged = paths.hictkInput(input, processed)) {
      assertTrue(staged.path().startsWith(programData.resolve("HiCT/stage")));
      assertTrue(Files.isSameFile(input, staged.path()));
    }
    final var noLinks = new WindowsPaths(true, environment::get, (command, variables) -> "",
      (link, target) -> { throw new IOException("different volumes"); });
    final var error = assertThrows(IOException.class, () -> noLinks.hictkInput(input, processed));
    assertTrue(error.getMessage().startsWith("Conversion could not start. The bundled hictk on Windows"));
    assertTrue(error.getMessage().endsWith("English letters and digits, then try again."));
    try (var entries = Files.list(programData.resolve("HiCT/stage"))) {
      assertEquals(0, entries.count());
    }
  }

  @Test
  @DisabledOnOs(value = OS.WINDOWS, disabledReason = "Simulating junctions requires symbolic links")
  void junctionIsReusedAndUnicodeLocalAppDataFallsBackToSystemDrive() throws Exception {
    final var toolDirectory = Files.createDirectory(root.resolve("nested folder ".repeat(10) + " & Δ"));
    final var tool = Files.writeString(toolDirectory.resolve("hict-moddotplot.exe"), "test");
    final var calls = new AtomicInteger();
    final WindowsPaths.CommandRunner runner = (cmd, env) -> {
      assertEquals("mklink /J \"%HICT_JUNCTION%\" \"%HICT_TARGET%\"", cmd);
      assertEquals(toolDirectory.toString(), env.get("HICT_TARGET"));
      calls.incrementAndGet();
      Files.createSymbolicLink(Path.of(env.get("HICT_JUNCTION")), toolDirectory);
      return "";
    };
    final var local = root.resolve("local");
    final var drive = root.resolve("drive");
    final var paths = new WindowsPaths(true, Map.of("LOCALAPPDATA", local.toString())::get, runner, Files::createLink);
    final var shortTool = paths.executable(tool);
    assertTrue(shortTool.startsWith(local.resolve("HiCT/t")));
    assertTrue(shortTool.getParent().getFileName().toString().matches("[0-9a-f]{8}"));
    assertTrue(Files.isSameFile(tool, shortTool));
    assertEquals(shortTool, paths.executable(tool));
    assertEquals(1, calls.get());

    final var onDrive = new WindowsPaths(true, Map.of("LOCALAPPDATA", root.resolve("Δ").toString(),
      "SystemDrive", drive.toString())::get, runner, Files::createLink);
    assertTrue(onDrive.executable(tool).startsWith(drive.resolve("HiCT-t")));
    assertEquals(2, calls.get());
  }
}
