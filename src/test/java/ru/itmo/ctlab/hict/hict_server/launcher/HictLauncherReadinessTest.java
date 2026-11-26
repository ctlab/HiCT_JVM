package ru.itmo.ctlab.hict.hict_server.launcher;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HictLauncherReadinessTest {
  @Test
  void anExitBeforeReadinessIsAlwaysAFailure() {
    final String expected = "HiCT stopped before it was ready. See the log below for the reason.";
    assertEquals(expected, HictLauncherGui.unexpectedExitMessage(0, false));
    assertEquals(expected, HictLauncherGui.unexpectedExitMessage(1, false));
    assertNull(HictLauncherGui.unexpectedExitMessage(0, true));
    assertEquals("Stopped unexpectedly (exit 1). See log.", HictLauncherGui.unexpectedExitMessage(1, true));
  }
}
