package ru.itmo.ctlab.hict.hict_server.tools;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HictCliWindowsArgumentsTest {
  @Test
  void retainsUnicodeAndArgumentBoundaries() {
    final String[] arguments = {
      "convert", "", "C:\\matrix Δ 測試 🧬\\", "literal \"quote\"", "@example", ""
    };
    final var payload = new StringBuilder();
    for (final String argument : arguments) {
      payload.append((char) (argument.length() + 1)).append(argument);
    }
    assertArrayEquals(arguments, HictCli.decodeWindowsLauncherArguments(payload.toString()));
  }

  @Test
  void rejectsMissingAndMalformedPayloads() {
    assertThrows(IllegalArgumentException.class, () -> HictCli.decodeWindowsLauncherArguments(null));
    assertThrows(IllegalArgumentException.class, () -> HictCli.decodeWindowsLauncherArguments(""));
    assertThrows(IllegalArgumentException.class, () -> HictCli.decodeWindowsLauncherArguments("\0"));
    assertThrows(IllegalArgumentException.class, () -> HictCli.decodeWindowsLauncherArguments("\3x"));
  }
}
