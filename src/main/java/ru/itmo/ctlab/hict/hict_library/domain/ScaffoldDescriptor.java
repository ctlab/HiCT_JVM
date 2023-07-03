package ru.itmo.ctlab.hict.hict_library.domain;

import org.jetbrains.annotations.NotNull;

public record ScaffoldDescriptor(
  long scaffoldId,
  @NotNull String scaffoldName,
  long spacerLength
) {
  public record ScaffoldBordersBP(long startBP, long endBP) {

  }
}
