package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

public record ScaffoldDescriptor(
  long scaffoldId,
  @NonNull @NotNull String scaffoldName,
  long spacerLength
) {
  public static record ScaffoldBordersBP(long startBP, long endBP) {

  }
}
