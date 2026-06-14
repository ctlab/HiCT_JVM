package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.List;

public record HictkLoadOptions(
  Path binTablePath,
  Path chromSizesPath,
  Long binSize,
  boolean oneBased,
  boolean countAsFloat
) {
  public static final @NotNull HictkLoadOptions EMPTY = new HictkLoadOptions(null, null, null, false, false);

  public @NotNull List<Path> dependencyPaths() {
    final var dependencies = new java.util.ArrayList<Path>();
    if (this.binTablePath != null) {
      dependencies.add(this.binTablePath);
    }
    if (this.chromSizesPath != null) {
      dependencies.add(this.chromSizesPath);
    }
    return List.copyOf(dependencies);
  }
}
