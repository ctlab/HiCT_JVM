package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import java.nio.file.Path;
import java.util.List;

/** Tiny letter windows need seeds shorter than the repeat itself. Both profiles retain all chains. */
record AlignmentDetailProfile(String name, int kmer, int window, int chainScore, int seeds, int alignmentScore) {
  static AlignmentDetailProfile forWindows(int rows, int columns) {
    if (rows < 1 || columns < 1 || rows > 4096 || columns > 4096)
      throw new IllegalArgumentException("Alignment windows must contain 1 to 4096 bases");
    return Math.max(rows, columns) <= 64
      ? new AlignmentDetailProfile("short-window-v1", 2, 1, 2, 1, 2)
      : new AlignmentDetailProfile("seeded-window-v1", 7, 3, 12, 2, 12);
  }
  List<String> command(Path aligner, Path rows, Path columns) {
    return List.of(aligner.toString(), "-t", "1", "-k", Integer.toString(kmer), "-w", Integer.toString(window),
      "-m", Integer.toString(chainScore), "-n", Integer.toString(seeds), "-s", Integer.toString(alignmentScore),
      "-P", "--dual=no", "--no-long-join", "-c", "--eqx", rows.toString(), columns.toString());
  }
}
