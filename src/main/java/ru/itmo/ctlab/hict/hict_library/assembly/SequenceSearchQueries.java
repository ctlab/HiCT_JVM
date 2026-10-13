package ru.itmo.ctlab.hict.hict_library.assembly;

import java.util.*;

/** Strict input parsing; user headers are data, never command-line arguments. */
public final class SequenceSearchQueries {
  private SequenceSearchQueries() { }
  public record Query(String id, String description, String sequence) { }

  public static List<Query> parse(String input) {
    if (input == null || input.isBlank()) throw new IllegalArgumentException("Enter at least one nucleotide sequence");
    var lines = input.strip().split("\\R");
    boolean fasta = lines[0].startsWith(">");
    var result = new ArrayList<Query>();
    String description = null;
    var bases = new StringBuilder();
    for (var line : lines) {
      if (line.isBlank()) continue;
      if (line.startsWith(">")) {
        if (!fasta) throw new IllegalArgumentException("Do not mix FASTA and one-sequence-per-line input");
        if (description != null) append(result, description, bases.toString());
        description = line.substring(1).strip(); bases.setLength(0);
        if (description.isBlank()) throw new IllegalArgumentException("A FASTA header is empty");
      } else if (fasta) bases.append(line.replaceAll("\\s+", ""));
      else append(result, null, line.replaceAll("\\s+", ""));
    }
    if (fasta) append(result, description, bases.toString());
    return List.copyOf(result);
  }

  private static void append(List<Query> queries, String description, String text) {
    var bases = text.toUpperCase(Locale.ROOT);
    if (bases.isEmpty() || !bases.matches("[ACGTRYSWKMBDHVN]+"))
      throw new IllegalArgumentException("Queries must contain DNA IUPAC letters; empty sequences, gaps and RNA U are not accepted");
    String id = "query_" + (queries.size() + 1);
    queries.add(new Query(id, description == null ? id : description, bases));
  }
}
