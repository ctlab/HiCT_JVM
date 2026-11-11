package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ru.itmo.ctlab.hict.hict_library.assembly.*;
import ru.itmo.ctlab.hict.hict_server.tracks.SequenceSearchMetadata;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.function.*;

/** SeqKit adapter: bounded sequence chunks, explicit modes, stable source-anchored BED. */
public final class SequenceSearchRunner {
  public record Options(boolean bothStrands, int mismatches, boolean degenerate, int threads,
                        String forwardColor, String reverseColor) {
    public Options(boolean bothStrands, int mismatches, boolean degenerate, int threads) {
      this(bothStrands, mismatches, degenerate, threads, "220,38,38", "37,99,235");
    }
    public Options {
      if (mismatches < 0 || threads < 1) throw new IllegalArgumentException("Invalid search settings");
      if (mismatches > 0 && degenerate) throw new IllegalArgumentException("Mismatch search does not support IUPAC expansion");
      forwardColor = normalizeRgb(forwardColor, "forward color");
      reverseColor = normalizeRgb(reverseColor, "reverse color");
    }

    private static String normalizeRgb(final String value, final String name) {
      if (value == null || !value.matches("(?:[0-9]{1,3},){2}[0-9]{1,3}"))
        throw new IllegalArgumentException(name + " must be an RGB triplet");
      for (var part : value.split(",")) if (Integer.parseInt(part) > 255)
        throw new IllegalArgumentException(name + " components must be between 0 and 255");
      return value;
    }
  }
  public record Result(Path bed, long hits, long fragments, long elapsedNs) { }

  public Result run(Path executable, Path directory, Path chunkFasta, AssemblySequenceSnapshot snapshot,
                    List<SequenceSearchQueries.Query> queries, Options options,
                    Consumer<String> log, Consumer<Process> processStarted, BooleanSupplier cancelled) throws Exception {
    return run(executable,directory,chunkFasta,snapshot,queries,options,log,processStarted,cancelled,snapshot.fingerprint());
  }
  public Result run(Path executable, Path directory, Path chunkFasta, AssemblySequenceSnapshot snapshot,
                    List<SequenceSearchQueries.Query> queries, Options options,
                    Consumer<String> log, Consumer<Process> processStarted, BooleanSupplier cancelled, String fingerprint) throws Exception {
    long startNs = System.nanoTime();
    for (var query : queries) {
      if (options.mismatches() >= query.sequence().length())
        throw new IllegalArgumentException("Mismatch allowance must be smaller than every query");
      if (options.mismatches() > 0 && !query.sequence().matches("[ACGT]+"))
        throw new IllegalArgumentException("Mismatch queries must use A, C, G and T only");
    }
    Files.createDirectories(directory);
    Path patterns = directory.resolve("queries.fasta"), raw = directory.resolve("seqkit-hits.bed");
    try (var out = Files.newBufferedWriter(patterns, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
      for (var query : queries) out.write(">" + query.id() + "\n" + query.sequence() + "\n");
    }
    var command = new ArrayList<>(List.of(executable.toString(), "locate", "--bed", "--ignore-case",
      "--pattern-file", patterns.toString(), "--max-mismatch", Integer.toString(options.mismatches()),
      "--threads", Integer.toString(options.threads())));
    if (!options.bothStrands()) command.add("--only-positive-strand");
    if (options.degenerate()) command.add("--degenerate");
    command.add(chunkFasta.toString());
    if (cancelled.getAsBoolean()) throw new CancellationException();
    log.accept("Searching overlapping chunks with SeqKit; " + (options.bothStrands() ? "both strands" : "forward strand")
      + ", allowed substitutions: " + options.mismatches());
    var process = new ProcessBuilder(command).redirectOutput(raw.toFile()).start();
    processStarted.accept(process);
    try {
      try (var stderr = process.errorReader(StandardCharsets.UTF_8)) {
        String line;
        while ((line = stderr.readLine()) != null) { log.accept(line); if (cancelled.getAsBoolean()) throw new CancellationException(); }
      }
      int code = process.waitFor();
      if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new CancellationException();
      if (code != 0) throw new IOException("SeqKit exited with status " + code);
    } finally {
      if (process.isAlive()) process.destroyForcibly();
      processStarted.accept(null);
    }
    Path bed = directory.resolve("sequence-search.bed");
    long[] counts = anchorHits(raw, bed, snapshot, queries, fingerprint,
      options.forwardColor(), options.reverseColor());
    log.accept("Search complete: " + counts[0] + " matches, " + counts[1] + " source fragments");
    return new Result(bed, counts[0], counts[1], System.nanoTime() - startNs);
  }

  static long[] anchorHits(Path raw, Path bed, AssemblySequenceSnapshot snapshot,
                           List<SequenceSearchQueries.Query> queries) throws IOException {
    return anchorHits(raw,bed,snapshot,queries,snapshot.fingerprint(),"220,38,38","37,99,235");
  }
  static long[] anchorHits(Path raw, Path bed, AssemblySequenceSnapshot snapshot,
                           List<SequenceSearchQueries.Query> queries, String fingerprint) throws IOException {
    return anchorHits(raw, bed, snapshot, queries, fingerprint, "220,38,38", "37,99,235");
  }
  static long[] anchorHits(Path raw, Path bed, AssemblySequenceSnapshot snapshot,
                           List<SequenceSearchQueries.Query> queries, String fingerprint,
                           String forwardColor, String reverseColor) throws IOException {
    var byId = new HashMap<String, SequenceSearchQueries.Query>();
    queries.forEach(q -> byId.put(q.id(), q));
    String runId = UUID.randomUUID().toString();
    long hits = 0, fragments = 0;
    try (var reader = Files.newBufferedReader(raw, StandardCharsets.UTF_8);
         var writer = Files.newBufferedWriter(bed, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
      writer.write("# HiCT source-anchored sequence search; BED12+1; zero-based half-open coordinates;"
        + " forwardColor=" + forwardColor + "; reverseColor=" + reverseColor + "\n");
      String line;
      while ((line = reader.readLine()) != null) {
        if (Thread.currentThread().isInterrupted()) throw new java.io.InterruptedIOException("Search cancelled");
        if (line.isBlank() || line.startsWith("#")) continue;
        var cells = line.split("\t");
        if (cells.length != 6 || !cells[0].startsWith("hict_chunk_")) throw new IOException("Unexpected SeqKit output");
        var chunk = cells[0].substring("hict_chunk_".length()).split("_");
        if (chunk.length != 2) throw new IOException("Invalid search chunk identifier");
        long offset = Long.parseLong(chunk[0]), ownedEnd = Long.parseLong(chunk[1]);
        long start = Math.addExact(offset, Long.parseLong(cells[1])), end = Math.addExact(offset, Long.parseLong(cells[2]));
        if (offset < 0 || ownedEnd <= offset || ownedEnd > snapshot.length() || start < offset || end <= start || end > snapshot.length())
          throw new IOException("Out-of-bounds SeqKit hit");
        // A hit belongs to exactly one chunk by its forward assembly start, for either strand.
        if (start >= ownedEnd) continue;
        var query = byId.get(cells[3]);
        if (query == null || end - start != query.sequence().length() || !(cells[5].equals("+") || cells[5].equals("-")))
          throw new IOException("SeqKit returned an unexpected query, length, or strand");
        var path = snapshot.slice(start, end);
        var metadata = new SequenceSearchMetadata(runId + ":" + (++hits), query.id(), query.description(), cells[5], path,
          fingerprint, forwardColor, reverseColor);
        String encoded = metadata.encode();
        for (var p : path.fragments()) {
          if (p.source().chars().anyMatch(Character::isWhitespace)) throw new IOException("FASTA identifiers in BED must not contain whitespace");
          String strand = (p.reversed() != cells[5].equals("-")) ? "-" : "+";
          String color = "+".equals(strand) ? forwardColor : reverseColor;
          writer.write(p.source() + "\t" + p.start() + "\t" + p.end() + "\t" + query.id() + "\t0\t" + strand
            + "\t" + p.start() + "\t" + p.end() + "\t" + color + "\t1\t" + p.length() + "\t0\t" + encoded + "\n");
          fragments++;
        }
      }
    }
    return new long[]{hits, fragments};
  }
}
