package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ru.itmo.ctlab.hict.hict_library.assembly.*;
import ru.itmo.ctlab.hict.hict_server.tracks.SequenceSearchMetadata;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.*;
import java.util.concurrent.CancellationException;

/** Local BLAST+ HSPs: gaps, partial query coverage, both strands and source-anchored coordinates. */
public final class BlastSearchRunner {
  public record Options(boolean bothStrands, double evalue, double identity, double coverage, int maxTargets) {
    public Options {
      if (!Double.isFinite(evalue) || evalue <= 0 || !Double.isFinite(identity) || identity < 0 || identity > 100
          || !Double.isFinite(coverage) || coverage < 0 || coverage > 100 || maxTargets < 1 || maxTargets > 1_000_000)
        throw new IllegalArgumentException("Invalid BLAST thresholds or target limit");
    }
  }
  public SequenceSearchRunner.Result run(Path executable, Path directory, Path subjects, AssemblySequenceSnapshot snapshot,
      List<SequenceSearchQueries.Query> queries, Options options, String fingerprint,
      Consumer<String> log, Consumer<Process> processStarted, BooleanSupplier cancelled) throws Exception {
    long start = System.nanoTime();
    Path queryFile = directory.resolve("queries.fasta"), raw = directory.resolve("blast-hits.tsv");
    try (var out = Files.newBufferedWriter(queryFile, StandardOpenOption.CREATE_NEW)) {
      for (var q : queries) out.write(">" + q.id() + "\n" + q.sequence() + "\n");
    }
    String task = queries.stream().anyMatch(q -> q.sequence().length() < 50) ? "blastn-short" : "blastn";
    var command = List.of(executable.toString(), "-query", queryFile.toString(), "-subject", subjects.toString(),
      "-task", task, "-strand", options.bothStrands() ? "both" : "plus", "-evalue", Double.toString(options.evalue()),
      "-perc_identity", Double.toString(options.identity()), "-qcov_hsp_perc", Double.toString(options.coverage()),
      "-max_target_seqs", Integer.toString(options.maxTargets()), "-dust", "no",
      "-outfmt", "6 qseqid sseqid qstart qend sstart send pident length mismatch gapopen evalue bitscore qlen",
      "-out", raw.toString());
    if (cancelled.getAsBoolean()) throw new CancellationException();
    var builder = new ProcessBuilder(command).redirectErrorStream(true);
    // All sequences stay local, including disabling NCBI usage telemetry.
    builder.environment().put("BLAST_USAGE_REPORT", "false");
    log.accept("Local BLAST+ " + task + "; gapped HSPs, at most " + options.maxTargets() + " subject records per query. No sequence is uploaded.");
    var process = builder.start(); processStarted.accept(process);
    try {
      try (var reader = process.inputReader()) {
        for (String line; (line = reader.readLine()) != null;) { log.accept(line); if (cancelled.getAsBoolean()) throw new CancellationException(); }
      }
      if (process.waitFor() != 0) throw new IOException("BLAST failed; see job log");
      if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new CancellationException();
    } finally { if (process.isAlive()) process.destroyForcibly(); processStarted.accept(null); }
    var bed = directory.resolve("sequence-search.bed");
    long[] counts = anchorHits(raw, bed, snapshot, queries, fingerprint);
    log.accept("BLAST complete: " + counts[0] + " local alignments, " + counts[1] + " source fragments. Raw HSP scores: " + raw.getFileName());
    return new SequenceSearchRunner.Result(bed, counts[0], counts[1], System.nanoTime()-start);
  }
  static long[] anchorHits(Path raw, Path bed, AssemblySequenceSnapshot snapshot,
      List<SequenceSearchQueries.Query> queries, String fingerprint) throws IOException {
    var byId = new HashMap<String, SequenceSearchQueries.Query>(); queries.forEach(q -> byId.put(q.id(),q));
    String run = UUID.randomUUID().toString(); long hits = 0, fragments = 0;
    try (var input = Files.newBufferedReader(raw); var output = Files.newBufferedWriter(bed, StandardOpenOption.CREATE_NEW)) {
      output.write("# HiCT local BLAST alignments; BED12+1; coordinates are subject spans including alignment gaps\n");
      for (String line; (line = input.readLine()) != null;) {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedIOException("BLAST result conversion cancelled");
        if (line.isBlank() || line.startsWith("#")) continue;
        var f = line.split("\t");
        if (f.length != 13 || !f[1].startsWith("hict_chunk_")) throw new IOException("Unexpected BLAST output");
        var q = byId.get(f[0]); var span = f[1].substring(11).split("_");
        if (q == null || span.length != 2) throw new IOException("Unknown BLAST query/subject");
        long offset = Long.parseLong(span[0]), ownedEnd = Long.parseLong(span[1]);
        long qs = Long.parseLong(f[2]), qe = Long.parseLong(f[3]), ss = Long.parseLong(f[4]), se = Long.parseLong(f[5]);
        long a = Math.addExact(offset, Math.min(ss,se)-1), b = Math.addExact(offset,Math.max(ss,se));
        if (Math.min(qs,qe) < 1 || Math.max(qs,qe) > q.sequence().length() || Long.parseLong(f[12]) != q.sequence().length()
            || offset < 0 || a < offset || b <= a || b > ownedEnd || ownedEnd > snapshot.length()) throw new IOException("Out-of-bounds BLAST HSP");
        String strand = (ss > se) != (qs > qe) ? "-" : "+";
        String description = q.description() + "; BLAST query " + qs + "–" + qe + "/" + f[12]
          + ", identity " + f[6] + "%, alignment columns " + f[7] + ", gap openings " + f[9] + ", E=" + f[10] + ", bits=" + f[11];
        var path = snapshot.slice(a,b);
        var metadata = new SequenceSearchMetadata(run+":"+(++hits),q.id(),description,strand,path,fingerprint);
        for (var p : path.fragments()) {
          if (p.source().chars().anyMatch(Character::isWhitespace)) throw new IOException("BED identifiers cannot contain whitespace");
          output.write(p.source()+"\t"+p.start()+"\t"+p.end()+"\t"+q.id()+"\t0\t"+((p.reversed()!=strand.equals("-"))?"-":"+")
            +"\t"+p.start()+"\t"+p.end()+"\t30,110,190\t1\t"+p.length()+"\t0\t"+metadata.encode()+"\n");
          fragments++;
        }
      }
    }
    return new long[]{hits,fragments};
  }
}
