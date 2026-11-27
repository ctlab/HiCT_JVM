package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSearchQueries;
import ru.itmo.ctlab.hict.hict_server.util.ProcessTree;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** External subject coordinates are retained as BLAST coordinates and never converted to assembly BED. */
public final class RemoteBlastRunner {
  @FunctionalInterface
  interface ProcessStarter {
    Process start(ProcessBuilder builder) throws IOException;
  }

  private final Supplier<NcbiBlastResultRetriever> retrievalFactory;
  private final ProcessStarter processStarter;
  public RemoteBlastRunner() { this(NcbiBlastResultRetriever::new); }
  RemoteBlastRunner(Supplier<NcbiBlastResultRetriever> retrievalFactory) {
    this(retrievalFactory, ProcessBuilder::start);
  }
  RemoteBlastRunner(Supplier<NcbiBlastResultRetriever> retrievalFactory, ProcessStarter processStarter) {
    this.retrievalFactory = Objects.requireNonNull(retrievalFactory);
    this.processStarter = Objects.requireNonNull(processStarter);
  }
  static final long MAX_OUTPUT_BYTES = 32L * 1024 * 1024;
  static final int MAX_REPORT_HITS = 20_000;
  private static final Semaphore REMOTE_CLIENT = new Semaphore(1, true);
  private static long lastStartedNanos;
  private static boolean hasStarted;
  public record Result(Path report, Path raw, long hits, String querySha256) { }

  public Result run(Path executable, Path directory, List<SequenceSearchQueries.Query> queries,
                    RemoteBlastOptions options, Consumer<String> log, Consumer<Process> processStarted,
                    BooleanSupplier cancelled) throws Exception {
    return run(executable, directory, queries, options, log, processStarted, cancelled,
      Duration.ofSeconds(options.timeoutSeconds()), Duration.ofSeconds(10));
  }

  Result run(Path executable, Path directory, List<SequenceSearchQueries.Query> queries,
             RemoteBlastOptions options, Consumer<String> log, Consumer<Process> processStarted,
             BooleanSupplier cancelled, Duration timeout, Duration spacing) throws Exception {
    var queryFile = directory.resolve("queries.fasta");
    var raw = directory.resolve("remote-blast.tsv");
    var diagnostics = directory.resolve("remote-blast.log");
    var report = directory.resolve("remote-blast.json");
    var text = new StringBuilder();
    for (var query : queries) text.append('>').append(query.id()).append('\n').append(query.sequence()).append('\n');
    Files.writeString(queryFile, text, StandardOpenOption.CREATE_NEW);
    var sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.toString().getBytes(StandardCharsets.UTF_8)));
    boolean acquired = false, succeeded = false;
    Process process = null;
    try {
      log.accept(options.requestId() == null
        ? "Waiting for the NCBI remote BLAST client. Only the pasted queries and generated query IDs are uploaded."
        : "Waiting to download the existing NCBI request. Pasted queries remain local for result verification.");
      while (!(acquired = REMOTE_CLIENT.tryAcquire(200, TimeUnit.MILLISECONDS))) checkCancelled(cancelled);
      checkCancelled(cancelled);
      long remaining;
      while (hasStarted && (remaining = spacing.toNanos() - (System.nanoTime() - lastStartedNanos)) > 0) {
        checkCancelled(cancelled);
        TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)));
      }
      if (options.requestId() != null) {
        var result = retrievalFactory.get().retrieve(directory, queries, options, sha, log, cancelled, timeout, () -> {
          lastStartedNanos = System.nanoTime(); hasStarted = true;
        });
        succeeded = true;
        return result;
      }
      var command = command(executable, queryFile, raw, queries, options);
      var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(diagnostics.toFile());
      builder.environment().put("BLAST_USAGE_REPORT", "false");
      var started = Instant.now();
      var deadline = System.nanoTime() + timeout.toNanos();
      lastStartedNanos = System.nanoTime();
      hasStarted = true;
      process = processStarter.start(builder);
      processStarted.accept(process);
      log.accept("NCBI remote BLAST submitted to " + options.database() + ". Waiting for provider results; no progress percentage is available.");
      while (!process.waitFor(200, TimeUnit.MILLISECONDS)) {
        checkCancelled(cancelled);
        if (System.nanoTime() >= deadline) throw new TimeoutException("NCBI BLAST exceeded the selected timeout. The remote request may continue at NCBI; HiCT will not resubmit it automatically.");
        if (Files.exists(raw) && Files.size(raw) > MAX_OUTPUT_BYTES || Files.size(diagnostics) > 1024 * 1024)
          throw new IOException("Remote BLAST output exceeded the bounded result size. Reduce queries or targets and run a new search.");
      }
      checkCancelled(cancelled);
      if (Files.size(diagnostics) > 1024 * 1024) throw new IOException("Remote BLAST diagnostics exceeded the size limit");
      var diagnosticsText = Files.readString(diagnostics);
      if (!diagnosticsText.isBlank()) log.accept(diagnosticsText.substring(0, Math.min(16_384, diagnosticsText.length())));
      if (process.exitValue() != 0) throw new IOException("NCBI remote BLAST failed (exit " + process.exitValue() + "): "
        + diagnosticsText.substring(0, Math.min(2000, diagnosticsText.length())).strip()
        + " Check the connection and database settings before submitting a new search. No automatic resubmission was made.");
      if (!Files.isRegularFile(raw) || Files.size(raw) > MAX_OUTPUT_BYTES) throw new IOException("Remote BLAST returned no usable bounded output");
      var parsed = parse(raw, queries);
      var document = reportDocument(queries, options, parsed, sha, started);
      Files.writeString(report, document.encode(), StandardOpenOption.CREATE_NEW);
      if (Files.size(report) > MAX_OUTPUT_BYTES * 2) throw new IOException("Remote BLAST report exceeded the size limit");
      succeeded = true;
      return new Result(report, raw, parsed.count, sha);
    } finally {
      ProcessTree.stop(process);
      boolean interrupted = Thread.interrupted();
      try { if (process != null) process.waitFor(3, TimeUnit.SECONDS); }
      catch (InterruptedException stopping) { interrupted = true; }
      processStarted.accept(null);
      if (acquired) REMOTE_CLIENT.release();
      try { if (!succeeded) { Files.deleteIfExists(raw); Files.deleteIfExists(report); } }
      finally { if (interrupted) Thread.currentThread().interrupt(); }
    }
  }

  static JsonObject reportDocument(List<SequenceSearchQueries.Query> queries, RemoteBlastOptions options,
                                   Parsed parsed, String sha, Instant started) {
    return new JsonObject().put("format", "hict-remote-blast").put("version", 1)
      .put("provider", "NCBI BLAST+").put("database", options.database()).put("remoteDatabaseName", options.remoteDatabaseName())
      .put("startedAt", started.toString()).put("completedAt", Instant.now().toString())
      .put("querySha256", sha).put("coordinateSystem", "ONE_BASED_INCLUSIVE")
      .put("externalSubjects", true).put("task", task(queries)).put("lowComplexityMasking", true)
      .put("thresholds", new JsonObject().put("evalue", options.alignment().evalue()).put("identity", options.alignment().identity())
          .put("coverage", options.alignment().coverage()).put("maxTargets", options.alignment().maxTargets()))
      .put("queries", new JsonArray(queries.stream().map(q -> new JsonObject().put("id", q.id())
          .put("description", q.description()).put("length", q.sequence().length())).toList()))
      .put("hitCount", parsed.count).put("truncated", parsed.count > MAX_REPORT_HITS).put("hits", parsed.hits);
  }

  static List<String> command(Path executable, Path query, Path raw, List<SequenceSearchQueries.Query> queries, RemoteBlastOptions options) {
    var alignment = options.alignment();
    return List.of(executable.toString(), "-remote", "-db", options.remoteDatabaseName(), "-query", query.toString(),
      "-task", task(queries), "-strand", alignment.bothStrands() ? "both" : "plus", "-evalue", Double.toString(alignment.evalue()),
      "-perc_identity", Double.toString(alignment.identity()), "-qcov_hsp_perc", Double.toString(alignment.coverage()),
      "-max_target_seqs", Integer.toString(alignment.maxTargets()), "-dust", "yes",
      "-outfmt", "6 qseqid sseqid saccver qstart qend sstart send pident length mismatch gapopen evalue bitscore qlen slen stitle",
      "-out", raw.toString());
  }

  private static String task(List<SequenceSearchQueries.Query> queries) {
    return queries.stream().anyMatch(q -> q.sequence().length() < 50) ? "blastn-short" : "blastn";
  }
  private static void checkCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new CancellationException();
  }

  record Parsed(JsonArray hits, long count) { }
  static Parsed parse(Path raw, List<SequenceSearchQueries.Query> queries) throws IOException {
    var byId = new HashMap<String, SequenceSearchQueries.Query>();
    queries.forEach(q -> byId.put(q.id(), q));
    var hits = new JsonArray();
    long count = 0;
    try (var input = Files.newBufferedReader(raw)) {
      for (String line; (line = input.readLine()) != null;) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException();
        if (line.isBlank() || line.startsWith("#")) continue;
        var f = line.split("\t", 16);
        if (f.length != 16) throw new IOException("Unexpected remote BLAST table format");
        try {
          var query = byId.get(f[0]);
          long qs = Long.parseLong(f[3]), qe = Long.parseLong(f[4]), ss = Long.parseLong(f[5]), se = Long.parseLong(f[6]);
          long queryLength = Long.parseLong(f[13]), subjectLength = Long.parseLong(f[14]);
          double identity = Double.parseDouble(f[7]), evalue = Double.parseDouble(f[11]), bits = Double.parseDouble(f[12]);
          long length = Long.parseLong(f[8]), mismatches = Long.parseLong(f[9]), gaps = Long.parseLong(f[10]);
          if (query == null || queryLength != query.sequence().length() || Math.min(qs, qe) < 1 || Math.max(qs, qe) > queryLength
            || subjectLength < 1 || subjectLength > 9007199254740991L || Math.min(ss, se) < 1 || Math.max(ss, se) > subjectLength || length < 1 || length > 9007199254740991L
            || mismatches < 0 || gaps < 0 || mismatches > length || gaps > length || !Double.isFinite(identity) || identity < 0 || identity > 100
            || !Double.isFinite(evalue) || evalue < 0 || !Double.isFinite(bits) || bits < 0 || f[1].isBlank() || f[1].length() > 1000 || f[2].length() > 1000)
            throw new IllegalArgumentException("Invalid HSP values");
          long querySpan = Math.abs(qe - qs) + 1, subjectSpan = Math.abs(se - ss) + 1;
          long gapBases = 2 * length - querySpan - subjectSpan;
          if (length < querySpan || length < subjectSpan || length > querySpan + subjectSpan
            || mismatches > Math.min(querySpan, subjectSpan) || gaps > gapBases
            || (gaps == 0 && gapBases != 0)
            || gaps < (length > querySpan ? 1 : 0) + (length > subjectSpan ? 1 : 0))
            throw new IllegalArgumentException("Inconsistent gapped alignment spans");
          count++;
          if (hits.size() < MAX_REPORT_HITS) hits.add(new JsonObject().put("queryId", f[0]).put("subjectId", f[1])
            .put("accession", f[2]).put("queryStart", qs).put("queryEnd", qe).put("subjectStart", ss).put("subjectEnd", se)
            .put("identity", identity).put("alignmentLength", length).put("mismatches", mismatches).put("gapOpenings", gaps)
            .put("evalue", evalue).put("bitscore", bits).put("queryLength", queryLength).put("subjectLength", subjectLength)
            .put("strand", (ss > se) != (qs > qe) ? "-" : "+").put("title", f[15].substring(0, Math.min(4096, f[15].length()))));
        } catch (RuntimeException invalid) { throw new IOException("Invalid external BLAST HSP: " + invalid.getMessage(), invalid); }
      }
    }
    return new Parsed(hits, count);
  }
}
