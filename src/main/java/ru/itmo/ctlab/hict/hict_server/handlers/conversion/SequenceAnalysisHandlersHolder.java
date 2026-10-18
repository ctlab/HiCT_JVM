package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.Vertx;
import io.vertx.core.json.*;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.HttpException;
import ru.itmo.ctlab.hict.hict_library.assembly.*;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Jobs use frozen source paths and never hold the assembly scheduler during analysis. */
public final class SequenceAnalysisHandlersHolder extends HandlersHolder implements AutoCloseable {
  private final Vertx vertx;
  private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();
  private final ThreadPoolExecutor workers = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
    new ArrayBlockingQueue<>(8), runnable -> { var t = new Thread(runnable, "hict-sequence-analysis"); t.setDaemon(true); return t; });

  public SequenceAnalysisHandlersHolder(Vertx vertx) { this.vertx = vertx; }

  @Override public void addHandlersToRouter(Router router) {
    router.post("/sequence/region").handler(ctx -> {
      var shared = vertx.sharedData().getLocalMap("hict_server");
      var scheduler = (ShareableWrappers.RequestTaskSchedulerWrapper) shared.get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (scheduler == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      scheduler.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null, () -> {
        boolean secondary = "SECONDARY".equals(shared.get("assemblyInfoSource"));
        var file = (ShareableWrappers.ChunkedFileWrapper) shared.get(secondary ? "chunkedFileSecondary" : "chunkedFile");
        var fasta = (ShareableWrappers.PathWrapper) shared.get(secondary ? "linkedFastaSecondaryPath" : "linkedFastaPrimaryPath");
        if (fasta == null && !secondary) fasta = (ShareableWrappers.PathWrapper) shared.get("linkedFastaPath");
        if (file == null || fasta == null) return new JsonObject().put("linked", false);
        var request = ctx.body().asJsonObject();
        try {
          long start = request.getLong("startBp", -1L), end = request.getLong("endBp", -1L);
          return new JsonObject().put("linked", true).put("startBp", start)
            .put("sequence", file.getChunkedFile().getFastaProcessor().readRegion(fasta.getPath(), start, end));
        } catch (IllegalArgumentException e) { throw new HttpException(400, e.getMessage()); }
      }, result -> ctx.response().putHeader("content-type", "application/json").end(result.encode()));
    });
    router.post("/sequence/settings").handler(ctx -> {
      var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) vertx.sharedData().getLocalMap("hict_server").get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (wrapper == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      wrapper.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null, () -> {
        var request = ctx.body().asJsonObject();
        try {
          if (request != null && request.containsKey("gapLengthBp")) System.setProperty(AssemblyExportSettings.GAP_LENGTH_KEY, Long.toString(gapLength(request)));
          var result = new JsonObject().put("gapLengthBp", AssemblyExportSettings.gapLengthBp())
            .put("blastBundlePlatforms", "Linux x86_64");
          var shared = vertx.sharedData().getLocalMap("hict_server");
          boolean secondary = "SECONDARY".equals(shared.get("assemblyInfoSource"));
          result.put("mapOpen", shared.get(secondary ? "chunkedFileSecondary" : "chunkedFile") != null);
          result.put("fastaLinked", shared.get(secondary ? "linkedFastaSecondaryPath" : "linkedFastaPrimaryPath") != null
            || !secondary && shared.get("linkedFastaPath") != null);
          // Plain gap settings reads do not extract the native payload.
          if (request != null && (request.getBoolean("checkTools", false) || request.getBoolean("checkBlast", false))) {
            var tools = SequenceToolAvailability.inspect(); result.put("tools", tools);
            result.put("blastAvailable", tools.getJsonObject("blastn").getBoolean("available"));
            result.put("blastUnavailableReason", tools.getJsonObject("blastn").getString("reason"));
          }
          return result;
        } catch (IllegalArgumentException e) { throw new HttpException(400,e.getMessage()); }
      }, result -> ctx.response().putHeader("content-type","application/json").end(result.encode()));
    });
    router.post("/sequence/cleanup").handler(ctx -> {
      var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) vertx.sharedData().getLocalMap("hict_server").get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (wrapper == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      wrapper.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null,
        () -> new JsonObject().put("removed", SequenceOverlayCleanup.collect(vertx.sharedData().getLocalMap("hict_server"))),
        result -> ctx.response().putHeader("content-type", "application/json").end(result.encode()));
    });
    router.post("/sequence/search/jobs").handler(ctx -> {
      var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) vertx.sharedData().getLocalMap("hict_server")
        .get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (wrapper == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      wrapper.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null,
        () -> submitSearch(ctx.body().asJsonObject()),
        result -> ctx.response().putHeader("content-type", "application/json").setStatusCode(202).end(result.encode()));
    });
    for (String kind : List.of("moddotplot", "dotplot")) router.post("/sequence/" + kind + "/jobs").handler(ctx -> {
      var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) vertx.sharedData().getLocalMap("hict_server")
        .get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (wrapper == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      wrapper.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null,
        () -> submitOverlay(ctx.body().asJsonObject(), kind.equals("dotplot")),
        result -> ctx.response().putHeader("content-type", "application/json").setStatusCode(202).end(result.encode()));
    });
    router.post("/sequence/jobs/list").handler(ctx -> ctx.response().putHeader("content-type", "application/json")
      .end(new JsonArray(jobs.values().stream().sorted(Comparator.comparingLong(j -> j.created))
        .map(Job::statusJson).toList()).encode()));
    router.post("/sequence/jobs/:jobId/stop").handler(ctx -> {
      var job = jobs.get(ctx.pathParam("jobId"));
      if (job == null) { ctx.fail(new HttpException(404, "Sequence analysis job not found")); return; }
      job.cancel();
      ctx.response().putHeader("content-type", "application/json").end(job.statusJson().encode());
    });
  }

  private JsonObject submitSearch(JsonObject request) throws Exception {
    if (request == null) throw new HttpException(400, "Search settings are required");
    // Absence of a checkbox must never be interpreted as end-user consent to a scope/mode.
    if (!request.containsKey("bothStrands") || !request.containsKey("scope") || !request.containsKey("mode"))
      throw new HttpException(400, "Choose search scope, strands and matching mode explicitly");
    List<SequenceSearchQueries.Query> queries;
    SequenceSearchRunner.Options options;
    try {
      queries = SequenceSearchQueries.parse(request.getString("queries"));
      String mode = request.getString("mode");
      if ("blast".equals(mode)) blastOptions(request);
      if (!List.of("exact", "substitutions", "blast").contains(mode)) throw new IllegalArgumentException("Choose exact, substitutions or BLAST search");
      int mismatches = !mode.equals("substitutions") ? 0 : request.getInteger("mismatches", 1);
      if (mode.equals("substitutions") && mismatches < 1) throw new IllegalArgumentException("Choose at least one allowed substitution");
      var threadSelection = ru.itmo.ctlab.hict.hict_library.util.SequenceProcessingThreads.resolve(
        request.getInteger("threads", -1), "HICT_SEQUENCE_THREADS", Long.MAX_VALUE);
      request.put("requestedThreads", threadSelection.requested()).put("effectiveThreads", threadSelection.effective())
        .put("threadMode", threadSelection.mode());
      options = new SequenceSearchRunner.Options(request.getBoolean("bothStrands"), mismatches,
        mode.equals("exact") && request.getBoolean("degenerate", false), threadSelection.effective(),
        color(request, "forwardColor", "#dc2626"), color(request, "reverseColor", "#2563eb"));
      for (var query : queries) {
        if (mismatches >= query.sequence().length()) throw new IllegalArgumentException("Substitutions must be fewer than query bases");
        if (mismatches > 0 && !query.sequence().matches("[ACGT]+")) throw new IllegalArgumentException("Substitution queries must use A, C, G and T only");
      }
    } catch (IllegalArgumentException invalid) { throw new HttpException(400, invalid.getMessage()); }
    var shared = vertx.sharedData().getLocalMap("hict_server");
    boolean secondary = "SECONDARY".equals(shared.get("assemblyInfoSource"));
    var mapWrapper = (ShareableWrappers.ChunkedFileWrapper) shared.get(secondary ? "chunkedFileSecondary" : "chunkedFile");
    var fastaWrapper = (ShareableWrappers.PathWrapper) shared.get(secondary ? "linkedFastaSecondaryPath" : "linkedFastaPrimaryPath");
    if (fastaWrapper == null && !secondary) fastaWrapper = (ShareableWrappers.PathWrapper) shared.get("linkedFastaPath");
    if (mapWrapper == null || fastaWrapper == null) throw new HttpException(409, "Link the FASTA file for the active map before searching. Use Assembly → Link FASTA, or open the map and FASTA together in the Open Wizard.");
    request = request.copy().put("gapLengthBp", gapLength(request));
    var layout = AssemblySequenceLayout.capture(mapWrapper.getChunkedFile(), gapLength(request));
    var snapshot = layout.sequence();
    request = request.copy().put("assemblyFingerprint", layout.fingerprint(gapLength(request)));
    String scope = request.getString("scope");
    if ("selection".equals(scope)) {
      Long start = request.getLong("startBp"), end = request.getLong("endBp");
      if (start == null || end == null) throw new HttpException(400, "Selection bounds are required");
      try { layout = layout.slice(start, end); snapshot = layout.sequence(); }
      catch (IllegalArgumentException e) { throw new HttpException(400, e.getMessage()); }
    } else if (!"assembly".equals(scope)) throw new HttpException(400, "Unknown search scope");
    if (snapshot.length() == 0) throw new HttpException(400, "The selected sequence is empty");
    var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    var outputRoot = data.resolve("processed/sequence-analysis"); Files.createDirectories(outputRoot);
    if (!outputRoot.toRealPath().startsWith(data)) throw new HttpException(400, "Sequence output directory must remain inside DATA_DIR");
    try { validateOutputRequest(request, data, ".bed"); }
    catch (FileAlreadyExistsException e) { throw new HttpException(409, e.getMessage()); }
    catch (IllegalArgumentException e) { throw new HttpException(400, e.getMessage()); }
    var id = UUID.randomUUID().toString(); var directory = outputRoot.resolve(id); Files.createDirectory(directory);
    var executable = SequenceToolAvailability.require(request.getString("mode").equals("blast") ? "blastn" : "seqkit");
    var fasta = fastaWrapper.getPath().toRealPath();
    var sourceStamp = Stamp.read(fasta);
    var job = new Job(id, directory, data, snapshot, request.copy(), String.valueOf(shared.get(secondary ? "openedSecondaryFilename" : "openedFilename")));
    var finalSnapshot = snapshot;
    var finalLayout = layout;
    var processor = mapWrapper.getChunkedFile().getFastaProcessor();
    jobs.put(id, job);
    try {
      workers.execute(() -> runSearch(job, processor, fasta, sourceStamp, executable, queries, options, finalSnapshot, finalLayout));
    } catch (RejectedExecutionException busy) {
      jobs.remove(id);
      throw new HttpException(429, "The sequence analysis queue is full; wait for a job to finish");
    }
    return job.statusJson();
  }

  private void runSearch(Job job, FASTAProcessor processor, Path fasta, Stamp stamp, Path executable,
                         List<SequenceSearchQueries.Query> queries, SequenceSearchRunner.Options options,
                         AssemblySequenceSnapshot snapshot, AssemblySequenceLayout layout) {
    job.thread = Thread.currentThread();
    String terminalState = "finished";
    try (var log = Files.newBufferedWriter(job.directory.resolve("job.log"), StandardOpenOption.CREATE_NEW)) {
      job.logWriter = log;
      job.checkCancelled(); stamp.check(fasta);
      job.state = "preparing"; job.save();
      job.sourceFasta = fasta.toString(); job.sourceStamp = stamp.toString();
      job.toolSha256 = sha256(executable); job.toolCommand = executable.toString();
      job.log("Captured " + snapshot.length() + " active bases in " + snapshot.fragments().size() + " source fragments; snapshot " + snapshot.fingerprint());
      boolean blast = job.request.getString("mode").equals("blast");
      var blastOptions = blast ? blastOptions(job.request) : null;
      int overlap = queries.stream().mapToInt(q -> q.sequence().length()).max().orElseThrow() - 1;
      int chunk = Math.max(4 * 1024 * 1024, Math.addExact(overlap, 1));
      if (blast) {
        overlap = 0; chunk = Integer.MAX_VALUE;
        for (long bp = 0; bp < snapshot.length(); bp = layout.segmentEnd(bp))
          if (layout.segmentEnd(bp)-bp > Integer.MAX_VALUE) throw new IllegalArgumentException("BLAST subject exceeds 2^31-1 bases; split its scaffold or search a smaller selection");
      }
      var input = job.directory.resolve("assembly-chunks.fasta");
      var digest = java.security.MessageDigest.getInstance("SHA-256");
      try (var out = new java.security.DigestOutputStream(new BufferedOutputStream(Files.newOutputStream(input, StandardOpenOption.CREATE_NEW)), digest)) {
        processor.writeSearchChunks(fasta, out, layout, chunk, overlap);
      }
      job.inputSha256 = HexFormat.of().formatHex(digest.digest());
      stamp.check(fasta); job.checkCancelled();
      job.state = "running"; job.save();
      java.util.function.Consumer<Process> monitor = process -> {
        job.process = process; if (process != null && job.cancelled.get()) process.destroyForcibly();
      };
      var result = blast
        ? new BlastSearchRunner().run(executable, job.directory, input, snapshot, queries, blastOptions,
            job.request.getString("assemblyFingerprint"), job::log, monitor, job.cancelled::get)
        : new SequenceSearchRunner().run(executable, job.directory, input, snapshot, queries, options,
            job::log, monitor, job.cancelled::get, job.request.getString("assemblyFingerprint"));
      stamp.check(fasta); job.checkCancelled();
      var delivered = publishRequestedOutput(result.bed(), job, ".bed");
      job.output = job.data.relativize(delivered).toString().replace('\\', '/');
      job.hits = result.hits(); job.fragments = result.fragments(); job.elapsedNs = result.elapsedNs();
    } catch (Exception failure) {
      terminalState = job.cancelled.get() || failure instanceof InterruptedException || failure instanceof InterruptedIOException || failure instanceof CancellationException ? "cancelled" : "failed";
      job.error = terminalState.equals("failed") ? Objects.toString(failure.getMessage(), failure.getClass().getSimpleName()) : null;
      job.log(job.error == null ? "Sequence analysis cancelled" : "Sequence analysis failed: " + job.error);
    } finally {
      job.logWriter = null; job.process = null; job.thread = null;
      // Publish a terminal response only after its provenance is durable enough to read.
      synchronized (job) {
        job.finished = System.currentTimeMillis(); job.state = terminalState;
        try { job.save(); } catch (IOException failure) {
          job.state = "failed"; job.error = "Could not save analysis provenance: " + failure.getMessage();
        }
      }
      job.snapshot = null; // Source path is now on disk; do not retain a genome per completed job.
      trimCompletedJobs();
      Thread.interrupted();
    }
  }

  private JsonObject submitOverlay(JsonObject request, boolean alignment) throws Exception {
    if (request == null || !(request.getValue("scope") instanceof String) || !(request.getValue("mergeCompatible") instanceof Boolean))
      throw new HttpException(400, "Choose analysis scope and whether to accumulate compatible regions");
    JsonObject settings;
    List<Long> resolutions;
    try {
      if (alignment) {
        var selected = ru.itmo.ctlab.hict.hict_library.util.SequenceProcessingThreads.resolve(
          request.getInteger("threads", -1), "HICT_ALIGNMENT_THREADS", Long.MAX_VALUE);
        request.put("requestedThreads", selected.requested()).put("effectiveThreads", selected.effective()).put("threadMode", selected.mode());
        settings = new JsonObject().put("k", integer(request, "k", 15, 1, 28))
          .put("window", integer(request, "window", 10, 1, 255)).put("chainScore", integer(request, "chainScore", 40, 1, 1_000_000))
          .put("sampleBp", integer(request, "sampleBp", 100, 1, 1_000_000)).put("minAlignmentLength", integer(request, "minAlignmentLength", 100, 1, 100_000_000))
          .put("threads", selected.effective());
      } else {
        settings = new JsonObject().put("k", integer(request, "k", 21, 1, 65535))
          .put("identity", request.getDouble("identity", 80.0)).put("sketch_size", integer(request, "sketchSize", 1000, 1, 1_000_000))
          .put("delta", request.getDouble("delta", 0.5)).put("ambiguous", false);
        double identity = settings.getDouble("identity"), delta = settings.getDouble("delta");
        if (!Double.isFinite(identity) || identity < 0 || identity > 100 || !Double.isFinite(delta) || delta < 0 || delta > 1)
          throw new IllegalArgumentException("Invalid ModDotPlot parameters");
      }
      var values = request.getJsonArray("resolutions");
      if (values == null || values.isEmpty()) throw new IllegalArgumentException("Choose map resolutions for identity windows");
      resolutions = values.stream().map(v -> exactLong(v, "resolution")).distinct().sorted().toList();
    } catch (IllegalArgumentException | ClassCastException e) { throw new HttpException(400, e.getMessage()); }
    var shared = vertx.sharedData().getLocalMap("hict_server");
    boolean secondary = "SECONDARY".equals(shared.get("assemblyInfoSource"));
    var wrapper = (ShareableWrappers.ChunkedFileWrapper) shared.get(secondary ? "chunkedFileSecondary" : "chunkedFile");
    var fastaWrapper = (ShareableWrappers.PathWrapper) shared.get(secondary ? "linkedFastaSecondaryPath" : "linkedFastaPrimaryPath");
    if (fastaWrapper == null && !secondary) fastaWrapper = (ShareableWrappers.PathWrapper) shared.get("linkedFastaPath");
    if (wrapper == null || fastaWrapper == null) throw new HttpException(409, "Link the FASTA file for the active map before generating a dotplot. Use Assembly → Link FASTA, or open the map and FASTA together in the Open Wizard.");
    request = request.copy().put("gapLengthBp", gapLength(request));
    var map = wrapper.getChunkedFile();
    var layout = AssemblySequenceLayout.capture(map, gapLength(request));
    long length = map.getAssemblyInfo().contigs().stream().mapToLong(t -> t.descriptor().getLengthBp()).sum();
    AssemblyAnalysisPlan analysis;
    try {
      AssemblyAnalysisPlan.Region rows, columns;
      if (request.getString("scope").equals("assembly")) { rows = new AssemblyAnalysisPlan.Region(0, length); columns = rows; }
      else if (request.getString("scope").equals("selection")) {
        var bounds = request.getJsonObject("region");
        if (bounds == null) throw new IllegalArgumentException("Selection row and column bounds are required");
        for (String key : List.of("rowStart", "rowEnd", "columnStart", "columnEnd"))
          exactLong(bounds.getValue(key), key);
        rows = new AssemblyAnalysisPlan.Region(bounds.getLong("rowStart"), bounds.getLong("rowEnd"));
        columns = new AssemblyAnalysisPlan.Region(bounds.getLong("columnStart"), bounds.getLong("columnEnd"));
      } else throw new IllegalArgumentException("Unknown analysis scope");
      analysis = AssemblyAnalysisPlan.capture(map, resolutions, rows, columns);
    } catch (IllegalArgumentException | ClassCastException e) { throw new HttpException(400, e.getMessage()); }
    var executable = SequenceToolAvailability.require("hict-moddotplot");
    Path aligner = alignment ? SequenceToolAvailability.requireAligner() : null;
    var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    var root = data.resolve("processed/sequence-analysis"); Files.createDirectories(root);
    if (!root.toRealPath().startsWith(data)) throw new HttpException(400, "Analysis output must remain inside DATA_DIR");
    try { validateOutputRequest(request, data, ".hict.hdf5"); }
    catch (FileAlreadyExistsException e) { throw new HttpException(409, e.getMessage()); }
    catch (IllegalArgumentException e) { throw new HttpException(400, e.getMessage()); }
    var fasta = fastaWrapper.getPath().toRealPath(); var stamp = Stamp.read(fasta);
    var template = map.getHdfFilePath().toRealPath(); var templateStamp = Stamp.read(template);
    var id = UUID.randomUUID().toString(); var directory = root.resolve(id); Files.createDirectory(directory);
    var job = new Job(id, directory, data, analysis.sequence(), request.copy(), template.toString(), alignment ? "dotplot" : "moddotplot");
    var processor = map.getFastaProcessor();
    jobs.put(id, job);
    try { workers.execute(() -> runOverlay(job, processor, fasta, stamp, template, templateStamp, executable, aligner, analysis, layout, settings)); }
    catch (RejectedExecutionException e) { jobs.remove(id); throw new HttpException(429, "Sequence analysis queue is full"); }
    return job.statusJson();
  }

  private void runOverlay(Job job, FASTAProcessor processor, Path fasta, Stamp stamp, Path template,
                             Stamp templateStamp, Path executable, Path aligner, AssemblyAnalysisPlan analysis, AssemblySequenceLayout layout, JsonObject settings) {
    job.thread = Thread.currentThread(); String terminal = "finished";
    try (var log = Files.newBufferedWriter(job.directory.resolve("job.log"), StandardOpenOption.CREATE_NEW)) {
      job.logWriter = log; job.checkCancelled(); stamp.check(fasta); templateStamp.check(template);
      job.state = "preparing"; job.save();
      job.sourceFasta = fasta.toString(); job.sourceStamp = stamp.toString();
      job.toolCommand = executable.toString(); job.toolSha256 = sha256(executable);
      long start = analysis.sequence().length(), end = 0;
      double delta = settings.getDouble("delta", 0.0);
      for (var level : analysis.levels()) {
        for (var axis : List.of(level.rows(), level.columns())) for (var w : axis) {
          start = Math.min(start, Math.max(0L, (long) Math.floor(w.start() - level.resolution() * delta)));
          end = Math.max(end, Math.min(analysis.sequence().length(), (long) Math.ceil((aligner == null ? w.end() : w.ownedEnd()) + level.resolution() * delta)));
        }
      }
      var input = job.directory.resolve("assembly.fasta");
      var digest = java.security.MessageDigest.getInstance("SHA-256");
      try (var out = new java.security.DigestOutputStream(new BufferedOutputStream(Files.newOutputStream(input, StandardOpenOption.CREATE_NEW)), digest)) {
        processor.writeLayout(fasta, out, layout, start, end, "analysis");
      }
      job.inputSha256 = HexFormat.of().formatHex(digest.digest());
      job.checkCancelled(); stamp.check(fasta);
      var levels = new JsonArray();
      for (var level : analysis.levels()) levels.add(new JsonObject().put("resolution", level.resolution())
        .put("rows", windowsJson(level.rows(), aligner != null, layout)).put("columns", windowsJson(level.columns(), aligner != null, layout)));
      String sourceIdentity = fasta + "|" + stamp;
      String gapPolicy = "preserve-agp;separator=" + gapLength(job.request);
      settings = settings.copy(); // Scientific parameters only; execution controls do not invalidate reuse.
      JsonObject execution = new JsonObject();
      if (aligner == null) {
        long tasks = 0;
        for (var level : analysis.levels()) {
          long rowBlocks = (level.rows().size() + 255L) / 256L;
          long columnBlocks = (level.columns().size() + 255L) / 256L;
          tasks = Math.addExact(tasks, Math.multiplyExact(rowBlocks, columnBlocks));
        }
        var selected = ru.itmo.ctlab.hict.hict_library.util.SequenceProcessingThreads.boundToTasks(
          ru.itmo.ctlab.hict.hict_library.util.SequenceProcessingThreads.resolve(
            job.request.getInteger("threads", -1), "HICT_MODDOTPLOT_THREADS", tasks), tasks);
        job.request.put("requestedThreads", selected.requested()).put("effectiveThreads", selected.effective())
          .put("threadMode", selected.mode());
        execution.put("threads", selected.effective()).put("mode", selected.mode());
        job.log("ModDotPlot threads: requested=" + selected.requested() + ", effective=" + selected.effective()
          + ", mode=" + selected.mode() + ", allocatedCPUs=" + ru.itmo.ctlab.hict.hict_library.util.ProcessingThreads.available());
      }

      var keyText = template + "|" + templateStamp + "|" + sourceIdentity + "|" + settings.encode()
        + "|" + gapPolicy + "|" + analysis.levels().stream().map(AssemblyAnalysisPlan.Level::resolution).toList() + "|" + job.kind + "|" + job.toolSha256 + "|" + (aligner == null ? "1616ae94" : sha256(aligner));
      String key = HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(keyText.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      var registry = job.directory.getParent().resolve("overlay-" + key + ".json");
      Path previous = null;
      if (job.request.getBoolean("mergeCompatible") && Files.isRegularFile(registry)) {
        var index = new JsonObject(Files.readString(registry));
        var candidate = job.data.resolve(index.getString("outputFile")).normalize();
        if (!candidate.startsWith(job.directory.getParent()) || !Files.isRegularFile(candidate))
          throw new IOException("Previous overlay is unavailable; its index was retained. Generate a separate overlay or restore the file.");
        previous = candidate;
      }
      var output = job.directory.resolve(job.kind + ".hict.hdf5.pending");
      var published = job.directory.resolve(job.kind + ".hict.hdf5");
      var plan = new JsonObject().put("kind", job.kind).put("aligner", aligner == null ? null : aligner.toString()).put("settings", settings)
        .put("execution", execution).put("template", template.toString()).put("output", output.toString())
        .put("gapPolicy", gapPolicy)
        .put("input", input.toString()).put("inputOffset", layout.padded(start, true)).put("inputLength", layout.padded(end, false) - layout.padded(start, true))
        .put("sequenceLength", layout.length()).put("sourceFastaIdentity", sourceIdentity)
        .put("inputSha256", job.inputSha256).put("snapshot", analysis.sequence().fingerprint())
        .put("sourceFragments", fragmentsJson(analysis.sequence())).put("levels", levels);
      if (previous != null) plan.put("previous", previous.toString());
      var planFile = job.directory.resolve("analysis-plan.json"); Files.writeString(planFile, plan.encodePrettily(), StandardOpenOption.CREATE_NEW);
      job.log("Dotplots use source-bin anchored windows in the captured active assembly. Existing scores move with edits; changed joins are not silently recomputed.");
      job.log(previous == null ? "Creating a separate overlay version" : "Accumulating compatible regions; reclaiming the superseded matrix after successful publication and detach");
      job.state = "running"; job.save();
      long timer = System.nanoTime();
      var process = new ProcessBuilder(executable.toString(), "--plan", planFile.toString()).redirectErrorStream(true).start();
      job.process = process;
      try {
        job.checkCancelled();
        try (var lines = new BufferedReader(new InputStreamReader(process.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
          for (String line; (line = lines.readLine()) != null;) { job.checkCancelled(); job.log(line); }
        }
        int code = process.waitFor();
        if (code != 0) throw new IOException("Dotplot adapter exited with code " + code + "; see job log");
      } finally { if (process.isAlive()) { process.descendants().forEach(ProcessHandle::destroy); process.destroyForcibly(); } }
      stamp.check(fasta); templateStamp.check(template); job.checkCancelled();
      if (!Files.isRegularFile(output)) throw new IOException("Dotplot did not create its sidecar");
      job.elapsedNs = System.nanoTime() - timer;
      try { Files.move(output, published, StandardCopyOption.ATOMIC_MOVE); }
      catch (AtomicMoveNotSupportedException e) { Files.move(output, published); }
      var delivered = publishRequestedOutput(published, job, ".hict.hdf5");
      job.output = job.data.relativize(delivered).toString().replace('\\', '/');
      var managedOutput = job.data.relativize(published).toString().replace('\\', '/');
      atomicJson(registry, new JsonObject().put("outputFile", managedOutput).put("jobId", job.id).put("settings", settings));
      job.log("Saved overlay: " + job.output + ". Generation is complete; display attachment is a separate step.");
      if (previous != null) atomicJson(job.directory.getParent().resolve("superseded-" + job.id + ".json"),
        new JsonObject().put("previous", job.data.relativize(previous).toString().replace('\\', '/')).put("replacement", job.output));
    } catch (Exception failure) {
      terminal = job.cancelled.get() || failure instanceof InterruptedException || failure instanceof InterruptedIOException || failure instanceof CancellationException ? "cancelled" : "failed";
      job.error = terminal.equals("failed") ? Objects.toString(failure.getMessage(), failure.getClass().getSimpleName()) : null;
      job.log(job.error == null ? "Dotplot cancelled; previous overlay retained" : "Dotplot failed: " + job.error);
    } finally {
      job.logWriter = null; job.process = null; job.thread = null;
      synchronized (job) {
        job.finished = System.currentTimeMillis(); job.state = terminal;
        try { job.save(); } catch (IOException e) { job.state = "failed"; job.error = "Could not save analysis provenance: " + e.getMessage(); }
      }
      job.snapshot = null; // Source path is now on disk; do not retain a genome per completed job.
      trimCompletedJobs();
      Thread.interrupted();
    }
  }
  private void trimCompletedJobs() {
    jobs.values().stream().filter(j -> j.finished > 0).sorted(Comparator.comparingLong((Job j) -> j.finished).reversed())
      .skip(32).forEach(j -> jobs.remove(j.id, j));
  }

  private static long exactLong(Object value, String name) {
    if (!(value instanceof Number n) || !Double.isFinite(n.doubleValue()) || n.doubleValue() != n.longValue())
      throw new IllegalArgumentException(name + " must be an integer");
    return n.longValue();
  }
  private static int integer(JsonObject request, String name, int fallback, int min, int max) {
    long value = exactLong(request.getValue(name, fallback), name);
    if (value < min || value > max) throw new IllegalArgumentException(name + " outside allowed range");
    return (int) value;
  }
  private static JsonArray windowsJson(List<AssemblyAnalysisPlan.Window> windows, boolean alignment, AssemblySequenceLayout layout) {
    return new JsonArray(windows.stream().map(w -> {
      long left = layout.segmentStart(w.start()), right = layout.segmentEnd(w.start());
      var row = new JsonArray().add(w.stripe()).add(w.bin()).add(layout.padded(w.start(), true))
        .add(layout.padded(Math.min(right, alignment ? w.ownedEnd() : w.end()), false));
      if (!alignment) row.add(layout.padded(left, true)).add(layout.padded(right, false));
      return row;
    }).toList());
  }
  private static BlastSearchRunner.Options blastOptions(JsonObject request) {
    return new BlastSearchRunner.Options(request.getBoolean("bothStrands"), request.getDouble("evalue", 10.0),
      request.getDouble("identity", 70.0), request.getDouble("coverage", 50.0), integer(request,"maxTargets",500,1,1_000_000),
      color(request, "forwardColor", "#dc2626"), color(request, "reverseColor", "#2563eb"));
  }

  private static String color(JsonObject request, String key, String fallback) {
    String value = request.getString(key, fallback);
    if (value == null || !value.matches("#[0-9a-fA-F]{6}")) throw new IllegalArgumentException(key + " must be a six-digit color");
    return Integer.parseInt(value.substring(1, 3), 16) + "," + Integer.parseInt(value.substring(3, 5), 16)
      + "," + Integer.parseInt(value.substring(5, 7), 16);
  }

  static void validateOutputRequest(JsonObject request, Path data, String extension) throws IOException {
    String name = request.getString("outputFilename", "").trim();
    if (name.isEmpty()) return;
    if (!name.toLowerCase(Locale.ROOT).endsWith(extension)) throw new IllegalArgumentException("Saved output must end with " + extension);
    Path target = data.resolve(name).normalize();
    if (Path.of(name).isAbsolute() || !target.startsWith(data) || target.startsWith(data.resolve("processed/sequence-analysis")))
      throw new IllegalArgumentException("Saved output must be a relative path under DATA_DIR and outside managed sidecars");
    Files.createDirectories(target.getParent());
    if (!target.getParent().toRealPath().startsWith(data) || Files.isSymbolicLink(target))
      throw new IllegalArgumentException("Saved output path must remain inside DATA_DIR");
    if (Files.exists(target) && !request.getBoolean("overwrite", false))
      throw new FileAlreadyExistsException("Output already exists: " + name + ". Enable overwrite or choose another name.");
  }

  private static Path publishRequestedOutput(Path source, Job job, String extension) throws IOException {
    String name = job.request.getString("outputFilename", "").trim();
    if (name.isEmpty()) return source;
    validateOutputRequest(job.request, job.data, extension);
    Path target = job.data.resolve(name).normalize();
    Path pending = target.resolveSibling(target.getFileName() + ".pending-" + job.id);
    Files.copy(source, pending, StandardCopyOption.REPLACE_EXISTING);
    var options = job.request.getBoolean("overwrite", false)
      ? new StandardCopyOption[]{StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING}
      : new StandardCopyOption[]{StandardCopyOption.ATOMIC_MOVE};
    try { Files.move(pending, target, options); }
    catch (AtomicMoveNotSupportedException e) {
      if (job.request.getBoolean("overwrite", false)) Files.move(pending, target, StandardCopyOption.REPLACE_EXISTING);
      else Files.move(pending, target);
    } finally { Files.deleteIfExists(pending); }
    job.log("Published reusable DATA artifact: " + name);
    return target;
  }
  private static long gapLength(JsonObject request) {
    long value = exactLong(request.getValue("gapLengthBp", AssemblyExportSettings.gapLengthBp()), "gapLengthBp");
    if (value < 0 || value > 1_000_000) throw new IllegalArgumentException("Scaffold separator must be between 0 and 1,000,000 Ns");
    return value;
  }
  private static JsonArray fragmentsJson(AssemblySequenceSnapshot snapshot) {
    return new JsonArray(snapshot.fragments().stream().map(f -> new JsonObject().put("source", f.source())
      .put("start", f.start()).put("end", f.end()).put("reversed", f.reversed())).toList());
  }
  private static void atomicJson(Path path, JsonObject value) throws IOException {
    var temp = path.resolveSibling(path.getFileName() + ".pending"); Files.writeString(temp, value.encodePrettily());
    try { Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
    catch (AtomicMoveNotSupportedException e) { Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING); }
  }

  private record Stamp(long size, java.nio.file.attribute.FileTime modified, Object key) {
    static Stamp read(Path path) throws IOException {
      var a = Files.readAttributes(path, BasicFileAttributes.class); return new Stamp(a.size(), a.lastModifiedTime(), a.fileKey());
    }
    void check(Path path) throws IOException { if (!equals(read(path))) throw new IOException("Linked FASTA changed during the analysis; rerun using a stable source"); }
  }

  private static String sha256(Path path) throws Exception {
    var digest = java.security.MessageDigest.getInstance("SHA-256");
    try (var input = new java.security.DigestInputStream(Files.newInputStream(path), digest)) { input.transferTo(OutputStream.nullOutputStream()); }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static final class Job {
    final String id, mapFilename, kind, snapshotId; final long sequenceLength; final Path directory, data;
    volatile AssemblySequenceSnapshot snapshot;
    final JsonObject request; final long created = System.currentTimeMillis(); final AtomicBoolean cancelled = new AtomicBoolean();
    final ArrayDeque<String> logs = new ArrayDeque<>();
    volatile String state = "submitted", output, error;
    volatile String sourceFasta, sourceStamp, toolCommand, toolSha256, inputSha256;
    volatile long finished, hits, fragments, elapsedNs;
    volatile Process process; volatile Thread thread; volatile BufferedWriter logWriter;
    Job(String id, Path directory, Path data, AssemblySequenceSnapshot snapshot, JsonObject request, String mapFilename) {
      this(id, directory, data, snapshot, request, mapFilename, "search");
    }
    Job(String id, Path directory, Path data, AssemblySequenceSnapshot snapshot, JsonObject request, String mapFilename, String kind) {
      this.id = id; this.directory = directory; this.data = data; this.snapshot = snapshot; this.request = request; this.mapFilename = mapFilename; this.kind = kind;
      this.snapshotId = snapshot.fingerprint(); this.sequenceLength = snapshot.length();
    }
    void checkCancelled() { if (cancelled.get() || Thread.currentThread().isInterrupted()) throw new CancellationException(); }
    void cancel() {
      if (finished > 0) return;
      cancelled.set(true); var p = process; if (p != null) { p.descendants().forEach(ProcessHandle::destroy); p.destroyForcibly(); }
      var t = thread; if (t != null) t.interrupt();
    }
    synchronized void log(String line) {
      String bounded = line.length() > 4096 ? line.substring(0, 4096) + "…" : line;
      logs.addLast(bounded); if (logs.size() > 1000) logs.removeFirst();
      try { if (logWriter != null) { logWriter.write(line); logWriter.newLine(); logWriter.flush(); } }
      catch (IOException ignored) { /* Final provenance write reports persistent disk errors. */ }
    }
    synchronized JsonObject statusJson() {
      return new JsonObject().put("jobId", id).put("kind", kind).put("status", state).put("createdAtMs", created)
        .put("finishedAtMs", finished).put("outputFile", output).put("mapFilename", mapFilename)
        .put("snapshot", snapshotId).put("sequenceLength", sequenceLength).put("hits", hits)
        .put("fragments", fragments).put("analysisNs", elapsedNs).put("error", error)
        .put("requestedThreads", request.getInteger("requestedThreads", request.getInteger("threads", -1)))
        .put("effectiveThreads", request.getInteger("effectiveThreads"))
        .put("threadMode", request.getString("threadMode"))
        .put("logs", new JsonArray(new ArrayList<>(logs)));
    }
    void save() throws IOException {
      var provenance = statusJson().put("request", request).put("sourceFasta", sourceFasta).put("sourceStamp", sourceStamp)
        .put("toolCommand", toolCommand).put("toolSha256", toolSha256).put("preparedInputSha256", inputSha256)
        .put("sourceFragments", new JsonArray(snapshot.fragments().stream()
        .map(f -> new JsonObject().put("source", f.source()).put("start", f.start()).put("end", f.end()).put("reversed", f.reversed())).toList()));
      var temp = directory.resolve("job.json.pending"); Files.writeString(temp, provenance.encodePrettily());
      try { Files.move(temp, directory.resolve("job.json"), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
      catch (AtomicMoveNotSupportedException e) { Files.move(temp, directory.resolve("job.json"), StandardCopyOption.REPLACE_EXISTING); }
    }
  }

  @Override public void close() { jobs.values().forEach(Job::cancel); workers.shutdownNow(); }
}
