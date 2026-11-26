package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ru.itmo.ctlab.hict.hict_server.util.ProcessTree;

import ru.itmo.ctlab.hict.hict_server.jobs.BackgroundJob;

import io.vertx.core.Vertx;
import io.vertx.core.json.*;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.HttpException;
import ru.itmo.ctlab.hict.hict_library.assembly.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.converters.FastaOnlyAssembly;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.request.sequence.AlignmentDetailRequestDTO;
import ru.itmo.ctlab.hict.hict_server.dto.request.sequence.RepeatPeriodRequestDTO;
import ru.itmo.ctlab.hict.hict_server.dto.request.sequence.SequenceAnalysisBudgetRequestDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.sequence.SequenceAnalysisEstimateResponseDTO;
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
  private final ConcurrentHashMap<AlignmentKey, String> alignmentJobs = new ConcurrentHashMap<>();
  private static final int MAX_ALIGNMENT_SEGMENTS = 4096;
  private static final int MAX_ALIGNMENT_RECORDS = 256;
  static final long MINIMUM_FREE_DISK_BYTES = 1L << 30;
  static final String LOW_DISK_ERROR = "Stopped: free disk space fell below 1 GB. Choose a coarser finest size or a smaller region.";
  static final String MEMORY_ERROR = "Stopped: not enough memory for this size.";
  private final ThreadPoolExecutor workers = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
    new ArrayBlockingQueue<>(8), runnable -> { var t = new Thread(runnable, "hict-sequence-analysis"); t.setDaemon(true); return t; });
  private final ThreadPoolExecutor remoteWorkers = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
    new ArrayBlockingQueue<>(2), runnable -> { var t = new Thread(runnable, "hict-remote-blast"); t.setDaemon(true); return t; });
  private final ScheduledExecutorService diskMonitor = Executors.newSingleThreadScheduledExecutor(runnable -> {
    var thread = new Thread(runnable, "hict-sequence-disk-monitor");
    thread.setDaemon(true);
    return thread;
  });
  private final FreeDiskSpace freeDiskSpace;

  @FunctionalInterface
  interface FreeDiskSpace { long usableBytes(Path path) throws IOException; }

  public SequenceAnalysisHandlersHolder(Vertx vertx) {
    this(vertx, path -> Files.getFileStore(path).getUsableSpace());
  }

  SequenceAnalysisHandlersHolder(Vertx vertx, FreeDiskSpace freeDiskSpace) {
    this.vertx = vertx;
    this.freeDiskSpace = freeDiskSpace;
  }

  public List<BackgroundJob> backgroundJobs() {
    return jobs.values().stream().map(job -> {
      if (job.kind.equals("overview-cache")) {
        var status = job.statusJson();
        var logs = status.getJsonArray("logs");
        return new BackgroundJob(job.id, job.kind, job.state, job.state.equals("finished") ? 1.0 : null,
          job.created, null, job.finished, logs.isEmpty() ? job.state : logs.getString(logs.size()-1), job.error,
          job.output == null ? List.of() : List.of(new BackgroundJob.Output(job.output, "attach_overview", null, null, job.mapFilename)),
          new BackgroundJob.Details("Overlay overview cache", null, null, job.request.getString("sourceFilename")));
      }
      if (job.kind.equals("remote_blast")) return new BackgroundJob(job.id, job.kind, job.state,
        job.state.equals("finished") ? 1.0 : null, job.created, null, job.finished,
        job.state.equals("running") ? "Waiting for NCBI results. Cancel stops HiCT waiting; a submitted search may continue at NCBI." : job.state,
        job.error, job.state.equals("finished") && job.output != null
          ? List.of(new BackgroundJob.Output(job.id, "open_blast_results", null)) : List.of(),
        new BackgroundJob.Details("NCBI BLAST · " + job.request.getString("remoteDatabase"), null, null, null));
      if (job.request.containsKey("sourceFastaFilename"))
        return new BackgroundJob(job.id, job.kind, job.state, job.state.equals("finished") ? 1.0 : null,
          job.created, null, job.finished, job.state, job.error,
          job.output == null ? List.of() : List.of(new BackgroundJob.Output(job.output, "open_map",
            job.request.getString("sourceFastaFilename"))),
          new BackgroundJob.Details("ModDotPlot from FASTA", null, null, job.request.getString("sourceFastaFilename")));
      return new BackgroundJob(
      job.id, job.kind, job.state, job.state.equals("finished") ? 1.0 : null, job.created, null, job.finished,
      job.repeatPeriod == null ? job.state : repeatSummary(job), job.error, job.output == null ? List.of() : List.of(
        new BackgroundJob.Output(job.output,
          job.kind.equals("search") ? "open_tracks"
            : job.kind.equals("moddotplot") ? "attach_identity" : "attach_dotplot", null, null, job.mapFilename, job.kind.equals("search") ? null : resultDisplayMode(job.request))));
    }).toList();
  }

  static String resultDisplayMode(JsonObject request) {
    var value = request.getValue("displayMode", "selection".equals(request.getString("scope")) ? "overlay" : "below");
    if (!(value instanceof String mode) || !Set.of("overlay", "below", "above", "none").contains(mode))
      throw new IllegalArgumentException("displayMode must be overlay, below, above or none");
    return (String) value;
  }

  public boolean cancelBackgroundJob(String id) {
    final var job = jobs.get(id);
    if (job == null) return false;
    job.cancel();
    return true;
  }

  @Override public void addHandlersToRouter(Router router) {
    router.post("/sequence/overview/jobs").handler(ctx -> {
      var shared = vertx.sharedData().getLocalMap("hict_server");
      var scheduler = (ShareableWrappers.RequestTaskSchedulerWrapper) shared.get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (scheduler == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      scheduler.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null,
        () -> submitOverview(ctx.body().asJsonObject()), result -> ctx.response()
          .putHeader("content-type", "application/json").end(result.encode()));
    });
    router.post("/sequence/repeat_period").handler(ctx -> {
      final var shared = vertx.sharedData().getLocalMap("hict_server");
      final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) shared.get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (wrapper == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      wrapper.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null,
        () -> repeatPeriod(ctx.body().asJsonObject()), result -> ctx.response()
          .setStatusCode(Set.of("submitted", "running").contains(result.getString("status")) ? 202 : 200)
          .putHeader("content-type", "application/json").end(result.encode()));
    });
    router.post("/sequence/alignment_detail").handler(ctx -> {
      final var shared = vertx.sharedData().getLocalMap("hict_server");
      final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) shared.get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (wrapper == null) {
        ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
        return;
      }
      wrapper.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null,
        () -> alignmentDetail(ctx.body().asJsonObject()), result -> ctx.response()
          .setStatusCode(Set.of("submitted", "running").contains(result.getString("status")) ? 202 : 200)
          .putHeader("content-type", "application/json").end(result.encode()));
    });
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
          var window=file.getChunkedFile().getFastaProcessor().readTrackRegion(fasta.getPath(), start, end,
            request.getBoolean("includeGaps", false),request.getLong("assemblyAnchorBp"));
          return JsonObject.mapFrom(window).put("linked", true);
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
            .put("blastBundlePlatforms", "Linux x86_64").put("repeatPeriod", true).put("remoteBlastRidRetrieval", true);
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
    router.post("/sequence/remote_blast/retrieve").handler(ctx -> {
      var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) vertx.sharedData().getLocalMap("hict_server")
        .get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (wrapper == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      wrapper.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null, () -> {
        try {
          var request = ru.itmo.ctlab.hict.hict_server.dto.request.sequence.RemoteBlastRetrievalRequestDTO.fromJson(ctx.body().asJsonObject());
          return submitRemoteSearch(request.settings(), request.queries(), true);
        } catch (IllegalArgumentException invalid) { throw new HttpException(400, invalid.getMessage()); }
      }, result -> ctx.response().putHeader("content-type", "application/json").setStatusCode(202).end(result.encode()));
    });
    for (String kind : List.of("moddotplot", "dotplot")) for (String action : List.of("jobs", "estimate"))
      router.post("/sequence/" + kind + "/" + action).handler(ctx -> {
      var body = ctx.body().asJsonObject();
      boolean preview = action.equals("estimate");
      if (body != null && body.containsKey("sourceFastaFilename")) {
        vertx.executeBlocking(() -> submitOverlay(body, kind.equals("dotplot"), preview), false)
          .onSuccess(result -> ctx.response().putHeader("content-type", "application/json")
            .setStatusCode(preview ? 200 : 202).end(result.encode()))
          .onFailure(ctx::fail);
        return;
      }
      var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) vertx.sharedData().getLocalMap("hict_server")
        .get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (wrapper == null) { ctx.fail(new IllegalStateException("Request scheduler is not initialized")); return; }
      wrapper.getRequestTaskScheduler().submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null,
        () -> submitOverlay(body, kind.equals("dotplot"), preview),
        result -> ctx.response().putHeader("content-type", "application/json").setStatusCode(preview ? 200 : 202).end(result.encode()));
    });
    router.post("/sequence/jobs/list").handler(ctx -> ctx.response().putHeader("content-type", "application/json")
      .end(new JsonArray(jobs.values().stream().sorted(Comparator.comparingLong(j -> j.created))
        .map(Job::statusJson).toList()).encode()));
    router.post("/sequence/remote_blast/result").handler(ctx -> {
      var request = ctx.body().asJsonObject();
      var id = request == null ? null : request.getValue("jobId");
      var format = request == null ? "JSON" : request.getValue("format", "JSON");
      if (!(id instanceof String) || !(format instanceof String fmt) || !Set.of("JSON", "TSV").contains(fmt)) {
        ctx.fail(new HttpException(400, "jobId and a JSON or TSV result format are required")); return;
      }
      var job = jobs.get(id);
      if (job == null || !job.kind.equals("remote_blast")) { ctx.fail(new HttpException(404, "Remote BLAST job not found")); return; }
      if (!job.state.equals("finished") || job.output == null) { ctx.fail(new HttpException(409, "Remote BLAST results are not ready")); return; }
      vertx.executeBlocking(() -> {
        var path = job.directory.resolve(fmt.equals("TSV") ? "remote-blast.tsv" : "remote-blast.json");
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > RemoteBlastRunner.MAX_OUTPUT_BYTES * 2)
          throw new IOException("Remote BLAST result is unavailable or too large");
        return Files.readString(path);
      }, false).onSuccess(text -> ctx.response().putHeader("content-type", fmt.equals("TSV")
          ? "text/tab-separated-values; charset=utf-8" : "application/json").end(text))
        .onFailure(ctx::fail);
    });
    router.post("/sequence/jobs/:jobId/stop").handler(ctx -> {
      var job = jobs.get(ctx.pathParam("jobId"));
      if (job == null) { ctx.fail(new HttpException(404, "Sequence analysis job not found")); return; }
      job.cancel();
      ctx.response().putHeader("content-type", "application/json").end(job.statusJson().encode());
    });
  }

  private record AlignmentKey(String assembly, Path fasta, Stamp stamp, AlignmentDetailRequestDTO window,
                              String rows, String columns) { }

  private static JsonObject repeatStatus(Job job) {
    return job.statusJson().put("result", job.repeatPeriod);
  }

  private static String repeatSummary(Job job) {
    final var result = job.repeatPeriod;
    return "Captured assembly " + (job.request.getLong("startBp") + 1) + "–" + job.request.getLong("endBp")
      + " (1-based inclusive), " + result.getInteger("length") + " bp. "
      + (result.getInteger("periodBp") == null ? "No supported period."
        : "Period " + result.getInteger("periodBp") + " bp; "
          + Math.round(result.getDouble("identity") * 1000) / 10.0 + "% matching informative pairs. ")
      + result.getString("message");
  }

  private JsonObject repeatPeriod(JsonObject body) throws Exception {
    final RepeatPeriodRequestDTO request;
    try { request = RepeatPeriodRequestDTO.fromJson(body); }
    catch (IllegalArgumentException invalid) { throw new HttpException(400, invalid.getMessage()); }
    if (request.jobId() != null) {
      final var job = jobs.get(request.jobId());
      if (job == null || !job.kind.equals("repeat_period")) throw new HttpException(404, "Repeat measurement job not found");
      return repeatStatus(job);
    }
    final var shared = vertx.sharedData().getLocalMap("hict_server");
    final boolean secondary = "SECONDARY".equals(shared.get("assemblyInfoSource"));
    final var map = (ShareableWrappers.ChunkedFileWrapper) shared.get(secondary ? "chunkedFileSecondary" : "chunkedFile");
    var linked = (ShareableWrappers.PathWrapper) shared.get(secondary ? "linkedFastaSecondaryPath" : "linkedFastaPrimaryPath");
    if (linked == null && !secondary) linked = (ShareableWrappers.PathWrapper) shared.get("linkedFastaPath");
    if (map == null || linked == null) throw new HttpException(409, "Link the FASTA for the active assembly before measuring repeats");
    final var file = map.getChunkedFile();
    final var fasta = linked.getPath().toRealPath();
    final var stamp = Stamp.read(fasta);
    final var snapshot = AssemblySequenceSnapshot.capture(file);
    if (request.endBp() > snapshot.length()) throw new HttpException(400, "The repeat interval is outside the current assembly");
    final var sequence = file.getFastaProcessor().readTrackRegion(fasta, request.startBp(), request.endBp(), false).sequence();
    stamp.check(fasta);
    final var id = UUID.randomUUID().toString();
    final var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    final var job = new Job(id, data, data, snapshot, body.copy(), file.getHdfFilePath().toString(), "repeat_period");
    job.sourceFasta = fasta.toString(); job.sourceStamp = stamp.toString();
    jobs.put(id, job);
    try { workers.execute(() -> {
      job.thread = Thread.currentThread();
      final long started = System.nanoTime();
      String terminal = "finished";
      try {
        job.checkCancelled(); job.state = "running";
        final var result = RepeatPeriod.estimate(sequence, job::checkCancelled);
        job.checkCancelled();
        job.repeatPeriod = JsonObject.mapFrom(result).put("startBp", request.startBp()).put("endBp", request.endBp())
          .put("coordinateSystem", "assembly_bp_gapless").put("snapshot", job.snapshotId)
          .put("sourceFasta", job.sourceFasta).put("sourceStamp", job.sourceStamp)
          .put("inputSha256", HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
            .digest(sequence.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
      } catch (Exception failure) {
        terminal = job.cancelled.get() || failure instanceof CancellationException || failure instanceof InterruptedException
          ? "cancelled" : "failed";
        job.error = terminal.equals("failed") ? Objects.toString(failure.getMessage(), "Repeat measurement failed") : null;
      } finally {
        job.elapsedNs = System.nanoTime() - started;
        job.snapshot = null;
        synchronized (job) {
          // Cancellation can race the last comparison; never publish a cancelled result.
          if (job.cancelled.get()) { terminal = "cancelled"; job.repeatPeriod = null; }
          job.thread = null; job.state = terminal; job.finished = System.currentTimeMillis();
        }
        trimCompletedJobs(); Thread.interrupted();
      }
    }); } catch (RejectedExecutionException full) {
      jobs.remove(id); throw new HttpException(503, "The sequence analysis queue is full");
    }
    return repeatStatus(job);
  }

  private JsonObject alignmentDetail(JsonObject body) throws Exception {
    final AlignmentDetailRequestDTO request;
    try { request = AlignmentDetailRequestDTO.fromJson(body); }
    catch (IllegalArgumentException invalid) { throw new HttpException(400, invalid.getMessage()); }
    if (request.jobId() != null) {
      final var job = jobs.get(request.jobId());
      if (job == null || !job.kind.equals("alignment_detail")) throw new HttpException(404, "Alignment job not found");
      return alignmentStatus(job);
    }
    final var shared = vertx.sharedData().getLocalMap("hict_server");
    final boolean secondary = "SECONDARY".equals(shared.get("assemblyInfoSource"));
    final var map = (ShareableWrappers.ChunkedFileWrapper) shared.get(secondary ? "chunkedFileSecondary" : "chunkedFile");
    var linked = (ShareableWrappers.PathWrapper) shared.get(secondary ? "linkedFastaSecondaryPath" : "linkedFastaPrimaryPath");
    if (linked == null && !secondary) linked = (ShareableWrappers.PathWrapper) shared.get("linkedFastaPath");
    if (map == null || linked == null) throw new HttpException(409, "Link the FASTA for the active assembly before viewing alignment detail");
    final var file = map.getChunkedFile();
    final var fasta = linked.getPath().toRealPath();
    final var stamp = Stamp.read(fasta);
    final var snapshot = AssemblySequenceSnapshot.capture(file);
    final FASTAProcessor.SequenceWindow rowWindow;
    final FASTAProcessor.SequenceWindow colWindow;
    try {
      rowWindow = file.getFastaProcessor().readTrackRegion(fasta, request.rowStart(), request.rowEnd(), request.includeGaps());
      colWindow = file.getFastaProcessor().readTrackRegion(fasta, request.columnStart(), request.columnEnd(), request.includeGaps());
    } catch (IllegalArgumentException invalid) {
      throw new HttpException(400, invalid.getMessage());
    }
    stamp.check(fasta);
    final var key = new AlignmentKey(snapshot.fingerprint(), fasta, stamp, request, rowWindow.sequence(), colWindow.sequence());
    final var id = alignmentJobs.get(key);
    final var previous = id == null ? null : jobs.get(id);
    if (previous != null && previous.state.equals("finished")) return alignmentStatus(previous);
    final var aligner = SequenceToolAvailability.requireAligner();
    final var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    final var jobId = UUID.randomUUID().toString();
    final var directory = Path.of(System.getProperty("java.io.tmpdir")).resolve("hict-alignment-detail-" + jobId);
    final var job = new Job(jobId, directory, data, snapshot, body.copy()
      .put("requestedThreads", 1).put("effectiveThreads", 1).put("threadMode", "single"),
      file.getHdfFilePath().toString(), "alignment_detail");
    jobs.put(job.id, job);
    alignmentJobs.put(key, job.id);
    try { workers.execute(() -> runAlignmentDetail(job, aligner, key, rowWindow.coordinateSystem())); }
    catch (RejectedExecutionException full) {
      jobs.remove(job.id);
      alignmentJobs.remove(key, job.id);
      throw new HttpException(503, "The sequence analysis queue is full");
    }
    return alignmentStatus(job);
  }

  private static JsonObject alignmentStatus(Job job) {
    final var status = job.statusJson();
    if (job.alignmentDetail != null) status.put("result", job.alignmentDetail);
    return status;
  }

  private void runAlignmentDetail(Job job, Path aligner, AlignmentKey key, String coordinateSystem) {
    final var rowPath = job.directory.resolve("row.fa");
    final var colPath = job.directory.resolve("column.fa");
    final var errors = job.directory.resolve("aligner.log");
    job.thread = Thread.currentThread();
    final long started = System.nanoTime();
    String terminal = "finished";
    try {
      job.checkCancelled();
      Files.createDirectory(job.directory);
      Files.writeString(rowPath, ">row\n" + key.rows + "\n");
      Files.writeString(colPath, ">column\n" + key.columns + "\n");
      final var profile = AlignmentDetailProfile.forWindows(key.rows.length(), key.columns.length());
      final var command = profile.command(aligner, rowPath, colPath);
      job.toolCommand = String.join(" ", command);
      job.toolSha256 = sha256(aligner);
      job.checkCancelled();
      job.state = "running";
      final var process = new ProcessBuilder(command).redirectError(errors.toFile()).start();
      job.process = process;
      final var segments = new JsonArray();
      int alignments = 0;
      try (var reader = process.inputReader()) {
        for (String line; (line = reader.readLine()) != null; ) {
          job.checkCancelled();
          if (line.isBlank()) continue;
          if (++alignments > MAX_ALIGNMENT_RECORDS || segments.size() >= MAX_ALIGNMENT_SEGMENTS) {
            throw new IOException("The region contains too many alignments. Select a smaller interval.");
          }
          final var result = AlignmentDetail.parsePaf(line, key.rows, key.columns, key.window.rowStart(),
            key.window.columnStart(), MAX_ALIGNMENT_SEGMENTS - segments.size());
          for (var segment : result) segments.add(JsonObject.mapFrom(segment).put("alignment", alignments - 1));
        }
      }
      if (process.waitFor() != 0) throw new IOException("The bundled aligner could not calculate alignment detail");
      job.checkCancelled();
      job.hits = alignments;
      job.fragments = segments.size();
      job.alignmentDetail = new JsonObject().put("coordinateSystem", coordinateSystem)
        .put("segments", segments).put("alignmentCount", alignments)
        .put("method", "minimap2-extended-cigar-v2").put("profile", JsonObject.mapFrom(profile))
        .put("rowStart", key.window.rowStart()).put("rowEnd", key.window.rowEnd())
        .put("columnStart", key.window.columnStart()).put("columnEnd", key.window.columnEnd())
        .put("snapshot", job.snapshotId)
        .put("aligner", aligner.getFileName().toString()).put("alignerSha256", job.toolSha256)
        .put("rowSha256", sequenceSha256(key.rows)).put("columnSha256", sequenceSha256(key.columns))
        .put("message", alignments == 0 ? "No alignment found in the selected intervals" : null);
    } catch (Exception failure) {
      terminal = job.cancelled.get() || failure instanceof InterruptedException || failure instanceof CancellationException
        ? "cancelled" : "failed";
      job.error = terminal.equals("failed") ? Objects.toString(failure.getMessage(), "Alignment failed") : null;
    } finally {
      final var process = job.process;
      if (process != null && process.isAlive()) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
      }
      job.process = null;
      job.elapsedNs = System.nanoTime() - started;
      job.thread = null;
      job.snapshot = null;
      for (var path : List.of(rowPath, colPath, errors, job.directory)) {
        try { Files.deleteIfExists(path); }
        catch (IOException ignored) { path.toFile().deleteOnExit(); }
      }
      synchronized (job) {
        job.state = terminal;
        job.finished = System.currentTimeMillis();
      }
      if (!terminal.equals("finished")) alignmentJobs.remove(key, job.id);
      trimCompletedJobs();
      Thread.interrupted();
    }
  }

  private JsonObject submitSearch(JsonObject request) throws Exception {
    if (request == null) throw new HttpException(400, "Search settings are required");
    // Absence of a checkbox must never be interpreted as end-user consent to a scope/mode.
    if (!request.containsKey("bothStrands") || !request.containsKey("scope") || !request.containsKey("mode"))
      throw new HttpException(400, "Choose search scope, strands and matching mode explicitly");
    List<SequenceSearchQueries.Query> queries;
    SequenceSearchRunner.Options options;
    try {
      if (!(request.getValue("queries") instanceof String input) || !(request.getValue("mode") instanceof String mode)
        || !(request.getValue("scope") instanceof String) || !(request.getValue("bothStrands") instanceof Boolean))
        throw new IllegalArgumentException("Choose valid sequence text, scope, strands and matching mode");
      queries = SequenceSearchQueries.parse(input);
      var target = request.getValue("searchTarget", "LOCAL_ASSEMBLY");
      if ("NCBI_REMOTE".equals(target)) return submitRemoteSearch(request, queries);
      if (!"LOCAL_ASSEMBLY".equals(target)) throw new IllegalArgumentException("Choose Local assembly or NCBI remote search explicitly");
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

  private JsonObject submitRemoteSearch(JsonObject request, List<SequenceSearchQueries.Query> queries) throws Exception {
    return submitRemoteSearch(request, queries, false);
  }
  private JsonObject submitRemoteSearch(JsonObject request, List<SequenceSearchQueries.Query> queries, boolean retrievalOnly) throws Exception {
    final RemoteBlastOptions options;
    try { options = RemoteBlastOptions.fromJson(request, queries); }
    catch (RuntimeException invalid) { throw new HttpException(400, invalid.getMessage()); }
    if (retrievalOnly != (options.requestId() != null)) throw new HttpException(400,
      "Use the download-only NCBI retrieval endpoint for an existing request ID");
    var shared = vertx.sharedData().getLocalMap("hict_server");
    var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    var executable = options.requestId() == null ? SequenceToolAvailability.require("blastn") : null;
    var outputRoot = data.resolve("processed/sequence-analysis");
    Files.createDirectories(outputRoot);
    if (!outputRoot.toRealPath().startsWith(data)) throw new HttpException(400, "Sequence output directory must remain inside DATA_DIR");
    var id = UUID.randomUUID().toString();
    var directory = Files.createDirectory(outputRoot.resolve(id));
    var settings = request.copy().put("searchTarget", "NCBI_REMOTE").put("remoteDatabase", options.database())
      .put("remoteRequestId", options.requestId()).put("requestedThreads", null).put("effectiveThreads", null).put("threadMode", "remote")
      .put("queryBases", queries.stream().mapToLong(q -> q.sequence().length()).sum());
    var job = new Job(id, directory, data, new AssemblySequenceSnapshot(List.of()), settings, "", "remote_blast");
    jobs.put(id, job);
    final Runnable work = () -> runRemoteSearch(job, executable, queries, options);
    job.removeQueuedWork = () -> remoteWorkers.remove(work);
    try { remoteWorkers.execute(work); }
    catch (RejectedExecutionException busy) {
      jobs.remove(id);
      Files.deleteIfExists(directory);
      throw new HttpException(429, "The remote BLAST queue is full. Wait for a remote job to finish.");
    }
    return job.statusJson();
  }

  private void runRemoteSearch(Job job, Path executable, List<SequenceSearchQueries.Query> queries, RemoteBlastOptions options) {
    job.thread = Thread.currentThread();
    String terminal = "finished";
    long started = System.nanoTime();
    try (var log = Files.newBufferedWriter(job.directory.resolve("job.log"), StandardOpenOption.CREATE_NEW)) {
      job.logWriter = log;
      job.checkCancelled();
      job.state = "preparing";
      job.toolCommand = options.requestId() == null ? executable.toString() : "NCBI Common URL API (GET)";
      job.toolSha256 = executable == null ? null : sha256(executable);
      if (options.requestId() != null) job.state = "running";
      job.save();
      var result = new RemoteBlastRunner().run(executable, job.directory, queries, options, job::log, process -> {
        job.process = process;
        if (process != null) {
          try { job.inputSha256 = sha256(job.directory.resolve("queries.fasta")); }
          catch (Exception failure) { throw new IllegalStateException("Could not fingerprint the submitted queries", failure); }
          if (job.cancelled.get()) ProcessTree.stop(process);
          else job.state = "running";
        }
      }, job.cancelled::get);
      job.checkCancelled();
      var report = new JsonObject(Files.readString(result.report()))
        .put("jobId", job.id).put("client", "HiCT remote BLAST v1").put("toolSha256", job.toolSha256);
      Files.writeString(result.report(), report.encode());
      if (Files.size(result.report()) > RemoteBlastRunner.MAX_OUTPUT_BYTES * 2)
        throw new IOException("Remote BLAST report exceeded the size limit");
      job.output = job.data.relativize(result.report()).toString().replace('\\', '/');
      job.inputSha256 = result.querySha256();
      job.hits = result.hits();
    } catch (Exception failure) {
      terminal = job.cancelled.get() || failure instanceof InterruptedException || failure instanceof CancellationException ? "cancelled" : "failed";
      job.error = terminal.equals("failed") ? Objects.toString(failure.getMessage(), "Remote BLAST failed") : null;
      job.output = null;
      job.log(job.error == null ? "Remote BLAST cancelled. A submitted NCBI request may continue remotely." : job.error);
    } finally {
      job.elapsedNs = System.nanoTime() - started;
      job.logWriter = null;
      job.process = null;
      job.thread = null;
      job.removeQueuedWork = null;
      synchronized (job) {
        if (job.cancelled.get()) { terminal = "cancelled"; job.output = null; }
        job.finished = System.currentTimeMillis();
        try { job.save(terminal); job.state = terminal; }
        catch (IOException failure) { job.state = "failed"; job.output = null; job.error = "Could not save remote BLAST provenance: " + failure.getMessage(); }
      }
      job.snapshot = null;
      trimCompletedJobs();
      Thread.interrupted();
    }
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

  private JsonObject submitOverview(JsonObject request) throws Exception {
    var shared = vertx.sharedData().getLocalMap("hict_server");
    var left = (ShareableWrappers.ChunkedFileWrapper) shared.get("chunkedFile");
    var right = (ShareableWrappers.ChunkedFileWrapper) shared.get("chunkedFileSecondary");
    if (left == null || right == null || request == null ||
        !Objects.equals(request.getString("sourceFilename"), shared.get("openedSecondaryFilename")))
      throw new HttpException(409, "The attached overlay changed.");
    var secondary = right.getChunkedFile();
    var direct = Arrays.stream(secondary.getResolutions()).filter(bp -> bp > 0).boxed().toList();
    var extra = AssemblyAnalysisPlan.pyramidLevels(direct, left.getChunkedFile().getResolutions()).stream()
      .filter(bp -> !direct.contains(bp)).toList();
    if (extra.isEmpty()) throw new HttpException(409, "The attached overlay already has overview levels.");
    var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    var source = secondary.getHdfFilePath().toRealPath();
    var template = left.getChunkedFile().getHdfFilePath().toRealPath();
    var tool = SequenceToolAvailability.require("hict-moddotplot");
    var sourceStamp = Stamp.read(source);
    var templateStamp = Stamp.read(template);
    var unit = switch (secondary.getSourceMetadata().units()) {
      case IDENTITY_PERCENT -> "identity_percent";
      case ALIGNMENT_PRESENCE, ALIGNMENT_SAMPLE_COUNT -> "alignment_presence";
      default -> "counts";
    };
    var key = sequenceSha256(source + "|" + sourceStamp + "|" + template + "|" + templateStamp + "|" + extra + "|" + Stamp.read(tool));
    var root = data.resolve("processed/sequence-analysis");
    Files.createDirectories(root);
    if (!root.toRealPath().startsWith(data)) throw new HttpException(400, "Cache must remain inside DATA_DIR");
    var cache = root.resolve("overview-" + key + ".hict.hdf5");
    for (var job : jobs.values())
      if (key.equals(job.request.getString("cacheKey")) && (job.finished == 0 ||
          ("finished".equals(job.state) && Files.isRegularFile(cache)))) return job.statusJson();
    var id = UUID.randomUUID().toString();
    var dir = Files.createDirectory(root.resolve(id));
    var job = new Job(id, dir, data, new AssemblySequenceSnapshot(List.of()), request.copy().put("cacheKey", key),
      String.valueOf(shared.get("openedFilename")), "overview-cache");
    jobs.put(id, job);
    if (Files.isRegularFile(cache)) {
      job.output = data.relativize(cache).toString().replace('\\', '/');
      job.state = "finished"; job.finished = System.currentTimeMillis(); job.save();
      trimCompletedJobs();
      return job.statusJson();
    }
    var plan = new JsonObject().put("kind", job.kind).put("source", source.toString()).put("template", template.toString())
      .put("unit", unit).put("directLevels", new JsonArray(direct)).put("overviewLevels", new JsonArray(extra));
    try { workers.execute(() -> {
      var pending = dir.resolve("overview.pending");
      job.thread = Thread.currentThread();
      try (var log = Files.newBufferedWriter(dir.resolve("job.log"))) {
        job.checkCancelled(); job.logWriter = log; job.state = "running"; job.save();
        if (Files.size(source) > Files.getFileStore(root).getUsableSpace()/2)
          throw new IOException("Insufficient space for the overview cache");
        sourceStamp.check(source); templateStamp.check(template);
        var file = dir.resolve("analysis-plan.json");
        Files.writeString(file, plan.put("output", pending.toString()).encode());
        runAdapter(job, tool, file);
        sourceStamp.check(source); templateStamp.check(template);
        verifyLevels(pending, extra);
        synchronized (job) {
          job.checkCancelled();
          Files.move(pending, cache, StandardCopyOption.ATOMIC_MOVE);
          job.output = data.relativize(cache).toString().replace('\\', '/');
          job.state = "finished"; job.finished = System.currentTimeMillis();
        }
      } catch (Exception failure) {
        job.state = job.cancelled.get() ? "cancelled" : "failed";
        if (!job.cancelled.get()) job.error = failure.getMessage();
      } finally {
        job.finished = System.currentTimeMillis(); job.thread = null; job.logWriter = null;
        try { Files.deleteIfExists(pending); job.save(); } catch (IOException e) { job.log(e.getMessage()); }
        trimCompletedJobs(); Thread.interrupted();
      }
    }); } catch (RejectedExecutionException full) { jobs.remove(id); throw new HttpException(429, "Sequence analysis queue is full"); }
    return job.statusJson();
  }

  private static void verifyLevels(Path output, List<Long> overviews) throws IOException {
    if (!Files.isRegularFile(output)) throw new IOException("The adapter did not create its output");
    if (overviews.isEmpty()) return;
    try (var reader = ch.systemsx.cisd.hdf5.HDF5Factory.openForReading(output.toFile())) {
      for (long bp : overviews)
        if (!reader.object().hasAttribute("/resolutions/" + bp, "source_resolution"))
          throw new IOException("The adapter did not generate the " + bp + " bp overview. Check the installed sequence tool version.");
    }
  }

  private JsonObject submitOverlay(JsonObject request, boolean alignment, boolean preview) throws Exception {
    if (request == null || !(request.getValue("scope") instanceof String) || !(request.getValue("mergeCompatible") instanceof Boolean))
      throw new HttpException(400, "Choose analysis scope and whether to accumulate compatible regions");
    SequenceAnalysisBudgetRequestDTO budgetRequest;
    JsonObject settings;
    List<Long> resolutions;
    try {
      budgetRequest = SequenceAnalysisBudgetRequestDTO.fromJson(request);
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
      if (values == null || values.isEmpty()) throw new IllegalArgumentException("Choose at least one dotplot resolution");
      resolutions = values.stream().map(v -> exactLong(v, "resolution")).distinct().sorted().toList();
      if (resolutions.getFirst() <= 0) throw new IllegalArgumentException("Dotplot resolutions must be positive");
      if (!alignment && resolutions.getFirst() < settings.getInteger("k"))
        throw new IllegalArgumentException("Each ModDotPlot window must be at least as long as its k-mer size");
    } catch (IllegalArgumentException | ClassCastException e) { throw new HttpException(400, e.getMessage()); }
    if (request.containsKey("sourceFastaFilename")) {
      if (alignment || !request.getString("scope").equals("assembly") || request.getBoolean("mergeCompatible"))
        throw new HttpException(400, "A FASTA ModDotPlot uses the full file and creates a separate map.");
      return submitFastaModDotPlot(request, settings, resolutions, preview, budgetRequest.acceptLargeOutput());
    }
    var shared = vertx.sharedData().getLocalMap("hict_server");
    boolean secondary = "SECONDARY".equals(shared.get("assemblyInfoSource"));
    var wrapper = (ShareableWrappers.ChunkedFileWrapper) shared.get(secondary ? "chunkedFileSecondary" : "chunkedFile");
    var fastaWrapper = (ShareableWrappers.PathWrapper) shared.get(secondary ? "linkedFastaSecondaryPath" : "linkedFastaPrimaryPath");
    if (fastaWrapper == null && !secondary) fastaWrapper = (ShareableWrappers.PathWrapper) shared.get("linkedFastaPath");
    if (wrapper == null || fastaWrapper == null) throw new HttpException(409, "Link the FASTA file for the active map before generating a dotplot. Use Assembly → Link FASTA, or open the map and FASTA together in the Open Wizard.");
    try { request = request.copy().put("gapLengthBp", gapLength(request)).put("displayMode", resultDisplayMode(request)); }
    catch (IllegalArgumentException invalid) { throw new HttpException(400, invalid.getMessage()); }
    var map = wrapper.getChunkedFile();
    var layout = AssemblySequenceLayout.capture(map, gapLength(request));
    long length = map.getAssemblyInfo().contigs().stream().mapToLong(t -> t.descriptor().getLengthBp()).sum();
    var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    var root = data.resolve("processed/sequence-analysis"); Files.createDirectories(root);
    if (!root.toRealPath().startsWith(data)) throw new HttpException(400, "Analysis output must remain inside DATA_DIR");
    var fasta = fastaWrapper.getPath().toRealPath();
    var stamp = Stamp.read(fasta);
    final SequenceAnalysisEstimateResponseDTO estimate;
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
      var primary = (ShareableWrappers.ChunkedFileWrapper) shared.get("chunkedFile");
      var pyramid = AssemblyAnalysisPlan.pyramidLevels(resolutions, primary.getChunkedFile().getResolutions());
      var overview = pyramid.stream().filter(bp -> !resolutions.contains(bp)).toList();
      final long bytesPerWindow = 256L + (alignment ? 0L : Long.BYTES * settings.getInteger("sketch_size"));
      estimate = estimateOverlay(map, rows, columns, resolutions, overview, pyramid,
        Math.max(stamp.size(), layout.length()), alignment, bytesPerWindow, freeDiskSpace.usableBytes(root));
      if (preview) return estimate.toJson();
      requireOverlayBudget(estimate, budgetRequest.acceptLargeOutput());
      request.put("overviewLevels", new JsonArray(overview));
      analysis = AssemblyAnalysisPlan.capture(map, resolutions, rows, columns);
    } catch (ArithmeticException overflow) {
      throw new HttpException(413, "This analysis is too large. Select a smaller region or fewer resolutions.");
    } catch (IllegalArgumentException | ClassCastException e) { throw new HttpException(400, e.getMessage()); }
    var executable = SequenceToolAvailability.require("hict-moddotplot");
    Path aligner = alignment ? SequenceToolAvailability.requireAligner() : null;
    try { validateOutputRequest(request, data, ".hict.hdf5"); }
    catch (FileAlreadyExistsException e) { throw new HttpException(409, e.getMessage()); }
    catch (IllegalArgumentException e) { throw new HttpException(400, e.getMessage()); }
    var template = map.getHdfFilePath().toRealPath(); var templateStamp = Stamp.read(template);
    var id = UUID.randomUUID().toString(); var directory = root.resolve(id); Files.createDirectory(directory);
    var job = new Job(id, directory, data, analysis.sequence(), request.copy(), data.relativize(template).toString().replace('\\', '/'), alignment ? "dotplot" : "moddotplot");
    var processor = map.getFastaProcessor();
    jobs.put(id, job);
    try { workers.execute(() -> runOverlay(job, processor, fasta, stamp, template, templateStamp, executable, aligner, analysis, layout, settings, estimate.expectedBytes())); }
    catch (RejectedExecutionException e) { jobs.remove(id); Files.delete(directory); throw new HttpException(429, "Sequence analysis queue is full"); }
    return job.statusJson();
  }

  private static SequenceAnalysisEstimateResponseDTO estimateOverlay(
      ChunkedFile map, AssemblyAnalysisPlan.Region rows, AssemblyAnalysisPlan.Region columns,
      List<Long> requested, List<Long> overview, List<Long> pyramid, long fastaBytes,
      boolean alignment, long bytesPerWindow, long usableBytes) {
    var budget = new OverlayBudget(usableBytes, Runtime.getRuntime().maxMemory() / 4 / bytesPerWindow);
    try {
      AssemblyAnalysisPlan.estimate(map, pyramid, rows, columns, fastaBytes, alignment, budget::observe);
    } catch (AssemblyAnalysisPlan.ResourceLimitException limit) {
      budget.hardLimit(limit.resolution(), limit.getMessage());
    }
    return new SequenceAnalysisEstimateResponseDTO(rows.end() - rows.start(), columns.end() - columns.start(),
      requested, overview, budget.expectedBytes, budget.worstCaseBytes, budget.diskBudgetBytes,
      budget.warningLevel, budget.limitingLevel, budget.hardLimitReason);
  }

  static void requireOverlayBudget(SequenceAnalysisEstimateResponseDTO estimate, boolean acceptLargeOutput) {
    if (estimate.limitingLevel() != null) throw new HttpException(413, estimate.hardLimitReason());
    if (estimate.warningLevel() != null && !acceptLargeOutput)
      throw new HttpException(413, levelBudgetMessage(estimate.warningLevel()));
  }

  static SequenceAnalysisEstimateResponseDTO estimateFastaLengths(List<Long> contigLengths,
      List<Long> requested, List<Long> overview, long fastaBytes, boolean alignment,
      long bytesPerWindow, long usableBytes) {
    var budget = new OverlayBudget(usableBytes, Runtime.getRuntime().maxMemory() / 4 / bytesPerWindow);
    long length = contigLengths.stream().mapToLong(Long::longValue).reduce(0, Math::addExact);
    var pyramid = new TreeSet<>(requested);
    pyramid.addAll(overview);
    try {
      AssemblyAnalysisPlan.estimateFasta(contigLengths, List.copyOf(pyramid), fastaBytes, alignment, budget::observe);
    } catch (AssemblyAnalysisPlan.ResourceLimitException limit) {
      budget.hardLimit(limit.resolution(), limit.getMessage());
    }
    return new SequenceAnalysisEstimateResponseDTO(length, length, requested, overview, budget.expectedBytes,
      budget.worstCaseBytes, budget.diskBudgetBytes, budget.warningLevel, budget.limitingLevel, budget.hardLimitReason);
  }

  private static String levelBudgetMessage(long bp) {
    return "The analysis exceeds the memory or disk budget at " + bp
      + " bp. Select a smaller region or fewer resolutions.";
  }

  private static final class OverlayBudget {
    final long usableBytes, diskBudgetBytes, maxWindows;
    long expectedBytes, worstCaseBytes;
    Long warningLevel, limitingLevel;
    String hardLimitReason;

    OverlayBudget(long usableBytes, long maxWindows) {
      this.usableBytes = usableBytes;
      this.diskBudgetBytes = usableBytes / 2;
      this.maxWindows = maxWindows;
    }

    void observe(long bp, AssemblyAnalysisPlan.ResourceEstimate estimate) {
      expectedBytes = estimate.expectedBytes();
      worstCaseBytes = estimate.worstCaseBytes();
      if (estimate.windows() > maxWindows || expectedBytes > usableBytes - MINIMUM_FREE_DISK_BYTES)
        hardLimit(bp, levelBudgetMessage(bp));
      if (warningLevel == null && worstCaseBytes > diskBudgetBytes) warningLevel = bp;
    }

    void hardLimit(long bp, String reason) {
      if (limitingLevel != null) return;
      limitingLevel = bp;
      hardLimitReason = reason;
    }
  }

  private JsonObject submitFastaModDotPlot(JsonObject request, JsonObject settings, List<Long> resolutions,
      boolean preview, boolean acceptLargeOutput) throws Exception {
    var shared = vertx.sharedData().getLocalMap("hict_server");
    var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    if (!(request.getValue("sourceFastaFilename") instanceof String filename) || filename.isBlank())
      throw new HttpException(400, "Choose a FASTA file.");
    var candidate = data.resolve(filename).normalize();
    if (!candidate.startsWith(data) || !Files.isRegularFile(candidate))
      throw new HttpException(400, "Choose a FASTA file inside the data directory.");
    var fasta = candidate.toRealPath();
    if (!fasta.startsWith(data))
      throw new HttpException(400, "The FASTA file must remain inside the data directory.");
    var sourceStamp = Stamp.read(fasta);
    var root = data.resolve("processed/sequence-analysis");
    Files.createDirectories(root);
    if (!root.toRealPath().startsWith(data)) throw new HttpException(400, "Analysis output must remain inside DATA_DIR");
    final FastaOnlyAssembly.Scan scan;
    try {
      scan = FastaOnlyAssembly.scan(fasta, FastaOnlyAssembly.DEFAULT_MIN_N_RUN, () -> false, null);
      var lengths = scan.records().stream().flatMap(record -> FastaOnlyAssembly.segments(record, true).stream())
        .map(segment -> segment.end() - segment.start()).toList();
      long bytesPerWindow = 256L + Long.BYTES * settings.getInteger("sketch_size");
      var estimate = estimateFastaLengths(lengths, resolutions, List.of(), Math.max(sourceStamp.size(), scan.totalBases()),
        false, bytesPerWindow, freeDiskSpace.usableBytes(root));
      if (preview) return estimate.toJson();
      requireOverlayBudget(estimate, acceptLargeOutput);
    } catch (ArithmeticException overflow) {
      throw new HttpException(413, "This analysis is too large. Select a smaller region or fewer resolutions.");
    } catch (IllegalArgumentException invalid) {
      throw new HttpException(400, invalid.getMessage());
    }
    var executable = SequenceToolAvailability.require("hict-moddotplot");
    var options = request.copy().put("gapLengthBp", 0L).put("displayMode", "none").put("overviewLevels", new JsonArray());
    try { validateOutputRequest(options, data, ".hict.hdf5"); }
    catch (FileAlreadyExistsException e) { throw new HttpException(409, e.getMessage()); }
    catch (IllegalArgumentException e) { throw new HttpException(400, e.getMessage()); }
    var id = UUID.randomUUID().toString();
    var directory = Files.createDirectory(root.resolve(id));
    var job = new Job(id, directory, data, new AssemblySequenceSnapshot(List.of()), options, "", "moddotplot");
    jobs.put(id, job);
    try {
      workers.execute(() -> prepareFastaModDotPlot(job, fasta, sourceStamp, executable, settings, resolutions, scan));
    } catch (RejectedExecutionException e) {
      jobs.remove(id);
      Files.delete(directory);
      throw new HttpException(429, "Sequence analysis queue is full");
    }
    return job.statusJson();
  }

  private void prepareFastaModDotPlot(Job job, Path fasta, Stamp sourceStamp, Path executable,
                                     JsonObject settings, List<Long> resolutions, FastaOnlyAssembly.Scan scan) {
    var template = job.directory.resolve("fasta-template.hict.hdf5");
    job.thread = Thread.currentThread();
    ScheduledFuture<?> monitoring = null;
    try {
      job.checkCancelled();
      monitoring = startOverlayMonitoring(job);
      job.state = "preparing";
      job.log("Reading the FASTA and building its sequence-only assembly.");
      sourceStamp.check(fasta);
      scan.warnings().forEach(job::log);
      FastaOnlyAssembly.write(scan, template, () -> job.cancelled.get() || job.stopReason != null, null);
      sourceStamp.check(fasta);
      stopOverlayMonitoring(job, monitoring);
      job.checkCancelled();
      try (var map = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(template, 1, 2))) {
        var layout = AssemblySequenceLayout.capture(map, 0L);
        var full = new AssemblyAnalysisPlan.Region(0, layout.sequence().length());
        long bytesPerWindow = 256L + Long.BYTES * settings.getInteger("sketch_size");
        var estimate = estimateOverlay(map, full, full, resolutions, List.of(), resolutions,
          Math.max(sourceStamp.size(), scan.totalBases()),
          false, bytesPerWindow, freeDiskSpace.usableBytes(job.directory));
        requireOverlayBudget(estimate, SequenceAnalysisBudgetRequestDTO.fromJson(job.request).acceptLargeOutput());
        var analysis = AssemblyAnalysisPlan.capture(map, resolutions, full, full);
        job.snapshot = analysis.sequence();
        job.snapshotId = job.snapshot.fingerprint();
        job.sequenceLength = job.snapshot.length();
        runOverlay(job, map.getFastaProcessor(), fasta, sourceStamp, template, Stamp.read(template),
          executable, null, analysis, layout, settings, estimate.expectedBytes());
      }
    } catch (Exception | OutOfMemoryError failure) {
      synchronized (job) {
        if (job.finished == 0) {
          job.state = job.stopReason == null && (job.cancelled.get() || failure instanceof CancellationException)
            ? "cancelled" : "failed";
          if (job.state.equals("failed")) {
            job.error = job.stopReason != null ? job.stopReason : failure instanceof OutOfMemoryError ? MEMORY_ERROR
              : failure instanceof HttpException http ? http.getPayload()
              : Objects.toString(failure.getMessage(), failure.getClass().getSimpleName());
            job.log(job.error);
          }
          job.finished = System.currentTimeMillis();
          try { job.save(); } catch (IOException e) { job.log("Could not save job details: " + e.getMessage()); }
          job.snapshot = null;
        }
      }
    } finally {
      stopOverlayMonitoring(job, monitoring);
      Thread.interrupted();
      try {
        Files.deleteIfExists(template);
        Files.deleteIfExists(job.directory.resolve("assembly.fasta"));
        Files.deleteIfExists(job.directory.resolve("moddotplot.hict.hdf5.pending"));
      } catch (IOException e) { job.log("Could not remove temporary analysis files: " + e.getMessage()); }
      job.thread = null;
      trimCompletedJobs();
      Thread.interrupted();
    }
  }

  void runOverlay(Job job, FASTAProcessor processor, Path fasta, Stamp stamp, Path template,
                             Stamp templateStamp, Path executable, Path aligner, AssemblyAnalysisPlan analysis,
                             AssemblySequenceLayout layout, JsonObject settings, long estimatedOutputBytes) {
    job.thread = Thread.currentThread();
    var terminal = "finished";
    ScheduledFuture<?> monitoring = null;
    var output = job.directory.resolve(job.kind + ".hict.hdf5.pending");
    try (var log = Files.newBufferedWriter(job.directory.resolve("job.log"), StandardOpenOption.CREATE_NEW)) {
      job.logWriter = log; job.checkCancelled(); stamp.check(fasta); templateStamp.check(template);
      monitoring = startOverlayMonitoring(job);
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
        + "|" + gapPolicy + "|" + job.request.getJsonArray("overviewLevels").encode() + "|" + analysis.levels().stream().map(AssemblyAnalysisPlan.Level::resolution).toList() + "|" + job.kind + "|" + job.toolSha256 + "|" + (aligner == null ? "1616ae94" : sha256(aligner));
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
      final long usable = freeDiskSpace.usableBytes(job.directory);
      final long previousBytes = previous == null ? 0L : Files.size(previous);
      if (previousBytes > usable || estimatedOutputBytes > usable - previousBytes) {
        throw new IOException("The overlay output exceeds available disk space. Select a smaller region or use another data directory.");
      }
      var published = job.directory.resolve(job.kind + ".hict.hdf5");
      var plan = new JsonObject().put("kind", job.kind).put("aligner", aligner == null ? null : aligner.toString()).put("settings", settings)
        .put("execution", execution).put("template", template.toString()).put("output", output.toString())
        .put("gapPolicy", gapPolicy)
        .put("input", input.toString()).put("inputOffset", layout.padded(start, true)).put("inputLength", layout.padded(end, false) - layout.padded(start, true))
        .put("sequenceLength", layout.length()).put("sourceFastaIdentity", sourceIdentity)
        .put("inputSha256", job.inputSha256).put("snapshot", analysis.sequence().fingerprint())
        .put("sourceFragments", fragmentsJson(analysis.sequence())).put("levels", levels)
        .put("overviewLevels", job.request.getJsonArray("overviewLevels"));
      if (previous != null) plan.put("previous", previous.toString());
      var planFile = job.directory.resolve("analysis-plan.json"); Files.writeString(planFile, plan.encodePrettily(), StandardOpenOption.CREATE_NEW);
      job.log("Dotplots use source-bin anchored windows in the captured active assembly. Existing scores move with edits; changed joins are not silently recomputed.");
      job.log(previous == null ? "Creating a separate overlay version" : "Accumulating compatible regions; reclaiming the superseded matrix after successful publication and detach");
      job.state = "running"; job.save();
      long timer = System.nanoTime();
      runAdapter(job, executable, planFile);
      verifyLevels(output, job.request.getJsonArray("overviewLevels").stream().map(n -> ((Number)n).longValue()).toList());
      stamp.check(fasta); templateStamp.check(template);
      stopOverlayMonitoring(job, monitoring);
      checkOverlayDisk(job);
      job.checkCancelled();
      if (!Files.isRegularFile(output)) throw new IOException("Dotplot did not create its sidecar");
      job.elapsedNs = System.nanoTime() - timer;
      try { Files.move(output, published, StandardCopyOption.ATOMIC_MOVE); }
      catch (AtomicMoveNotSupportedException e) { Files.move(output, published); }
      var delivered = publishRequestedOutput(published, job, ".hict.hdf5");
      job.output = job.data.relativize(delivered).toString().replace('\\', '/');
      var managedOutput = job.data.relativize(published).toString().replace('\\', '/');
      if (!job.request.containsKey("sourceFastaFilename"))
        atomicJson(registry, new JsonObject().put("outputFile", managedOutput).put("jobId", job.id).put("settings", settings));
      job.log("Saved overlay: " + job.output + ". Generation is complete; display attachment is a separate step.");
      if (previous != null) atomicJson(job.directory.getParent().resolve("superseded-" + job.id + ".json"),
        new JsonObject().put("previous", job.data.relativize(previous).toString().replace('\\', '/')).put("replacement", job.output));
    } catch (Exception | OutOfMemoryError failure) {
      if (failure instanceof OutOfMemoryError) job.stopReason = MEMORY_ERROR;
      terminal = job.stopReason == null && (job.cancelled.get() || failure instanceof InterruptedException
        || failure instanceof InterruptedIOException || failure instanceof CancellationException) ? "cancelled" : "failed";
      job.error = terminal.equals("failed") ? Objects.toString(job.stopReason,
        Objects.toString(failure.getMessage(), failure.getClass().getSimpleName())) : null;
      job.log(job.error == null ? "Dotplot cancelled; previous overlay retained" : "Dotplot failed: " + job.error);
    } finally {
      stopOverlayMonitoring(job, monitoring);
      job.logWriter = null; job.process = null; job.thread = null;
      Thread.interrupted();
      if (!terminal.equals("finished")) removePartialOverlay(job, output);
      synchronized (job) {
        job.finished = System.currentTimeMillis(); job.state = terminal;
        try { job.save(); }
        catch (IOException e) {
          job.state = "failed";
          if (job.error == null) job.error = "Could not save analysis provenance: " + e.getMessage();
        }
      }
      job.snapshot = null; // Source path is now on disk; do not retain a genome per completed job.
      trimCompletedJobs();
      Thread.interrupted();
    }
  }

  void checkOverlayDisk(Job job) throws IOException {
    if (freeDiskSpace.usableBytes(job.directory) < MINIMUM_FREE_DISK_BYTES) {
      job.stopForFailure(LOW_DISK_ERROR);
      throw new IOException(LOW_DISK_ERROR);
    }
  }

  private ScheduledFuture<?> startOverlayMonitoring(Job job) throws IOException {
    checkOverlayDisk(job);
    job.diskMonitorActive = true;
    return diskMonitor.scheduleWithFixedDelay(() -> monitorOverlayDisk(job), 1, 1, TimeUnit.SECONDS);
  }

  private void monitorOverlayDisk(Job job) {
    synchronized (job) {
      if (!job.diskMonitorActive || job.cancelled.get()) return;
      try { checkOverlayDisk(job); }
      catch (IOException failure) {
        job.stopForFailure(Objects.toString(failure.getMessage(), LOW_DISK_ERROR));
      }
    }
  }

  private static void stopOverlayMonitoring(Job job, ScheduledFuture<?> monitoring) {
    if (monitoring == null) return;
    synchronized (job) { job.diskMonitorActive = false; }
    monitoring.cancel(false);
  }

  private static void removePartialOverlay(Job job, Path output) {
    try {
      Files.deleteIfExists(output);
      for (var name : List.of("assembly.fasta", "rows.fasta", "columns.fasta", "alignment.paf"))
        Files.deleteIfExists(job.directory.resolve(name));
      try (var scratch = Files.newDirectoryStream(job.directory, "samples-*.sqlite*")) {
        for (var file : scratch) Files.deleteIfExists(file);
      }
    } catch (IOException failure) {
      job.log("Could not remove temporary analysis files: " + failure.getMessage());
    }
  }

  private static void runAdapter(Job job, Path executable, Path planFile) throws Exception {
    var process = new ProcessBuilder(executable.toString(), "--plan", planFile.toString()).redirectErrorStream(true).start();
      job.process = process;
      try {
        job.checkCancelled();
        try (var lines = new BufferedReader(new InputStreamReader(process.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
          for (String line; (line = lines.readLine()) != null;) {
            job.checkCancelled();
            job.log(line);
            if (line.contains("MemoryError") || line.contains("std::bad_alloc") || line.contains("Cannot allocate memory")) {
              job.stopForFailure(MEMORY_ERROR);
              job.checkCancelled();
            }
          }
        }
        int code = process.waitFor();
        if (code != 0) throw new IOException("Dotplot adapter exited with code " + code + "; see job log");
      } finally { if (process.isAlive()) ProcessTree.stop(process); job.process = null; }
  }
  private void trimCompletedJobs() {
    jobs.values().stream().filter(j -> j.finished > 0).sorted(Comparator.comparingLong((Job j) -> j.finished).reversed())
      .skip(32).forEach(j -> jobs.remove(j.id, j));
    alignmentJobs.entrySet().removeIf(entry -> !jobs.containsKey(entry.getValue()));
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

  record Stamp(long size, java.nio.file.attribute.FileTime modified, Object key) {
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

  private static String sequenceSha256(String sequence) throws java.security.NoSuchAlgorithmException {
    return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
      .digest(sequence.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
  }

  static final class Job {
    final String id, mapFilename, kind; final Path directory, data;
    volatile String snapshotId; volatile long sequenceLength;
    volatile AssemblySequenceSnapshot snapshot;
    final JsonObject request; final long created = System.currentTimeMillis(); final AtomicBoolean cancelled = new AtomicBoolean();
    final ArrayDeque<String> logs = new ArrayDeque<>();
    volatile String state = "submitted", output, error, stopReason;
    volatile String sourceFasta, sourceStamp, toolCommand, toolSha256, inputSha256;
    volatile JsonObject alignmentDetail, repeatPeriod;
    volatile long finished, hits, fragments, elapsedNs;
    volatile Process process; volatile Thread thread; volatile BufferedWriter logWriter;
    volatile Runnable removeQueuedWork;
    volatile boolean diskMonitorActive;
    Job(String id, Path directory, Path data, AssemblySequenceSnapshot snapshot, JsonObject request, String mapFilename) {
      this(id, directory, data, snapshot, request, mapFilename, "search");
    }
    Job(String id, Path directory, Path data, AssemblySequenceSnapshot snapshot, JsonObject request, String mapFilename, String kind) {
      this.id = id; this.directory = directory; this.data = data; this.snapshot = snapshot; this.request = request; this.mapFilename = mapFilename; this.kind = kind;
      this.snapshotId = snapshot.fingerprint(); this.sequenceLength = kind.equals("remote_blast") ? request.getLong("queryBases", 0L) : snapshot.length();
    }
    void checkCancelled() {
      if (stopReason != null || cancelled.get() || Thread.currentThread().isInterrupted()) throw new CancellationException();
    }
    synchronized void stopForFailure(String reason) {
      if (finished > 0 || stopReason != null) return;
      stopReason = reason;
      interruptWork();
    }
    private void interruptWork() {
      ProcessTree.stop(process);
      var runningThread = thread;
      if (runningThread != null) runningThread.interrupt();
    }
    synchronized void cancel() {
      if (finished > 0) return;
      cancelled.set(true);
      interruptWork();
      if (thread == null) {
        var remove = removeQueuedWork;
        if (remove != null) remove.run();
        removeQueuedWork = null;
        state = "cancelled"; finished = System.currentTimeMillis();
      }
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
        .put("snapshot", kind.equals("remote_blast") ? null : snapshotId).put("sequenceLength", sequenceLength).put("hits", hits)
        .put("searchTarget", request.getString("searchTarget", "LOCAL_ASSEMBLY"))
        .put("remoteDatabase", request.getString("remoteDatabase"))
        .put("remoteRequestId", request.getString("remoteRequestId"))
        .put("fragments", fragments).put("analysisNs", elapsedNs).put("error", error)
        .put("requestedThreads", request.getInteger("requestedThreads", request.getInteger("threads", -1)))
        .put("effectiveThreads", request.getInteger("effectiveThreads"))
        .put("threadMode", request.getString("threadMode"))
        .put("logs", new JsonArray(new ArrayList<>(logs)));
    }
    void save() throws IOException {
      save(null);
    }
    void save(String terminalState) throws IOException {
      var provenance = statusJson().put("request", request).put("sourceFasta", sourceFasta).put("sourceStamp", sourceStamp)
        .put("toolCommand", toolCommand).put("toolSha256", toolSha256).put("preparedInputSha256", inputSha256)
        .put("sourceFragments", new JsonArray(snapshot.fragments().stream()
        .map(f -> new JsonObject().put("source", f.source()).put("start", f.start()).put("end", f.end()).put("reversed", f.reversed())).toList()));
      if (terminalState != null) provenance.put("status", terminalState);
      var temp = directory.resolve("job.json.pending"); Files.writeString(temp, provenance.encodePrettily());
      try { Files.move(temp, directory.resolve("job.json"), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
      catch (AtomicMoveNotSupportedException e) { Files.move(temp, directory.resolve("job.json"), StandardCopyOption.REPLACE_EXISTING); }
    }
  }

  @Override public void close() {
    jobs.values().forEach(Job::cancel);
    workers.shutdownNow();
    remoteWorkers.shutdownNow();
    diskMonitor.shutdownNow();
  }
}
