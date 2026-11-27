package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import ru.itmo.ctlab.hict.hict_library.assembly.AssemblyAnalysisPlan;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSourceDigest;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.SequenceCoverage;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/** Manages registered dotplot versions for the active map on the assembly scheduler. */
public final class DotplotResultsHandlers extends HandlersHolder {
  private static final String METADATA = "/sequence_analysis";
  private static final String UPDATES = METADATA + "/updates";
  private static final String CONTENT_TYPE = "content-type";
  private final Vertx vertx;
  private final FileOpHandlersHolder files;

  public DotplotResultsHandlers(Vertx vertx, FileOpHandlersHolder files) {
    this.vertx = vertx;
    this.files = files;
  }

  @Override
  public void addHandlersToRouter(Router router) {
    router.post("/sequence/overlays/list").handler(ctx -> reply(ctx, () -> list(active())));
    router.post("/sequence/overlays/open").handler(ctx -> reply(ctx, () -> open(active(), required(ctx, "filename"))));
    router.post("/sequence/overlays/show").handler(ctx -> reply(ctx, () -> {
      var state = active();
      var entry = find(state, required(ctx, "id"));
      if (!(ctx.body().asJsonObject().getValue("shown") instanceof Boolean shown)) {
        throw new HttpException(400, "Choose whether to show the result set");
      }
      var secondary = shown ? attach(state, entry) : detach(state, entry);
      return outcome(state, entry, secondary);
    }));
    router.post("/sequence/overlays/delete").handler(ctx -> reply(ctx, () -> delete(active(), required(ctx, "id"))));
    router.get("/sequence/overlays/download").handler(ctx -> {
      var scheduler = scheduler(ctx);
      if (scheduler == null) return;
      scheduler.submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null, () -> {
        var state = active();
        var entry = find(state, ctx.request().getParam("id"));
        return new Download(entry.file(), filename(state, entry.metadata().getString("kind")));
      }, download -> ctx.response().putHeader(CONTENT_TYPE, "application/octet-stream")
        .putHeader("content-disposition", "attachment; filename*=UTF-8''"
          + URLEncoder.encode(download.name(), StandardCharsets.UTF_8).replace("+", "%20"))
        .sendFile(download.file().toString()));
    });
  }

  private record Download(Path file, String name) { }
  private record State(Path data, Path root, ChunkedFile primary, String mapFilename, LocalMap<String, Object> shared) { }
  private record Bounds(long start, long end) { }
  private record Region(int index, Bounds rows, Bounds columns, List<Long> levels, long finest, String addedAt, boolean split) { }
  private record Result(String id, String kind, String name, String settingsSummary, String file, String downloadFilename,
                        long sizeBytes, List<Long> resolutions, boolean shown, String displayMode, List<Region> regions) { }
  private record OverlayMetadata(String kind, String name, JsonObject settings) { }

  private State active() throws IOException {
    var shared = vertx.sharedData().<String, Object>getLocalMap("hict_server");
    if (!(shared.get("chunkedFile") instanceof ShareableWrappers.ChunkedFileWrapper open)) {
      throw new HttpException(409, "Open a map before managing dotplot results");
    }
    var data = ((ShareableWrappers.PathWrapper) shared.get("dataDirectory")).getPath().toRealPath();
    var processed = data.resolve("processed");
    var root = processed.resolve("sequence-analysis");
    for (var directory : List.of(processed, root)) {
      if (Files.exists(directory, LinkOption.NOFOLLOW_LINKS) && !directory.equals(directory.toRealPath())) {
        throw new HttpException(400, "The dotplot result directory must remain inside DATA_DIR");
      }
    }
    var primary = open.getChunkedFile();
    return new State(data, root, primary, relative(data, primary.getHdfFilePath().toRealPath()), shared);
  }

  private RequestTaskScheduler scheduler(RoutingContext ctx) {
    var shared = vertx.sharedData().<String, Object>getLocalMap("hict_server");
    if (shared.get(RequestTaskScheduler.LOCAL_MAP_KEY) instanceof ShareableWrappers.RequestTaskSchedulerWrapper wrapper) {
      return wrapper.getRequestTaskScheduler();
    }
    ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
    return null;
  }

  private void reply(RoutingContext ctx, RequestTaskScheduler.ThrowingSupplier<?> work) {
    var scheduler = scheduler(ctx);
    if (scheduler != null) scheduler.submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null, work,
      result -> ctx.response().putHeader(CONTENT_TYPE, "application/json").end(Json.encode(result)));
  }

  private static String required(RoutingContext ctx, String key) {
    var body = ctx.body().asJsonObject();
    if (body != null && body.getValue(key) instanceof String value && !value.isBlank()) return value;
    throw new HttpException(400, "Missing or invalid field: " + key);
  }

  private static String relative(Path data, Path file) {
    return data.relativize(file).toString().replace('\\', '/');
  }

  private static String filename(State state, String kind) {
    return Path.of(state.mapFilename()).getFileName().toString().replaceFirst("(?i)\\.hict\\.hdf5$", "")
      + "." + kind + ".hict.hdf5";
  }

  private static DotplotResultRegistry.Entry find(State state, String id) throws IOException {
    return DotplotResultRegistry.entries(state.data(), state.root(), state.mapFilename()).stream()
      .filter(entry -> entry.metadata().getString("setId").equals(id)).findFirst()
      .orElseThrow(() -> new HttpException(404, "The dotplot result set is unavailable"));
  }

  private static List<Result> list(State state) throws IOException {
    var results = new ArrayList<Result>();
    for (var entry : DotplotResultRegistry.entries(state.data(), state.root(), state.mapFilename())) {
      results.add(describe(state, entry));
    }
    return results;
  }

  private static Result describe(State state, DotplotResultRegistry.Entry entry) throws IOException {
    var metadata = entry.metadata();
    var name = metadata.getString("name");
    var kind = metadata.getString("kind");
    var shown = shown(state, entry);
    String mode = preferredMode(metadata);
    if (shown && state.shared().get(RenderPipelineConfig.LOCAL_MAP_KEY) instanceof ShareableWrappers.RenderPipelineConfigWrapper pipeline) {
      mode = pipeline.getRenderPipelineConfig().secondaryDisplayMode().orElse(mode);
    }
    try (var reader = HDF5Factory.openForReading(entry.file().toFile())) {
      return new Result(metadata.getString("setId"), kind, name, name.substring(name.indexOf(" · ") + 3),
        metadata.getString("outputFile"), filename(state, kind), Files.size(entry.file()), levels(reader, "/resolutions"), shown,
        mode, regions(reader, state.primary(), metadata.getLong("updatedAtMs", 0L)));
    }
  }

  private static List<Region> regions(IHDF5Reader reader, ChunkedFile primary, long fallbackTime) {
    if (!reader.object().isGroup(UPDATES)) return List.of();
    var updates = reader.object().getGroupMembers(UPDATES).stream().map(name -> UPDATES + "/" + name)
      .sorted(Comparator.comparingLong(path -> number(reader, path, "ordinal", Long.MAX_VALUE))).toList();
    var result = new ArrayList<Region>();
    for (var update : updates) {
      var sizes = levels(reader, update);
      if (sizes.isEmpty()) continue;
      long finest = sizes.getFirst();
      var level = update + "/" + finest;
      var bounds = AssemblyAnalysisPlan.currentBounds(primary, finest, List.of(
        SequenceCoverage.readAxis(reader, level + "/row_bins"), SequenceCoverage.readAxis(reader, level + "/column_bins")));
      var row = bounds.get(0);
      var column = bounds.get(1);
      result.add(new Region(result.size() + 1, new Bounds(row.start(), row.end()), new Bounds(column.start(), column.end()),
        sizes, finest, Instant.ofEpochMilli(number(reader, update, "created_at_ms", fallbackTime)).toString(), row.split() || column.split()));
    }
    return result;
  }

  private static long number(IHDF5Reader reader, String path, String attribute, long fallback) {
    return reader.object().hasAttribute(path, attribute) ? reader.int64().getAttr(path, attribute) : fallback;
  }

  private static List<Long> levels(IHDF5Reader reader, String path) {
    return reader.object().getGroupMembers(path).stream().filter(name -> name.matches("[1-9][0-9]*"))
      .map(Long::parseLong).sorted().toList();
  }

  private Object open(State state, String filename) throws Exception {
    final Path input;
    try {
      var requested = Path.of(filename);
      if (requested.isAbsolute() || !filename.toLowerCase(java.util.Locale.ROOT).endsWith(".hict.hdf5")) {
        throw new HttpException(400, "Select a HiCT dotplot file inside the data folder");
      }
      var normalized = state.data().resolve(requested).normalize();
      if (!normalized.startsWith(state.data()) || !Files.isRegularFile(normalized)) {
        throw new HttpException(400, "Select a HiCT dotplot file inside the data folder");
      }
      input = normalized.toRealPath();
    } catch (InvalidPathException invalid) {
      throw new HttpException(400, "Select a HiCT dotplot file inside the data folder");
    }
    if (!input.startsWith(state.data())) throw new HttpException(400, "Select a HiCT dotplot file inside the data folder");
    var metadata = validate(state, input);
    for (var entry : DotplotResultRegistry.entries(state.data(), state.root(), state.mapFilename())) {
      if (entry.file().equals(input)) return outcome(state, entry, attach(state, entry));
    }
    // Reopening a user-saved copy makes a managed version. Deleting the set
    // can then safely remove the registered version without touching that copy.
    var id = UUID.randomUUID().toString();
    Files.createDirectories(state.root());
    var directory = Files.createDirectory(state.root().resolve(id));
    var managed = directory.resolve(metadata.kind() + ".hict.hdf5");
    var manifest = state.root().resolve("overlay-imported-" + id + ".json");
    var entry = new DotplotResultRegistry.Entry(manifest, managed, new JsonObject()
      .put("id", id).put("setId", id).put("kind", metadata.kind()).put("name", metadata.name())
      .put("settings", metadata.settings()).put("mapFilename", state.mapFilename()).put("displayMode", "below")
      .put("outputFile", relative(state.data(), managed)).put("updatedAtMs", Files.getLastModifiedTime(input).toMillis()));
    try {
      Files.copy(input, managed);
      validate(state, managed);
      SequenceAnalysisHandlersHolder.atomicJson(manifest, entry.metadata());
      return outcome(state, entry, attach(state, entry));
    } catch (Exception failure) {
      if (shown(state, entry)) files.closeSecondarySource();
      for (var path : List.of(manifest, managed, directory)) {
        try { Files.deleteIfExists(path); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
      }
      throw failure;
    }
  }

  private static OverlayMetadata validate(State state, Path input) {
    if (!HDF5Factory.isHDF5File(input.toFile())) throw new HttpException(400, "Select a complete HiCT sequence overlay");
    try (var overlay = HDF5Factory.openForReading(input.toFile())) {
      if (!overlay.object().isGroup(METADATA) || !overlay.object().hasAttribute(METADATA, "complete")
          || !overlay.bool().getAttr(METADATA, "complete") || !overlay.object().hasAttribute(METADATA, "compatibility")) {
        throw new HttpException(400, "Select a complete HiCT sequence overlay");
      }
      var compatibility = new JsonObject(overlay.string().getAttr(METADATA, "compatibility"));
      try (var primary = HDF5Factory.openForReading(state.primary().getHdfFilePath().toFile())) {
        var sizes = compatibility.getJsonArray("resolutions");
        var nativeLevels = sizes == null ? levels(primary, "/resolutions")
          : sizes.stream().map(value -> ((Number) value).longValue()).toList();
        if (!SequenceSourceDigest.digest(primary, nativeLevels).equals(compatibility.getString("sourceMetadataSha256"))) {
          var original = overlay.object().hasAttribute("/", "template_name") ? overlay.string().getAttr("/", "template_name") : null;
          throw new HttpException(409, "This dotplot file belongs to a different map. " +
            (original == null ? "It was made for other contigs or resolutions. Open its original map first, then open the dotplot file again."
              : "It was made for “" + original + "” (other contigs or resolutions). Open that map first, then open the dotplot file again."));
        }
      }
      var settings = compatibility.getJsonObject("settings", new JsonObject());
      var parameters = settings.getJsonObject("parameters", new JsonObject());
      if ("alignment_sample_count".equals(compatibility.getString("unit"))) {
        var aligner = settings.getString("aligner", "minimap2");
        return new OverlayMetadata("dotplot", DotplotResultRegistry.name(parameters, Path.of(aligner)), parameters);
      }
      return new OverlayMetadata("moddotplot", DotplotResultRegistry.name(parameters, null), parameters);
    }
  }

  private JsonObject attach(State state, DotplotResultRegistry.Entry entry) throws Exception {
    return files.openSecondarySource(relative(state.data(), entry.file()), false);
  }

  private JsonObject detach(State state, DotplotResultRegistry.Entry entry) {
    return shown(state, entry) ? files.closeSecondarySource() : null;
  }

  private static boolean shown(State state, DotplotResultRegistry.Entry entry) {
    var opened = state.shared().get("openedSecondaryFilename");
    return entry.metadata().getString("outputFile").equals(opened)
      || opened != null && opened.equals(entry.metadata().getString("displayFile"));
  }

  private static String preferredMode(JsonObject metadata) {
    var mode = metadata.getString("displayMode", "below");
    return "none".equals(mode) ? "below" : mode;
  }

  private static JsonObject outcome(State state, DotplotResultRegistry.Entry entry, JsonObject secondary) {
    return new JsonObject().put("id", entry.metadata().getString("setId"))
      .put("kind", entry.metadata().getString("kind")).put("shown", shown(state, entry))
      .put("displayMode", preferredMode(entry.metadata())).put("secondarySource", secondary);
  }

  private JsonObject delete(State state, String id) throws IOException {
    synchronized (DotplotResultRegistry.WRITING) {
      var entry = find(state, id);
      if (DotplotResultRegistry.WRITING.contains(state.root().resolve(entry.metadata().getString("setId")))) {
        throw new HttpException(409, "Wait for the dotplot job to finish before deleting this result set");
      }
      var secondary = detach(state, entry);
      ru.itmo.ctlab.hict.hict_server.jobs.JobArtifacts.requireClosedMap(state.shared(), entry.file());
      Files.delete(entry.file());
      Files.deleteIfExists(entry.manifest());
      return outcome(state, entry, secondary);
    }
  }
}
