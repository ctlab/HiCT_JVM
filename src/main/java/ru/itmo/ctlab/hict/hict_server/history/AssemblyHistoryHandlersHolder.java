package ru.itmo.ctlab.hict.hict_server.history;

import ru.itmo.ctlab.hict.hict_server.dto.RequestJson;

import ru.itmo.ctlab.hict.hict_server.NoOpenFileException;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.history.EditJournal;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.CompactAssemblyInfoDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.util.*;

/** A journal per assembly source within the open primary session. */
public final class AssemblyHistoryHandlersHolder extends HandlersHolder implements RequestTaskScheduler.AssemblyTaskInterceptor {
  public static final String HEADER = "X-HiCT-History";
  private static final Map<String, String> EDITS = Map.ofEntries(
    Map.entry("/reverse_selection_range", "Reverse selection"), Map.entry("/move_selection_range", "Move selection"),
    Map.entry("/split_contig_at_bin", "Split contig"), Map.entry("/group_contigs_into_scaffold", "Group into scaffold"),
    Map.entry("/split_contig_at_bp", "Cut contig at base"),
    Map.entry("/ungroup_contigs_from_scaffold", "Ungroup scaffold"), Map.entry("/unscaffold_all", "Unscaffold all"), Map.entry("/move_selection_to_debris", "Move to debris"),
    Map.entry("/load_agp", "Import AGP"), Map.entry("/apply_juicebox_assembly", "Import Juicebox assembly"),
    Map.entry("/names/contig", "Rename contig"), Map.entry("/names/scaffold", "Rename scaffold"),
    Map.entry("/names/import", "Import names"));
  private final LocalMap<String, Object> map;
  private final RequestTaskScheduler scheduler;
  private final Map<ChunkedFile, EditJournal<List<ChunkedFile.AssemblySnapshot>>> journals = new IdentityHashMap<>();
  private EditJournal<List<ChunkedFile.AssemblySnapshot>> journal;
  private List<ChunkedFile> files = List.of();
  private ChunkedFile primary;

  public AssemblyHistoryHandlersHolder(Vertx vertx, RequestTaskScheduler scheduler) {
    this.map = vertx.sharedData().getLocalMap("hict_server"); this.scheduler = scheduler;
    scheduler.setAssemblyTaskInterceptor(this);
  }

  private List<ChunkedFile> openFiles() {
    var result = new ArrayList<ChunkedFile>(2);
    for (var key : List.of("chunkedFile", "chunkedFileSecondary"))
      if (map.get(key) instanceof ShareableWrappers.ChunkedFileWrapper wrapper) result.add(wrapper.getChunkedFile());
    return List.copyOf(result);
  }
  private List<ChunkedFile.AssemblySnapshot> capture() { return files.stream().map(ChunkedFile::snapshotAssembly).toList(); }
  private void ensureSession() {
    final var opened = openFiles();
    final boolean removedSource = files.stream().anyMatch(file -> !opened.contains(file));
    files = opened;
    final var currentPrimary = map.get("chunkedFile") instanceof ShareableWrappers.ChunkedFileWrapper wrapper
      ? wrapper.getChunkedFile() : null;
    if (primary != currentPrimary) {
      journals.clear();
      primary = currentPrimary;
    }
    if (primary == null) {
      journal = null;
      return;
    }
    journals.keySet().removeIf(file -> !files.contains(file));
    if (removedSource) {
      for (var retained : journals.values()) {
        retained.transformStates(states -> states.stream().filter(state -> opened.contains(state.owner())).toList());
      }
    }
    final var active = "SECONDARY".equals(map.get("assemblyInfoSource")) && files.size() > 1
      ? files.get(1) : primary;
    journal = journals.computeIfAbsent(active, ignored -> new EditJournal<>(capture()));
  }
  private static boolean same(List<ChunkedFile.AssemblySnapshot> a, List<ChunkedFile.AssemblySnapshot> b) {
    if (a.size() != b.size()) return false;
    for (int i=0; i<a.size(); i++) {
      var x=a.get(i); var y=b.get(i);
      if (x.owner()!=y.owner() || x.contigs()!=y.contigs() || !x.scaffolds().equals(y.scaffolds())
        || x.descriptors()!=y.descriptors() || x.contigNames()!=y.contigNames() || x.scaffoldNames()!=y.scaffoldNames()
        || x.annotations().gaps()!=y.annotations().gaps() || x.annotations().components()!=y.annotations().components()) return false;
    }
    return true;
  }
  private void restore(List<ChunkedFile.AssemblySnapshot> states) {
    // Lock both sources in the same order for a coherent rollback, including failed overlay edits.
    for (var f : files) { f.getContigTree().getRootLock().writeLock().lock(); f.getScaffoldTree().getRootLock().writeLock().lock(); }
    try {
      for (var state : states) {
        if (files.contains(state.owner())) state.owner().restoreAssembly(state);
      }
    }
    finally { for (int i=files.size()-1; i>=0; i--) {
      files.get(i).getScaffoldTree().getRootLock().writeLock().unlock(); files.get(i).getContigTree().getRootLock().writeLock().unlock();
    } }
  }
  private long invalidate() {
    var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
    long version = 0;
    if (stats != null) {
      var next = TileStatisticHolder.resetRangesWithIncrementedVersion(stats,
        files.stream().mapToInt(f -> f.getResolutions().length).max().orElse(1));
      map.put("TileStatisticHolder", next); version = next.versionCounter().get();
    }
    scheduler.bumpAssemblyGeneration(); TileHandlersHolder.clearExpectedProfileCache(map);
    if (map.get("Track1DManager") instanceof ShareableWrappers.Track1DManagerWrapper wrapper)
      wrapper.getTrack1DManager().invalidateInMemoryCache();
    FileOpHandlersHolder.refreshSecondaryCompatibility(map);
    return version;
  }

  private void requireVersion(String session, long revision) {
    try { journal.requireVersion(session, revision); }
    catch (IllegalStateException conflict) {
      throw new io.vertx.ext.web.handler.HttpException(409, conflict.getMessage());
    }
  }

  @Override public <T> T execute(RoutingContext ctx, RequestTaskScheduler.ThrowingSupplier<T> action) throws Exception {
    ensureSession();
    var label = EDITS.get(ctx.normalizedPath());
    var before = label != null && journal != null ? capture() : null;
    try {
      if (ctx.normalizedPath().equals("/split_contig_at_bp")) {
        if (journal == null) throw new io.vertx.ext.web.handler.HttpException(404, "Open a map before cutting a contig");
        final String session = ctx.request().getHeader("X-HiCT-History-Session");
        final String revision = ctx.request().getHeader("X-HiCT-History-Revision");
        if (session == null || revision == null)
          throw new io.vertx.ext.web.handler.HttpException(400, "Exact cuts require the displayed history session and revision headers");
        final long expected = RequestJson.requireLong(RequestJson.requireObject(ctx), "expectedRevision");
        final long displayedRevision;
        try { displayedRevision = Long.parseLong(revision); }
        catch (NumberFormatException invalid) { throw new io.vertx.ext.web.handler.HttpException(400, "Invalid displayed history revision"); }
        if (displayedRevision != expected)
          throw new io.vertx.ext.web.handler.HttpException(409, "The cut revision does not match the displayed assembly");
        requireVersion(session, expected);
      }
      boolean sequenceSnapshot = ctx.normalizedPath().startsWith("/sequence/")
        && (ctx.normalizedPath().endsWith("/jobs") || ctx.normalizedPath().equals("/sequence/region")
          || ctx.normalizedPath().equals("/sequence/alignment_detail")
          || ctx.normalizedPath().equals("/sequence/repeat_period"));
      boolean coordinateExport = Set.of("/get_fasta_for_assembly","/get_fasta_for_selection","/get_agp_for_assembly").contains(ctx.normalizedPath());
      if ((label != null || sequenceSnapshot || coordinateExport || ctx.normalizedPath().equals("/session/snapshot")) && journal != null && ctx.request().getHeader("X-HiCT-History-Session") != null)
        requireVersion(ctx.request().getHeader("X-HiCT-History-Session"),
          Long.parseLong(ctx.request().getHeader("X-HiCT-History-Revision")));
      var result = action.get();
      if (before != null) {
        var after = capture();
        if (!same(before, after)) journal.append(label, after);
      }
      return result;
    } catch (Exception | Error failure) {
      if (before != null && !same(before, capture())) { restore(before); invalidate(); }
      throw failure;
    } finally {
      ensureSession();
      ctx.response().putHeader(HEADER, journal == null ? "null" : Json.encode(journal.summary()));
    }
  }

  @Override public void addHandlersToRouter(Router router) {
    router.post("/assembly/history").handler(ctx -> scheduler.submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY,
      null, () -> {
        var body = RequestJson.requireObject(ctx);
        if (journal == null) return new JsonObject().put("summary", null).put("entries", List.of());
        var summary = journal.summary();
        int offset = body.getValue("offset") == null ? Math.max(0, summary.position() - 49) : RequestJson.requireInt(body, "offset");
        int limit = body.getValue("limit") == null ? 100 : RequestJson.requireInt(body, "limit");
        return new JsonObject().put("summary", summary).put("offset", offset)
          .put("entries", journal.page(offset, limit));
      }, result -> ctx.response().end(Json.encodeToBuffer(result))));
    router.post("/assembly/history/restore").handler(ctx -> scheduler.submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY,
      null, () -> {
        if (journal == null) throw new NoOpenFileException("Open a file before using history");
        var body = RequestJson.requireObject(ctx);
        requireVersion(RequestJson.requireString(body, "session"), RequestJson.requireLong(body, "revision"));
        int target = journal.target(RequestJson.requireString(body, "action"), RequestJson.optionalLong(body, "id"));
        var previous = capture();
        try {
          final var states = journal.stateAt(target);
          restore(states);
          if (files.stream().anyMatch(file -> states.stream().noneMatch(state -> state.owner() == file))) {
            FileOpHandlersHolder.synchronizeOverlayAssemblyForSharedState(map);
          }
          long version = invalidate();
          var response = CompactAssemblyInfoDTO.response(FileOpHandlersHolder.resolveActiveAssemblyChunkedFile(map),
            version, body.getValue("assemblyFormat"));
          journal.movedTo(target);
          return response;
        } catch (Exception | Error failure) { restore(previous); invalidate(); throw failure; }
      }, result -> ctx.response().end(Json.encodeToBuffer(result))));
  }
}
