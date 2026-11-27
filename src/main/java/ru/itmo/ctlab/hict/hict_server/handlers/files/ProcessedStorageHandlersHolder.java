package ru.itmo.ctlab.hict.hict_server.handlers.files;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.HttpException;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.jobs.JobArtifacts;
import ru.itmo.ctlab.hict.hict_server.util.cache.FileFingerprintService;
import ru.itmo.ctlab.hict.hict_server.util.cache.MatrixConversionCacheManager;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;

public final class ProcessedStorageHandlersHolder extends HandlersHolder {
  private static final int OLD_DAYS = 30;
  private static final Pattern CACHE_FILE = Pattern.compile("[a-f0-9]{64}\\.h5");
  private static final Pattern RECEIPT = Pattern.compile("[a-f0-9]{64}\\.json");
  private static final Pattern OVERVIEW = Pattern.compile("overview-[a-f0-9]{64}\\.hict\\.hdf5");
  private static final Pattern SELF_DOTPLOT = Pattern.compile(".+\\.self(?:\\.k[0-9]+w[0-9]+)?\\.hict\\.hdf5");
  private static final Pattern FASTA_ONLY = Pattern.compile(".+\\.(?:whole|split[0-9]+)\\.hict\\.hdf5");
  private static final Set<String> SEQUENCE_MAPS = Set.of("dotplot.hict.hdf5", "moddotplot.hict.hdf5");
  private static final Set<String> SEQUENCE_SCRATCH = Set.of("assembly.fasta", "assembly-chunks.fasta",
    "fasta-template.hict.hdf5", "moddotplot.hict.hdf5.pending", "dotplot.hict.hdf5.pending", "overview.pending");
  private static final Set<String> TERMINAL = Set.of("finished", "failed", "cancelled");
  private static final Set<String> ACTIONS = Set.of("cache", "old", "maps");

  private final Vertx vertx;
  private final RequestTaskScheduler scheduler;
  private final BooleanSupplier jobsActive;

  public ProcessedStorageHandlersHolder(Vertx vertx, RequestTaskScheduler scheduler,
                                        BooleanSupplier jobsActive) {
    this.vertx = vertx;
    this.scheduler = scheduler;
    this.jobsActive = jobsActive;
  }

  @Override
  public void addHandlersToRouter(Router router) {
    router.get("/storage/processed").handler(ctx ->
      scheduler.submit(ctx, RequestTaskScheduler.RequestPriority.UI_UX, null, this::inventory,
        response -> ctx.json(response)));
    router.post("/storage/processed/cleanup").handler(ctx ->
      scheduler.submit(ctx, RequestTaskScheduler.RequestPriority.ASSEMBLY, null,
        () -> cleanup(ctx.body().asJsonObject()), response -> ctx.json(response)));
  }

  private LocalMap<String, Object> shared() {
    return vertx.sharedData().getLocalMap("hict_server");
  }

  private JsonObject inventory() throws IOException {
    final var scan = scan();
    final var map = shared();
    final var entries = scan.entries.values().stream()
      .sorted(Comparator.comparingLong((Entry entry) -> entry.attributes.size()).reversed()
        .thenComparing(entry -> entry.path.toString()))
      .map(entry -> entry.toJson(protection(map, entry)))
      .toList();
    return new JsonObject()
      .put("roots", scan.roots.stream().map(Path::toString).toList())
      .put("files", entries)
      .put("oldDays", OLD_DAYS)
      .put("jobsActive", jobsActive.getAsBoolean())
      .put("unreadable", scan.unreadable.size());
  }

  private JsonObject cleanup(JsonObject request) throws IOException {
    final var action = request == null ? "" : request.getString("action", "");
    final var requested = request == null ? new JsonArray() : request.getJsonArray("ids", new JsonArray());
    if (!ACTIONS.contains(action) || requested.isEmpty()) {
      throw new HttpException(400, "Choose files and a cleanup action.");
    }
    final var ids = new HashSet<String>();
    for (var item : requested) {
      if (!(item instanceof String id)) throw new HttpException(400, "Invalid storage file selection.");
      ids.add(id);
    }
    requireIdle();
    final var scan = scan();
    final var map = shared();
    final var skipped = new ArrayList<JsonObject>();
    long bytes = 0;
    int deleted = 0;
    synchronized (map) {
      requireIdle();
      for (var entry : scan.entries.values()) {
        if (!ids.remove(entry.id())) continue;
        if (!action.equals(entry.action)) {
          skipped.add(skip(entry.path, "File is ineligible for this cleanup"));
          continue;
        }
        final var reason = protection(map, entry);
        if (reason != null) {
          skipped.add(skip(entry.path, reason));
          continue;
        }
        try {
          final var now = safeAttributes(entry.path);
          if (now == null || !entry.id().equals(fileId(entry.path, entry.category, now))) {
            skipped.add(skip(entry.path, "File changed during cleanup"));
            continue;
          }
          if (jobsActive.getAsBoolean()) {
            skipped.add(skip(entry.path, "A background job started"));
            continue;
          }
          Files.delete(entry.path);
          bytes += entry.attributes.size();
          deleted++;
        } catch (IOException failure) {
          skipped.add(skip(entry.path, "Could not remove file"));
        }
      }
      if (deleted > 0 && action.equals("cache")
        && map.get("Track1DManager") instanceof ShareableWrappers.Track1DManagerWrapper wrapper) {
        wrapper.getTrack1DManager().invalidateInMemoryCache();
        wrapper.getTrack1DManager().clearPrecomputeStatus();
      }
    }
    return new JsonObject().put("deleted", deleted).put("bytes", bytes)
      .put("changedOrMissing", ids.size()).put("skipped", skipped);
  }

  private void requireIdle() {
    if (jobsActive.getAsBoolean()) {
      throw new HttpException(409, "Wait for background jobs to finish before cleaning up storage.");
    }
  }

  private static JsonObject skip(Path path, String reason) {
    return new JsonObject().put("path", path.toString()).put("reason", reason);
  }

  private static String protection(LocalMap<String, Object> map, Entry entry) {
    if (entry.protectedReason != null) return entry.protectedReason;
    if (!entry.path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".hict.hdf5")) return null;
    try {
      JobArtifacts.requireClosedMap(map, entry.path);
      return null;
    } catch (HttpException opened) {
      return "Open in the workspace";
    } catch (IOException failure) {
      return "Could not check whether this map is open";
    }
  }

  private Scan scan() throws IOException {
    final var map = shared();
    final var data = ((ShareableWrappers.PathWrapper) map.get("dataDirectory")).getPath().toRealPath();
    final var configured = map.get("processedDirectory") instanceof ShareableWrappers.PathWrapper wrapper
      ? wrapper.getPath() : data.resolve("processed");
    final var processed = Files.isDirectory(configured) ? configured.toRealPath() : configured.toAbsolutePath().normalize();
    final var cache = new MatrixConversionCacheManager(data, processed, new FileFingerprintService());
    final var sequence = data.resolve("processed/sequence-analysis");
    final var fastaOnly = data.resolve("fasta-only");
    final var trackCache = processed.resolve("track_precompute");
    final var coverageCache = processed.resolve("coverage");
    final var receipts = cache.metadataDirectory();
    final var roots = new ArrayList<Path>();
    if (!data.startsWith(processed) && processed.getParent() != null) {
      roots.add(processed);
    } else {
      roots.add(trackCache);
      roots.add(coverageCache);
      roots.add(receipts);
    }
    if (roots.stream().noneMatch(sequence::startsWith)) roots.add(sequence);
    if (roots.stream().noneMatch(fastaOnly::startsWith)) roots.add(fastaOnly);
    final var scan = new Scan(List.copyOf(roots), new LinkedHashMap<>(), new HashSet<>());
    final long cutoffMs = System.currentTimeMillis() - Duration.ofDays(OLD_DAYS).toMillis();
    final Map<Path, Boolean> historicalJobs = new LinkedHashMap<>();
    for (var root : roots) {
      try {
        if (Files.notExists(root, LinkOption.NOFOLLOW_LINKS)) continue;
        if (!root.equals(root.toRealPath())) continue;
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) throws IOException {
            return attributes.isOther() || !dir.equals(dir.toRealPath())
              ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path path, BasicFileAttributes attributes) {
            if (!attributes.isRegularFile()) return FileVisitResult.CONTINUE;
            final var parent = path.getParent();
            final var name = path.getFileName().toString();
            Entry entry = new Entry(path, attributes, "other", null, null);
            if (parent.equals(coverageCache) && name.matches("[0-9a-f]{64}\\.bin")) {
              entry = new Entry(path, attributes, "coverage", "cache", null);
            } else if (parent.equals(trackCache) && CACHE_FILE.matcher(name).matches()
                || parent.equals(sequence) && OVERVIEW.matcher(name).matches()) {
              entry = new Entry(path, attributes, "cache", "cache", null);
            } else if (parent.equals(receipts) && RECEIPT.matcher(name).matches()) {
              indexConversion(scan, cache, path);
              entry = new Entry(path, attributes, "metadata", null, null);
            } else if (parent.equals(fastaOnly) && FASTA_ONLY.matcher(name).matches()
                || parent.equals(processed) && SELF_DOTPLOT.matcher(name).matches()) {
              entry = new Entry(path, attributes, "maps", "maps", null);
            } else if (parent.getParent() != null && parent.getParent().equals(sequence) && isJobId(parent)) {
              entry = sequenceEntry(entry, historicalJobs, cutoffMs);
            } else if (JobArtifacts.isStagingDirectoryName(parent.getFileName().toString())) {
              entry = scratch(entry, cutoffMs, true);
            }
            scan.entries.putIfAbsent(path, entry);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path path, IOException failure) {
            scan.unreadable.add(path);
            return FileVisitResult.CONTINUE;
          }
        });
      } catch (IOException failure) {
        scan.unreadable.add(root);
      }
    }
    return scan;
  }

  private static void indexConversion(Scan scan, MatrixConversionCacheManager cache, Path receipt) {
    try {
      final var recorded = cache.recordedMap(receipt);
      if (recorded == null) return;
      final var attributes = safeAttributes(recorded.path());
      if (attributes == null) return;
      final var fingerprint = recorded.fingerprint();
      final var reason = attributes.size() != fingerprint.sizeBytes()
        || attributes.lastModifiedTime().toMillis() != fingerprint.modifiedAtMs()
        ? "Changed since conversion"
        : recorded.sourceAvailable() ? null : "Original conversion input is unavailable";
      final var entry = new Entry(recorded.path(), attributes, "maps", "maps", reason);
      scan.entries.merge(recorded.path(), entry, (previous, candidate) ->
        previous.protectedReason == null && "maps".equals(previous.action) && candidate.protectedReason != null
          && previous.attributes.lastModifiedTime().equals(candidate.attributes.lastModifiedTime())
          ? previous : candidate);
    } catch (RuntimeException | IOException failure) {
      scan.unreadable.add(receipt);
    }
  }

  private static Entry sequenceEntry(Entry entry, Map<Path, Boolean> historicalJobs, long cutoffMs) {
    final var name = entry.path.getFileName().toString();
    final var parent = entry.path.getParent();
    if (SEQUENCE_MAPS.contains(name)) return new Entry(entry.path, entry.attributes, "dotplots", "maps", null);
    if (SEQUENCE_SCRATCH.contains(name)) {
      return scratch(entry, cutoffMs, historicalJobs.computeIfAbsent(parent, ProcessedStorageHandlersHolder::terminalJob));
    }
    return new Entry(entry.path, entry.attributes, name.equals("job.json") ? "metadata" : "sequence", null, null);
  }

  private static Entry scratch(Entry entry, long cutoffMs, boolean inactive) {
    final var removable = inactive && entry.attributes.lastModifiedTime().toMillis() < cutoffMs;
    return new Entry(entry.path, entry.attributes, "scratch", removable ? "old" : null, null);
  }

  private static boolean terminalJob(Path directory) {
    final var receipt = directory.resolve("job.json");
    try {
      if (safeAttributes(receipt) == null) return false;
      final var job = new JsonObject(Files.readString(receipt));
      return directory.getFileName().toString().equals(job.getString("jobId"))
        && TERMINAL.contains(job.getString("status", ""));
    } catch (RuntimeException | IOException failure) {
      return false;
    }
  }

  private static boolean isJobId(Path directory) {
    final var name = directory.getFileName().toString();
    try {
      return UUID.fromString(name).toString().equals(name);
    } catch (IllegalArgumentException invalid) {
      return false;
    }
  }

  private static BasicFileAttributes safeAttributes(Path path) throws IOException {
    if (Files.notExists(path, LinkOption.NOFOLLOW_LINKS)) return null;
    final var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    return attributes.isRegularFile() && path.equals(path.toRealPath()) ? attributes : null;
  }

  private static String fileId(Path path, String category, BasicFileAttributes attributes) {
    final var stamp = path + "\n" + category + "\n" + attributes.size() + "\n"
      + attributes.lastModifiedTime() + "\n" + attributes.creationTime() + "\n" + attributes.fileKey();
    return UUID.nameUUIDFromBytes(stamp.getBytes(StandardCharsets.UTF_8)).toString();
  }

  private record Scan(List<Path> roots, Map<Path, Entry> entries, Set<Path> unreadable) {}

  private record Entry(Path path, BasicFileAttributes attributes, String category, String action, String protectedReason) {
    String id() {
      return fileId(path, category, attributes);
    }

    JsonObject toJson(String reason) {
      return new JsonObject().put("id", id()).put("path", path.toString()).put("category", category)
        .put("bytes", attributes.size()).put("modifiedAtMs", attributes.lastModifiedTime().toMillis())
        .put("action", action).put("protectedReason", reason);
    }
  }
}
