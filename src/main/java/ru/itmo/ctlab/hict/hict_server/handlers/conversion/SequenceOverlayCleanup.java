package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;
import java.nio.file.*;
import java.io.IOException;
import java.util.*;

/** Called only on the assembly scheduler: opening/closing a source cannot race deletion. */
public final class SequenceOverlayCleanup {
  private SequenceOverlayCleanup() { }
  public static int collect(LocalMap<String, Object> shared) throws IOException {
    var wrapper = (ShareableWrappers.PathWrapper) shared.get("dataDirectory");
    if (wrapper == null) return 0;
    Path data = wrapper.getPath().toRealPath(), root = data.resolve("processed/sequence-analysis");
    if (!Files.isDirectory(root) || !root.toRealPath().startsWith(data)) return 0;
    var open = new HashSet<Path>();
    for (String key : List.of("chunkedFile", "chunkedFileSecondary")) {
      var map = (ShareableWrappers.ChunkedFileWrapper) shared.get(key);
      if (map != null) open.add(map.getChunkedFile().getHdfFilePath().toRealPath());
    }
    // Keep the receipt graph: an attached A may outlive intermediate B in A -> B -> C.
    var successors = new LinkedHashMap<Path, Path>();
    try (var receipts = Files.newDirectoryStream(root, "superseded-*.json")) {
      for (var receipt : receipts) {
        var item = new JsonObject(Files.readString(receipt));
        Path old = managed(data, root, item.getString("previous"));
        Path next = managed(data, root, item.getString("replacement"));
        if (successors.putIfAbsent(old, next) != null) throw new IOException("Ambiguous overlay replacement receipt");
      }
    }
    int removed = 0;
    for (var entry : successors.entrySet()) {
      Path old = entry.getKey();
      if (!Files.isRegularFile(old) || open.contains(old.toRealPath())) continue;
      Path replacement = entry.getValue();
      var visited = new HashSet<Path>(); visited.add(old);
      // Every link must represent a successfully finished, durably recorded job.
      boolean valid = true;
      while (true) {
        if (!visited.add(replacement)) { valid = false; break; }
        var record = replacement.getParent().resolve("job.json");
        if (!Files.isRegularFile(record)) { valid = false; break; }
        var job = new JsonObject(Files.readString(record));
        if (!"finished".equals(job.getString("status"))
            || !replacement.equals(managed(data, root, job.getString("outputFile")))) { valid = false; break; }
        if (Files.isRegularFile(replacement)) break;
        replacement = successors.get(replacement);
        if (replacement == null) { valid = false; break; }
      }
      if (!valid) continue;
      // Reopen the surviving replacement before reclaiming any previous matrix.
      try (var reader = ch.systemsx.cisd.hdf5.HDF5Factory.openForReading(replacement.toFile())) {
        if (!reader.object().isGroup("/sequence_analysis") || !reader.bool().getAttr("/sequence_analysis", "complete")) continue;
      }
      Files.delete(old); removed++;
      // Preserve receipts, logs and old job provenance as an audit trail.
    }
    return removed;
  }
  static Path managed(Path data, Path root, String relative) throws IOException {
    if (relative == null) throw new IOException("Missing managed overlay path");
    var p = data.resolve(relative).normalize();
    if (!p.startsWith(root) || !p.getParent().getParent().equals(root)
        || !p.getParent().getFileName().toString().matches("[a-f0-9-]{36}")
        || !Set.of("dotplot.hict.hdf5", "moddotplot.hict.hdf5").contains(p.getFileName().toString())
        || Files.isSymbolicLink(p) || !p.getParent().toRealPath().startsWith(root.toRealPath()))
      throw new IOException("Refusing to remove an unmanaged overlay: " + relative);
    return p;
  }
}
