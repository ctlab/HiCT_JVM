package ru.itmo.ctlab.hict.hict_server.tracks;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.assembly.AssemblySequenceSnapshot;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** BED12+1 annotation metadata, independent of transient assembly coordinates. */
public record SequenceSearchMetadata(String hitId, String queryId, String description, String strand,
                                     AssemblySequenceSnapshot path, String snapshotFingerprint) {
  public static final String PREFIX = "hict-search-v1:";
  public String encode() {
    var pieces = new JsonArray();
    for (var p : path.fragments()) pieces.add(new JsonArray().add(p.source()).add(p.start()).add(p.end()).add(p.reversed()));
    var json = new JsonObject().put("hitId", hitId).put("queryId", queryId).put("description", description)
      .put("strand", strand).put("snapshot", snapshotFingerprint).put("path", pieces);
    return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(json.encode().getBytes(StandardCharsets.UTF_8));
  }
  public static SequenceSearchMetadata decode(String value) {
    if (value == null || !value.startsWith(PREFIX)) throw new IllegalArgumentException("Not a HiCT search annotation");
    var json = new JsonObject(new String(Base64.getUrlDecoder().decode(value.substring(PREFIX.length())), StandardCharsets.UTF_8));
    var pieces = new java.util.ArrayList<AssemblySequenceSnapshot.Fragment>();
    for (Object item : json.getJsonArray("path")) {
      var p = (JsonArray) item;
      pieces.add(new AssemblySequenceSnapshot.Fragment(p.getString(0), p.getLong(1), p.getLong(2), p.getBoolean(3)));
    }
    var strand = json.getString("strand");
    if (!"+".equals(strand) && !"-".equals(strand)) throw new IllegalArgumentException("Invalid search hit strand");
    return new SequenceSearchMetadata(json.getString("hitId"), json.getString("queryId"), json.getString("description"),
      strand, new AssemblySequenceSnapshot(pieces), json.getString("snapshot"));
  }
}
