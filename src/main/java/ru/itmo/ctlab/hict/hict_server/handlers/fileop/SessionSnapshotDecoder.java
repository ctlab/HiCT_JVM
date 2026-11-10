package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.SessionGraphValidator;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/** Pure structural decoder for the resource portion of a session.
 * This is the internal snapshot version, not the portable v3 envelope.
 * Does not open files or trust the producer's verification flags.
 */
public final class SessionSnapshotDecoder {
  public static final int MAX_BYTES = 8 * 1024 * 1024;
  private static final long MAX_INTEGER = 9_007_199_254_740_991L;
  public record Decoded(SessionSnapshotDTO snapshot, List<Track1DManager.TrackSummary> tracks) {}
  private int values;
  private long stringBytes;
  private SessionSnapshotDecoder() {}

  public static Decoded decode(JsonObject input) {
    var decoder = new SessionSnapshotDecoder();
    decoder.bound(input, "snapshot", 0);
    if (input == null) fail("snapshot", "expected an object");
    if (input.encode().getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) fail("snapshot", "exceeds 8 MiB");
    // Own the input tree so later caller changes cannot alter the prepared document.
    var root = input.copy();
    if (integer(root, "snapshotVersion", "snapshot", 1, MAX_INTEGER) != 1) fail("snapshot.snapshotVersion", "unsupported version");
    var primary = object(root, "primary", "snapshot");
    source(primary, "primary");
    JsonObject secondary = null;
    if (root.getValue("secondary") != null) {
      secondary = object(root, "secondary", "snapshot");
      source(secondary, "secondary");
    }
    String active = choice(root, "assemblySource", "snapshot", "PRIMARY", "SECONDARY");
    if (active.equals("SECONDARY") && secondary == null) fail("snapshot.assemblySource", "secondary source is missing");
    var trackArray = array(root, "tracks", "snapshot");
    var definitions = new ArrayList<Track1DManager.TrackSummary>();
    var ids = new HashSet<String>();
    for (int i = 0; i < trackArray.size(); i++) {
      String path = "tracks[" + i + "]";
      var track = asObject(trackArray.getValue(i), path);
      String id = string(track, "trackId", path, false);
      if (!id.matches("trk_[1-9][0-9]*") || !ids.add(id)) fail(path + ".trackId", "invalid or duplicate ID");
      try { if (Long.parseLong(id.substring(4)) == Long.MAX_VALUE) fail(path + ".trackId", "out of range"); }
      catch (NumberFormatException invalid) { fail(path + ".trackId", "out of range"); }
      String type = choice(track, "type", path, "BED", "VCF", "GFF_GTF", "BIGWIG", "BAM", "COOLER_WEIGHTS");
      String file = string(track, "sourceFile", path, false);
      if (type.equals("COOLER_WEIGHTS")) {
        String reference = choice(track, "sourceReference", path, "PRIMARY", "SECONDARY");
        if (reference.equals("SECONDARY") && secondary == null) fail(path + ".sourceReference", "secondary source is missing");
        if (!(file.equals("__internal__/cooler_weights/" + reference)
            || (reference.equals("PRIMARY") && file.equals("__internal__/cooler_weights"))))
          fail(path + ".sourceFile", "inconsistent virtual source");
      } else fingerprint(object(track, "fileFingerprint", path), path + ".fileFingerprint");
      String color = string(track, "color", path, false);
      if (!color.matches("#[0-9a-fA-F]{6}")) fail(path + ".color", "expected #RRGGBB");
      double min = number(track, "rangeMin", path), max = number(track, "rangeMax", path);
      if (min >= max) fail(path + ".rangeMax", "must exceed rangeMin");
      definitions.add(new Track1DManager.TrackSummary(id, string(track, "name", path, false), type, file, color,
        bool(track, "visible", path), integer(track, "featureCount", path, -1, MAX_INTEGER),
        choice(track, "renderStyle", path, "SIGNAL", "FEATURE"),
        choice(track, "renderMode", path, "COVERAGE", "READ_DENSITY"),
        choice(track, "aggregationMode", path, "MAX", "MEAN", "SUM"),
        bool(track, "logScale", path), bool(track, "rangeAuto", path), min, max));
    }
    var graph = object(root, "renderPipeline", "snapshot");
    SessionGraphValidator.validate(graph, ids);
    var visualization = object(root, "visualizationOptions", "snapshot");
    SessionVisualizationCodec.decode(visualization);
    var workspace = object(root, "workspace", "snapshot");
    workspace(workspace, active.equals("SECONDARY") ? secondary : primary);
    return new Decoded(new SessionSnapshotDTO(1,
      bool(root, "assemblyVerified", "snapshot"), bool(root, "filesFingerprinted", "snapshot"),
      bool(root, "tracksVerified", "snapshot"), primary, secondary, active, graph, trackArray, workspace, visualization),
      List.copyOf(definitions));
  }

  private static void workspace(JsonObject workspace, JsonObject activeSource) {
    if (workspace.getValue("workspaceState") == null) return;
    var state = object(workspace, "workspaceState", "workspace");
    String path = "workspace.workspaceState";
    if (integer(state, "version", path, 1, MAX_INTEGER) != 1) fail(path + ".version", "unsupported version");
    if (state.containsKey("view")) {
      var view = object(state, "view", path);
      if (view.containsKey("center")) {
        var center = array(view, "center", path + ".view");
        if (center.size() != 2) fail(path + ".view.center", "expected two coordinates");
        for (int i = 0; i < 2; i++) {
          if (!(center.getValue(i) instanceof Number n) || !Double.isFinite(n.doubleValue()))
            fail(path + ".view.center[" + i + "]", "expected a finite coordinate");
        }
      }
      if (view.containsKey("resolution") && number(view, "resolution", path + ".view") <= 0)
        fail(path + ".view.resolution", "must be positive");
      number(view, "rotation", path + ".view");
    }
    if (state.containsKey("axisScopes")) {
      var scopes = object(state, "axisScopes", path);
      var assembly = activeSource.getJsonObject("assemblyInfo");
      for (String axis : new String[]{"row", "column"}) {
        String field = path + ".axisScopes." + axis;
        var scope = object(scopes, axis, path + ".axisScopes");
        String kind = choice(scope, "kind", field, "all", "contig", "scaffold");
        string(scope, "label", field, false);
        if (kind.equals("all")) continue;
        long id = integer(scope, "id", field, 0, MAX_INTEGER);
        var descriptors = assembly.getJsonArray(kind.equals("contig") ? "contigDescriptors" : "scaffoldDescriptors");
        boolean found = false;
        long start = 0;
        for (int i = 0; i < descriptors.size(); i++) {
          var descriptor = descriptors.getJsonObject(i);
          long end;
          if (kind.equals("contig")) end = start + descriptor.getLong("contigLengthBp");
          else {
            start = descriptor.getJsonObject("scaffoldBordersBP").getLong("startBP");
            end = descriptor.getJsonObject("scaffoldBordersBP").getLong("endBP");
          }
          if (descriptor.getLong(kind + "Id") == id) {
            found = true;
            if (scope.containsKey("startBp") && integer(scope, "startBp", field, 0, MAX_INTEGER) != start)
              fail(field + ".startBp", "does not match component interval");
            if (scope.containsKey("endBp") && integer(scope, "endBp", field, 0, MAX_INTEGER) != end)
              fail(field + ".endBp", "does not match component interval");
            break;
          }
          start = end;
        }
        if (!found) fail(field + ".id", "references a missing component");
      }
    }
  }

  private static void source(JsonObject source, String path) {
    string(source, "filename", path, false);
    String fasta = string(source, "fastaFilename", path, true);
    fingerprint(object(source, "fileFingerprint", path), path + ".fileFingerprint");
    if (!fasta.isBlank()) fingerprint(object(source, "fastaFingerprint", path), path + ".fastaFingerprint");
    String agp = string(source, "agp", path, true);
    String hash = string(source, "agpSha256", path, false);
    try {
      String actual = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(agp.getBytes(StandardCharsets.UTF_8)));
      if (!actual.equals(hash)) fail(path + ".agpSha256", "does not match AGP content");
    } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    var assembly = object(source, "assemblyInfo", path);
    var contigs = array(assembly, "contigDescriptors", path + ".assemblyInfo");
    var contigIds = new HashSet<Long>();
    Set<String> resolutions = null;
    long length = 0;
    for (int i = 0; i < contigs.size(); i++) {
      String field = path + ".assemblyInfo.contigDescriptors[" + i + "]";
      var contig = asObject(contigs.getValue(i), field);
      long id = integer(contig, "contigId", field, 0, Integer.MAX_VALUE);
      if (!contigIds.add(id)) fail(field + ".contigId", "duplicate ID");
      name(contig, "contigName", field);
      name(contig, "contigOriginalName", field);
      string(contig, "contigSourceName", field, false);
      integer(contig, "contigOffsetInSource", field, 0, Integer.MAX_VALUE);
      integer(contig, "contigDirection", field, 0, 1);
      long bp = integer(contig, "contigLengthBp", field, 1, MAX_INTEGER);
      if (bp > MAX_INTEGER - length) fail(field + ".contigLengthBp", "assembly length exceeds supported range");
      length += bp;
      var bins = object(contig, "contigLengthBins", field);
      var visibility = object(contig, "contigPresenceAtResolution", field);
      if (!bins.fieldNames().equals(visibility.fieldNames())) fail(field, "resolution fields disagree");
      if (resolutions == null) resolutions = Set.copyOf(bins.fieldNames());
      else if (!resolutions.equals(bins.fieldNames())) fail(field, "contig resolution sets disagree");
      for (String key : bins.fieldNames()) {
        decimalKey(key, field + ".contigLengthBins." + key, 1);
        integer(bins, key, field + ".contigLengthBins", 0, MAX_INTEGER);
        integer(visibility, key, field + ".contigPresenceAtResolution", 0, 1);
      }
    }
    var scaffolds = array(assembly, "scaffoldDescriptors", path + ".assemblyInfo");
    var scaffoldIds = new HashSet<Long>();
    long previousEnd = 0;
    for (int i = 0; i < scaffolds.size(); i++) {
      String field = path + ".assemblyInfo.scaffoldDescriptors[" + i + "]";
      var scaffold = asObject(scaffolds.getValue(i), field);
      long id = integer(scaffold, "scaffoldId", field, 0, MAX_INTEGER);
      if (!scaffoldIds.add(id)) fail(field + ".scaffoldId", "duplicate ID");
      name(scaffold, "scaffoldName", field);
      name(scaffold, "scaffoldOriginalName", field);
      integer(scaffold, "spacerLength", field, 0, MAX_INTEGER);
      var borders = object(scaffold, "scaffoldBordersBP", field);
      long start = integer(borders, "startBP", field + ".scaffoldBordersBP", 0, length);
      long end = integer(borders, "endBP", field + ".scaffoldBordersBP", 0, length);
      if (start < previousEnd || end <= start) fail(field + ".scaffoldBordersBP", "overlapping or empty interval");
      if (integer(borders, "lengthBp", field + ".scaffoldBordersBP", 1, length) != end - start)
        fail(field + ".scaffoldBordersBP.lengthBp", "does not match interval");
      previousEnd = end;
    }
    overrides(object(source, "contigNameOverrides", path), contigIds, path + ".contigNameOverrides");
    overrides(object(source, "scaffoldNameOverrides", path), scaffoldIds, path + ".scaffoldNameOverrides");
    object(source, "capabilities", path);
  }

  private static void overrides(JsonObject names, Set<Long> ids, String path) {
    for (String key : names.fieldNames()) {
      long id = decimalKey(key, path + "." + key, 0);
      if (!ids.contains(id)) fail(path + "." + key, "references a missing component");
      name(names, key, path);
    }
  }

  private static long decimalKey(String key, String path, long min) {
    try {
      long value = Long.parseLong(key);
      if (value < min || value > MAX_INTEGER || !Long.toString(value).equals(key)) fail(path, "invalid integer key");
      return value;
    } catch (NumberFormatException invalid) { fail(path, "invalid integer key"); return 0; }
  }

  private static void fingerprint(JsonObject json, String path) {
    choice(json, "algorithm", path, "SHA-256");
    integer(json, "sizeBytes", path, 0, MAX_INTEGER);
    if (!string(json, "sha256", path, false).matches("[0-9a-f]{64}")) fail(path + ".sha256", "invalid SHA-256 digest");
  }

  private void bound(Object value, String path, int depth) {
    if (depth > 64 || ++values > 100000) fail(path, "exceeds snapshot complexity limit");
    if (value instanceof JsonObject object) {
      for (String key : object.fieldNames()) {
        if (Set.of("__proto__", "constructor", "prototype").contains(key)) fail(path + "." + key, "reserved key");
        countString(key, path);
        bound(object.getValue(key), path + "." + key, depth + 1);
      }
    } else if (value instanceof JsonArray array) {
      for (int i = 0; i < array.size(); i++) bound(array.getValue(i), path + "[" + i + "]", depth + 1);
    } else if (value instanceof String text) countString(text, path);
    else if (value instanceof Number number) {
      if (!Double.isFinite(number.doubleValue())) fail(path, "expected a finite number");
    } else if (value != null && !(value instanceof Boolean)) fail(path, "unsupported JSON value");
  }

  private void countString(String value, String path) {
    if (value.length() > MAX_BYTES) fail(path, "exceeds 8 MiB");
    stringBytes += value.getBytes(StandardCharsets.UTF_8).length;
    if (stringBytes > MAX_BYTES) fail(path, "exceeds 8 MiB");
  }

  private static JsonObject asObject(Object value, String path) {
    if (!(value instanceof JsonObject)) fail(path, "expected an object");
    return (JsonObject)value;
  }
  private static JsonObject object(JsonObject parent, String key, String path) {
    return asObject(parent.getValue(key), path + "." + key);
  }
  private static JsonArray array(JsonObject parent, String key, String path) {
    if (!(parent.getValue(key) instanceof JsonArray)) fail(path + "." + key, "expected an array");
    return parent.getJsonArray(key);
  }
  private static String string(JsonObject parent, String key, String path, boolean empty) {
    Object value = parent.getValue(key);
    if (!(value instanceof String) || (!empty && ((String)value).isBlank())) fail(path + "." + key, "expected a string" + (empty ? "" : " with content"));
    return (String)value;
  }
  private static void name(JsonObject parent, String key, String path) {
    String value = string(parent, key, path, false);
    try { ChunkedFile.validateDisplayName(value); }
    catch (IllegalArgumentException invalid) { fail(path + "." + key, invalid.getMessage()); }
  }
  private static boolean bool(JsonObject parent, String key, String path) {
    if (!(parent.getValue(key) instanceof Boolean)) fail(path + "." + key, "expected a boolean");
    return parent.getBoolean(key);
  }
  private static double number(JsonObject parent, String key, String path) {
    if (!(parent.getValue(key) instanceof Number n) || !Double.isFinite(n.doubleValue())) fail(path + "." + key, "expected a finite number");
    return ((Number)parent.getValue(key)).doubleValue();
  }
  private static long integer(JsonObject parent, String key, String path, long min, long max) {
    number(parent, key, path);
    try {
      long value = new BigDecimal(parent.getValue(key).toString()).longValueExact();
      if (value < min || value > max) fail(path + "." + key, "integer is out of range");
      return value;
    } catch (ArithmeticException | NumberFormatException invalid) { fail(path + "." + key, "expected an exact integer"); return 0; }
  }
  private static String choice(JsonObject parent, String key, String path, String... choices) {
    String value = string(parent, key, path, false);
    if (!Set.of(choices).contains(value)) fail(path + "." + key, "unsupported value " + value);
    return value;
  }
  private static void fail(String path, String message) {
    throw new IllegalArgumentException(path + ": " + message);
  }
}
