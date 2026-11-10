package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.SessionGraphValidator;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Owns unpublished restore resources. Closing a candidate never touches the live session.
 * Accepts an internally captured snapshot and typed track definitions; external documents
 * must pass their versioned document validator before entering this preparation layer.
 */
public final class SessionRestoreCandidate implements AutoCloseable {
  private ChunkedFile primary;
  private ChunkedFile secondary;
  private Track1DManager tracks;
  private RenderPipelineConfig pipeline;
  private ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions visualization;
  private Path primaryFasta;
  private Path secondaryFasta;

  private SessionRestoreCandidate() {}

  public ChunkedFile primary() { return primary; }
  public ChunkedFile secondary() { return secondary; }
  public Track1DManager tracks() { return tracks; }
  public RenderPipelineConfig pipeline() { return pipeline; }
  public ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions visualization() { return visualization; }
  public Path primaryFasta() { return primaryFasta; }
  public Path secondaryFasta() { return secondaryFasta; }

  public static SessionRestoreCandidate prepare(Path dataDirectory, Path processedDirectory, JsonObject document) throws Exception {
    return prepare(dataDirectory, processedDirectory, document, 1, 2);
  }

  /** Pool sizes apply to the prepared readers, so a published candidate serves tiles like a normally opened file. */
  public static SessionRestoreCandidate prepare(Path dataDirectory, Path processedDirectory, JsonObject document,
      int minDatasetPool, int maxDatasetPool) throws Exception {
    var decoded = SessionSnapshotDecoder.decode(document);
    return prepare(dataDirectory, processedDirectory, decoded.snapshot(), decoded.tracks(), minDatasetPool, maxDatasetPool);
  }

  static SessionRestoreCandidate prepare(Path dataDirectory, Path processedDirectory,
      SessionSnapshotDTO saved, List<Track1DManager.TrackSummary> definitions) throws Exception {
    return prepare(dataDirectory, processedDirectory, saved, definitions, 1, 2);
  }

  static SessionRestoreCandidate prepare(Path dataDirectory, Path processedDirectory,
      SessionSnapshotDTO saved, List<Track1DManager.TrackSummary> definitions,
      int minDatasetPool, int maxDatasetPool) throws Exception {
    if (saved.snapshotVersion() != 1) throw new IllegalArgumentException("Unsupported internal snapshot version");
    if (!Set.of("PRIMARY", "SECONDARY").contains(saved.assemblySource())
        || ("SECONDARY".equals(saved.assemblySource()) && saved.secondary() == null))
      throw new IllegalArgumentException("assemblySource references an unavailable source");
    if (definitions.size() != saved.tracks().size())
      throw new IllegalArgumentException("tracks do not match the saved definitions");
    final var ids = definitions.stream().map(Track1DManager.TrackSummary::getTrackId).collect(Collectors.toSet());
    SessionGraphValidator.validate(saved.renderPipeline(), ids);
    var visualization = SessionVisualizationCodec.decode(saved.visualizationOptions());
    final var root = dataDirectory.toRealPath();
    final var resources = new ArrayList<Resource>();
    final var primaryPath = resource(root, saved.primary(), "filename", "fileFingerprint", "primary", resources);
    final var secondaryPath = saved.secondary() == null ? null
      : resource(root, saved.secondary(), "filename", "fileFingerprint", "secondary", resources);
    final var primaryFasta = fasta(root, saved.primary(), "primary", resources);
    final var secondaryFasta = saved.secondary() == null ? null : fasta(root, saved.secondary(), "secondary", resources);
    for (int i = 0; i < definitions.size(); i++) {
      var track = saved.tracks().getJsonObject(i);
      var definition = definitions.get(i);
      var summary = track.copy();
      summary.remove("fileFingerprint");
      summary.remove("sourceReference");
      if (!summary.equals(JsonObject.mapFrom(definition)))
        throw new IllegalArgumentException("tracks[" + i + "] differs from its typed definition");
      if ("COOLER_WEIGHTS".equals(definition.getType())) {
        String source = definition.getSourceFile().endsWith("/SECONDARY") ? "SECONDARY" : "PRIMARY";
        if (!source.equals(track.getString("sourceReference"))
            || ("SECONDARY".equals(source) && saved.secondary() == null))
          throw new IllegalArgumentException("tracks[" + i + "].sourceReference is unavailable or inconsistent");
      } else {
        resource(root, track, "sourceFile", "fileFingerprint", "tracks[" + i + "]", resources);
      }
    }
    // Check every file before allocating a reader or preparing any layout.
    for (var resource : resources) resource.verify();
    final var candidate = new SessionRestoreCandidate();
    try {
      candidate.primary = open(primaryPath, saved.primary(), minDatasetPool, maxDatasetPool);
      if (secondaryPath != null) candidate.secondary = open(secondaryPath, saved.secondary(), minDatasetPool, maxDatasetPool);
      candidate.primaryFasta = primaryFasta;
      candidate.secondaryFasta = secondaryFasta;
      candidate.tracks = new Track1DManager(root, processedDirectory);
      candidate.tracks.restoreTrackDefinitions(definitions);
      var restored = candidate.tracks.listTracks().stream().map(JsonObject::mapFrom).toList();
      var expected = definitions.stream().map(JsonObject::mapFrom).toList();
      if (!restored.equals(expected)) throw new IllegalArgumentException("Prepared track definitions changed");
      if (primaryFasta != null) {
        candidate.primary.getFastaProcessor().analyzeLinkCandidate(primaryFasta);
        candidate.tracks.setLinkedFastaAliasesBySource(
          candidate.primary.getFastaProcessor().buildSourceNameAliases(primaryFasta));
      }
      if (secondaryFasta != null) candidate.secondary.getFastaProcessor().analyzeLinkCandidate(secondaryFasta);
      candidate.pipeline = RenderPipelineConfig.fromJson(saved.renderPipeline());
      candidate.visualization = visualization;
      // Readers/layout preparation may take time. Reject content changed during that window.
      for (var resource : resources) resource.verify();
      return candidate;
    } catch (Exception | Error failure) {
      try { candidate.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private static ChunkedFile open(Path path, JsonObject saved, int minPool, int maxPool) throws Exception {
    final var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, minPool, maxPool));
    try {
      SessionAssemblyRestorer.restore(file, saved);
      return file;
    } catch (Exception | Error failure) {
      try { file.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private static Path fasta(Path root, JsonObject source, String field, List<Resource> resources) throws IOException {
    return source.getString("fastaFilename", "").isBlank() ? null
      : resource(root, source, "fastaFilename", "fastaFingerprint", field, resources);
  }

  private static Path resource(Path root, JsonObject object, String filenameKey, String fingerprintKey,
      String field, List<Resource> resources) throws IOException {
    String name = object.getString(filenameKey);
    if (name == null || name.isBlank()) throw new IllegalArgumentException(field + "." + filenameKey + " is required");
    Path lexical = root.resolve(name).normalize();
    if (!lexical.startsWith(root)) throw new IllegalArgumentException(field + "." + filenameKey + " is outside DATA_DIR");
    // Same confinement as /open and /link_fasta: the name must stay inside DATA_DIR, while a symlink
    // placed there by the user (a common way to expose maps stored elsewhere) is followed.
    Path real = lexical.toRealPath();
    if (!Files.isRegularFile(real))
      throw new IllegalArgumentException(field + "." + filenameKey + " must resolve to a regular file");
    JsonObject json = object.getJsonObject(fingerprintKey);
    String fingerprintField = field + "." + fingerprintKey;
    if (json == null || !(json.getValue("algorithm") instanceof String algorithm) || !SessionFileFingerprint.supports(algorithm)
        || !(json.getValue("sizeBytes") instanceof Number size)
        || size.doubleValue() < 0 || !Double.isFinite(size.doubleValue())
        || size.doubleValue() != size.longValue()
        || !(json.getValue("sha256") instanceof String hash) || !hash.matches("[0-9a-f]{64}"))
      throw new IllegalArgumentException(fingerprintField + " is not a valid SHA-256 fingerprint");
    var expected = new SessionFileFingerprint(json.getString("algorithm"), json.getLong("sizeBytes"), json.getString("sha256"));
    resources.add(new Resource(lexical, real, expected, fingerprintField));
    return real;
  }

  private record Resource(Path requested, Path resolved, SessionFileFingerprint expected, String field) {
    void verify() throws IOException {
      if (!Objects.equals(requested.toRealPath(), resolved))
        throw new IOException(field + ": " + requested.getFileName() + " now resolves to a different file");
      final var reason = expected.explainDifference(SessionFileFingerprint.compute(resolved, expected.algorithm()));
      if (reason != null)
        throw new IOException(field + ": " + requested.getFileName() + " changed since the session was saved, " + reason);
      if (!Objects.equals(requested.toRealPath(), resolved))
        throw new IOException(field + ": " + requested.getFileName() + " now resolves to a different file");
    }
  }

  /** Hands the prepared resources to the caller. After this call close() releases nothing. */
  public SessionRestoreCandidate detach() {
    final var owned = new SessionRestoreCandidate();
    owned.primary = primary;
    owned.secondary = secondary;
    owned.tracks = tracks;
    owned.pipeline = pipeline;
    owned.visualization = visualization;
    owned.primaryFasta = primaryFasta;
    owned.secondaryFasta = secondaryFasta;
    primary = null;
    secondary = null;
    tracks = null;
    return owned;
  }

  @Override public void close() throws Exception {
    Exception failure = null;
    if (tracks != null) {
      try { tracks.close(); } catch (Exception e) { failure = e; }
      tracks = null;
    }
    for (var file : new ChunkedFile[]{secondary, primary}) {
      if (file != null) try { file.close(); }
      catch (Exception e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
    }
    secondary = null;
    primary = null;
    if (failure != null) throw failure;
  }
}
