package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.nio.file.Path;
import ch.systemsx.cisd.hdf5.HDF5Factory;

/** Explicit estimate provenance, independent of signal units or normalization availability. */
public final class ApproximationProvenance {
  public static final String MARKER = "hict-approximate-export";
  public static final String GROUP = "/hict/approximate_export";
  public record Summary(int schemaVersion, String method, String resolutionPolicy,
                        String description, boolean upstreamApproximate) {}
  private ApproximationProvenance() {}

  public static @Nullable Summary read(IHDF5Reader reader, List<Long> resolutions) {
    // Sequence-derived values have their own authoritative score semantics;
    // an inherited contact template does not make them rebinned contacts.
    if (reader.object().isGroup("/sequence_analysis")) return null;
    if (!reader.object().hasAttribute("/", MARKER)) return null;
    try {
      if (!reader.bool().getAttr("/", MARKER)) return null;
      final var level = levelMetadata(reader, resolutions.isEmpty() ? 0 : resolutions.getFirst());
      return new Summary(1,
        text(reader, "method", level.getString("method", "unspecified")),
        text(reader, "resolution_policy", level.getString("resolution_policy", "unspecified")),
        text(reader, "description", "Estimated source values."),
        reader.object().isGroup(GROUP) && reader.object().hasAttribute(GROUP, "upstream_approximate")
          ? reader.bool().getAttr(GROUP, "upstream_approximate") : level.getBoolean("upstream_approximate", false));
    } catch (RuntimeException unreadable) {
      // An explicit but malformed marker must not make a source look unestimated.
      return new Summary(1, "unspecified", "unspecified", "This source declares approximation provenance, but its details are unreadable.", false);
    }
  }

  public static void mark(IHDF5Writer target, Summary summary) {
    target.bool().setAttr("/", MARKER, true);
    if (!target.object().isGroup(GROUP)) target.object().createGroup(GROUP);
    target.int32().setAttr(GROUP, "schema_version", summary.schemaVersion);
    target.string().setAttrVL(GROUP, "method", summary.method);
    target.string().setAttrVL(GROUP, "resolution_policy", summary.resolutionPolicy);
    target.string().setAttrVL(GROUP, "description", summary.description);
    target.bool().setAttr(GROUP, "upstream_approximate", summary.upstreamApproximate);
  }

  /** Preserve source provenance on import or exact bin remapping; this adds no estimation step. */
  public static void copy(IHDF5Reader source, IHDF5Writer target, List<Long> resolutions) {
    final var summary = read(source, resolutions);
    if (summary == null) return;
    mark(target, summary);
    if (source.object().isDataSet(GROUP + "/agp"))
      target.string().writeVL(GROUP + "/agp", source.string().read(GROUP + "/agp"));
    for (long resolution : resolutions) {
      final var metadata = levelMetadata(source, resolution);
      if (!metadata.isEmpty()) target.string().writeVL(GROUP + "/levels/" + resolution, metadata.encode());
      final String root = coolerRoot(target, resolution);
      if (root != null) {
        final var outputMetadata = jsonAttribute(target, root);
        outputMetadata.put("hict_approximate_export", true).put("source_approximation", JsonObject.mapFrom(summary));
        target.string().setAttrVL(root, "metadata", outputMetadata.encode());
      }
    }
  }

  /** Tool preparation may replace Cooler root attributes; retain provenance from the original input. */
  public static void copyOriginalInput(Path source, Path target, List<Long> resolutions) {
    try (var reader = HDF5Factory.openForReading(source.toFile())) {
      if (read(reader, resolutions) == null) return;
      try (var writer = HDF5Factory.open(target.toFile())) { copy(reader, writer, resolutions); }
    }
  }

  private static String text(IHDF5Reader reader, String name, String fallback) {
    if (reader.object().isGroup(GROUP) && reader.object().hasAttribute(GROUP, name)) {
      final String value = reader.string().getAttr(GROUP, name);
      if (!value.isBlank()) return value;
    }
    return fallback;
  }
  private static JsonObject levelMetadata(IHDF5Reader reader, long resolution) {
    final String stored = GROUP + "/levels/" + resolution;
    if (reader.object().isDataSet(stored)) return new JsonObject(reader.string().read(stored));
    final String root = coolerRoot(reader, resolution);
    return root == null ? new JsonObject() : jsonAttribute(reader, root);
  }
  private static JsonObject jsonAttribute(IHDF5Reader reader, String root) {
    if (!reader.object().hasAttribute(root, "metadata")) return new JsonObject();
    return new JsonObject(reader.string().getAttr(root, "metadata"));
  }
  private static @Nullable String coolerRoot(IHDF5Reader reader, long resolution) {
    final String level = "/resolutions/" + resolution;
    if (reader.object().isDataSet(level + "/pixels/count")) return level;
    return reader.object().isDataSet("/pixels/count") ? "/" : null;
  }
}
