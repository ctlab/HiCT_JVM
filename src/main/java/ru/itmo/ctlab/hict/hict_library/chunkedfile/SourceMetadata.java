package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Declared source semantics. Missing metadata is unknown, never inferred from filenames or values. */
@Slf4j
public record SourceMetadata(Units units, double scoreScale, ContactDataState contactDataState,
                             List<Long> importedWeightResolutions, boolean weightMetadataComplete) {
  public enum Units { CONTACT_COUNT, ALIGNMENT_SAMPLE_COUNT, ALIGNMENT_PRESENCE, IDENTITY_PERCENT, SEQUENCE_ONLY, UNKNOWN }
  public enum ContactDataState { PRESENT, ABSENT, UNKNOWN }
  public static final String VERSION = "hict_source_metadata_version";
  public static final String UNITS = "hict_source_units";
  public static final String CONTACT_STATE = "contact_data_state";
  public static final String SCALE = "hict_score_scale";
  public static final String WEIGHTS = "hict_imported_weights";

  public SourceMetadata {
    importedWeightResolutions = List.copyOf(importedWeightResolutions);
  }

  public static SourceMetadata read(IHDF5Reader reader, List<Long> resolutions) {
    try {
      return readDeclared(reader, resolutions);
    } catch (RuntimeException malformedMetadata) {
      log.warn("Ignoring unreadable optional source capability metadata: {}", malformedMetadata.getMessage());
      boolean overlay = reader.object().isGroup("/sequence_analysis");
      return new SourceMetadata(Units.UNKNOWN, 1, overlay ? ContactDataState.ABSENT : ContactDataState.UNKNOWN,
        List.of(), overlay);
    }
  }

  private static SourceMetadata readDeclared(IHDF5Reader reader, List<Long> resolutions) {
    Units units = Units.UNKNOWN;
    ContactDataState contacts = ContactDataState.UNKNOWN;
    double scale = 1;
    var weighted = new ArrayList<Long>();
    boolean weightsKnown = false;
    if (reader.object().hasAttribute("/", VERSION) && reader.int32().getAttr("/", VERSION) == 1) {
      units = parse(Units.class, attribute(reader, "/", UNITS), Units.UNKNOWN);
      String contactState = attribute(reader, "/", CONTACT_STATE);
      if (contactState.isEmpty()) contactState = attribute(reader, "/", "hict_contact_data_state");
      contacts = parse(ContactDataState.class, contactState, ContactDataState.UNKNOWN);
      if (reader.object().hasAttribute("/", SCALE)) scale = reader.float64().getAttr("/", SCALE);
      weightsKnown = true;
      for (long resolution : resolutions) {
        String state = attribute(reader, "/resolutions/" + resolution, WEIGHTS);
        if ("PRESENT".equals(state)) weighted.add(resolution);
        else if (!"ABSENT".equals(state)) weightsKnown = false;
      }
    }
    // Existing sequence overlays have authoritative units even before the new
    // root metadata was introduced. Never inherit contact semantics from their template.
    if (reader.object().isGroup("/sequence_analysis")) {
      units = switch (attribute(reader, "/sequence_analysis", "score_unit")) {
        case "identity_percent" -> Units.IDENTITY_PERCENT;
        case "alignment_sample_count" -> Units.ALIGNMENT_SAMPLE_COUNT;
        default -> Units.UNKNOWN;
      };
      contacts = ContactDataState.ABSENT;
      weighted.clear();
      weightsKnown = true;
      if (reader.object().hasAttribute("/sequence_analysis", "score_scale")) {
        scale = reader.float64().getAttr("/sequence_analysis", "score_scale");
      } else {
        // Encoded scores cannot be interpreted without their declared scale.
        units = Units.UNKNOWN;
        scale = 1;
      }
    }
    if (!Double.isFinite(scale) || scale <= 0) {
      // Invalid scale cannot authorize numeric interpretation of a source.
      units = Units.UNKNOWN;
      scale = 1;
    }
    if (units == Units.SEQUENCE_ONLY) contacts = ContactDataState.ABSENT;
    return new SourceMetadata(units, scale, contacts, weighted, weightsKnown);
  }

  /** Conversion preserves explicit semantics and records whether weights really came from Cooler. */
  public static void writeConverted(IHDF5Reader source, IHDF5Writer target, List<Long> resolutions) {
    var metadata = read(source, resolutions);
    target.int32().setAttr("/", VERSION, 1);
    target.string().setAttr("/", UNITS, metadata.units().name());
    target.string().setAttr("/", CONTACT_STATE, metadata.contactDataState().name());
    target.float64().setAttr("/", SCALE, metadata.scoreScale());
    for (long resolution : resolutions) {
      String root = "/resolutions/" + resolution;
      target.string().setAttr(root, WEIGHTS, source.object().isDataSet(root + "/bins/weight") ? "PRESENT" : "ABSENT");
    }
  }

  private static String attribute(IHDF5Reader reader, String object, String name) {
    return reader.object().hasAttribute(object, name) ? reader.string().getAttr(object, name) : "";
  }

  private static <E extends Enum<E>> E parse(Class<E> type, String text, E fallback) {
    try { return Enum.valueOf(type, text); }
    catch (IllegalArgumentException ignored) { return fallback; }
  }
}
