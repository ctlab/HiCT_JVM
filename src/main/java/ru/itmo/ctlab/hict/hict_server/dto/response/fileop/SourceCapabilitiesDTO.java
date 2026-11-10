package ru.itmo.ctlab.hict.hict_server.dto.response.fileop;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.SourceMetadata;

import java.util.Arrays;
import java.util.List;

/** Additive API contract: unknown metadata is distinct from an unavailable operation. */
public record SourceCapabilitiesDTO(int schemaVersion, SourceMetadata.Units units, double scoreScale,
                                    SourceMetadata.ContactDataState contactDataState, List<Long> resolutions,
                                    List<Long> importedWeightResolutions, boolean weightMetadataComplete, Availability contactNormalization,
                                    Availability importedWeights) {
  public enum State { AVAILABLE, UNAVAILABLE, UNKNOWN }
  public record Availability(State state, String reasonCode, String reason) { }

  public static SourceCapabilitiesDTO fromFile(ChunkedFile file) {
    var metadata = file.getSourceMetadata();
    var units = metadata.units();
    boolean notContacts = metadata.contactDataState() == SourceMetadata.ContactDataState.ABSENT
      || (units != SourceMetadata.Units.UNKNOWN && units != SourceMetadata.Units.CONTACT_COUNT);
    var contact = notContacts
      ? new Availability(State.UNAVAILABLE, "NOT_CONTACT_DATA", "Contact normalization and resolution scaling do not apply to this source.")
      : units == SourceMetadata.Units.CONTACT_COUNT || metadata.contactDataState() == SourceMetadata.ContactDataState.PRESENT
        ? new Availability(State.AVAILABLE, "", "")
        : new Availability(State.UNKNOWN, "LEGACY_SOURCE_UNITS", "This file does not declare its signal units. Check the source before applying contact normalization.");
    var weights = notContacts
      ? new Availability(State.UNAVAILABLE, "NOT_CONTACT_DATA", "Imported contact balancing weights do not apply to this source.")
      : !metadata.importedWeightResolutions().isEmpty()
        ? new Availability(State.AVAILABLE, "", "")
        : metadata.weightMetadataComplete()
          ? new Availability(State.UNAVAILABLE, "NO_IMPORTED_WEIGHTS", "This source has no imported balancing weights.")
          : new Availability(State.UNKNOWN, "LEGACY_WEIGHT_METADATA", "This file does not record whether its weight vectors were imported or filled with ones.");
    return new SourceCapabilitiesDTO(1, units, metadata.scoreScale(), metadata.contactDataState(),
      Arrays.stream(file.getResolutions()).filter(r -> r > 0).boxed().toList(),
      metadata.importedWeightResolutions(), metadata.weightMetadataComplete(), contact, weights);
  }
}
