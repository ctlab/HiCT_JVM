package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SourceCapabilitiesDTO;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SourceMetadataTest {
  @TempDir Path directory;

  private Path fixture(String name) throws Exception {
    return ReinforcementHdf5Fixture.create(directory, name, List.of(4L, 10L));
  }

  private SourceCapabilitiesDTO capabilities(Path path) {
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      return SourceCapabilitiesDTO.fromFile(file);
    }
  }

  @Test void oldFilesStayUnknownAndConvertedUnweightedFilesDeclareNoImportedWeights() throws Exception {
    var path = fixture("legacy");
    var converted = capabilities(path);
    assertEquals(SourceMetadata.Units.UNKNOWN, converted.units());
    assertEquals(SourceCapabilitiesDTO.State.UNKNOWN, converted.contactNormalization().state());
    assertEquals("NO_IMPORTED_WEIGHTS", converted.importedWeights().reasonCode());
    try (var writer = HDF5Factory.open(path.toFile())) { writer.object().deleteAttribute("/", SourceMetadata.VERSION); }
    var legacy = capabilities(path);
    assertEquals(SourceMetadata.Units.UNKNOWN, legacy.units());
    assertEquals(SourceCapabilitiesDTO.State.UNKNOWN, legacy.importedWeights().state());
    assertEquals(List.of(4L, 10L), legacy.resolutions());
  }

  @Test void explicitContactsAndPerResolutionWeightsRemainDistinctFromUnitVectors() throws Exception {
    var path = fixture("weighted");
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.string().setAttr("/", SourceMetadata.UNITS, "CONTACT_COUNT");
      writer.string().setAttr("/", SourceMetadata.CONTACT_STATE, "PRESENT");
      writer.string().setAttr("/resolutions/4", SourceMetadata.WEIGHTS, "PRESENT");
    }
    var result = capabilities(path);
    assertEquals(SourceCapabilitiesDTO.State.AVAILABLE, result.contactNormalization().state());
    assertEquals(SourceCapabilitiesDTO.State.AVAILABLE, result.importedWeights().state());
    assertEquals(List.of(4L), result.importedWeightResolutions());
    assertTrue(result.weightMetadataComplete());
  }

  @Test void sequenceOverlayMetadataOverridesContactTemplateAndRejectsWeightApplicability() throws Exception {
    var path = fixture("identity");
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.string().setAttr("/", SourceMetadata.UNITS, "CONTACT_COUNT");
      writer.string().setAttr("/", SourceMetadata.CONTACT_STATE, "PRESENT");
      writer.object().createGroup("/sequence_analysis");
      writer.string().setAttr("/sequence_analysis", "score_unit", "identity_percent");
      writer.int64().setAttr("/sequence_analysis", "score_scale", 10000);
    }
    var identity = capabilities(path);
    assertEquals(SourceMetadata.Units.IDENTITY_PERCENT, identity.units());
    assertEquals(10000, identity.scoreScale());
    assertEquals(SourceMetadata.ContactDataState.ABSENT, identity.contactDataState());
    assertEquals("NOT_CONTACT_DATA", identity.importedWeights().reasonCode());
    assertEquals(SourceCapabilitiesDTO.State.UNAVAILABLE, identity.contactNormalization().state());
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.string().setAttr("/sequence_analysis", "score_unit", "alignment_sample_count");
      writer.int64().setAttr("/sequence_analysis", "score_scale", 1);
    }
    assertEquals(SourceMetadata.Units.ALIGNMENT_SAMPLE_COUNT, capabilities(path).units());
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.object().deleteAttribute("/sequence_analysis", "score_scale");
    }
    assertEquals(SourceMetadata.Units.UNKNOWN, capabilities(path).units());
    assertEquals(SourceCapabilitiesDTO.State.UNAVAILABLE, capabilities(path).contactNormalization().state());
  }

  @Test void unsupportedAndMalformedMetadataCannotAuthorizeSemantics() throws Exception {
    var path = fixture("unknown");
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.int32().setAttr("/", SourceMetadata.VERSION, 999);
      writer.string().setAttr("/", SourceMetadata.UNITS, "CONTACT_COUNT");
    }
    assertEquals(SourceMetadata.Units.UNKNOWN, capabilities(path).units());
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.object().deleteAttribute("/", SourceMetadata.VERSION);
      writer.string().setAttr("/", SourceMetadata.VERSION, "invalid");
    }
    assertEquals(SourceMetadata.Units.UNKNOWN, capabilities(path).units());
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.object().deleteAttribute("/", SourceMetadata.VERSION);
      writer.int32().setAttr("/", SourceMetadata.VERSION, 1);
      writer.float64().setAttr("/", SourceMetadata.SCALE, Double.NaN);
    }
    assertEquals(SourceMetadata.Units.UNKNOWN, capabilities(path).units());
    assertEquals(1, capabilities(path).scoreScale());
  }

  @Test void conversionPreservesDeclaredUnitsAndOnlyMarksActuallyImportedWeights() throws Exception {
    fixture("convert");
    var input = directory.resolve("convert.mcool");
    try (var writer = HDF5Factory.open(input.toFile())) {
      writer.int32().setAttr("/", SourceMetadata.VERSION, 1);
      writer.string().setAttr("/", SourceMetadata.UNITS, "CONTACT_COUNT");
      writer.string().setAttr("/", SourceMetadata.CONTACT_STATE, "PRESENT");
      writer.float64().setAttr("/", SourceMetadata.SCALE, 1);
      writer.float64().writeArray("/resolutions/4/bins/weight", new double[]{2,3,4,5,6});
    }
    var output = directory.resolve("converted.hict.hdf5");
    new ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter().convert(
      new ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions(input, output, List.of(4L,10L),64,1,
        ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions.CompressionAlgorithm.DEFLATE,"",false,1,false,
        ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions.ExportMode.INTERNAL), ignored -> { });
    var result = capabilities(output);
    assertEquals(SourceMetadata.Units.CONTACT_COUNT, result.units());
    assertEquals(List.of(4L), result.importedWeightResolutions());
    assertTrue(result.weightMetadataComplete());
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(output, 1, 2))) {
      var matrix = file.getMatrixQueries().getSubmatrix(
        ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor.fromBpResolution(4, file), 0,0,5,5,false);
      assertArrayEquals(new double[]{2,3,4,5,6}, matrix.rowWeights());
    }
  }
}
