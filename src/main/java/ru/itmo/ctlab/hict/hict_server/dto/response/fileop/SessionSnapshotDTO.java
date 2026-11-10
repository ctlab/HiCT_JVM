package ru.itmo.ctlab.hict.hict_server.dto.response.fileop;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.stream.Collectors;

/** Server capture used to construct a portable session, not itself a saved session document. */
public record SessionSnapshotDTO(int snapshotVersion, boolean assemblyVerified, boolean filesFingerprinted, boolean tracksVerified, JsonObject primary, JsonObject secondary,
                                 String assemblySource, JsonObject renderPipeline, JsonArray tracks,
                                 JsonObject workspace) {
  public static JsonObject source(ChunkedFile file, String filename, String fastaFilename) {
    var agp = file.getAgpProcessor().getAssemblyAGPRecords().stream()
      .map(Object::toString).collect(Collectors.joining("\n", "", "\n"));
    final String hash;
    try {
      hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
        .digest(agp.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
    return new JsonObject().put("filename", filename).put("fastaFilename", fastaFilename)
      .put("agp", agp).put("agpSha256", hash)
      .put("assemblyInfo", JsonObject.mapFrom(AssemblyInfoDTO.generateFromChunkedFile(file)))
      .put("contigNameOverrides", JsonObject.mapFrom(file.getContigNameOverrides()))
      .put("scaffoldNameOverrides", JsonObject.mapFrom(file.getScaffoldNameOverrides()))
      .put("capabilities", JsonObject.mapFrom(SourceCapabilitiesDTO.fromFile(file)));
  }
}
