package ru.itmo.ctlab.hict.hict_library.assembly;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/** Shared hand-enumerated oracles, exercised through production projection and rendering code. */
class ReinforcementFixtureTest {
  @TempDir Path directory;

  private byte[] resource(String name) throws Exception {
    try (var stream = getClass().getResourceAsStream("/reinforcement/" + name)) {
      assertNotNull(stream, name);
      return stream.readAllBytes();
    }
  }

  private JsonObject fixture() throws Exception {
    return new JsonObject(new String(resource("contracts.json"), StandardCharsets.UTF_8));
  }

  @Test void zeroRowLog2AndCustomMultiplierKeepTheirNumericMeaning() throws Exception {
    var fixture = fixture();
    var matrix = fixture.getJsonObject("matrix");
    var pipelineJson = fixture.getJsonObject("pipeline");
    var pipeline = RenderPipelineConfig.fromJson(pipelineJson);
    // A custom graph must survive serialization without becoming a plain source.
    assertEquals(pipelineJson, pipeline.toJson());
    var context = new RenderPipelineConfig.MutablePixelContext();
    long total = 0;
    for (int row = 0; row < 4; row++) {
      long rowSum = 0;
      for (int col = 0; col < 4; col++) {
        int value = matrix.getJsonArray("values").getJsonArray(row).getInteger(col);
        context.primaryValue = value;
        assertEquals(matrix.getJsonArray("log2OnePlus").getJsonArray(row).getDouble(col),
          pipeline.evaluate(true, context), 1e-12, "log2 at " + row + "," + col);
        assertEquals(value * 7.0, pipeline.evaluate(false, context), 1e-12);
        rowSum += value;
      }
      assertEquals(matrix.getJsonArray("rowSums").getLong(row), rowSum);
      total += rowSum;
    }
    assertEquals(matrix.getLong("total"), total);
  }

  @Test void forwardReverseGapsAndEverySubwindowMatchTheLiteralSequence() throws Exception {
    var sequence = fixture().getJsonObject("sequence");
    var components = new ArrayList<SequenceTrackProjection.Component>();
    for (var item : sequence.getJsonArray("components")) {
      var c = (JsonObject) item;
      components.add(new SequenceTrackProjection.Component(c.getLong("start"), c.getLong("end"),
        c.getString("contig"), "synthetic", c.getString("source"), c.getLong("sourceStart"), c.getBoolean("reversed")));
    }
    var gaps = List.of(new AssemblySequenceLayout.Gap(8, 3));
    for (boolean gzip : List.of(false, true)) {
      var path = directory.resolve(gzip ? "assembly.fa.gz" : "assembly.fa");
      if (gzip) {
        try (var out = new GZIPOutputStream(Files.newOutputStream(path))) { out.write(resource("assembly.fa")); }
      } else Files.write(path, resource("assembly.fa"));
      try (var fasta = new IndexedFastaSource(path); var reader = fasta.open()) {
        for (var entry : sequence.getJsonObject("records")) {
          var bytes = new ByteArrayOutputStream();
          reader.copy(entry.getKey(), 0, ((String) entry.getValue()).length(), false, bytes);
          assertEquals(entry.getValue(), bytes.toString(StandardCharsets.US_ASCII));
        }
        for (boolean padded : List.of(false, true)) {
          var projection = new SequenceTrackProjection(components, padded ? gaps : List.of());
          var expected = sequence.getString(padded ? "padded" : "unpadded");
          assertEquals(expected.length(), projection.length());
          for (int start = 0; start < expected.length(); start++) {
            for (int end = start + 1; end <= expected.length(); end++) {
              var bytes = new ByteArrayOutputStream();
              for (var piece : projection.window(start, end)) {
                if (piece.gap()) bytes.write("N".repeat((int) (piece.end() - piece.start())).getBytes(StandardCharsets.US_ASCII));
                else reader.copy(piece.source(), piece.sourceStart(), piece.sourceEnd(), piece.reversed(), bytes);
              }
              assertEquals(expected.substring(start, end), bytes.toString(StandardCharsets.US_ASCII),
                "gzip=" + gzip + " padded=" + padded + " interval=" + start + ":" + end);
            }
          }
          if (padded) {
            assertEquals(11, projection.sequencePosition(8), "Boundary selects the real base after the gap");
            var positions = sequence.getJsonArray("sourcePositions");
            for (int i = 0; i < positions.size(); i++)
              assertEquals(positions.getLong(i), projection.window(i, i + 1).getFirst().sourceStart(), "base " + i);
          }
        }
      }
    }
  }
}
