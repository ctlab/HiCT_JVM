package ru.itmo.ctlab.hict.hict_server;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;

import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class ApiRobustnessTest {
  private static final Pattern JAVA_TEXT = Pattern.compile(
    "java\\.|javax\\.|Exception|Cannot invoke|Cannot read field|ClassCast|IndexOutOfBounds|For input string|ru\\.itmo|io\\.vertx");

  @TempDir Path data;

  @Test
  void invalidRequestsPreserveAssemblyAndHistory() throws Exception {
    ReinforcementHdf5Fixture.create(data, "map", List.of(4L, 10L));
    Files.writeString(data.resolve("bad-number.agp"), "# header\nscaffold1\t1\tbad\t1\tW\tctg\t1\t5\t+\n");
    Files.writeString(data.resolve("bad-columns.agp"), "# header\nshort\tline\n");
    Files.writeString(data.resolve("empty.agp"), "# header\n");
    final var api = new ApiHttpIntegrationTest();
    api.tempDataDir = data;
    try {
      api.startValidationServer();
      assertEquals(false, new JsonObject(api.get("/session/availability").body()).getBoolean("available"));
      rejected(409, api.post("/reverse_selection_range", "{\"startBP\":0,\"endBP\":1}"));
      rejected(409, api.get("/get_tile?level=1"));
      rejected(409, api.post("/assembly/history/restore", "{\"action\":\"undo\"}"));
      for (var body : List.of("", "{", "[]", "{}", "{\"filename\":2}", "{\"filename\":\"bad-number.agp\"}")) {
        rejected(400, api.post("/open", body));
      }
      assertEquals(200, api.post("/open", "{\"filename\":\"map.hict.hdf5\"}").statusCode());
      assertEquals(true, new JsonObject(api.get("/session/availability").body()).getBoolean("available"));
      final String agp = api.post("/get_agp_for_assembly", "{}").body();
      final String history = api.post("/assembly/history", "{}").body();
      assertFalse(agp.isEmpty());
      final var invalidRanges = List.of(
        "", "{}", "{\"startBP\":\"5\",\"endBP\":10}", "{\"startBP\":1.25,\"endBP\":2}",
        "{\"startBP\":-1,\"endBP\":10}", "{\"startBP\":2,\"endBP\":2}",
        "{\"startBP\":2,\"endBP\":1}", "{\"startBP\":0,\"endBP\":9223372036854775807}");
      for (var endpoint : List.of("/reverse_selection_range", "/move_selection_range",
        "/group_contigs_into_scaffold", "/ungroup_contigs_from_scaffold", "/move_selection_to_debris")) {
        for (var body : invalidRanges) {
          final var request = endpoint.equals("/move_selection_range") && body.startsWith("{\"startBP")
            ? new JsonObject(body).put("targetStartBP", 0).encode() : body;
          rejected(400, api.post(endpoint, request));
          unchanged(api, agp, history);
        }
      }
      rejected(400, api.post("/move_selection_range", "{\"startBP\":0,\"endBP\":1,\"targetStartBP\":9223372036854775807}"));
      for (var body : List.of("", "{}", "{\"splitPx\":\"10\",\"bpResolution\":4}",
        "{\"splitPx\":0,\"bpResolution\":4}", "{\"splitPx\":-1,\"bpResolution\":4}",
        "{\"splitPx\":10000,\"bpResolution\":4}", "{\"splitPx\":1,\"bpResolution\":7}")) {
        rejected(400, api.post("/split_contig_at_bin", body));
        unchanged(api, agp, history);
      }
      for (var body : List.of("", "{}", "{\"agpFilename\":\"missing.agp\"}",
        "{\"agpFilename\":\"empty.agp\"}", "{\"agpFilename\":\"bad-columns.agp\"}",
        "{\"agpFilename\":\"bad-number.agp\"}")) {
        final var result = api.post("/load_agp", body);
        rejected(400, result);
        if (body.contains("bad-")) assertTrue(result.body().contains("line 2"), result.body());
        unchanged(api, agp, history);
      }
      rejected(400, api.post("/assembly/history", ""));
      rejected(400, api.post("/assembly/history", "{\"offset\":\"bad\"}"));
      rejected(400, api.post("/assembly/history/restore", "{\"action\":\"undo\"}"));
      rejected(400, api.post("/set_visualization_options", "{\"colormap\":42}"));
      rejected(400, api.post("/tracks/query_1d", "{\"widthPx\":16385}"));
      rejected(404, api.post("/names/contig", "{\"contigId\":99999,\"newName\":\"test\"}"));
      rejected(404, api.post("/names/scaffold", "{\"scaffoldId\":99999,\"newName\":\"test\"}"));
      rejected(409, api.post("/get_fasta_for_assembly", "{}"));
      for (var query : List.of("level=0", "level=3", "level=999", "level=no", "level=1&row=no", "bpResolution=7")) {
        rejected(400, api.get("/get_tile?" + query));
      }
      for (int level = 1; level <= 2; level++) {
        assertEquals(200, api.get("/get_tile?level=" + level + "&tile_size=8").statusCode());
      }
      final var noSecondary = api.get("/get_tile?level=1&tile_size=8&source=SECONDARY");
      assertEquals(200, noSecondary.statusCode());
      assertEquals("true", noSecondary.headers().firstValue("x-hict-cancelled").orElseThrow());
      assertTrue(new JsonObject(noSecondary.body()).getBoolean("cancelled"));
      unchanged(api, agp, history);
    } finally {
      api.tearDown();
    }
  }

  private static void rejected(int status, HttpResponse<String> response) {
    assertEquals(status, response.statusCode(), () -> response.uri() + " " + response.body());
    final var error = new JsonObject(response.body()).getString("error");
    assertNotNull(error);
    assertFalse(JAVA_TEXT.matcher(error).find(), error);
  }

  private static void unchanged(ApiHttpIntegrationTest api, String agp, String history) throws Exception {
    assertEquals(agp, api.post("/get_agp_for_assembly", "{}").body());
    assertEquals(new JsonObject(history), new JsonObject(api.post("/assembly/history", "{}").body()));
  }
}
