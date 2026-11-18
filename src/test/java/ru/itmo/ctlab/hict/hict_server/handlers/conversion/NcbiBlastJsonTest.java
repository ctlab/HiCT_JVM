package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSearchQueries;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NcbiBlastJsonTest {
  @TempDir Path directory;
  private byte[] resource(String name) throws Exception {
    try (var stream = getClass().getResourceAsStream("/remote-blast/" + name)) { return stream.readAllBytes(); }
  }
  private RemoteBlastOptions options(List<SequenceSearchQueries.Query> queries, String database) {
    return RemoteBlastOptions.fromJson(new JsonObject().put("mode", "blast").put("scope", "database")
      .put("remoteConsent", true).put("remoteDatabase", database).put("bothStrands", true), queries);
  }
  private JsonObject synthetic() {
    var hsp = new JsonObject().put("query_from", 1).put("query_to", 4).put("query_strand", "Plus")
      .put("hit_from", 105).put("hit_to", 101).put("hit_strand", "Minus")
      .put("align_len", 5).put("identity", 4).put("gaps", 1).put("qseq", "ACG-T").put("hseq", "ACGTT")
      .put("evalue", 1e-8).put("bit_score", 20d);
    var hit = new JsonObject().put("len", 1000).put("description", new JsonArray().add(new JsonObject()
      .put("id", "ref|NC_000001.2|").put("accession", "NC_000001").put("title", "External <literal>\twith\nwhitespace")))
      .put("hsps", new JsonArray().add(hsp));
    var search = new JsonObject().put("query_title", "query_1").put("query_len", 4).put("hits", new JsonArray().add(hit));
    var report = new JsonObject().put("program", "blastn").put("version", "BLASTN fixture")
      .put("params", new JsonObject().put("expect", 10)).put("search_target", new JsonObject().put("db", "nt"))
      .put("results", new JsonObject().put("search", search));
    return new JsonObject().put("BlastOutput2", new JsonArray().add(new JsonObject().put("report", report)));
  }
  private JsonObject report(JsonObject document) { return document.getJsonArray("BlastOutput2").getJsonObject(0).getJsonObject("report"); }
  private JsonObject search(JsonObject document) { return report(document).getJsonObject("results").getJsonObject("search"); }
  private JsonObject hsp(JsonObject document) { return search(document).getJsonArray("hits").getJsonObject(0).getJsonArray("hsps").getJsonObject(0); }
  private NcbiBlastJson.Decoded decode(JsonObject document, List<SequenceSearchQueries.Query> queries, String name) throws Exception {
    return NcbiBlastJson.decode(document.toBuffer().getBytes(), queries, options(queries, "nt"), directory.resolve(name));
  }

  @Test void decodesTheCompletedPublicReferenceWithExactScoresAndVersionedAccessions() throws Exception {
    var queries = SequenceSearchQueries.parse(new String(resource("ncbi-public-160.fasta"), StandardCharsets.UTF_8));
    var decoded = NcbiBlastJson.decode(resource("ncbi-public-160-json2.json"), queries,
      options(queries, "16S_ribosomal_RNA"), directory.resolve("public.tsv"));
    assertEquals(10, decoded.parsed().count());
    var hit = decoded.parsed().hits().getJsonObject(0);
    assertEquals("NR_180316.1", hit.getString("accession"));
    assertEquals(1L, hit.getLong("queryStart")); assertEquals(160L, hit.getLong("queryEnd"));
    assertEquals(10L, hit.getLong("subjectStart")); assertEquals(169L, hit.getLong("subjectEnd"));
    assertEquals(100d, hit.getDouble("identity")); assertEquals(289.825d, hit.getDouble("bitscore"));
    assertEquals(3.01593e-78, hit.getDouble("evalue")); assertEquals(0L, hit.getLong("mismatches"));
    assertEquals("+", hit.getString("strand"));
    assertEquals("FULL_QUERY", decoded.provenance().getJsonArray("queryVerification").getJsonObject(0).getString("scope"));
    assertEquals(160, decoded.provenance().getJsonArray("queryVerification").getJsonObject(0).getInteger("alignedBasesVerified"));
    assertFalse(hit.containsKey("sourceFragments"));
  }

  @Test void checksGappedExternalCoordinatesAndPreservesLiteralTitles() throws Exception {
    var result = decode(synthetic(), SequenceSearchQueries.parse("ACGT"), "gapped.tsv");
    var hit = result.parsed().hits().getJsonObject(0);
    assertEquals("NC_000001.2", hit.getString("accession")); assertEquals("-", hit.getString("strand"));
    assertEquals(80d, hit.getDouble("identity")); assertEquals(1L, hit.getLong("gapOpenings"));
    assertEquals("External <literal> with whitespace", hit.getString("title"));
  }

  @Test void rejectsWrongQueriesAndInconsistentAlignmentsInsteadOfReturningPartialScientificResults() throws Exception {
    var queries = SequenceSearchQueries.parse("ACGT");
    int index = 0;
    for (String key : List.of("query_title", "query_len", "qseq", "hseq", "identity", "gaps", "query_strand", "hit_strand", "query_to", "hit_to", "evalue")) {
      var document = synthetic();
      switch (key) {
        case "query_title" -> search(document).put(key, "unrelated");
        case "query_len" -> search(document).put(key, 5);
        case "qseq" -> hsp(document).put(key, "ACG-A");
        case "hseq" -> hsp(document).put(key, "ACGT");
        case "identity" -> hsp(document).put(key, 3);
        case "gaps" -> hsp(document).put(key, 0);
        case "query_strand" -> hsp(document).put(key, "Minus");
        case "hit_strand" -> hsp(document).put(key, "Plus");
        case "query_to" -> hsp(document).put(key, 5);
        case "hit_to" -> hsp(document).put(key, 0);
        case "evalue" -> hsp(document).put(key, -1);
      }
      var path = directory.resolve("bad-" + index++ + ".tsv");
      assertThrows(IOException.class, () -> NcbiBlastJson.decode(document.toBuffer().getBytes(), queries, options(queries, "nt"), path), key);
    }
  }

  @Test void distinguishesPartialAndNoAlignmentVerificationRatherThanClaimingFullSequenceValidation() throws Exception {
    var queries = SequenceSearchQueries.parse("ACGTA");
    var document = synthetic(); search(document).put("query_len", 5);
    var partial = decode(document, queries, "partial.tsv").provenance().getJsonArray("queryVerification").getJsonObject(0);
    assertEquals("ALIGNED_BASES_ONLY", partial.getString("scope")); assertEquals(4, partial.getInteger("alignedBasesVerified"));
    search(document).put("hits", new JsonArray());
    var empty = decode(document, queries, "empty.tsv");
    assertEquals(0, empty.parsed().count());
    assertEquals("LABEL_AND_LENGTH_ONLY", empty.provenance().getJsonArray("queryVerification").getJsonObject(0).getString("scope"));
  }

  @Test void filtersRetrievedAlignmentsWithExplicitLocalThresholds() throws Exception {
    var queries = SequenceSearchQueries.parse("ACGT");
    var options = new RemoteBlastOptions("nt", 60, new BlastSearchRunner.Options(true, 1e-5, 90, 50, 10));
    var result = NcbiBlastJson.decode(synthetic().toBuffer().getBytes(), queries, options, directory.resolve("filtered.tsv"));
    assertEquals(0, result.parsed().count());
    assertEquals("FULL_QUERY", result.provenance().getJsonArray("queryVerification").getJsonObject(0).getString("scope"));
  }

  @Test void acceptsAnOriginalUniqueFastaHeaderAndRejectsAnotherDatabaseOrProgram() throws Exception {
    var queries = SequenceSearchQueries.parse(">Original header\nACGT");
    var document = synthetic(); search(document).put("query_title", "Original header");
    assertEquals(1, decode(document, queries, "named.tsv").parsed().count());
    report(document).put("program", "blastp"); assertThrows(IOException.class, () -> decode(document, queries, "protein.tsv"));
    report(document).put("program", "blastn").getJsonObject("search_target").put("db", "refseq_rna");
    assertThrows(IOException.class, () -> decode(document, queries, "wrong-db.tsv"));
  }

  @Test void rejectsDeepAndMalformedProviderDocumentsBeforeObjectAllocation() {
    var queries = SequenceSearchQueries.parse("ACGT");
    for (String body : List.of("[".repeat(33) + "]".repeat(33), "<html>not ready</html>", "{\"BlastOutput2\":[]}"))
      assertThrows(IOException.class, () -> NcbiBlastJson.decode(body.getBytes(StandardCharsets.UTF_8), queries, options(queries, "nt"), directory.resolve("invalid.tsv")));
  }

  @Test void verifiesNonPalindromicReverseQueryBasesAndAppliesTheRequestedStrands() throws Exception {
    var queries = SequenceSearchQueries.parse("AACG");
    var document = synthetic();
    hsp(document).put("query_from", 4).put("query_to", 1).put("query_strand", "Minus")
      .put("qseq", "CGT-T").put("hseq", "CGTTT");
    var decoded = decode(document, queries, "reverse.tsv");
    var hit = decoded.parsed().hits().getJsonObject(0);
    assertEquals(4L, hit.getLong("queryStart")); assertEquals(1L, hit.getLong("queryEnd"));
    assertEquals("+", hit.getString("strand")); // Both query and subject run in reverse.
    assertEquals("FULL_QUERY", decoded.provenance().getJsonArray("queryVerification").getJsonObject(0).getString("scope"));
    var forwardOnly = new RemoteBlastOptions("nt", 60, new BlastSearchRunner.Options(false, 1e-5, 70, 50, 10));
    assertEquals(0, NcbiBlastJson.decode(document.toBuffer().getBytes(), queries, forwardOnly, directory.resolve("forward-only.tsv")).parsed().count());
    assertThrows(IOException.class, () -> decode(document, SequenceSearchQueries.parse("ACGT"), "wrong-reverse.tsv"));
  }
}
