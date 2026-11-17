package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSearchQueries;
import java.nio.file.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RemoteBlastRunnerTest {
  @TempDir Path dir;
  private final List<SequenceSearchQueries.Query> queries = SequenceSearchQueries.parse(">Private local description\nACGT");
  private JsonObject request() {
    return new JsonObject().put("mode", "blast").put("scope", "database").put("searchTarget", "NCBI_REMOTE")
      .put("bothStrands", true).put("remoteConsent", true).put("remoteDatabase", "nt");
  }
  private RemoteBlastOptions options() { return RemoteBlastOptions.fromJson(request(), queries); }
  private String row() { return "query_1\tref|NC_000001.1|\tNC_000001.1\t1\t4\t104\t101\t100\t4\t0\t0\t0\t20\t4\t1000\tExternal subject <literal>\n"; }

  @Test void requiresExplicitConsentDatabaseAndRemoteScope() {
    for (Object consent : new Object[]{false, "true", 1})
      assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request().put("remoteConsent", consent), queries));
    var absent = request(); absent.remove("remoteConsent");
    assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(absent, queries));
    for (Object database : new Object[]{"", "nt -subject local.fa", "../local", 1})
      assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request().put("remoteDatabase", database), queries));
    assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request().put("scope", "assembly"), queries));
    assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request().put("mode", "exact"), queries));
    assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request().put("outputFilename", "results.bed"), queries));
  }

  @Test void boundsRemoteRequestsAndRejectsCoercedSettings() {
    for (Object timeout : new Object[]{0, 31, 1.5, "10"})
      assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request().put("remoteTimeoutMinutes", timeout), queries));
    for (Object targets : new Object[]{0, 501, 1.5, "50"})
      assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request().put("maxTargets", targets), queries));
    assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request(), java.util.Collections.nCopies(11, queries.getFirst())));
    assertThrows(IllegalArgumentException.class, () -> RemoteBlastOptions.fromJson(request(), SequenceSearchQueries.parse("A".repeat(100001))));
    assertEquals(600, options().timeoutSeconds());
  }

  @Test void buildsDatabaseCommandWithoutLocalSubjectOrThreadFlags() {
    var command = RemoteBlastRunner.command(Path.of("blastn"), dir.resolve("q.fa"), dir.resolve("out.tsv"), queries, options());
    assertTrue(command.contains("-remote")); assertTrue(command.contains("-db"));
    assertEquals("nt", command.get(command.indexOf("-db") + 1));
    assertEquals("blastn-short", command.get(command.indexOf("-task") + 1));
    assertEquals("yes", command.get(command.indexOf("-dust") + 1));
    assertFalse(command.contains("-subject")); assertFalse(command.contains("-num_threads"));
    assertFalse(command.stream().anyMatch(arg -> arg.contains("Private local description")));
    var ribosomal = RemoteBlastOptions.fromJson(request().put("remoteDatabase", "16S_ribosomal_RNA"), queries);
    var ribosomalCommand = RemoteBlastRunner.command(Path.of("blastn"), dir.resolve("q.fa"), dir.resolve("out.tsv"), queries, ribosomal);
    assertEquals("rRNA_typestrains/16S_ribosomal_RNA", ribosomalCommand.get(ribosomalCommand.indexOf("-db") + 1));
  }

  @Test void preservesExternalAccessionsCoordinatesAndScoresWithoutAssemblyAnchoring() throws Exception {
    var raw = dir.resolve("raw.tsv"); Files.writeString(raw, row());
    var parsed = RemoteBlastRunner.parse(raw, queries);
    assertEquals(1, parsed.count());
    var hit = parsed.hits().getJsonObject(0);
    assertEquals("NC_000001.1", hit.getString("accession"));
    assertEquals(104L, hit.getLong("subjectStart")); assertEquals(101L, hit.getLong("subjectEnd"));
    assertEquals("-", hit.getString("strand")); assertEquals(0d, hit.getDouble("evalue"));
    assertEquals("External subject <literal>", hit.getString("title"));
    assertFalse(hit.containsKey("path")); assertFalse(hit.containsKey("sourceFragments"));
  }

  @Test void rejectsMalformedExternalHitsRatherThanInventingCoordinates() throws Exception {
    var raw = dir.resolve("raw.tsv");
    for (String bad : List.of(row().replace("query_1", "unknown"), row().replace("\t104\t101\t", "\t1001\t101\t"),
      row().replace("\t1000\t", "\t9223372036854775807\t"), row().replace("\t100\t4\t", "\tNaN\t4\t"),
      row().replace("\t4\t1000\t", "\t5\t1000\t"), row().replace("\t4\t0\t0\t", "\t4\t0\t1\t"), "not a BLAST row")) {
      Files.writeString(raw, bad);
      assertThrows(java.io.IOException.class, () -> RemoteBlastRunner.parse(raw, queries));
    }
  }

  @Test void gappedReverseAlignmentRetainsDistinctQueryAndSubjectSpans() throws Exception {
    var raw = dir.resolve("gapped.tsv");
    Files.writeString(raw, "query_1\texternal\tNC_000001.1\t1\t4\t105\t101\t80\t5\t0\t1\t1e-3\t20\t4\t1000\tGapped external subject\n");
    var hit = RemoteBlastRunner.parse(raw, queries).hits().getJsonObject(0);
    assertEquals(5, hit.getInteger("alignmentLength"));
    assertEquals(105L, hit.getLong("subjectStart"));
    assertEquals(101L, hit.getLong("subjectEnd"));
    assertEquals(4L, hit.getLong("queryEnd"));
    assertEquals(1, hit.getInteger("gapOpenings"));
  }

  private Path executable(String body) throws Exception {
    assumeTrue(Files.isExecutable(Path.of("/bin/sh")), "Controlled process test requires a POSIX shell");
    var file = dir.resolve("controlled-blastn");
    Files.writeString(file, "#!/bin/sh\n" + body);
    assertTrue(file.toFile().setExecutable(true));
    return file;
  }

  @Test void controlledProcessProducesAReportAndUploadsOnlyGeneratedQueryIds() throws Exception {
    var executable = executable("while [ \"$1\" != \"-out\" ]; do shift; done\nprintf '" + row().replace("\n", "\\n").replace("\t", "\\t") + "' > \"$2\"\n");
    var result = new RemoteBlastRunner().run(executable, dir, queries, options(), ignored -> {}, ignored -> {}, () -> false,
      Duration.ofSeconds(5), Duration.ZERO);
    var report = new JsonObject(Files.readString(result.report()));
    assertEquals("hict-remote-blast", report.getString("format"));
    assertTrue(report.getBoolean("externalSubjects")); assertEquals("ONE_BASED_INCLUSIVE", report.getString("coordinateSystem"));
    assertEquals(1, result.hits()); assertEquals(">query_1\nACGT\n", Files.readString(dir.resolve("queries.fasta")));
    assertEquals("Private local description", report.getJsonArray("queries").getJsonObject(0).getString("description"));
    assertFalse(Files.exists(dir.resolve("sequence-search.bed")));
  }

  @Test void timeoutStopsTheOwnedProcessAndDoesNotPublishAReport() throws Exception {
    var executable = executable("sleep 30\n");
    var process = new AtomicReference<Process>();
    assertThrows(TimeoutException.class, () -> new RemoteBlastRunner().run(executable, dir, queries, options(), ignored -> {},
      p -> { if (p != null) process.set(p); }, () -> false, Duration.ofMillis(50), Duration.ZERO));
    assertNotNull(process.get()); assertFalse(process.get().isAlive());
    assertFalse(Files.exists(dir.resolve("remote-blast.json"))); assertFalse(Files.exists(dir.resolve("remote-blast.tsv")));
  }

  @Test void cancellationStopsTheOwnedProcessAndDoesNotPublishAReport() throws Exception {
    var executable = executable("sleep 30\n");
    var cancel = new AtomicBoolean(); var process = new AtomicReference<Process>();
    assertThrows(CancellationException.class, () -> new RemoteBlastRunner().run(executable, dir, queries, options(), ignored -> {},
      p -> { if (p != null) { process.set(p); cancel.set(true); } }, cancel::get, Duration.ofSeconds(5), Duration.ZERO));
    assertNotNull(process.get()); assertFalse(process.get().isAlive());
    assertFalse(Files.exists(dir.resolve("remote-blast.json")));
  }
}
