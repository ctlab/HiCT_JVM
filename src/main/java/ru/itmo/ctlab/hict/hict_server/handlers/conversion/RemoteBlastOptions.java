package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSearchQueries;
import java.util.List;
import java.util.Set;

/** An explicit upload decision, independent of the linked FASTA and assembly scope. */
public record RemoteBlastOptions(String database, int timeoutSeconds, BlastSearchRunner.Options alignment) {
  public static final Set<String> DATABASES = Set.of("nt", "refseq_rna", "16S_ribosomal_RNA");
  public String remoteDatabaseName() {
    return database.equals("16S_ribosomal_RNA") ? "rRNA_typestrains/16S_ribosomal_RNA" : database;
  }
  public static RemoteBlastOptions fromJson(JsonObject request, List<SequenceSearchQueries.Query> queries) {
    if (!"blast".equals(request.getString("mode")) || !"database".equals(request.getString("scope")))
      throw new IllegalArgumentException("NCBI remote search requires BLAST mode and database scope");
    if (!Boolean.TRUE.equals(request.getValue("remoteConsent")))
      throw new IllegalArgumentException("Confirm that these query sequences may be sent to NCBI");
    var databaseValue = request.getValue("remoteDatabase");
    if (!(databaseValue instanceof String database) || !DATABASES.contains(database)) throw new IllegalArgumentException("Choose a supported NCBI database");
    var timeout = integer(request, "remoteTimeoutMinutes", 10, 1, 30);
    var targets = integer(request, "maxTargets", 50, 1, 500);
    if (!(request.getValue("bothStrands") instanceof Boolean)) throw new IllegalArgumentException("Choose the DNA strands explicitly");
    if (queries.isEmpty() || queries.size() > 10 || queries.stream().mapToLong(q -> q.sequence().length()).sum() > 100_000)
      throw new IllegalArgumentException("Remote BLAST accepts at most 10 queries and 100,000 total bases per job");
    if (!(request.getValue("outputFilename", "") instanceof String output) || !output.isBlank())
      throw new IllegalArgumentException("Remote results are saved as a report, not a BED track. Export them from the results viewer.");
    return new RemoteBlastOptions(database, timeout * 60,
      new BlastSearchRunner.Options(request.getBoolean("bothStrands"), number(request, "evalue", 1e-5),
        number(request, "identity", 70), number(request, "coverage", 50), targets));
  }

  private static int integer(JsonObject request, String key, int fallback, int min, int max) {
    var value = request.getValue(key, fallback);
    if (!(value instanceof Number n) || n.doubleValue() != n.intValue() || n.intValue() < min || n.intValue() > max)
      throw new IllegalArgumentException(key + " must be an integer from " + min + " to " + max);
    return n.intValue();
  }

  private static double number(JsonObject request, String key, double fallback) {
    var value = request.getValue(key, fallback);
    if (!(value instanceof Number n) || !Double.isFinite(n.doubleValue())) throw new IllegalArgumentException(key + " must be a finite number");
    return n.doubleValue();
  }
}
