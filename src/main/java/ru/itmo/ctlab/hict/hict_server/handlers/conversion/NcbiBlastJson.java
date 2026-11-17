package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSearchQueries;

import java.io.IOException;
import java.io.BufferedOutputStream;
import java.util.concurrent.CancellationException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/** Decode documented JSON2_S results without assigning external subjects to the assembly. */
final class NcbiBlastJson {
  private static final long MAX_INTEGER = 9_007_199_254_740_991L;
  record Decoded(RemoteBlastRunner.Parsed parsed, JsonObject provenance) { }

  static Decoded decode(byte[] bytes, List<SequenceSearchQueries.Query> queries,
                        RemoteBlastOptions options, Path raw) throws IOException {
    if (bytes.length > RemoteBlastRunner.MAX_OUTPUT_BYTES) throw invalid("result exceeds the size limit");
    final JsonObject document;
    try {
      String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes)).toString();
      checkStructure(text);
      document = new JsonObject(text);
    } catch (CancellationException cancelled) { throw cancelled; }
    catch (Exception malformed) {
      throw invalid("result is not a bounded JSON2_S report");
    }
    var reports = array(document, "BlastOutput2");
    if (reports.size() != queries.size()) throw invalid("returned query count differs from the pasted queries");
    var seen = new HashSet<String>();
    var verification = new JsonArray();
    var providers = new JsonArray();
    long written = 0;
    try (var output = new BufferedOutputStream(Files.newOutputStream(raw, StandardOpenOption.CREATE_NEW))) {
      for (Object entry : reports) {
        checkCancelled();
        var report = object(asObject(entry), "report");
        if (!"blastn".equals(text(report, "program", 100))) throw invalid("request is not a nucleotide BLAST search");
        var database = text(object(report, "search_target"), "db", 512);
        if (!database.equals(options.remoteDatabaseName()) && !(options.database().equals("nt") && database.equals("core_nt")))
          throw invalid("request database differs from the selected database");
        var parameters = object(report, "params");
        if (parameters.encode().length() > 16_384) throw invalid("provider parameters exceed the size limit");
        providers.add(new JsonObject().put("version", text(report, "version", 200))
          .put("database", database).put("parameters", parameters.copy()));
        var search = object(object(report, "results"), "search");
        String title = text(search, "query_title", 4096);
        var query = identify(queries, title);
        if (!seen.add(query.id()) || integer(search, "query_len", 1) != query.sequence().length())
          throw invalid("returned query identity or length differs from the pasted queries");
        var covered = new BitSet(query.sequence().length());
        var acceptedSubjects = new HashSet<String>();
        for (Object hitValue : array(search, "hits")) {
          checkCancelled();
          var hit = asObject(hitValue);
          var descriptions = array(hit, "description");
          if (descriptions.isEmpty()) throw invalid("subject description is missing");
          var description = asObject(descriptions.getValue(0));
          String subject = text(description, "id", 1000);
          String accession = text(description, "accession", 1000);
          if (subject.chars().anyMatch(c -> c < 32 || c == 127) || accession.chars().anyMatch(c -> c < 32 || c == 127))
            throw invalid("subject identifiers contain control characters");
          // JSON2_S sometimes omits the version in accession while retaining it in id.
          for (String part : subject.split("\\|")) if (part.matches(java.util.regex.Pattern.quote(accession) + "\\.[0-9]+")) accession = part;
          String subjectTitle = text(description, "title", 65_536).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
          subjectTitle = subjectTitle.substring(0, Math.min(4096, subjectTitle.length()));
          long subjectLength = integer(hit, "len", 1);
          for (Object hspValue : array(hit, "hsps")) {
            checkCancelled();
            var hsp = asObject(hspValue);
            long qs = integer(hsp, "query_from", 1), qe = integer(hsp, "query_to", 1);
            long ss = integer(hsp, "hit_from", 1), se = integer(hsp, "hit_to", 1);
            long length = integer(hsp, "align_len", 1), identities = integer(hsp, "identity", 0), gaps = integer(hsp, "gaps", 0);
            if (Math.max(qs, qe) > query.sequence().length() || Math.max(ss, se) > subjectLength)
              throw invalid("alignment coordinates exceed their sequence");
            String queryAligned = text(hsp, "qseq", bytes.length).toUpperCase(Locale.ROOT);
            String subjectAligned = text(hsp, "hseq", bytes.length).toUpperCase(Locale.ROOT);
            if (queryAligned.length() != length || subjectAligned.length() != length
                || !queryAligned.matches("[ACGTRYSWKMBDHVN-]+") || !subjectAligned.matches("[ACGTURYSWKMBDHVNX-]+"))
              throw invalid("alignment strings do not match their declared length/alphabet");
            long observedIdentity = 0, observedGaps = 0, openings = 0, mismatches = 0;
            for (int i = 0; i < queryAligned.length(); i++) {
              if ((i & 8191) == 0) checkCancelled();
              char q = queryAligned.charAt(i), s = subjectAligned.charAt(i);
              if (q == '-' && s == '-') throw invalid("alignment contains a double-gap column");
              if (q == '-' || s == '-') {
                observedGaps++;
                if (q == '-' && (i == 0 || queryAligned.charAt(i - 1) != '-')) openings++;
                if (s == '-' && (i == 0 || subjectAligned.charAt(i - 1) != '-')) openings++;
              } else if (q == s) observedIdentity++;
              else mismatches++;
            }
            String actualQuery = queryAligned.replace("-", "");
            String actualSubject = subjectAligned.replace("-", "");
            String expectedQuery = query.sequence().substring((int) Math.min(qs, qe) - 1, (int) Math.max(qs, qe));
            if (qs > qe) expectedQuery = reverseComplement(expectedQuery);
            if (!actualQuery.equals(expectedQuery)) throw invalid("returned aligned bases do not match the pasted query");
            if (actualSubject.length() != Math.abs(se - ss) + 1 || actualQuery.length() != Math.abs(qe - qs) + 1
                || observedIdentity != identities || observedGaps != gaps)
              throw invalid("alignment strings, scores and coordinate spans disagree");
            strand(hsp, "query_strand", qs, qe);
            strand(hsp, "hit_strand", ss, se);
            covered.set((int) Math.min(qs, qe) - 1, (int) Math.max(qs, qe));
            double identity = identities * 100d / length;
            double evalue = number(hsp, "evalue"), bits = number(hsp, "bit_score");
            double coverage = actualQuery.length() * 100d / query.sequence().length();
            if (identity < options.alignment().identity() || evalue > options.alignment().evalue()
                || coverage < options.alignment().coverage() || (!options.alignment().bothStrands() && qs > qe)) continue;
            if (!acceptedSubjects.contains(subject) && acceptedSubjects.size() >= options.alignment().maxTargets()) continue;
            acceptedSubjects.add(subject);
            byte[] row = (String.join("\t", query.id(), subject, accession, Long.toString(qs), Long.toString(qe),
              Long.toString(ss), Long.toString(se), Double.toString(identity), Long.toString(length),
              Long.toString(mismatches), Long.toString(openings), Double.toString(evalue), Double.toString(bits),
              Integer.toString(query.sequence().length()), Long.toString(subjectLength), subjectTitle) + "\n").getBytes(StandardCharsets.UTF_8);
            if (row.length > RemoteBlastRunner.MAX_OUTPUT_BYTES - written) throw invalid("tabular result exceeds the size limit");
            output.write(row); written += row.length;
          }
        }
        verification.add(new JsonObject().put("queryId", query.id()).put("providerQueryTitle", title)
          .put("alignedBasesVerified", covered.cardinality()).put("queryLength", query.sequence().length())
          .put("scope", covered.cardinality() == query.sequence().length() ? "FULL_QUERY"
            : covered.isEmpty() ? "LABEL_AND_LENGTH_ONLY" : "ALIGNED_BASES_ONLY"));
      }
    } catch (CancellationException cancelled) { throw cancelled; }
    catch (IOException failure) { throw failure; }
    catch (RuntimeException malformed) { throw invalid("result contains malformed query or alignment fields"); }
    return new Decoded(RemoteBlastRunner.parse(raw, queries), new JsonObject()
      .put("providerReports", providers).put("queryVerification", verification)
      .put("filterScope", "Identity, e-value, per-HSP query coverage, strands and target limits are applied locally to the retrieved request. No search is submitted."));
  }

  private static SequenceSearchQueries.Query identify(List<SequenceSearchQueries.Query> queries, String title) throws IOException {
    String first = title.split("\\s+", 2)[0];
    var ids = queries.stream().filter(q -> q.id().equals(first)).toList();
    if (ids.size() == 1) return ids.getFirst();
    var descriptions = queries.stream().filter(q -> q.description().equals(title)).toList();
    if (descriptions.size() != 1) throw invalid("returned query labels do not identify the pasted queries uniquely");
    return descriptions.getFirst();
  }
  private static String reverseComplement(String sequence) {
    String alphabet = "ACGTRYSWKMBDHVN", complement = "TGCAYRSWMKVHDBN";
    var result = new StringBuilder(sequence.length());
    for (int i = sequence.length() - 1; i >= 0; i--) result.append(complement.charAt(alphabet.indexOf(sequence.charAt(i))));
    return result.toString();
  }
  private static void strand(JsonObject hsp, String key, long from, long to) throws IOException {
    String strand = text(hsp, key, 10);
    if (!strand.equals("Plus") && !strand.equals("Minus") || from != to && strand.equals("Minus") != (from > to))
      throw invalid("alignment strand and coordinates disagree");
  }
  private static JsonObject asObject(Object value) throws IOException {
    if (!(value instanceof JsonObject object)) throw invalid("expected a result object");
    return object;
  }
  private static JsonObject object(JsonObject object, String key) throws IOException { return asObject(object.getValue(key)); }
  private static JsonArray array(JsonObject object, String key) throws IOException {
    if (!(object.getValue(key) instanceof JsonArray array)) throw invalid("missing result array " + key);
    return array;
  }
  private static String text(JsonObject object, String key, int max) throws IOException {
    if (!(object.getValue(key) instanceof String value) || value.isBlank() || value.length() > max)
      throw invalid("invalid text field " + key);
    return value;
  }
  private static long integer(JsonObject object, String key, long min) throws IOException {
    if (!(object.getValue(key) instanceof Number number) || !Double.isFinite(number.doubleValue())
        || number.doubleValue() != number.longValue() || number.longValue() < min || number.longValue() > MAX_INTEGER)
      throw invalid("invalid integer field " + key);
    return number.longValue();
  }
  private static double number(JsonObject object, String key) throws IOException {
    if (!(object.getValue(key) instanceof Number number) || !Double.isFinite(number.doubleValue()) || number.doubleValue() < 0)
      throw invalid("invalid numeric field " + key);
    return number.doubleValue();
  }
  private static void checkStructure(String text) throws IOException {
    boolean string = false, escaped = false; int depth = 0, items = 0;
    for (int i = 0; i < text.length(); i++) {
      if ((i & 8191) == 0) checkCancelled();
      char c = text.charAt(i);
      if (string) { if (escaped) escaped = false; else if (c == '\\') escaped = true; else if (c == '"') string = false; }
      else if (c == '"') string = true;
      else if (c == '{' || c == '[') { if (++depth > 32 || ++items > 1_000_000) throw invalid("JSON structure exceeds the limit"); }
      else if (c == '}' || c == ']') { if (--depth < 0) throw invalid("invalid JSON structure"); }
      else if ((c == ',' || c == ':') && ++items > 1_000_000) throw invalid("JSON item count exceeds the limit");
    }
    if (depth != 0 || string) throw invalid("incomplete JSON structure");
  }
  private static void checkCancelled() { if (Thread.currentThread().isInterrupted()) throw new CancellationException(); }
  private static IOException invalid(String reason) { return new IOException("Cannot retrieve this NCBI request: " + reason + ". Check its RID and original queries."); }
}
