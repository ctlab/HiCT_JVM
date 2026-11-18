package ru.itmo.ctlab.hict.hict_library.assembly;

import java.util.Locale;

/** Bounded ungapped autocorrelation. A candidate lag is exploratory, not a repeat annotation. */
public final class RepeatPeriod {
  public static final int MAX_BASES = 4096;
  public record Result(int length, Integer periodBp, Double copies, Double identity,
                       int comparedPairs, int matches, int ambiguousBases,
                       double compositionIdentity, String method, String message) { }
  private RepeatPeriod() { }

  public static Result estimate(String input, Runnable checkCancelled) {
    if (input == null || input.isEmpty() || input.length() > MAX_BASES)
      throw new IllegalArgumentException("Select 1 to " + MAX_BASES + " bases for repeat measurement");
    final var sequence = input.toUpperCase(Locale.ROOT);
    final int n = sequence.length();
    final int[] counts = new int[4];
    int informative = 0;
    for (int i = 0; i < n; i++) {
      final int base = "ACGT".indexOf(sequence.charAt(i));
      if (base >= 0) { counts[base]++; informative++; }
    }
    double baseline = 0;
    if (informative > 0) for (int count : counts) baseline += Math.pow((double) count / informative, 2);
    // Prefer the shortest qualifying lag. Two complete copies are required; the
    // last copy may be partial. Ambiguous IUPAC symbols never count as matches.
    for (int period = 1; period <= n / 2; period++) {
      checkCancelled.run();
      int compared = 0, matches = 0;
      for (int i = period; i < n; i++) {
        char a = sequence.charAt(i), b = sequence.charAt(i - period);
        if ("ACGT".indexOf(a) < 0 || "ACGT".indexOf(b) < 0) continue;
        compared++;
        if (a == b) matches++;
      }
      final double identity = compared == 0 ? 0 : (double) matches / compared;
      final boolean exact = compared == n - period && matches == compared;
      // Imperfect candidates need enough informative positions and support above
      // this interval's composition baseline. This is deliberately not a p-value.
      if (exact || (compared >= 8 && compared >= .8 * (n - period)
          && identity >= .8 && identity >= baseline + .2))
        return new Result(n, period, (double) n / period, identity, compared, matches,
          n - informative, baseline, "ungapped-autocorrelation-v1",
          exact ? "Exact periodicity in this interval; a partial final copy is allowed."
            : "Candidate ungapped period; validate with a dot plot. Indels are not modeled.");
    }
    checkCancelled.run();
    return new Result(n, null, null, null, 0, 0, n - informative, baseline,
      "ungapped-autocorrelation-v1", "No supported period with at least two copies. Indels are not modeled.");
  }
}
