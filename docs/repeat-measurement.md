# FASTA interval and repeat measurement

At readable FASTA zoom, Ctrl/Command-drag selects whole letters on either axis.
Right-click → Drag to select bases offers the same gesture without a modifier;
plain drag otherwise pans. The selection includes both endpoint letters and is
stored as a zero-based half-open interval in the gap-free current assembly.
The card displays one-based inclusive coordinates and its exact base count.
Reverse-row copying returns the reverse complement in reading direction, including
IUPAC symbols. It does not reverse the interval's assembly coordinates. Shift-arrow
extends within the visible bases; Escape cancels an unfinished gesture or clears
the measurement. Selection overlays are excluded from figure exports. Panning
keeps the frozen selection; assembly/scope/FASTA changes invalidate it. It is
transient and does not enter portable sessions.

## Estimator contract

An explicit Estimate period action submits `/sequence/repeat_period` with the
displayed history headers and `startBp,endBp`. The serial assembly scheduler
captures an immutable sequence interval, source-file stamp and assembly fingerprint.
The shared bounded sequence worker queue performs the comparisons, so no analysis
holds the assembly scheduler. Reads are limited to 4096 bases. Queue saturation
returns 503. Both sequence-job and global Jobs cancellation APIs apply. Completed
results retain the interval, assembly fingerprint, source path/stat stamp and a
SHA-256 of the captured forward sequence in the server's bounded job history;
the card and Jobs summary identify the captured interval. Closing or cancelling
the card ignores late results and requests cancellation of outstanding work.

`ungapped-autocorrelation-v1` evaluates integer lags `p = 1 … floor(n/2)` for
selected length `n`. It compares `sequence[i]` with `sequence[i-p]`. A pair is
informative only when both symbols are A, C, G or T. All other IUPAC symbols,
including N, are excluded. `identity = matching pairs / informative pairs`.
`compositionIdentity = Σ_b (count_b / informative bases)²` over A/C/G/T.

The shortest qualifying lag is reported:

- Exact periodicity: every pair is informative and matches.
- Imperfect candidate: at least eight informative pairs, informative support at
  least 80% of the lag overlap, pair identity at least 80%, and identity at least
  0.20 above the interval's composition baseline.

The interval contains at least two full copies of the lag, with a possible partial
final copy. `copies = n/p` is an interval-length ratio, not an annotation of repeat
boundaries. Results also report pair counts, ambiguity and the method version.
If no lag qualifies, the period/identity/copies fields are null. All-ambiguous
intervals provide no repeat evidence. Exact homopolymers have period one.

These deterministic thresholds describe an exploratory visual measurement.
Overlapping comparisons are correlated; this is not a significance test. The
method does not model indels, identify a repeat family, establish biological
repeat boundaries or distinguish chance periodicity in short intervals. Strong
composition bias can suppress an imperfect candidate. Mixed motifs and larger
periods may need a longer interval or dot-plot/alignment analysis. Reverse
complementation preserves the estimated lag and pair counts. The server analyzes
the captured forward assembly interval for either selected strand.
`inputSha256` hashes that frozen sequence's UTF-8 bytes before estimator uppercasing;
the file stat stamp is a change guard, not a whole-FASTA checksum.

## Verification

Library tests cover `CTCTCTCTCTCT`, `CTCTCTCTCT`, partial copies, homopolymers,
one point substitution, lowercase, ambiguity, composition bias, size bounds and
cancellation. An independent shift oracle checks exact motifs. Request tests
cover safe coordinates above 2.5 Gb and malformed/out-of-bounds intervals.
The real HTTP integration test occupies the sequence worker deterministically,
cancels queued work through Jobs, edits/reverses the live assembly, and verifies
that the remaining job uses its original captured input and snapshot. It checks
stale-history rejection, settings capability, summaries and unchanged native bytes.

The Chromium acceptance script is `scripts/check_fasta_measurement_browser.py`.
It creates fresh physical data and known repeats, tests both axes/strands,
clipboard contents, exact and imperfect estimates, ambiguity, keyboard extension,
Escape, delayed submissions, edit invalidation, panning, compact card layout,
selection-free image export and transient session restore. Detailed evidence and
artifact hashes are recorded in `reinforcement-validation.md`. Broad platform,
secondary-source, accessibility and large-map performance qualification remain
separate release gates.
