# Reinforcement contracts and deterministic fixtures

Status: W0 foundation. This document defines the target contracts and distinguishes
them from existing behavior. It does not certify the later work packages.

## Coordinates

Internal sequence and annotation intervals are zero-based, half-open `[start,end)`.
Lengths are `end-start`. A base position is valid only when `0 <= p < length`.
AGP and displayed positions require explicit conversion at the boundary: AGP
`start1=start0+1`, `end1=end0`. BED stays zero-based. Never apply display offsets
to stored source coordinates.

Three domains must remain distinct:

1. Source coordinates refer to an immutable reference record and source identity.
2. Assembly coordinates concatenate active components in their current order and
   orientation, without inserted export gaps. A reversed component of length L,
   source offset S and assembly offset A maps `[a,b)` to
   `[S+L-(b-A), S+L-(a-A))`, with reverse-complement sequence.
3. Padded sequence/export coordinates additionally include explicitly declared
   gaps. A gap has length and position, but no invented source interval. A join
   selects the first real base to its right. Gaps inside FASTA records remain real
   source bases unless the optional FASTA splitting operation explicitly separates
   them while retaining provenance.

`SequenceTrackProjection` already implements the distinction between assembly and
padded sequence domains. The shared test exercises every nonempty subwindow of a
two-component fixture with a reversed slice and a gap, using both plain and gzip
FASTA. Source intervals and expected letters are independent, literal oracles.

Bins at resolution R cover `[kR,min((k+1)R,L))` within their reference domain.
The final partial bin is not padded with invented bases. Source-specific stripes
and contig boundaries must be projected before choosing matrix indices; a global
`bp/R` cannot substitute for HiCT's assembly projection. Pixel coordinates include
the selected source resolution and map scale. Row and column scopes are separate
transforms. Neither a scope nor source swap changes provenance.

Edits publish a new assembly revision. Sequence, tracks, tiles, cache keys and
exports must use the same revision. Reversal preserves component source intervals
and changes orientation. Moving/grouping preserves sequence content; splitting
currently removes one finest-resolution bin and therefore is not lossless.
Undo must restore the exact prior assembly, including names, gaps and orientation.
Tests for an intentional removal must assert the removed interval explicitly.

## Signal and coverage

`contracts.json` has a symmetric integer contact matrix with an entirely zero
row. Its full matrix sum is 33 (off-diagonal contributions occur twice). Log2 is
`log(1+x)/log(2)` and has literal expected values. The custom graph multiplies
the source by seven and must serialize unchanged. These fixtures validate the
existing expression evaluator, not the currently broken sidebar transaction.

Units belong to the source: `contact_count`, `alignment_sample_count`,
`alignment_presence`, `identity_fraction` or `sequence_only`. Do not infer units
from a filename or whether the source is primary/secondary. Percent and encoded
integer identity need an explicit scale. A computed zero differs from uncomputed
coverage. Metadata must preserve that distinction through reduction and export.
Overlay replacement includes zeros and preserves cells outside the replaced area.

Independent pyramids include fine-only, coarse-only, disjoint and non-nested
levels. The target W8 choice minimizes absolute log-resolution distance, with
coarser level winning an exact tie, and hysteresis around a transition. This is a
target requirement, not a claim that current exact-only selection implements it.
The fixture also includes unsorted, duplicate and empty levels.

## Source capability contract

Implemented subset: `/open`, nested `/attach.openFileResponse` and attached
`/secondary/status` and `/secondary/open` responses now carry `capabilities`.
Its `schemaVersion` is 1, with declared `units`, `scoreScale`, `contactDataState`,
`resolutions`, `importedWeightResolutions`, `weightMetadataComplete`, and
`contactNormalization` / `importedWeights` availability objects. Availability
is `AVAILABLE`, `UNAVAILABLE` or `UNKNOWN`, with a reason code and readable reason.
`contactNormalization` expresses applicability of contact-specific operations;
it does not advertise the not-yet-implemented square-root algorithm.

New conversions persist root attributes `hict_source_metadata_version`,
`hict_source_units`, `contact_data_state`, `hict_score_scale`, and per-resolution
`hict_imported_weights` (`PRESENT` / `ABSENT`). They preserve explicitly declared
source semantics and mark only actual Cooler `bins/weight` datasets as imported.
Legacy files remain unknown, even when their stored vectors happen to be ones.
Existing sequence-overlay `score_unit` / `score_scale` metadata takes precedence
over any template metadata. Unknown versions or unreadable optional metadata do
not authorize invented units. Source files are opened read-only.

The frontend accepts supported, valid capability objects, preserves behavior
when metadata is unknown, and disables inapplicable contact scaling / imported
weight controls with accessible explanations. Imported weights are checked at
the active resolution. A partially described resolution remains unknown.

The opening response will contain a versioned capability object per source:
source id, content identity, semantic units, available resolutions, sequence
availability, contact-data state (`PRESENT`, `ABSENT`, `UNKNOWN`), and normalization
availability. Each unavailable operation carries a stable reason code and
human-readable explanation. Legacy metadata must report unknown semantics rather
than incorrectly classifying an identity overlay as contacts. Normalization
availability distinguishes imported weights, computed square-root coverage and
pending computation. Frontend parsing, Attach, secondary status and `/open` must
agree; explicit `ABSENT` disables contact controls with an accessible reason.

Source identity/fingerprints, linked-sequence capability updates, and eligibility
across the rest of the interface remain integration work. The later FASTA-only
package must persist absence consistently with this contract and implement
transparent tiles; declaring metadata alone does not implement a sequence-only session.

## Common background-job contract (target)

Every job has immutable `id`, `kind`, source identity and input revision, creation
time, status, optional start/end times, progress, error and output manifest.
States: `queued`, `running`, `cancelling`, `cancelled`, `failed`, `completed`.
Legal transitions are queued to running/cancelled/failed, running to
cancelling/completed/failed, and cancelling to cancelled/failed. Terminal states
are immutable. A completion racing cancellation has one serialized winner;
never publish a completed artifact from a job already committed to cancellation.
Retries have a new id and reference the prior attempt. Progress is indeterminate
when total work is unknown; stage-local progress is not overall completion.

Completion requires an atomically published, validated manifest (path, role,
format, size, checksum where available and provenance). Closing a modal detaches
the observer and does not cancel work. Polling stops at a terminal state and reload
reconciles by id. Stale revisions cannot replace the active session. W13 supplies
persistence, scheduling and the UI; this foundation does not introduce a second
scheduler or claim restart recovery is implemented.

## Fixture use and limitations

Committed synthetic inputs live in `src/test/resources/reinforcement/`.
`ReinforcementFixtureTest` exercises production sequence and rendering code.
Alignment CIGAR fixtures include substitution, insertion, deletion, duplicates and
reverse orientation; they are input contracts for W7/W9, not executable alignment
parity yet. Catalog snippets are semantic test cases, explicitly not complete
JSON Schema instances; W15 must add complete valid/invalid documents against its
final schema. Legacy settings and malformed sessions are supplied for W1/W2.
No invalid session is fed into a user's running server.

Large real inputs must remain outside git and be copied physically into an
isolated data directory. Record SHA-256 and bytes before a run, the exact JAR and
WebUI revisions/hashes, machine/browser versions and command. The local baseline
manifest identifies available real fixtures without redistributing user data.
`ReinforcementHdf5Fixture` now generates tiny real Cooler and HiCT files with
primary `[4,10]`, disjoint `[3,7]`, fine-only `[1]` and coarse-only `[25]` levels.
The 17 bp source has four contact records. Tests exercise production conversion,
full/single-pixel reads, a zero row, terminal partial bins, reversal and restoration.
When upper-triangle contacts collapse into a coarse diagonal, count each stored
contact once; full symmetric matrix sums are not invariant under this rebinning.
The coarse-only fixture intentionally has a hidden sub-resolution contig; its
bin-domain reads remain valid and must not be confused with visible-pixel reads.

Generate fixtures using the test classes and a development server JAR:

```sh
./gradlew testClasses -x copyWebUI
java -cp "build/classes/java/test:<server-fat.jar>" ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture /tmp/hict-fixtures-new
python scripts/prepare_reinforcement_sources.py --data-dir /tmp/hict-fixtures-new
```

Use the existing validation Python environment with h5py and NumPy. The preparer
adds a declared synthetic weighted source and an identity overlay using the
production overlay writer. It refuses to overwrite existing outputs. Full catalog
fixtures and the remainder of W0 capability integration are still outstanding.
