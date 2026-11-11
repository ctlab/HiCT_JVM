# Assembly edit history

The production application keeps a server-session journal of reversal, translocation,
scaffolding, unscaffolding, debris, native split, AGP/Juicebox import, and name edits.
Undo/redo and entry selection restore saved assembly states, including split bins,
FASTA source offsets, imported gaps, display names, descriptor lookup and scaffold IDs.
No inverse approximation is used for splitting. Failed edits restore the pre-edit state.
Empty/unchanged operations do not discard the redo branch. A successful new edit
following an undo truncates later entries; entry IDs are never reused in the session.

The journal is intentionally session-local, not a saved project format. Closing/opening
a file, attaching/detaching an overlay, or switching the authoritative assembly source
starts a new journal. Exported AGP/FASTA still represent the current state only.
Visualization, normalization, track settings, file conversion and file linking are
not assembly edits and are not journaled. Two browser clients share the server journal;
revision/session guards prevent stale undo/redo and guarded structural requests.

## Cost and consistency

Contig/scaffold roots are persistent. A checkpoint retains their references and the
scaffold ID counter. Descriptor/name maps keep per-key change logs, so checkpoints
are O(1), lookups remain hash lookups, and restoring metadata costs the number of
changed keys between the states. AGP annotation maps are immutable and shared.
No whole-contig-list or contact-matrix copy is made by history. Overlay visibility
uses replacement descriptors, so earlier roots cannot be modified through aliases.

The server assembly queue has one worker to serialize commits and lifecycle changes;
other worker pools and matrix parallelism are unchanged. Successful edits add a
small constant-size summary header. Journal listing is paginated (maximum 200 entries).
Restoration still serializes a complete assembly response and refreshes dependent
views; those existing O(n) application costs must not be described as O(1) undo.

History has no automatic eviction. Retained changed tree paths, metadata and import
states consume memory proportional to the retained edits. Repeated complete AGP
imports can be much more expensive than reversals. Closing the map releases its
journal. This is the explicit tradeoff for retaining every state until branching.
A journal is not crash recovery and does not survive a server restart.

## API

See `src/main/resources/openapi/hict-api-v1.yaml` for `/assembly/history` and
`/assembly/history/restore`. Existing clients retain their response body contracts.
The new WebUI consumes `X-HiCT-History`, uses Ctrl/Cmd+Z and Ctrl/Cmd+Y or
Ctrl/Cmd+Shift+Z, and keeps native text-input undo intact. Queries and exports always
operate on the current restored assembly, and stale tile/track/export jobs are cancelled.

## Regression coverage

`EditJournalTest` checks branching, stale versions, pagination and randomized metadata
restoration against independent map copies. `HictToMcoolConverterTest` checks exact
matrix, AGP, FASTA and metadata recovery for native splits, debris, reversals, moves,
partial imports and forced failures after mutation. `RequestTaskSchedulerTest` checks
FIFO assembly execution independently of the parallel tile pool. Real g3 HTTP and
production-browser scenarios are maintained in the companion benchmark suite.
