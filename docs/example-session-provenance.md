# Example recipes and recorded session provenance

An example with assembly or track files opens the existing **Open File Wizard**.
The matrix, FASTA, assembly and available annotation tracks are preselected from
the recipe. Review them before Finish; you can rename or remove a track and
change the other opening choices. Unavailable optional tracks are left out, and
their download warnings remain in Jobs. Closing the wizard does not remove the
downloaded example.

When the server opens a matrix from a ready example cache, it records that source's
catalog declaration, publications, attribution, original recipe and recorded file
sizes/hashes. A converted result is associated only when the conversion cache
still matches both its input and output. Ordinary local maps have no inferred
example attribution. The record belongs to the opened source, independently for
Primary and Secondary, and does not change when the live catalog is edited.

**Save session** includes this optional record as
`snapshot.primary.exampleProvenance` or `snapshot.secondary.exampleProvenance`.
It includes the capture time, catalog fingerprint, input/opened matrix filenames
and whether the matrix was declared directly or associated through a current
conversion cache. `catalogExample` retains the original declaration, including
optional files that may not have downloaded; `files` identifies the files actually
recorded by the downloader. It is a record of the source dataset, not a claim
that the assembly or visualization still matches the original opening recipe.

**Open session** validates the record and restores it with the source. It does
not need the original catalog or download manifest. It does not fetch citation
URLs, download files or execute the recorded recipe. The assembly and tracks are
restored from the session's actual saved state. Older sessions without example
provenance remain supported.

These are recorded cache hashes, not a fresh full-file verification on every map
open, and they do not authenticate a session file. Example opening still verifies
downloads through Jobs; session restoration checks its separate source file
fingerprints. A full-file session fingerprint remains a separate explicit option.
Removing or invalidating cache metadata before opening a local file can leave it
without an example association; provenance is never guessed from a similar name.

The declaration remains available in saved session JSON and captured figure
provenance for citation and audit. Named figure snapshots retain it across
comparison and session restore; see [figure capture](figure-capture.md). A
dedicated provenance viewer remains separate publication work.
