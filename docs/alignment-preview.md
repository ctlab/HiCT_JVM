# Letter-scale alignment preview

The preview compares the horizontal and vertical FASTA intervals visible at
readable letter zoom. The original renderer placed a small symbol at each aligned
base centre. It now draws continuous CIGAR runs through full base intersections:
green forward matches, purple reverse matches, orange substitutions, dashed gray
ambiguity, and horizontal/vertical dashed indels. Each aligned base boundary is a
vertex, preserving geometry through nonuniform native fragments as well as the
uniform base grid. Adjacent operations meet at their boundaries. White halos keep
the lines visible on contact colors. The legend uses the same colors/directions.

PNG, SVG and PDF viewport exports use the same line geometry. SVG/PDF retain
vector paths instead of per-base icon images. Whole-map exports follow their
fitted scale; letter detail appears only when letters fit at that scale.

## Data and short repeats

`/sequence/alignment_detail` freezes the selected assembly/FASTA windows under
the history guard and runs the bundled minimap2-compatible aligner. Both windows
are bounded to 4096 bases. A finished result contains every reported extended
CIGAR alignment, with an alignment ID on each segment. The backend already read
all PAF lines before this increment: the advisor's CT example was lost by the
old seeding/score settings, rather than a first-record-only parser.

The profiles are deterministic:

| Profile | Window sizes | k | w | minimum chain score | minimum seed count | minimum alignment score |
|---|---|---|---|---|---|---|
| `short-window-v1` | Both ≤64 BP | 2 | 1 | 2 | 1 | 2 |
| `seeded-window-v1` | Either >64 BP | 7 | 3 | 12 | 2 | 12 |

Both use one thread, `-P`, `--dual=no`, `--no-long-join`, `-c` and `--eqx`.
`-P` retains reported chains and `--eqx` distinguishes matches/substitutions.
The parameter meanings and heuristic limits follow the
[minimap2 manual](https://lh3.github.io/minimap2/minimap2.html).
Short seeds recover the ten maximal exact diagonals of
`CTCTCTCTCTCT × CTCTCTCTCT`, including 2-BP edge overlaps. They also increase
coincidental short matches. This preview is a local sequence comparison, not a
homology significance test or an exhaustive enumeration of every possible
alignment. Larger-window heuristics can miss short/low-complexity hits. Single
bases have no 2-BP seed. Empty output is shown explicitly.

The parser checks each canonical `=`/`X` operation against the frozen row and
column bases, reverse-complementing the query strand when necessary. Inconsistent
labels fail rather than inventing a match or substitution. Non-ACGT positions
remain ambiguous. Indel and run bounds must consume exactly their declared PAF
intervals. Results include `minimap2-extended-cigar-v2`, the applied profile,
window bounds, assembly fingerprint, executed aligner name/SHA-256 and SHA-256
of each frozen sequence's UTF-8 bytes. These identify the computation actually
performed, including cached completed jobs.

## Limits and lifecycle

The server caps output at 256 reported alignments and 4096 segments. The UI caps
rendering at 4096 runs and 65,536 boundary vertices. Oversized output fails with
an actionable smaller-window message; it is not silently truncated. All work
uses the existing bounded sequence worker and Jobs cancellation path.

Navigation immediately cancels known jobs and debounces fresh previews. The
initial HTTP submission stays alive until it returns the job ID (with a 15-second
transport timeout), allowing obsolete submissions to be stopped. Polls remain
abortable. Generation guards prevent delayed responses from redrawing after
navigation, disabling the preview, assembly changes or disposal. Display-grid
metadata participates in the client request key, so cached data is reprojected
when the coordinate model changes. Export waits for the matching preview.

## Verification

Tests cover both profile boundaries, genuine canonical source/CIGAR agreement on
both strands, reverse-complement mismatch rejection, lowercase and ambiguity.
The stricter source check exposed a contradictory base in an older synthetic
reverse-strand fixture; the fixture now matches its intended CIGAR. Geometry
regressions cover continuous runs, single-base substitutions, reverse runs,
indels, >2.5 Gb coordinates, nonuniform mapping and invalid/bounded inputs.
Controller checks cover disable/resume, late responses and cancellation of a
late initial submission.

`scripts/check_alignment_runs_browser.py` uses fresh physical HDF5/FASTA inputs
and the real bundled aligner. An independent maximal-diagonal enumeration checks
all ten CT hits. It tests a point substitution, short and larger-window indels,
reverse complements, N/IUPAC, no-hit display, precise line vertices, further zoom,
exports, exact fragments, reversal, undo/redo and scoped session restoration.
Detailed results and artifact hashes are in `reinforcement-validation.md`.
Full secondary-source/platform, accessibility, dense-window performance and
final embedded-package qualification remain release gates.
