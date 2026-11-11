# Control coverage ledger

The historical third-pass capture contains 232 distinct DOM signatures across
18 menu destinations and sidebar states. The preserved fixture is
`src/test/resources/reinforcement/historical-control-signatures.json`.
It retains signatures and surfaces, not the old shorthand status. Dynamic labels
can create multiple signatures for one function; unnamed controls can collapse
different functions into one signature. It is not the complete set of UI actions.

## Reconciliation

```sh
python3 scripts/reconcile_control_inventory.py \
  --historical src/test/resources/reinforcement/historical-control-signatures.json \
  --webui ../HiCT_WebUI --output /tmp/hict-control-reconciliation.json
```

This assigns deterministic signature IDs and finds literal source candidates,
preferring ID, aria-label, title, then text. Literal search still finds candidates
for 170 signatures and none for 62 on WebUI `44ca0cd`. The authored
`src/test/resources/reinforcement/control-contracts.json` now adds prerequisites,
expected effects and source anchors for all 232 historical signatures. It has 227
source-inspected entries and five explicitly pending ambiguous signatures.
Inspection does not certify behavior: every functional outcome remains pending
until linked to a scenario-specific runtime result. Existing defect reports still
apply even to source-inspected controls.

The reconciler validates contract IDs, source-anchor existence and nonempty
expectations. It refuses stale anchors and rejects runtime-pass claims in static
contracts. It records both fixture digests and the reviewed WebUI revision.
Re-run after label or binding changes. Library-generated zoom/rotation controls
have separate OpenLayers source hashes and must not be confused with the toolbar
controls. The library hashes record inspected evidence, not a dependency upgrade
compatibility guarantee.

The remaining ambiguous signatures are historical indices 35, 38, 125, 180 and
193: toast close, unnamed buttons, and repeated Close buttons. Their source field
is explicitly only a surface entry point. Do not treat it as the actual element
owner. New captures preserve element HTML and the containing dialog/toast identity
so these signatures can be split into instance-specific scenarios. Shared file
pickers, analysis disclosures and dynamically generated preset/resolution rows
also require testing each applicable surface/state, not just one representative.
A visible node or successful request never establishes the required outcome.

The new capture confirms that historical index 38 merges dialog close buttons
with the OpenLayers zoom-slider thumb. Its contract explicitly separates dismissal
from dragging the zoom thumb. Treating every unnamed button as Close would be a
test-harness error and could hide a navigation failure.

Contract-authoring evidence: `/tmp/hict-reinforce-control-contracts/`. All 18 menu
surface checks and three missing-FASTA recovery checks passed again after adding
element/owner capture, with no page or HTTP errors. These results concern the menu
suite only; they do not promote the 232 contract outcomes to verified status.

## Menu contracts and browser run

`src/test/resources/reinforcement/menu-surfaces.json` defines stable IDs for all
18 historical destinations, source owners, prerequisites, opening expectations
and the separate functional outcome still required. These source owners were
checked against NavigationBar's menu handlers and mounted components.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_menu_surfaces_browser.py \
  --url http://127.0.0.1:8204 --api http://127.0.0.1:5204 \
  --file combined_ind2_4DN.hict.hdf5 --output /tmp/hict-menu-surfaces
```

Requires Playwright, a running development build with its benchmark hook, and a
disposable map under the server data directory. Each case gets a fresh browser
context. Map setup uses the benchmark hook; menu and recovery clicks are actual
UI interactions. This does not replace manual-open or wizard acceptance tests.

The run checks exact destination headings, the new local API documentation window,
page exceptions and HTTP errors. Sequence dialogs must settle to missing-FASTA
guidance, disable computation and open the FASTA selector through their recovery
button. Screenshots and visible controls are saved per destination at 1280×720.
Surface outcome and full functional outcome are separate fields. Conversion,
export, assembly application and jobs are not triggered by this surface suite.

Local evidence is under `/tmp/hict-reinforce-w0-capabilities/`: the initial
`menu-surfaces/` run checked opening only; `menu-surfaces-final/` adds settled
prerequisites and recovery. The capability suite also passed after commits
`31c34bfd` (JVM) and `44ca0cd` (WebUI), with no page or HTTP errors.

The surface inventory includes buttons lacking text, title and aria-label. These
are accessibility investigation candidates, not a computed-accessible-name audit:
SVG names or aria-labelledby can still supply a name. Existing review findings
about modal focus containment and debug notifications remain unresolved. Passing
these opening checks does not establish keyboard accessibility, comfortable
layout, every nested control, all application states, or release readiness.

## Dialog and graph audit update

JVM `dc40c18e` and WebUI `cb7ca73` pass all eighteen dialog destinations, actual
Close actions, Tab/Shift+Tab containment, visible focus return, informational
notice yielding and exact AGP preservation. The normally bundled JAR also passes
31 repeated acceptance suites, including 151 opening assertions, eight export
cases, real jobs and independent mixed layers. All fifteen sequence lifecycle
cases and the exact baked UI metadata pass. Results are retained under
`/tmp/hict-u13-dialog-regate-{core,open-session,quad,extra}.json` and
`/tmp/hict-u13-dialog-sequence-lifecycle/summary.json`.

The pipeline pixel audit then found transparent source-independent expressions.
JVM `91581c50` corrects that rendering path. Twelve pipeline scenarios, four
parameterized constant/coordinate/track regressions, the full 440-test JVM suite,
mixed-pyramid rendering and actual identity overlays pass. Detailed semantics
and evidence are in `docs/reinforcement-validation.md`.

## U13 runtime evidence on 2.0.0 candidates

The browser assertions below ran against disposable physical inputs. The current
revision and packaged-artifact status are recorded in
`docs/release-validation-2.0.0.md`. Evidence directories retain JSON, screenshots,
server logs and gate output under `/tmp/hict-u13-publication-*` and the additional
pointers named here. Repeated historical signatures require their applicable
instances to be exercised. A row with remaining assertions stays open.

| Historical indices | Executed browser assertions | Remaining assertions |
|---|---|---|
| 0 | Workspace brand click preserves exact view and native AGP hash | Complete |
| 1–6 | Navigation audit opens all six menus with ArrowDown, closes with Escape and verifies focus return and unchanged view | Complete |
| 7–11 | Workspace searches known contig/scaffold, verifies exact centers, independently applies row/column scopes, copies scope and queries exact 2×2 values, rejects an unknown scope | Complete for the audited contig/scaffold searches, independent scopes and absent-suggestion viewport preservation. |
| 12 | Opening gate checks reciprocal O/E palette and Observed restoration, numeric expression and histogram suites check signal queries | Full independent Expected/O/E boundary and zero-denominator runtime matrix |
| 13 | Navigation reload preserves exact pipeline and viewport through completed rendering | Explicit pre/post pixel comparison |
| 14–15 | Workspace verifies overlay persistence, ruler unit regressions verify coordinates, browser/export suites exercise ruler layouts | All three actual ruler-mode button states now cycle with exact viewport preservation. Tick coordinates and relative resets are covered by the shared ruler-layout regression suite. |
| 16–18 | Export suites use dropdown actions, workspace uses toolbar zoom, baseline repeatedly clicks OpenLayers zoom with rendered-frame assertions | Toolbar zoom center/finest/coarsest-limit assertions |
| 19–21 | Workspace verifies all three diagonal navigation formulas | Complete |
| 22–27, 30 | Structural suite verifies Reverse, Move, Split, Group, Ungroup, Unscaffold All and Debris with exact independent AGP and Undo/Redo/Undo | Move Escape now verifies inactive tool, hidden arrows and exact native snapshot preservation before the real move. Edited-sequence coverage beyond the existing edited-view suite remains. |
| 28 | Edited-view suite compares downloaded selected edited FASTA with source sequence and reverse complement | Selected gap-containing FASTA fixture |
| 29 | Workspace toolbar and sidebar bookmarks restore exact view, names survive export/import/remount and native-session reload, legacy unnamed files remain readable | Complete |
| 31, 49, 231 | Workspace opens notifications and clears them while exact native AGP remains unchanged | Explicit badge and retained background-job checks |
| 32 | Navigation collapses and restores sidebar with exact viewport preservation | Complete |
| 33–34 | Opening gate exercises plain picker, options wizard and Attach, concurrency gate preserves attached state and rejects stale saves, capability gate verifies fresh-window Attach | Complete for these opening paths |
| 35, 38, 125, 180, 193 | Workspace closes an actual bookmark toast and drags the actual OpenLayers thumb, menu capture records owning dialog/toast HTML, menu suite dismisses each destination with Escape | All eighteen menu destinations now pass actual dismissal, focus trapping, visible focus return and exact native AGP preservation. The zoom thumb and toast close also have separate workspace assertions. |
| 36–37 | Sequence/review gates link real FASTA through UI and query both axes, lifecycle applies real AGP, HTTP guards retain prior map on invalid references | Complete picker cancellation and rejected import interactions on both surfaces |
| 39–48 | Picker gate verifies relative Go/Enter, directory double-click, Tree/Explorer selection preservation, Up/Root, filter clearing, Show all, exact loaded 2×2 contacts and Dismiss/native-AGP preservation | Complete for rejected parent traversal, preserved listing, Root recovery and subsequent exact nested-file contacts. |
| 50–52 | Baseline clicks plus/minus through twenty changes, workspace resets nonzero rotation with center preservation | Explicit finest/coarsest endpoint clicks |
| 53–55 | Edited-view checks exact exported border stroke/outline settings | Border-panel collapse and each caption-side control through actual UI with exported placement |
| 56–60 | Workspace changes panels and remounts settings, history checks immutable restore IDs, duplicate labels, paging, branch truncation, focused native text Undo, structural suite checks exact journal round trips | Track/sequence restoration assertions for each journal action |
| 61–69, 229 | Opening gate checks displayed/stored/rendered bound coherence, source numerical regressions exercise transforms and graph synchronization | Keyboard/drag handles, Linear/Log slider sensitivity, invalid domains, Log2/Log10 and fractional scaling pass the signal audit. Independent source/half targeting remains. |
| 70–71 | Exact-range gate verifies independent source bounds and cache reuse after panning | Loaded-tile and exact-range buttons pass the independent known-count signal fixture. Additional independent-source extrema fixtures remain. |
| 72–82, 230 | Opening/capability gates check silent NaN policy, weight availability at changing levels, coherent pipeline, O/E palette, numerical rendering and migrated source controls | All advanced sidebar controls, policy variants and reset/apply failure interactions |
| 83–88 | Workspace saves/renames with Enter/Escape, exports/removes/imports/applies visualization-option presets and preserves a missing-overlay graph with guidance | Explicit persisted reload of accepted rename. Malformed later-item rejection, exact local threshold/pipeline preservation, multi-item IDs and legacy unnamed imports now pass. |
| 89–101 | ModDotPlot gate applies its ordinary preset rows and verifies primary pixels plus secondary percentage scale, opening suite checks default maps | All fourteen sidebar actions now pass known-count numerical transforms, rendered RGBA, missing-overlay preservation and a manual-range palette assertion. Both generated identity modes and all four editor presets also pass with real sources. |
| 102–110 | Batch gate checks hictk link target and notices, All/None/Invert/Converted/Unconverted exact filename sets and settings Back, real batch output matches independent contacts | Complete for the audited batch fixture, including exact empty active-job status |
| 111–112 | Batch conversion completes after dialog dismissal, export suite reads completed jobs | Refresh preserves a draft and reconciles exact job IDs |
| 113–124 | Export gate checks internal/hictk/auto modes, source versus edited layout, selected resolutions, threads request, every exported contact, actual gzip level or uncompressed pixels, nonuniform positive balanced weights | Source Browse, invalid numeric input and overwrite-cancel full interaction |
| 126–137 | Sequence lifecycle creates/query-checks tracks, source replacement gates verify atomic rollback, capability/mixed suites verify secondary identities and geometry, jobs suite verifies track-precompute lifecycle | Track controls gate now passes named BED browsing/addition, exact feature values, primary/secondary weights, secondary browsing/attachment/detachment, displayed/exported background pixels on both axes and precompute completion after closing the dialog. |
| 138–148 | Pipeline gate verifies complete graph export/import, rejected unsupported node preservation, draft preview/layout persistence, server-refresh conflict and stale automatic-threshold protection | Preview, Apply, Swap, enable toggles, primary preset Load, Reload and Reset now pass exact constant-matrix and rendered-color assertions. All four editor presets now pass Load/Apply with real primary and identity sources, preserved input matrices and actual rendered pixels. |
| 149–163 | Workspace independently toggles all eleven OSD fields, order, position, visibility and persistence | Field values across every source/state beyond existing resolution-refresh scenarios |
| 164–177 | Navigation verifies all eleven native level switches, final-level guard, valid interval restriction and reversed-bound rejection, completed coarse-level rendering and restoration | Complete |
| 178–183 | Jobs and lifecycle suites exercise actual dotplot generation, missing-FASTA guidance, recovery, job completion and output delivery | Every multi-file selection and readiness-refresh path |
| 184–194 | Lifecycle/search-delivery suites compare exact source and edited assembly hits, reverse complement, gapped search, imported track provenance and FASTA outputs | Every query-mode/IUPAC/color/scope boundary through actual controls |
| 195–205 | Real alignment jobs and base-level detail cover chosen regions, substitutions, gaps, reverse, N, stale responses, edits/Undo and exported pixel landmarks | All six alignment settings now run through the actual UI. Provenance and worker allocation agree, and a scalar PAF oracle matches all 1,600 result contacts. Extreme values remain covered separately by backend validation. |
| 206–211 | ModDotPlot suite computes real identity layers, verifies scale/zoom/threshold and primary isolation, jobs suite verifies output delivery/cancel | Every identity input now runs through the UI with exact k-mer/sketch/delta/worker provenance. Invalid delta step preserves the job list. Additional identity thresholds remain covered by the existing numerical identity suites. |
| 212–218 | Metadata gate checks real backend/UI versions, exact UI commit, citation and guide URL, menu capture opens About | Complete for all three tabs and four displayed mail link targets |
| 219–225 | Workspace verifies named bookmarks and contig/scaffold Go to, rename gate verifies native AGP override and viewport preservation | Reordered/hidden entity navigation fixtures. Name export/import/reset and all 1,765 contig pagination entries now pass. |
| 226–228 | History suite checks 121 real journal states, Earlier/Later preserve assembly, exact immutable ID restore, independent AGP, Redo/Undo and monotonic branch ID | Sequence and graph references for restored nontrivial states |

The static contracts have been aligned with the updated plan and current control
semantics. Open map uses the plain picker. Unscaffold All is Undoable. Visualization
presets save visualization options, and the pipeline editor exports complete custom
graphs. The pipeline preset dropdown chooses a preset and Load preset builds its draft. Preview publishes the graph and switches the editor layout. Automatic worker
allocation uses the supported value `-1`. These corrections remove earlier audit
assumptions that had introduced extra UI behavior. The owner subsequently selected
full custom graph capture in visualization presets. That extension and its
compatibility checks remain pending.

Additional evidence pointers include
`/tmp/hict-u13-picker-complete-controls-dir`,
`/tmp/hict-u13-navigation-controls-fixed-dir`,
`/tmp/hict-u13-navigation-workspace-regression-dir`,
`/tmp/hict-u13-batch-conversion-valid-dir` and
`/tmp/hict-u13-compression-actual-dir`.
