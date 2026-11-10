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
