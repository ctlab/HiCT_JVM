# Sequence result delivery, presentation and feature interaction

Closing a running ModDotPlot, assembly dotplot or sequence-search dialog no longer
removes the only completion watcher. The navbar's background-job polling delivers
finished results to the viewer that submitted them, normally within its next
poll (up to approximately three seconds while Jobs is closed). Closing a dialog
continues the calculation; its separate Cancel action still cancels it. Replacing
or closing the viewer prevents its deferred delivery. Failed/cancelled jobs are
not displayed automatically; saved outputs remain available through Jobs.

Overlay jobs retain their requested presentation in their request/provenance and
in the common Jobs output. “Open result” honors that preference. Selections
default to Overlay; whole-assembly generation defaults to below diagonal. Above
diagonal is also available. The sidebar's **Map layout** selector switches
between Overlay, Secondary below diagonal and Secondary above diagonal while
retaining the source processing branches and their color settings.

Both active-assembly generators preselect every stored resolution from the
finest through the current level. The FASTA-file dotplot dialog receives the same
levels when opened from an active map, initializing its base and zoom settings.
Without an active map it retains automatic pyramid construction. Above-diagonal
sequence actions are also accepted in imported presets and sessions.

Feature hover, left-click selection and context menus now use the packed lanes
actually drawn on each axis, with exclusive right/bottom bounds. A click on one
overlapping BED feature no longer chooses the first genomic interval in another
lane. Density summary bands do not pretend to identify an individual feature.
Interactive and export hover geometry share the same rendering output.

The feature context menu adds **Zoom to feature**, enabled with linked FASTA.
It selects the clicked feature and centers its midpoint at a scale based on the
measured sequence font, making bases readable on both FASTA axes.

Validation:

- JVM suite: 473 tests, zero failures/errors, seven optional skips.
- Production WebUI build/type checks and regression scripts pass. Targeted ESLint
  reports zero errors (12 existing warnings).
- `check-background-result-delivery.mjs` covers at-most-once delivery, disposal,
  cancellation/failure isolation and resolution defaults.
- `scripts/check_sequence_result_feature_ux_browser.py` uses a copied real contact
  map/reference, real ModDotPlot and search jobs, and six overlapping BED features.
  Both dialogs are closed while jobs run. The identity overlay appears without
  opening Jobs; the 28-hit search track appears automatically. Manual opening
  retains Overlay. All three layouts retain both source branches. All three
  generator dialogs have the expected default resolutions. All six lanes select
  the correct feature on both axes. Context-menu zoom centers the selected feature
  at approximately 20.29 CSS pixels per base with visible FASTA letters.

Automatic delivery is local to the submitting browser/viewer. After a page reload,
use Jobs to open retained outputs; the saved presentation preference still applies.
Both JVM and WebUI changes must be included when building the application.
