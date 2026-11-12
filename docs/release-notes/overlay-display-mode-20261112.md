# Overlay display-mode and session-save corrections

The Observed/Expected selector previously sent the shared sidebar's current
options through the standard renderer update. With a secondary map selected,
those options could belong to the secondary identity palette. The update also
replaced the custom rendering graph, dropping the overlay permanently.

The selector now reads primary options and the primary graph palette, preserves
the complete rendering graph, and uses the standard primary renderer for Expected
and O/E. Secondary source tiles retain their original graph and identity scaling.
Returning to Observed uses the original graph, including its colors, processing,
blend settings and triangle assignments. The selected sidebar source is hydrated
after the switch. The selector is disabled while its request is pending, and a
rejected update reads back the committed mode. Threshold handles use the displayed
signal domain to prevent a zero-width drag from creating a `5e-324` interval.

Session validation previously counted all scalar and container values across
both assemblies against a 100,000-value budget. A captured real session with
1,765 contigs per source contained 109,640 values in approximately 2 MiB and was
rejected while walking a secondary scaffold border. The coordinate was valid.
Both client and server now allow 1,000,000 values. The 8 MiB document limit,
160-level nesting limit, graph validation, file fingerprints and assembly checks
remain enforced. The error distinguishes excessive nesting from excessive values.

Validation:

- Full JVM suite: 471 tests, zero failures/errors, seven optional skips.
- The captured real snapshot decodes unchanged with both 1,765-contig assemblies.
- Multi-resolution dual-source decoder and client-parser regression fixtures.
- WebUI production build, type checks and bundled regression checks.
- `scripts/check_display_mode_roundtrip_browser.py` exercises split and blended
  identity graphs, Expected/O/E/Observed transitions, unchanged secondary tile
  pixels, exact restored primary tile pixels, session save/close/restore while O/E
  is active, sidebar thresholds and rejected requests on an isolated server.

These fixes require rebuilding both JVM and WebUI. They do not modify an existing
running application's graph or restart its server. An overlay graph already
replaced by the old selector must be reapplied from its preset or a saved graph;
the attached generated matrix itself remains available.
