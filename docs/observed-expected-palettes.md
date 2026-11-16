# Expected and O/E palettes

Choose **Show O/E** in the ribbon to compare contacts with the viewport's
within-scaffold distance profile (within contigs when unscaffolded). The current
profile averages positive finite processed signals at each native-bin separation
in the fitting viewport. A ratio of 1
is neutral white; values below 1 use the lower/depleted color, and values above 1
use the upper/enriched color. Undefined ratios remain transparent.

Under **Visualization → Color range**, choose the two colors and their lower and
upper ratio thresholds. The limits must satisfy `0 < lower < 1 < upper`.
Interpolation is logarithmic on either side of 1. For example, limits 0.25 and 4
make 0.5 and 2 halfway from neutral to their respective endpoint colors. Both
endpoints honor their chosen alpha. Zero is measured depletion, so it receives
the lower color; missing expected support stays transparent.

Drag a handle or focus it and use the arrow keys, Home/End or Page Up/Down.
Handles keep the limits on the appropriate side of 1. Adjusting a handle holds
the slider limits fixed so its domain cannot shrink under the pointer.
**Use full loaded range** resumes the loaded-tile limits; **Focus range** changes
adjustment precision. Changing the source, target or display mode starts a new
slider domain. The O/E rail marks neutral white at 1.

Expected and O/E remember their own colors and thresholds. Switching to Observed
preserves the existing observed graph and its palette. Preferences persist in
this browser when storage is available; restricted storage retains them for the
current interface lifetime. A saved session includes the active view's palette
and limits. Restoring it uses its stored colors, including in older sessions
whose O/E colors were formerly ignored in favor of hardcoded blue/red. The new
O/E default is blue–white–red with limits 0.5 and 2.

The rendering editor's **Colormap** node also offers **O/E (neutral 1)**. Supply a
signal that already represents ratios. Its `OBSERVED_OVER_EXPECTED` palette mode
uses the same mapping and endpoint alpha as Show O/E, with numeric min/max
connections or literal limits. Sidebar threshold/color edits preserve that mode
and its numeric input. This node colors ratios; it does not fit a distance profile.

At this implementation stage, that custom graph still controls the Observed
view. Show Expected/O/E retains separate primary-map processing. Editing the
active Show O/E palette directly through a projected pipeline graph remains part
of the pending P6 integration, along with named O/E palette presets; the separate
graph palette is its shared rendering
prerequisite. The inline viewport histogram and levels/midpoint controls are also
pending. This guide does not imply final publication qualification.
