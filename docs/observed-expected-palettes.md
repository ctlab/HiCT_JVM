# Expected and O/E palettes

Choose **Show O/E** in the ribbon to compare contacts with the viewport's
within-scaffold distance profile (within contigs when unscaffolded). The current
profile averages positive finite processed signals at each native-bin separation
in the fitting viewport. A ratio of 1 is neutral white; values below 1 use the
lower/depleted color, and values above 1 use the upper/enriched color. Undefined
ratios remain transparent.

Under **Visualization → Color range**, choose **Blue–white–red** or
**Purple–white–green** from **O/E palette**, or choose custom endpoint colors.
The limits must satisfy `0 < lower < 1 < upper`. Interpolation is logarithmic on
either side of 1: with limits 0.25 and 4, ratios 0.5 and 2 are halfway from neutral
to their respective endpoint colors. Both endpoints honor their chosen alpha.
Measured zero receives the lower color; missing expected support is transparent.

Drag a handle or focus it and use the arrow keys, Home/End or Page Up/Down.
Handles stay on the appropriate side of 1. Adjusting a handle holds the slider
limits fixed. **Use full loaded range** resumes loaded-tile limits;
**Focus range** changes adjustment precision. A source, target or display-mode
change starts a new slider domain. The rail marks neutral white at 1.

With Show O/E active, **View → Rendering pipeline…** opens the
**Primary O/E colormap** tab. It edits the active primary map even when the
sidebar is displaying Secondary. Presets, endpoint colors and thresholds are the
same palette used by the sidebar. Changes preview automatically; Apply waits for
the selected values. Newer choices replace superseded pending choices when an
update is slow. Failed or invalid changes leave the last applied values intact.

**Preview** collapses the editor so you can inspect the map. **Show editor**
returns to the controls and reloads changes made in the sidebar during Preview.
Changing the display mode during Preview returns to the corresponding editor.
**Reset** selects blue–white–red with limits 0.5 and 2.
**Export palette** asks for a destination for a small JSON settings file;
**Import palette** restores one. Reload palette returns to the committed values.
Closing with an invalid draft leaves those invalid values unapplied.

Use the arrow keys, Home or End on the **Rendering target** tabs to switch to
**Observed graph**. Switching tabs or closing an unchanged palette does not
replace that graph. The Observed graph retains its separate graph import/export,
inspection, preview, presets and range controls. Palette files and graph files
have distinct formats, so import each through its corresponding tab.

Expected and O/E remember separate colors and thresholds across mode changes.
Preferences persist in this browser when storage is available; restricted storage
retains them for the current interface lifetime. A session includes its active
view's palette and limits. Restoring an older session now uses its stored
endpoint colors, which the previous O/E renderer ignored in favor of hardcoded
blue/red. The new default is blue–white–red with limits 0.5 and 2.

An Observed graph's **Colormap** node also offers **O/E (neutral 1)** for a signal
that already represents ratios. It uses the same mapping and alpha as Show O/E,
with numeric min/max connections or literal limits. Sidebar threshold/color edits
preserve that mode and its numeric input. This node colors an existing ratio
signal; fitting the distance profile remains part of the primary O/E view.
