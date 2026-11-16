# Tracks and map sources

Open **View → Tracks and layers…**. The manager has two tabs. Use the arrow keys,
Home or End to switch tabs after focusing a tab heading.

**1D tracks** contains annotations and signals drawn along the map axes. Choose
**File** under **Add track**, browse for BED, VCF, GFF/GTF, BigWig or BAM, optionally
set a display name, then choose **Add file**. Each track has visibility, horizontal
and vertical axis buttons, color, name, order and Remove controls. Expand
**Signal settings** for signal ranges, log scale and format-specific options.
Changes to a field are committed when you leave it. Background refreshes preserve
the value while you are typing.

Choose **Map weights** under **Add track** to plot source-bin weights.
Select the primary or an attached secondary map, then choose **Add weights**.
An existing weights track is managed through the same list. With file weights selected, sources known to lack imported weights show a reason;
sources with unknown provenance retain the legacy option.
The track follows the weight source: stored Cooler weights, or computed
square-root coverage weights when that mode is selected. Its hint identifies the
active source. Matrix normalization is configured in **Visualization → Normalization**.
[Square-root coverage](coverage-normalization.md) explains row sums, the scale
factor and the treatment of shared bins after exact cuts.

**Sources and layers** contains secondary-map attachment and the assembly source
selector. Changing the assembly source starts a new edit journal. Earlier undo
and redo states are lost, so save a session first if you need that state later.
**Borders and labels…** opens and focuses the existing sidebar controls.

Drag the edge of either track panel to resize it. You can also focus its separator
and use the corresponding arrow keys in 10-pixel steps, Home for the minimum or
End for the largest allowed size. Panel sizes are constrained when the workspace
shrinks. Track appearance is under **Track appearance**. Optional advance
preparation is under **Preparation**; ongoing work also appears in **Jobs**.
Map weights are read directly and need no preparation.

On narrow windows, file badges wrap onto another row. Hover a truncated badge to
read its full filename. Menus stay within the window and scroll when necessary.
Below 700 px, the side panel sits below the map; scroll the workspace to reach
it. The collapse button points up/down there. HiCT remembers your explicit
choice to show or hide the panel across window changes and restarts.
