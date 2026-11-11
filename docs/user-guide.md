# HiCT user guide

## Open a map

Choose **Open map…** and double-click a `.hict.hdf5` file, or select it and press
Enter. The picker lists files inside the server's data directory. Use
**Open with options…** for `.hic`, `.cool` and `.mcool` conversion and optional
inputs. **Help → Examples… → Open example** prepares the small tardigrade map
with its matching FASTA. Its download needs internet access. Completed local
examples remain usable offline.

Opening fits the initial color range to the visible signal. Excluded weighted
bins are masked. The one-line note offers Change when you want another policy.
A second browser can attach to the server's current map. Changes share one server
assembly and save-conflict protection applies.

## Navigate and select

Drag to pan and use the wheel or zoom buttons to change scale. The ruler-corner
lock anchors the map to its top and left boundaries. Unlock it for free panning.
Click a contig, or hold Shift and drag a range to select. The search box locates
contigs, scaffolds and genomic coordinates. Row and column scope selectors limit
the displayed intervals independently. Ruler modes show global, contig-relative
or scaffold-relative coordinates. Hover reveals exact positions and hidden spans.

## Edit the assembly

Use the left toolbar or the selection's context menu to reverse, move, split,
group, ungroup, rename or send sequence to debris. Move displays insertion targets.
Split stays armed until a double-click chooses the cut position or Escape cancels
it. A split removes one finest-resolution contact bin, with Undo restoring it.
The Journal records edits and provides Undo and Redo. A new edit after Undo
starts a new history branch. Text-field Undo operates on the text being edited.

Save session through File to retain the edited assembly, input references,
tracks, scopes, visualization and view. Sessions preserve split lineage. Restore
validates referenced files before replacing the current map. Keep those files
available. Undo history belongs to the live session. Export AGP to preserve an
assembly layout and export FASTA to materialize the curated sequence.

## Signal and layers

Use the Visualization sidebar to change the color range, signal transform and
normalization, then Apply. Slider scale changes the slider's response. Signal
transform changes the displayed values. Square-root coverage uses a single
whole-map domain at each resolution. Bins without contacts remain empty.
Show O/E uses a diverging log-ratio range centered at one.

Presets store reusable settings. Manage opens preset management. The Rendering
pipeline editor supports custom signal expressions. A custom graph remains
explicitly identified when ordinary sidebar controls cannot rewrite it.

Tracks and layers manages annotations, signal tracks and secondary matrices.
Compatible matrix references may have different resolution pyramids. Each layer
chooses its nearest usable level while the primary assembly grid supplies
coordinates. Reorder tracks, hide either axis, or remove a track in that dialog.

## Sequence and analysis

Link a matching FASTA through File or the guidance in a sequence-dependent tool.
At letter zoom, sequence axes and exact alignment detail follow the assembly.
Alignment detail is bounded to the visible intervals and reports an empty result
when no alignment is found. **Open FASTA only…** creates an assembly without
contact signal. Its sequence and structural tools remain available.

Tools offers sequence search, alignment dotplots and ModDotPlot identity maps.
Exact and substitution searches use SeqKit. BLAST uses the bundled BLAST+ tools.
The job panel keeps progress, cancellation, errors and result actions available
when an analysis dialog is closed. Results retain their originating reference.
Stopping HiCT cancels work and native children. A later launch starts with fresh
job state. Cancelled downloads retain partial files for an explicit retry.

## Export

Choose PNG, SVG or PDF through Export above the map. Select the visible viewport
or a fitted whole map. Visible letter-scale alignment detail is included in all
three formats. Whole-map exports use their fitted scale. Assembly exports are
available through Assembly. FASTA export requires a linked sequence reference.
Check the exported layout and sequence against your analysis requirements.

## Keyboard and pointer actions

| Action | Input |
|---|---|
| Pan | Drag on the map |
| Zoom | Mouse wheel or zoom buttons |
| Select range | Shift-drag |
| Open selected file | Enter or double-click |
| Arm Split and choose cut | Split button, then double-click on the map |
| Cancel armed Split or Move | Escape |
| Assembly Undo | Ctrl-Z or Command-Z with focus outside text fields and dialogs |
| Assembly Redo | Ctrl-Shift-Z, Command-Shift-Z, Ctrl-Y or Command-Y |
| Move through search and scope suggestions | Up and Down arrows, Enter to accept |
| Navigate modal controls | Tab and Shift-Tab |
| Activate focused button | Enter or Space |

## Recover from a failure

An action failure shows one message with Details and Copy diagnostics. Notifications
retains background messages. Job failures remain in Jobs. A failed session save
keeps the assembly marked unsaved. Before opening another map or closing an edited
one, respond to the unsaved-edits dialog. Keep a copy of your source data.
