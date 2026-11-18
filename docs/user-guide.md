# HiCT user guide

## Open a map

Choose **Open map…** and double-click a `.hict.hdf5` file, or select it and press
Enter. The picker lists files inside the server's data directory. Use
**Open with options…** for `.hic`, `.cool` and `.mcool` conversion and optional
inputs. **File → Open Examples… → Open example** prepares Borrelia Hi-C, yeast Micro-C
or tardigrade with matching FASTA. Borrelia downloads about 2.31 MB and yeast
about 170.76 MB. The Borrelia preparation selects its 19 mapped replicons and
changes their headers while preserving every original base. Downloads need
internet access. Completed local examples remain usable offline.

Opening fits the initial color range to the visible signal. Excluded weighted
bins are masked. The one-line note offers Change when you want another policy.
A second browser can attach to the server's current map. Changes share one server
assembly and save-conflict protection applies.
The active Hi-C map, linked FASTA, and loaded AGP appear beside **Jobs**.

## Navigate and select

Drag to pan and use the wheel or zoom buttons to change scale. The ruler-corner
lock anchors the map to its top and left boundaries. Unlock it for free panning.
Hold Shift and click or drag to select entities for assembly editing. The
temporary drag box disappears on release; selected entity borders stay
highlighted. **Selection** chooses whether Shift selects contigs or scaffolds.
Hold Ctrl (Command on macOS) and drag a persistent region for dot plots, sequence
tools, histograms and exports. A region does not select entities for editing.
Click outside the region, use **Selection → Clear selection**, or press Escape
to clear it. Escape first closes an open dialog or cancels an armed edit tool.

**Selection → Select region by coordinates…** accepts independent row/column
intervals as assembly positions or `current-contig:position`. The default is
one-based, inclusive, matching copied coordinates. BED-style zero-based,
half-open coordinates are also available. Entry is validated against the current
row/column scopes. The selection's BP endpoints stay exact when changing display
scale. When both axes are narrowed, **Selection → Hide borders in scoped view**
removes ordinary border clutter; selected entity borders remain highlighted.

Use **Selection → Copy selection coordinates** for assembly positions you can
paste into notes or a spreadsheet. Positions are one-based and inclusive.
For a rectangle, the copied text gives the row and column bounds separately.
Use **Selection → Copy selection as PNG** to paste the selected rectangle into
another application. Both actions are also available by right-clicking inside
the rectangle. If your browser blocks image clipboard writes, choose
**Export selection as PNG** from that context menu to save a file.

The search box locates
contigs, scaffolds and genomic coordinates. Row and column scope selectors limit
the displayed intervals independently. Ruler modes show global, contig-relative
or scaffold-relative coordinates. Hover reveals exact positions and hidden spans.

## Edit the assembly

Use the left toolbar or the selection's context menu to reverse, move, split,
group, ungroup, rename or send sequence to debris. Move displays insertion targets.
Split stays armed until a double-click chooses the cut position or Escape cancels
it. On current servers, a split retains every base and all original contact bins;
children share a source bin if the cut crosses it. GTF/GFF feature menus also
offer exact cuts at their displayed start/end boundaries. Existing joins are
disabled. Older servers explicitly warn when using their historical bin-removing
split operation.
The Journal records edits and provides Undo and Redo. A new edit after Undo
starts a new history branch. Text-field Undo operates on the text being edited.

Save session through File to retain the edited assembly, input references,
tracks, scopes, visualization and view. Sessions preserve split lineage. Restore
validates referenced files before replacing the current map. Keep those files
available. Undo history belongs to the live session. Export AGP to preserve an
assembly layout and export FASTA to materialize the curated sequence.

Where supported, saving opens a destination chooser. Cancelling or failing to
write leaves your edits unsaved. If you make more edits while a session is being
written, save again to include them. The success notification offers **Open saved
session**; **File → Open session** also restores a file you choose.
Browsers without a writable-file picker use their download settings. Enable
“Ask where to save” there to choose each destination. HiCT reports **Download
started** and keeps edits marked unsaved because it cannot confirm completion.
Check the downloaded session before choosing Discard edits to close the map.

Map and FASTA input choosers remember their last selected folder/file separately
for each server. A remembered file is highlighted when it still exists; click
Load to confirm it. These inputs are on the HiCT server, whereas browser session
files and downloads are on your computer. Use **File → Open Examples** to open
the existing example chooser. See [file-destinations.md](file-destinations.md)
for supported save behavior and remaining platform qualification.

FASTA and AGP exports stream directly to writable browser destinations. The
Exporting notice offers Cancel while preparing or transferring the file, then
shows the result. A cancelled or failed transfer does not confirm a saved file.
The current Electron desktop window also streams FASTA/AGP to the native
destination you choose. Cancelling its chooser starts no transfer. Navigation
cancels an active transfer, and closing the last window waits for partial-file
cleanup. Older desktop engines, Tauri and download-only browsers still use a
buffered path; their large-file memory qualification remains pending.

## Signal and layers

Use the Visualization sidebar to change the color range, signal transform and
normalization, then Apply. Slider scale changes the slider's response. Signal
transform changes the displayed values. Square-root coverage uses a single
whole-map domain at each resolution. Bins without contacts remain empty.
Show O/E uses a diverging log-ratio range centered at one.

Manage opens preset management. Save captures the applied visualization options,
both rendering branches, enable and swap flags, background and border styles.
Matching editor layouts and disconnected inspectors are retained. Apply pending
changes before saving a preset. Selecting a saved preset restores the complete
visualization. Export and Import transfer the library as JSON, and native session
files retain it. Older option-only preset files remain supported. A missing
secondary source or track produces guidance before the preset is applied.

The opening wizard labels complete presets as source style when using their
options to construct its selected presentation. The Rendering pipeline editor
supports custom signal expressions and complete graph export. A custom graph
remains explicitly identified when ordinary sidebar controls cannot rewrite it.

Tracks and layers manages annotations, signal tracks and secondary matrices.
Compatible matrix references may have different resolution pyramids. Each layer
chooses its nearest usable level while the primary assembly grid supplies
coordinates. Reorder tracks, hide either axis, or remove a track in that dialog.

## Sequence and analysis

Link a matching FASTA through File or the guidance in a sequence-dependent tool.
At letter zoom, sequence axes and local alignment detail follow the assembly.
Continuous lines span the aligned letter intersections: green forward matches,
purple reverse matches and orange substitutions. Dashed gray lines mark ambiguous
bases and horizontal/vertical indels. Alignment detail is bounded to the visible intervals and reports an empty result
when no alignment is found. **Open FASTA only…** creates an assembly without
contact signal. Its sequence and structural tools remain available.
Tiny windows use shorter seeds so short repeats remain visible. These are local
sequence comparisons; a short match alone does not establish homology. View →
Automatically align visible FASTA turns this display off/on. See
[the alignment method and limits](alignment-preview.md).

On a FASTA strip, Ctrl/Command-drag across letters to measure an interval. You
can also right-click and choose **Drag to select bases**; choose **Drag to pan**
to return to navigation. Both endpoints include their letters. The small
measurement card gives the exact base count and assembly coordinates, and copies
the selected sequence. Selecting the reverse-strand row copies its reverse
complement in reading direction. Shift-arrows extend a selection within the
visible bases. Escape cancels an unfinished drag or clears the measurement.

**Estimate period** runs an ungapped autocorrelation job for up to 4096 bases.
The card and Jobs show its result. Ambiguous bases never count as matching
evidence. An imperfect candidate is exploratory; check it with a dot plot,
especially when insertions/deletions may be present. Expand **How the estimate
works** for the thresholds. Counts and coordinates use the gap-free assembly;
configured scaffold separators are not inserted into this measurement. Edits or
FASTA changes clear it, and transient measurements are not saved in sessions.
See [the method and limitations](repeat-measurement.md).

Tools offers sequence search, alignment dotplots and ModDotPlot identity maps.
Exact and substitution searches use SeqKit. BLAST uses the bundled BLAST+ tools.
**Tools → Search NCBI databases…** offers an explicit remote search without a
linked FASTA or contact map. It requires consent to upload the pasted queries and
keeps external subject coordinates in a separate report. See
[remote BLAST search and limits](remote-blast.md).
The job panel keeps progress, cancellation, errors and result actions available
when an analysis dialog is closed. Results retain their originating reference.
Stopping HiCT cancels work and native children. A later launch starts with fresh
job state. Cancelled downloads retain partial files for an explicit retry.

## Export

**Print size** offers the captured size or 86/178 mm column widths. The physical
size is retained in SVG, PDF and PNG; height follows the figure's aspect ratio.
The displayed DPI describes captured pixels, which are preserved without adding
image detail. Check labels at print size; SVG/PDF retain vector labels.

**Export** above the map opens a figure dialog and captures a preview. Choose
the viewport, whole map or selected rectangle, and whether to include rulers/FASTA
and 1D tracks. After changing options, choose **Refresh capture**. PNG, SVG and PDF
use the same frozen capture and each asks for a save destination. A concurrent
assembly/source/rendering change rejects the capture with a refresh message.
Closing during capture leaves the current workspace available.

Visible ModDotPlot split/overlay layers and letter-scale alignment detail are
included. Arrange the desired layers before opening Export. Figure metadata
records source units, assembly identity, rendering settings, base bounds for a
selection, rendered grid and crop geometry. File fingerprints identify their
algorithm; ordinary captures use quick fingerprints, not a fresh full-file hash.
See [figure capture and provenance](figure-capture.md). Check the final layout
and sequence against your analysis requirements. Assembly exports remain under
Assembly; FASTA export requires a linked reference.

To keep a before/after panel, enter a **Snapshot name** and choose **Save
snapshot**. After cutting, reversing or scaffolding, capture again and open
**Snapshots**. Select a saved name to view/export it, or **Compare to snapshot**
to pair it with the current capture. Choose panel labels and a side-by-side or
vertical layout, then **Update comparison**. Removing a snapshot offers Undo.
Save the session to keep the library; unsaved snapshot changes are protected
when closing or replacing the map. Sessions with figures require a version of
HiCT that supports session v5. The file limit is 32 MiB, including up to eight
snapshots with a combined 24 MiB storage budget. Reference datasets remain
separate files. See the figure guide for image and decoding limits.

**File → Open exported figure…** opens a read-only PNG/SVG viewer, including
when no map is open. Choose a file, use Fit or Actual size, and zoom with the
plus/minus buttons. Large previews scroll. Opening, cancelling or closing a
figure does not replace the assembly. Supported browser pickers remember the
figure folder; other environments use their standard file chooser. Previews
accept non-empty files up to 64 MiB and images up to 128 megapixels. PDF remains
an export format; this image viewer accepts PNG and SVG.

File → Export matrix writes `.cool` or multiresolution `.mcool` files. Choose
whether to export the current edited assembly or the saved source layout.
Cooler export supports Deflate, with level 0 producing uncompressed output.
Auto and hictk-assisted modes can balance output. The direct internal exporter
leaves balancing unavailable.

## Keyboard and pointer actions

| Action | Input |
|---|---|
| Pan | Drag on the map |
| Zoom | Mouse wheel or zoom buttons |
| Select contigs/scaffolds | Shift-drag |
| Keep a rectangle for sequence/dotplot analysis | Ctrl-drag |
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
