# Borrelia editing figure

[PDF](borrelia-editing.pdf) · [SVG](borrelia-editing.svg) ·
[preview](borrelia-editing-preview.png) · [caption and alt text](caption.md)

This real-data figure candidate demonstrates the advisor's map + ModDotPlot +
tracks, exact cut and scaffold workflow. It is separate from the read-only
manuscript. No biological correction is inferred from the deliberately selected
cut. Native matrix and reference files are not edited.

The three panels retain their captured geometry. The composition is 178 ×
179.106 mm, with measured minimum vector text 7.051 pt. The
[Bioinformatics instructions](https://academic.oup.com/bioinformatics/pages/author-guidelines)
specify 86/178 mm figure widths and require alt text (checked 2026-11-17).
The 7-point threshold is this project's readability check, not a quoted journal
rule. Main text and both axis titles/scale values were visually inspected in
browser and standalone renderings. PDF fonts are embedded and text is extractable.

The SVG contains the original contact-image bytes and full per-panel provenance.
The PDF includes matching XMP metadata and an attached `hict-provenance.json`;
adding these preserves its page drawing commands. Three source panels are kept
in [panels/](panels/). No contact image is resampled during composition.
The PNG is a review preview, not additional genomic detail. Native contact-image
sampling remains one pixel per displayed bin. Final journal artwork resolution/
TIFF requirements still need checking; a 300-dpi preview alone cannot establish
that embedded native imagery meets a photographic-resolution guideline.

## Data and rendering

- Source sample: [GSM7056007](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM7056007),
  S9 mapped to the B31 reference GCA_000008685.2_ASM868v2. Source links, terms and
  reference-header preparation are documented in [examples-catalog.md](../../examples-catalog.md).
- Contact grid: 10 kb, 155 bins, 19 mapped replicons. The converted native source
  SHA-256 is `f3f47e2f9af9eda24a4dd65e4dcef9972504b56203a6ba72b52183fc28e4932e`.
  The prepared FASTA SHA-256 is
  `ca26a9bd04a3d5cbdd9e80c67a2b2a8c5c10f7c0fc98fad4163daf4716c85b94`.
  After cutting, 156 displayed bin references include the crossing bin in both
  children; this does not introduce another source contact observation.
- Imported Cooler balancing is enabled, with eleven excluded bins at this level.
  The three panels share one pipeline and the same contact scale. Source counts
  match all 24,025 cells from the pinned public Cooler exactly. Native stored
  weights differ from that file's coefficients by at most 2.995e-13 relative;
  they pass a declared 1e-12 comparison, retain identical exclusion masks and
  produce identical displayed RGB values for all cells. They are not described
  as bitwise identical. See [counts-oracle.json](counts-oracle.json).
- ModDotPlot is the real bundled implementation, not synthetic image content.
  Its [analysis plan](moddotplot-analysis-plan.json) records source-bin anchors,
  k=21, sketch size 1,000, neighbor expansion 0.5 and minimum identity 80%.
  Scores are estimated identity, not alignments. Existing scores move with the
  edited source fragments; changing a join does not silently rerun the analysis.
- GC is `(G+C)/(A+C+G+T)` within each original-reference 10 kb window, including
  an exact final partial window. The numeric bedGraph and BigWig are retained.
  Independent Bio.SeqIO/pyBigWig reads verify all 155 windows across 19 records,
  maximum absolute discrepancy 1.610e-8. See [gc-independent-oracle.json](gc-independent-oracle.json).
- The GFF3 marker spans source Ch1 `[450123,460123)` in zero-based coordinates.
  The feature-menu cut at its left boundary yields children of 450,123 and
  460,601 bp. The 200 bases spanning the boundary match the original reference
  before and after grouping. Duplicated in-map names are hidden using the normal
  layer control; ruler names and all boundaries remain visible.

## Build identity and evidence

Captures use `2.0.59-0878f4d-webui_d27c8ac`, WebUI commit
`d27c8ac43ca6045d1195e38b97e9df369330a5f6`, JAR SHA-256
`21275d9ab1a5298665af230909d92d9d768298b000fb88a97df9e832aef51648`.
The fresh integrated build passes 641 JVM tests, with zero failures/errors and
seven optional skips. [capture-results.json](capture-results.json) records the
actual Jobs result, source hashes, named snapshots, all three export hashes,
rendering state and both-axis text bounds. [composition.json](composition.json)
records physical size, measured text, bundled font hashes, authoring-script hash
and renderer version. This browser/API development workflow is not a new portable
package or cross-platform qualification.

The fresh direct request to the NCBI FTP host timed out without receiving data.
The independent count oracle instead used the existing local cache after verifying
its exact pinned public SHA-256, `c2c5be6303c2698391022c3c5bdddb34b0d7d61f9100a9b050363f188c60f5de`.
The numerical result does not depend on a successful new network transfer.

## Reproduce

Build the JVM/WebUI pair with `./gradlew test shadowJar -PrequireBundledWebUI=true`.
Use an isolated data copy from **File → Open Examples… → Borrelia Hi-C quick
start**; the native matrix and prepared matching FASTA are the script inputs.
Preparation requires Python 3 with Playwright/Chromium and an installed UCSC
`bedGraphToBigWig`. Composition additionally uses pypdf, `rsvg-convert` and Poppler
(`pdfinfo`, `pdffonts`, `pdftotext`). Native analysis tools come from the fat JAR.

From `HiCT_JVM`, with fresh output directories:

```sh
python3 scripts/prepare_borrelia_publication_browser.py \
  --jar /path/to/current-fat.jar --webui ../HiCT_WebUI/dist \
  --matrix /path/to/borrelia.hict.hdf5 \
  --fasta /path/to/borrelia-matching.fa \
  --bedgraph-to-bigwig /path/to/bedGraphToBigWig \
  --hide-map-names --output /tmp/borrelia-captures

python3 scripts/compose_borrelia_publication_figure.py \
  --captures /tmp/borrelia-captures \
  --font-dir ../HiCT_WebUI/src/assets/fonts \
  --output /tmp/borrelia-composition
```

Fresh timestamps/session identifiers change artifact hashes. The workflow,
scientific values, source hashes, geometry and validation checks are reproducible;
the claim is not byte-identical newly timestamped files. The numerical contact
dump used [NativeMatrixDump.java](NativeMatrixDump.java) on a physical native
copy, compared against `cooler.Cooler(...).matrix(balance=False)[:]`. Its matrix
dimensions and resolution deliberately describe this dataset, not a generic tool.

Check this retained archive from its own directory:

```sh
sha256sum --check checksums.sha256
pdfinfo borrelia-editing.pdf
pdffonts borrelia-editing.pdf
pdftotext borrelia-editing.pdf -
```

The [caption](caption.md) supplies study attribution and alt text. The figure
does not add a new license claim for the underlying deposited data. Embedded
font licensing is retained in [FONT-LICENSE.txt](FONT-LICENSE.txt).
