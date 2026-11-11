# Examples catalog

`Help ▸ Examples...` lists example datasets from data, downloads them and hands them to the opening
wizard. Nothing about sections, cards or files is written into the application: it is all in JSON.

## Where catalogs live

- The bundled catalog: `src/main/resources/examples/hict-examples.json` (two DNA Zoo cards).
- Local additions: `<data folder>/examples/catalogs/*.json`, merged after the bundled one. A pipeline that
  discovers datasets should write its file there.

The shape is described by `src/main/resources/examples/hict-examples.schema.json` (JSON Schema 2020-12).
The server enforces the same rules in `ExamplesCatalog` (this build does not use a general JSON Schema
library): property names, text lengths, enumerations, `https://` addresses without credentials, plain
file names, positive sizes, `sha256` as 64 lower-case hex digits, unique ids, `sectionId` and recipe
references that exist, recipe roles that match (an `assembly` file is not an `agp`), and a 1 MiB limit.

```json
{
  "schemaVersion": 1,
  "catalogId": "my-lab",
  "sections": [{ "id": "mine", "title": "My datasets", "order": 5 }],
  "examples": [{
    "id": "ex1", "sectionId": "mine", "title": "…", "description": "what it shows",
    "publications": [{ "title": "…", "citation": "…", "url": "https://doi.org/…", "doi": "10.1000/xyz", "kind": "dataset" }],
    "files": [
      { "id": "matrix", "role": "matrix", "format": "hic", "url": "https://…/x.hic", "filename": "x.hic", "required": true, "bytes": 123, "sha256": "…" },
      { "id": "fasta",  "role": "fasta",  "format": "fasta.gz", "url": "https://…/x.fa.gz", "filename": "x.fa.gz", "required": true },
      { "id": "layout", "role": "assembly", "format": "assembly", "url": "https://…/x.assembly", "filename": "x.assembly", "required": false }
    ],
    "attribution": { "provider": "…", "url": "https://…", "termsUrl": "https://…", "acknowledgement": "text to show" },
    "openingRecipe": { "matrix": "matrix", "fasta": "fasta", "assembly": "layout", "referenceId": "x" },
    "provenance": { "origin": "pipeline", "retrieved": "2026-11-10", "reviewStatus": "draft" }
  }]
}
```

An invalid file is left out and reported with its file name and the JSON pointer of the first problem,
which the dialog shows; the rest of the catalog is unchanged. An example id defined by two catalogs is
ambiguous: the later file is not used. Local catalogs are labelled and are held to the same download rules
as the bundled one. A licence is shown only when the provider states one; nothing is invented.

## Downloads

The client sends an example id; the server resolves everything else from its own catalog.

- Addresses must be `https`, with no credentials, on the standard port, and resolve only to public
  addresses (not loopback, private, link-local, carrier-grade NAT, multicast). Each redirect (five at most,
  followed by hand) is checked again. A DNS answer that changes between the check and the connection is
  not pinned: the check is repeated per request, not per connection.
- Files go to `<data>/examples/<example id>/<filename>` via `<filename>.part`. The final path is checked to be
  inside the data folder, a file that fails its length or `sha256` is discarded, and the move into place is
  atomic. A download is complete only when the file is verified, not merely present.
- A partial file resumes with `Range` and `If-Range` (ETag or Last-Modified). A `206` that does not start
  where asked is an error; a `200` or `416` restarts from zero; a changed object restarts safely.
- Free space is checked before writing (the file plus a 512 MiB margin), and a size above the catalog's
  `bytes` is refused. Unknown lengths are accepted when the catalog gives no size.
- Required files must succeed. Optional files may fail and are listed as skipped.
- On success a `manifest.json` next to the files records the file hashes, the opening recipe, the
  publications and the attribution. Nothing downloaded is ever executed.
- Download completion is separate from opening: the card shows `Downloaded` and **Open in HiCT** starts the
  wizard with the recipe's files chosen. The wizard still converts a `.hic` first. Opening asks about
  unsaved edits as usual.

## API

`POST /examples/catalog`, `/examples/download/start` (`exampleId`), `/examples/download/status`,
`/examples/download/cancel`. One download at a time; starting the same example again joins it.

## Verified catalog examples

All three bundled cards passed download, conversion, readable main-map rendering
and linked FASTA reads in fresh Chromium contexts on 2026-11-11. Their manifests
record source SHA-256 values. The quick-start card opens directly after preparation.
The larger cards retain the wizard for conversion options.

| Example | Download seconds | Conversion and open seconds | Peak sampled Java heap | Final data plus FASTA index |
|---|---:|---:|---:|---:|
| Tardigrade | 6.98 | 13.29 | Run with a 2 GiB heap limit | 45.3 MB source downloads plus generated matrix and index |
| Cheetah | 177.21 | 664.72 | 2,588,244,992 bytes | 5,924,039,787 bytes |
| Aedes aegypti | 238.13 | 348.48 | 2,076,669,952 bytes | 5,046,898,838 bytes |

Tardigrade reached its readable map in 20.27 seconds with three clicks, through
Help, Examples and Open example. Its five mapped chromosomes match the FASTA
names and lengths. The reference also includes unplaced scaffolds. Cheetah opens
19 scaffold references and Aedes opens three chromosomes, each at 11 levels
from 5 kb through 10 Mb. Both returned 1,000 linked sequence bases without
reference warnings. Their original draft-component `.assembly` files remain
optional provenance downloads. Their opening recipes use the already-scaffolded
matrix and FASTA directly.

Cheetah's qualified conversion reused the completed 177.21-second download.
An earlier conversion trial was interrupted by replacing its running JAR and was
discarded. Qualified runs used isolated JAR and frontend copies. The large-map
heap was sampled every three seconds. Data and native conversion scratch were
sampled every five seconds, reaching 4,795,293,696 bytes for Cheetah and
4,292,390,912 bytes for Aedes. The final-data column includes allocated data
blocks and the private, uncompressed FASTA index. Sampled values can miss brief
peaks. Portable launchers reserve 50% of available RAM for Java by default.
An explicit `-Xmx` in `HICT_JAVA_OPTS` takes precedence.

Evidence resides under `/tmp/hict-u11-dnazoo-cheetah-ready/evidence`,
`/tmp/hict-u11-dnazoo-aedes-ready/evidence` and the directory named by
`/tmp/hict-u11-quick-qualified-dir`. These disposable local files accompany
this validation session. `scripts/check_quickstart_example_browser.py` reproduces
the empty-data quick-start gate against a running server.

## Download progress

Downloads continue while the Examples dialog is closed. The navbar Jobs panel
shows progress, cancellation and finished results. Reopening Examples also
restores the download status. Species photos are neither bundled nor linked.

## Quick-start candidates checked on 2026-11-11

The candidates were presented before adding a card, as U11 requests. Tardigrade
was selected for its small eukaryotic map and directly matching reference.
The following files were downloaded and inspected on disposable data. Sizes below are exact bytes.
The candidates have different genome scopes and reference preparation needs.

| Candidate | Matrix bytes | Reference bytes | Coordinate check |
|---|---:|---:|---|
| Borrelia Hi-C, GSM7056007 | 1,907,128 | 406,111 downloaded, 393,716 after preparation | All 19 mapped replicon lengths match the declared B31 reference. Select those records and give them the matrix's biological replicon names. The original nucleotide sequences are retained. |
| Yeast Micro-C, GSM9302472 | 166,935,036 | 3,820,548 | All 17 chromosome names and lengths match the UCSC sacCer3 FASTA directly. |
| DNA Zoo tardigrade, nHd_3.1_HiC | 12,554,347 | 32,696,429 | All five mapped chromosomes match the FASTA exactly in name and length. The FASTA additionally contains unplaced scaffolds absent from this map. |

Borrelia uses the S9 strain mapped against GCA_000008685.2_ASM868v2, as declared
by the deposit. The NCBI FASTA has accession headers and three additional plasmids.
The checked reference preparation selects the 19 matrix replicons, orders them
like the matrix and changes only their headers. At 1 kb through 10 kb, conversion
took 1.25 seconds and produced a 2,671,246-byte HiCT file. Chromium rendered the
map and a sequence read returned the expected first 1,000 bases. Cite Ren,
Takacs, Brandão, Jacobs-Wagner and Wang, PLOS Genetics 19, e1010857 (2023).
[Study and data availability](https://doi.org/10.1371/journal.pgen.1010857),
[GEO record](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM7056007),
[matrix](https://ftp.ncbi.nlm.nih.gov/geo/samples/GSM7056nnn/GSM7056007/suppl/GSM7056007_HiC_CJW_Bb285_rep1_mapq_30.mcool),
[original reference](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/008/685/GCA_000008685.2_ASM868v2/GCA_000008685.2_ASM868v2_genomic.fna.gz).

The yeast sample is WT biological replicate 3 from GSE310515. Its matrix contains
50, 100, 200, 400 and 500 bp levels. The tested import retained the 50 bp base and
added 1, 2, 5, 10 and 25 kb levels. Conversion took 43.47 seconds and produced a
217,241,391-byte HiCT file. Chromium rendered the map and returned linked sequence.
The study is *Cohesin acetylation and ATPase activity control cohesion and loop
architecture through distinct mechanisms*. The GEO record identifies the study
and submitters. The UCSC download description identifies its reference source
and public-use terms.
[GEO record](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM9302472),
[matrix](https://ftp.ncbi.nlm.nih.gov/geo/samples/GSM9302nnn/GSM9302472/suppl/GSM9302472_WT3.mcool),
[reference](https://hgdownload.soe.ucsc.edu/goldenPath/sacCer3/bigZips/sacCer3.fa.gz),
[reference description](https://hgdownload.soe.ucsc.edu/goldenPath/sacCer3/bigZips/).

NCBI places no restrictions on use or distribution of molecular database data
and describes possible submitter rights in its data-use policy. The Borrelia
article is distributed under Creative Commons Attribution. The accession,
reference version, source URLs and study citation accompany any prepared example.
[NCBI molecular data policy](https://www.ncbi.nlm.nih.gov/home/about/policies/),
[Borrelia article terms](https://journals.plos.org/plosgenetics/article?id=10.1371/journal.pgen.1010857).

The tardigrade reference is DNA Zoo's chromosome-length nHd_3.1_HiC assembly,
based on GCA_002082055.1. Its draft reference is Yoshida et al., PLOS Biology 15,
e2002266 (2017). DNA Zoo requests source attribution, its assembly-method citation
and the specified acknowledgement, with any draft-provider conditions retained.
[Dataset](https://www.dnazoo.org/assemblies/hypsibius_dujardini),
[matrix](https://dnazoo.s3.wasabisys.com/Hypsibius_dujardini/nHd_3.1_HiC.hic),
[reference](https://dnazoo.s3.wasabisys.com/Hypsibius_dujardini/nHd_3.1_HiC.fasta.gz),
[metadata](https://dnazoo.s3.wasabisys.com/Hypsibius_dujardini/README.json),
[DNA Zoo usage terms](https://www.dnazoo.org/usage).

The search also covered the 4DN portal. A suitably small, matched 4DN pair has
yet to be verified. An earlier 17,737,657-byte yeast Hi-C candidate, GSM4946272,
was excluded from these ready candidates because its 6,318 bp 2MICRON plasmid
is absent from the checked sacCer3 FASTA.

| File | SHA-256 |
|---|---|
| GSM7056007 matrix | `c2c5be6303c2698391022c3c5bdddb34b0d7d61f9100a9b050363f188c60f5de` |
| Original Borrelia reference | `888961e50c4ff32e0a641853eb9cbd498a33736d9b568c364b50632dc6b5c93c` |
| Prepared Borrelia FASTA, uncompressed | `ca26a9bd04a3d5cbdd9e80c67a2b2a8c5c10f7c0fc98fad4163daf4716c85b94` |
| GSM9302472 matrix | `11a8e588694cfd686958ab23576aa3030dcf69f0c868d760082b34fc5d918a4b` |
| UCSC sacCer3 FASTA | `e3a70396853eff5a012077efbd06cb97d858e12349745d102e316bb8f620f266` |
