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

## Seed cards and what has been run

The two DNA Zoo cards (Cheetah `aciJub1_HiC`, Aedes aegypti `AaegL5.0`) use object addresses and sizes
checked with HEAD requests on 2026-11-10 (200, `Accept-Ranges`, ETag). The catalog marks them `draft`:
**the full downloads and the opening of these two examples were not run**, so coordinate agreement between
each matrix and its FASTA is not verified. What was run: a real https download of the 71,567-byte Aedes
`.assembly` file through the production URL policy, a started download of the large Aedes `.hic` that was
cancelled and then resumed from the partial file (22.8 MB to 45.9 MB), and the wizard receiving the file.

## Limits

There is no persistent Background jobs panel yet: closing the dialog does not stop a download, and reopening
the dialog picks it up again. The download progress is shown only in the Examples dialog. Species photos are
neither bundled nor linked.
