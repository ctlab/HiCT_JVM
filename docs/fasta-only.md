# Opening a FASTA without a contact map

`File ▸ Open FASTA only...` makes an assembly from a FASTA file alone. There are no contacts and
no synthetic ones: the map is the contig layout. You can navigate, read bases, add tracks, move,
reverse, split and group contigs, undo, save a session and export, exactly as on a map with
data. Anything that needs contacts (colors, thresholds, normalization, scaling across zoom) says so
and is disabled.

## What is built

- **One contig per FASTA record.** The name is the text up to the first whitespace and must be
  unique (a repeated name is refused with both line numbers, because sequence lookup is by name).
  Plain and gzip files are read as a stream, so a few hundred megabases take seconds and little
  memory.
- **Never a zero-length contig.** An empty record and a record that is nothing but N are skipped and
  listed in the warnings. A file with no sequence at all is refused.
- **The file** is `fasta-only/<name>.whole.hict.hdf5` (or `.split<N>`) in the data folder, written to
  a temporary name and moved into place only when complete. It has the normal contig, scaffold and
  resolution structure, an empty contact store and the declared source `SEQUENCE_ONLY` with
  `contact_data_state = ABSENT`. Opening the same FASTA and settings again reuses it while it is
  newer than the FASTA. Resolutions are a short pyramid (the finest keeps the empty map under five
  million bins, then 10x steps).

## Splitting at runs of N

`Split at runs of N` with `Minimum consecutive N (bp)` (default 100, 1 to 1,000,000; both `N` and `n`
count) cuts a record wherever a run is at least that long.

- Pieces keep their original positions: a contig named `scafA:3151-5710` is bases 3151 to 5710 of
  `scafA`. Shorter runs stay inside a piece. A run at the start or end of a record is dropped (the
  piece simply begins or ends later) and is the only sequence missing from an export.
- The split is described as an AGP (`fasta-only/<name>.agp`, one object per record, components in
  order, a gap line of the true length wherever a run was removed) and applied through the normal AGP
  import. Each original record becomes a scaffold, so exporting the assembly writes the record back
  with its gaps at their real lengths. Checked byte for byte: an exported scaffold that was split in
  three equals the input record, including the lower-case `n` run kept inside a piece.
- The AGP gaps use the `contig` type with no linkage. If no run is long enough, nothing is split and
  the dialog says so.

## API

`POST /fasta_only/start` (`fastaFilename`, `splitAtN`, `minNRun`), `/fasta_only/status` and
`/fasta_only/cancel`. The job reports progress, can be cancelled and leaves no finished-looking
output when cancelled or failed. Paths are confined to the data folder.

## Limits

One FASTA is read at a time. The dialog is a separate entry from the opening wizard. A map built
this way has no matrix, so tile colors, the signal histogram and the dotplot overlays do not apply.
Adding a Hi-C matrix to such a map later is not supported; open the matrix and link the FASTA instead.
