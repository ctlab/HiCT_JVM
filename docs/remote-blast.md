# Search an NCBI nucleotide database

Choose **Tools → Search NCBI databases…**. This works without opening a contact
map or linking FASTA. Paste DNA sequences or FASTA records, choose a database and
confirm that these sequences may be sent to NCBI. Ordinary **Find sequences**
continues to search the linked assembly locally by default.

Starting a new remote search requires the bundled `blastn` client. It sends only
the pasted sequences and generated query IDs. FASTA descriptions stay in the local
report. Linked references, contact maps and assembly coordinates are not uploaded.
Consent is required for each new search and is reset when changing the database
or matching mode. Use local search for sequences you cannot share with NCBI.

The database choices are `nt`, `refseq_rna` and 16S ribosomal RNA. The latter uses
the remote name `rRNA_typestrains/16S_ribosomal_RNA`; the JSON report records both
the choice and exact command database name. Provider contents change over time.
The report records dates and parameters, not a frozen provider database release.

At most ten queries and 100,000 total bases are accepted per job. Choose a timeout
of 1–30 minutes, and up to 500 subject records per query. The default timeout is
ten minutes and the default target limit is 50. A subject can have multiple
alignments, so the target limit is not a row limit. Low-complexity masking is on.
The client uses `blastn-short` if any query is shorter than 50 bases, otherwise
`blastn`. Identity and coverage thresholds apply to each reported alignment.

## Waiting, cancellation and failures

Jobs shows queued, running, finished, failed or cancelled work. Provider waiting
has no invented percentage. One remote client runs per HiCT server, with room for
two queued jobs. Remote waiting does not occupy the local analysis worker pool.
Client starts are separated by at least ten seconds; BLAST+ handles provider
polling. NCBI recommends one remote BLAST+ application at a time; separate HiCT
servers do not share this application-level gate. See
[NCBI's BLAST+ remote-search guidance](https://www.ncbi.nlm.nih.gov/sites/books/NBK569839/)
and [developer usage guidance](https://blast.ncbi.nlm.nih.gov/Blast.cgi?CMD=Web&DOC_TYPE=DeveloperInfo&PAGE_TYPE=BlastDocs).

The timeout starts when the provider client starts, excluding time in HiCT's
queue. Cancel or Stop HiCT terminates its owned local client. A submitted request
may continue at NCBI; cancellation does not promise remote deletion. HiCT does not
automatically resubmit failed, timed-out or cancelled work. Inspect the diagnostic
in Jobs and explicitly start a new search when appropriate. Provider availability
and queue latency are outside HiCT's control.

## Recover an existing request

If you already have an NCBI request ID (RID) for a completed search, open
**Recover an existing NCBI request** in the same search dialog. Enter the RID
and paste the original queries with their original FASTA headers. Confirm the
displayed retrieval choice, then start the recovery job. Progress and results
appear in Jobs, even without an open map.

Recovery sends only the RID to NCBI. The pasted queries stay on the HiCT server
for comparison with the returned query labels, lengths and aligned bases. The
report distinguishes full, partial and absent aligned-base verification; a
matching label or length alone does not establish sequence identity. Inspect
**Queries and provenance** in the results for those details.

This path downloads existing results and does not require the local `blastn`
client. It never starts a new provider search, including against an older HiCT
server lacking retrieval support. Identity, e-value, per-HSP coverage and target
limits filter the downloaded alignments; they do not change the original NCBI
search. Original provider settings and local retrieval filters are recorded
separately.

An interrupted submission that never supplied a RID cannot be recovered through
this form. HiCT does not guess a request ID or automatically upload the queries
again. A new search remains a separate explicit submission.

## Read and save results

Completed jobs open **NCBI BLAST results**, including successful searches with no
hits. Reopen the report from Jobs after closing the dialog or changing maps.
Job history belongs to the running server and is fresh after restart.

Query and subject intervals use BLAST's **one-based inclusive** coordinates.
Descending endpoints represent a reverse orientation. Subjects are external
database records: results are never silently placed on the current assembly or
converted into an assembly BED track. Accession links open NCBI nucleotide
records. Query and subject filters affect the displayed rows only.

The viewer pages through up to 20,000 alignment rows. A larger bounded result is
explicitly marked as truncated in JSON; **Save raw TSV…** retains every returned
row. Raw results are limited to 32 MiB and JSON to 64 MiB; oversized results fail
with guidance to reduce query/target counts. Both save actions ask for a
destination using HiCT's file-save flow, with browser downloads as its fallback.

JSON includes external IDs, coordinates, scores, query lengths/descriptions,
query-file SHA-256, database names, dates, thresholds, job ID and the local client
executable SHA-256. This supports provenance without claiming that an evolving
remote database is reproducible from the checksum alone. Raw TSV fields are:

```text
qseqid sseqid saccver qstart qend sstart send pident length mismatch gapopen evalue bitscore qlen slen stitle
```

## Qualification

The controlled browser/lifecycle checks use an owned external command and real
HiCT Jobs, HTTP and UI paths. They cover upload consent, independent local work,
queue bounds, process cancellation, provider failure, timeout, invalid/oversized
results, external coordinates, pagination and complete exports. They do not
substitute for a successful live-provider check or platform qualification. Current
live-provider evidence and outstanding gates are recorded in
[reinforcement validation](reinforcement-validation.md).
