# Approximate Cooler export after sequence cuts

Exact HiCT cuts retain sequence and original contact bins. Two fragments can
reference the same bin where the cut crosses it. These references are not new
observations. A fixed-bin Cooler cannot represent that shared native geometry
directly, so exporting such an assembly requires an explicit approximation.

In **File → Export matrix**, select the current assembly or an AGP file and enable
**Approximate rebinning for sequence cuts**. This uses the internal exporter and
writes unbalanced float64 estimates. Existing balancing weights do not describe
the new grid and are not copied. The option is off by default; ordinary export
continues to reject unsupported split AGP components. Changing the option does
not change the active assembly or native contact values.

## Estimation model

The method is `uniform-independent-endpoints-v1`. Each endpoint of an original
contact is modeled as uniformly distributed within its original bin's actual
sequence interval. A short terminal bin uses its true length, rather than the
nominal resolution. For a source bin `i` and output bin `k`, the fraction `w(i,k)`
is the number of source bases mapped into `k` divided by the original bin width.
The AGP defines strand, order, scaffold coordinates and gaps.

For an unordered source contact between different bins, the exporter distributes
its value over all output endpoint pairs using the product of their fractions,
then combines equal unordered output pairs. For a source-diagonal contact of
value `C`, the same output bin receives `C*w(i,k)^2`; two different output bins
receive `2*C*w(i,k)*w(i,l)`. The factor two includes both exchangeable endpoint
orders. Native display references are never summed to construct this export.

If all source sequence is retained once, the sum of unordered contact values is
conserved, within float64 rounding tolerance. If source sequence is omitted,
the expected retained contribution is `C*f(i)*f(j)`, where `f` is the retained
fraction of that source bin. Missing sequence is not renormalized. Repeated or
overlapping source intervals and unspecified component strands are rejected.

Every selected stored resolution is projected independently. The metadata calls
this `independent_stored_levels`. Levels are estimates based on their respective
source grids; they are **not guaranteed to be exact aggregations of the finest
exported level**. No new read positions are inferred or recovered by this model.

## Output and validation

Both `.cool` and `.mcool` are supported. Each Cooler level contains:

- Float64 `pixels/count`, sorted unique upper-triangle pixels and standard indexes.
- `bins/hict_sequence_fraction`, the fraction of each output bin covered by
  retained AGP sequence. A gap-only bin has zero coverage and no invented contacts.
- Metadata identifying the approximation, endpoint assumption, resolution policy,
  original and expected retained totals, exported total and conservation tolerance.

The root is marked `hict-approximate-export=true`; `/hict/approximate_export/agp`
stores the applied layout. The writer scans original unordered source contacts
once, uses a bounded endpoint cache and bounded external merge sorting, checks
conservation, then publishes the completed file. Cancellation or failure removes
private intermediate files and preserves an existing output. Jobs retain the
completed download even after the export dialog is dismissed. Estimated exports
are excluded from the ordinary matrix-conversion cache.

The approximation concerns spatial allocation, not merely floating-point storage.
Use native HiCT data and the saved session when the original observations and exact
source references are required. The export does not apply the current visualization
pipeline, O/E colors or display transforms.

## Command line and API

With the packaged launcher (replace the JAR name with the downloaded artifact):

```sh
java -jar hict_server-fat.jar convert hict-to-mcool \
  --input=source.hict.hdf5 --output=estimated.mcool \
  --agp=edited.agp --apply-agp --approximate-rebin \
  --no-balance-exported-coolers --all-resolutions
```

For `POST /convert/jobs`, set `approximateRebin: true` and select an AGP through
`assemblyFilename` or `useCurrentAssembly: true`. `balanceExportedCoolers` defaults
to false in this mode; explicitly requesting true is rejected. The same policy
is supported by batch conversion with per-input AGP files. API details are in
`src/main/resources/openapi/hict-api-v1.yaml`.

Regression tests compare every interior cut of a small fixture on both strands
against independent enumeration of base endpoints, including repeated cuts,
gaps, partial retention, diagonal allocation, sorting, empty output and cleanup.
The browser acceptance downloads a real Jobs artifact and checks every cell with
the independent Python Cooler reader and a second base-endpoint oracle. These
checks establish the method on the fixtures; large-map performance and final
platform/package qualification remain separate release gates.
