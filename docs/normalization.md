# HiCT Normalization And Matrix Display Modes

This document describes HiCT's implementation of common Hi-C browser display 
controls. It is intended for users who are familiar with Juicebox/JBAT terminology 
and had previous experience using it.

## Display Modes

HiCT keeps the display mode separate from the color preset:

- `Observed` displays the currently selected contact signal after the active
  HiCT visualization settings are applied.
- `Expected` replaces each observed value with the mean contact value for the
  same genomic pixel distance.
- `Observed / Expected` divides the observed value by that distance-matched
  expected value. Empty, negative, non-finite, or near-zero expected values are
  rendered as zero to avoid false high-intensity artifacts.

Expected profiles are computed inside scaffold domains. If no scaffolds are
available, each contig is treated as an independent scaffold. Contacts crossing
different scaffold domains contribute zero expected signal, because distance
decay across unrelated assembly pieces is not biologically meaningful.

The expected profile is cached for the current viewport and resolution, then
refreshed when the viewport changes. This avoids tile-boundary artifacts while
keeping the calculation local to the region being inspected.

## Bias And Resolution Corrections

HiCT exposes bias correction as explicit visualization steps:

- `Apply weights from Cooler` multiplies the matrix by row and column weights
  imported from Cooler-compatible inputs when those weights are present.
- `Apply resolution scaling` and `Apply linear resolution scaling` apply HiCT's
  resolution-dependent coefficients stored in the opened `.hict.hdf5` file.
- `Apply pre log-normalization` and `Apply post log-normalization` apply
  `log(1 + x)` transforms before or after weight/resolution operations.

This maps to the same user-facing concepts as common Hi-C browsers: raw contacts
with no balancing, coverage-like row/column bias correction, square-root
coverage-style correction, and matrix balancing. In HiCT, these operations are
kept explicit so a bioinformatician can inspect exactly which signal transforms
are active.

## Square-Root Coverage

`Normalization` in the sidebar chooses one of three things the matrix is multiplied by:
nothing, the weights stored in the file (Cooler), or square-root coverage. They are
mutually exclusive.

Square-root coverage is the recipe Juicebox calls `VC_SQRT`. With `r_i` the coverage of bin
`i` (its row sum over the symmetric matrix, the diagonal counted once) and `d_i = sqrt(r_i)`:

```
A = sum over i,j of  M_ij / (d_i d_j)        S = sum over i,j of M_ij       (valid bins only)
f = sqrt(A / S)                              v_i = f * d_i                  output = M_ij / (v_i v_j)
```

Off-diagonal stored entries count twice in `A` and `S`, because only the upper triangle is
stored. A bin is valid when `d_i > 0`. A bin with no contacts has no divisor; its weight is
`NaN` and the existing missing-weights policy decides how it is drawn. The factor `f` keeps
the matrix total unchanged by the normalization.

**Domain.** Juicebox computes the vector for each chromosome from its own (cis) matrix. HiCT
computes it once for the whole opened map, so contacts between contigs count towards coverage
and every contig is on the same scale. The vector belongs to source bins, not to positions in
the assembly, so it does not change when you reverse, move or regroup contigs, and splitting
a contig only removes a bin. Contigs hidden at a resolution still contribute their contacts.
For a map that has a single chromosome the two definitions are the same matrix, and the
results agree to floating-point precision.

**Parity.** The reference is Juicebox commit `3c4936c0` (`NormalizationCalculations.computeVC`
and `NormalizationVectorUpdater.buildVCOrVCSQRT`). `src/test/resources/normalization/vc_sqrt_golden.json`
holds the output of that code on six matrices (including empty bins, a diagonal-only and an
all-zero matrix); `CoverageNormalizerTest` requires agreement within 2e-6 relative. Regenerate
it with `scripts/normalization/GoldenVcSqrt.java` (see the README there). A real map was also
compared end to end: on a 11145-bin, 10.97-million-contact map the server's weight times the
reference divisor was 1 within 1.5e-7 for every one of 10904 valid bins.

**How it runs.** Choosing square-root coverage starts one background job that reads the stored
contacts of every resolution twice. The sidebar shows its progress with a Cancel button and
keeps the previous normalization until the job has finished. The vectors of all resolutions
(and of an attached second source) are published together, so a tile never mixes normalized
and raw resolutions; a cancelled or failed job changes nothing. Switching between prepared
modes is immediate. Memory is bounded by `HICT_COVERAGE_MAX_BYTES` (default 1 GiB) and checked
before anything is read. Vectors live with the open map and are recomputed after the map is
closed and opened again.

API: `POST /normalization/coverage/prepare|status|cancel` and `POST /normalization/weights`
(`{"mode": "FILE" | "SQRT_COVERAGE"}`). Limits: it is unavailable for identity and presence
sources; a second source attached later must be prepared too (the sidebar offers a button).
A saved session records that square-root coverage was in use and recomputes it when the
session is restored.

## Practical Presets

For single Hi-C maps, start with `Mosquitoes Demo` or `Hi-C balanced red auto`.
For dotplots, use `Dotplot black` on a light background or `Dotplot overlay
black` when the dotplot is the top overlay layer. When two sources are overlaid,
HiCT forces the top layer's minimum color to transparent so applying a preset or
threshold does not cover the bottom layer with a solid no-signal color.

## References

The feature names above are compatible with terms used in public Hi-C tooling
documentation, especially matrix display choices such as observed, expected,
and observed/expected, and normalization names such as none, coverage,
coverage-sqrt, and balanced/KR. HiCT implements these ideas independently using
its own viewport-local, scaffold-aware distance profile and explicit rendering
pipeline primitives.

Reference material used for terminology:

- Juicebox/Juicer public documentation for observed, expected, O/E, and color
  range controls: https://github-wiki-see.page/m/aidenlab/Juicebox/wiki/Exploring-the-Data
- Juicer/Juicebox command-line `dump` terminology for matrix types and
  normalization vector names: https://github.com/aidenlab/juicer
- Juicer `pre` documentation listing built-in normalization names:
  https://github-wiki-see.page/m/aidenlab/juicer/wiki/Pre
