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
