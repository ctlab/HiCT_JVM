# Shared bins in expected fitting and histograms

An exact sequence cut can leave the same immutable contact bin referenced by
several fragments. Every displayed reference retains its original value. This
does not create additional observations or locate a contact within the cut bin.

Expected and O/E fitting use one unit of support per unordered physical bin pair
in the fitting window. Physical identity includes source, stored resolution,
stripe and bin index. For bins `a != b`, let `R(a)` and `C(a)` be the numbers of
references on the fitting window's row and column axes. Every displayed `a/b`
or `b/a` cell receives weight

```
1 / (R(a)*C(b) + R(b)*C(a))
```

A source-diagonal pair `a/a` receives `1 / (R(a)*C(a))`. Thus all aliases of one
unordered pair together have weight one, including when a rectangular viewport
contains unequal numbers of the two orientations. Padding has zero support.
Counts are computed over the whole fitting window and reused across tile chunks.
They do not restart at each tile boundary.

Within each scaffold, expected values are the weighted positive-finite means at
each absolute **display-bin separation**. Unscaffolded contigs form separate
domains. The existing policy of excluding zero/nonpositive values from fitting
is retained. Cross-domain pairs are excluded and their missing support is not
renormalized. A fitting window with no repeated references keeps its existing
means; the symmetric orientations then have identical values and separation.

This model describes the current displayed assembly. Splitting a source bin
can place its aliases at different separations, so it does not recover original
base-pair contact distances or independent observations within that bin.
Native values, source weights and the original file remain unchanged. Viewport
profiles, primary raster fallbacks and primary normalized numeric queries use
the same policy. Queries in BINS units fit their own axis, including hidden
contigs, and cannot reuse a cached PIXELS profile.

`POST /visualization/expected_profile` reports
`sampleSemantics=UNIQUE_UNORDERED_SOURCE_PAIR_MASS`,
`distanceSemantics=DISPLAY_BIN_DISTANCE`, and `sharedReferenceWeighting`.
These declarations describe fitting support, not a contact-count total.

Color histograms serve a different purpose: they describe the values displayed
on screen, including repeated references, zeros and both triangles when selected.
Their counts remain display samples so contrast fitting reflects visible color
coverage, including for arbitrary rendering expressions. Tile statistics declare
`countSemantics=DISPLAY_CELLS`; matrix statistics also identify display-cell counts.
The histogram dialog labels matrix samples as displayed cells and explains
repeated references when an exact-cut fragment is present. These counts must
never be presented as independent contact observations or added to native totals.

Regression coverage includes physical stripes near bin index two billion without
allocating source-sized arrays, reversed/padded axes, unordered pair conservation,
unequal rectangular multiplicities and chunk-invariant fits. The browser checker
`scripts/check_shared_expected_browser.py` independently enumerates unordered
pairs and checks actual expected-value queries, cached/fallback behavior, O/E
controls and the visible histogram explanation.
