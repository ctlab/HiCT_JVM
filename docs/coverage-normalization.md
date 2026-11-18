# Square-root coverage normalization

Choose **Visualization → Normalization → Square-root coverage** to reduce the
influence of uneven contact coverage. Expand **How coverage is calculated** for
the formula without leaving the map.

The total contacts of bin i, rᵢ, is the sum of its row in the symmetric raw contact
matrix. Summing its column gives the same result. Its diagonal entry contributes
once to that row sum. HiCT calculates coverage across the entire opened source
at each stored resolution, including contacts between contigs. Panning and
zooming within a resolution keep that coverage vector fixed.

For raw contact Mᵢⱼ, the normalized value is:

```
Nᵢⱼ = C × Mᵢⱼ / √(rᵢ × rⱼ)
S = Σᵢⱼ Mᵢⱼ
A = Σᵢⱼ Mᵢⱼ / √(rᵢ × rⱼ)
C = S / A
```

The sums use bins with positive finite coverage. S and A include both halves of
the symmetric matrix, so each off-diagonal contact contributes twice to those
whole-map sums. C is one factor for the source and resolution; it preserves the
whole-map symmetric contact total. Bins with zero coverage have no usable
coverage weight. An empty map cannot supply a coverage vector. Additional
selected transforms, scaling and colors contribute to the final displayed image.

For example, raw rows `[4, 1]` and `[1, 9]` have coverage 5 and 10. The symmetric
total is 15. Here A is approximately 1.9828427125 and C is 7.5648965526. The
normalized rows are approximately `[6.0519172421, 1.0698379303]` and
`[1.0698379303, 6.8084068974]`, whose entries still sum to 15. Counting the
upper triangle only would give a different total and scale.

Exact cuts can give multiple fragments references to the same stored contact
bin. Coverage preparation uses each stored contact once, retaining the source
vector through those cuts. Histograms or sums over displayed fragment cells can
include repeated source references; they describe the displayed grid.

The implementation and numerical contract are in
[CoverageNormalizer.java](../src/main/java/ru/itmo/ctlab/hict/hict_library/visualization/CoverageNormalizer.java).
This is the VC_SQRT recipe with one total-preserving scale factor. HiCT's domain
is the whole source, including inter-contig contacts; comparisons with another
tool require matching the matrix domain as well as the normalization name.
