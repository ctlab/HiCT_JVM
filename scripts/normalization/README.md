# Reference check for square-root coverage

`GoldenVcSqrt.java` runs the Juicebox `VC_SQRT` recipe (`NormalizationCalculations.computeVC`
followed by `NormalizationVectorUpdater.buildVCOrVCSQRT`) on a text matrix and prints the
resulting divisor vector as JSON. It belongs to the package `juicebox.tools.utils.norm` because it
calls package-private methods of Juicebox.

1. Check out Juicebox at `3c4936c02d56257c85dfc8e1d91794f6cda2b6a1` (aidenlab/Juicebox).
2. Compile it together with the driver:
   `javac -nowarn -d out -cp "$(find lib -name '*.jar' | tr '\n' ':')" -sourcepath src src/juicebox/tools/utils/norm/NormalizationCalculations.java`
   then `javac -d out -cp "out:lib/..." scripts/normalization/GoldenVcSqrt.java`.
3. Input file: the first line is the number of bins, then one `row col value` line per stored
   upper-triangle entry. Run `java -cp out:lib/... juicebox.tools.utils.norm.GoldenVcSqrt matrix.txt`.

`src/test/resources/normalization/vc_sqrt_golden.json` was produced this way. `CoverageNormalizerTest`
compares HiCT's `CoverageNormalizer` with it.
