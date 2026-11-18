# Signal fixture

`signal-with-gap.bw` is a synthetic 17-base reference named `tiny`. It contains
7 at [0,1), 9 at [2,3), and 4 at [3,5); other positions have no observation.
Coordinates are zero-based and half-open. The signal area is 24 over four
covered bases. The whole-reference covered mean is 6 and the gap-filled mean
is 24/17. `signal-with-gap.json` records the emitted SHA-256 and generator.

Generated with Python 3.12 and pyBigWig 0.3.25:

```python
import pyBigWig
with pyBigWig.open('signal-with-gap.bw', 'w') as writer:
    writer.addHeader([('tiny', 17)])
    writer.addEntries(['tiny'] * 3, [0, 2, 3], ends=[1, 3, 5],
                      values=[7.0, 9.0, 4.0])
```

The JVM regression uses the independent IGV BigWig reader and hand-enumerated
base intervals, including partial native bins, cuts, reversal and cached means.
No Python package or downloaded dataset is required to run that regression.


`signed-signal.bw` contains -7 at [0,1), -9 at [2,3), -4 at [3,5),
6 at [7,9), and measured zero at [10,11). Its area is -12 across seven
observed bases: covered mean -12/7 and zero-filled mean -12/17. An all-negative
[0,5) window has maximum -4, covered mean -6 and zero-filled mean -4.8.
Missing positions remain distinct from the measured zero. The regression also
reopens the persisted cache and checks that no preparation is scheduled.

`nonfinite-nan.bw` and `nonfinite-infinity.bw` deliberately contain one invalid
observation at [0,1). Query and preparation must report the invalid value,
without publishing an invented zero. JSON companions record generation inputs
and SHA256. All three use the same writer/header as the first example; pass
the companion intervals' start/end/value arrays to `addEntries`, converting
`"nan"` / `"inf"` to Python floats for the invalid fixtures.
