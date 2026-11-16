# Info during zoom and the yeast Micro-C diagonal

**Info** shows the current view's coordinates, resolution and source guidance.
It updates while new tiles load, without reading image pixels. Its existing
position, field selection and field order remain available under **View → Info
settings…**. Toggling Info also works with the pointer stationary. Coordinates
outside the assembly do not become clamped edge coordinates. Names containing
markup characters are displayed literally.

A source-specific balancing notice belongs to the map that produced it. Opening
another map dismisses it and closes its settings prompt. A late completion cannot
show an old map's notice or make its Change action operate on the new map.
Notification history retains earlier messages with their timestamps.

## The yeast example's empty band

For **Yeast Micro-C quick start** (GSM9302472 WT biological replicate 3), the narrow
empty band is already present in the deposited Cooler counts. It is not filled
in or removed by HiCT. An independent check compared 669,584 raw cells in 17
windows across chromosomes I, VI and M at 50, 100 and 500 bp; every cell matched.
The following totals scan the source's stored cis diagonals across all chromosomes:

| Source resolution | Empty bin offsets from the diagonal | Contacts on the first supported offset |
|---|---|---|
| 50 bp | 0, 1, 2, 3 | 1,365,532 at offset 4 |
| 100 bp | 0, 1 | 2,759,916 at offset 2 |
| 500 bp | None at the diagonal | 3,418,162 at offset 0 |

An offset is the difference between bin indices, not an exact read-pair distance.
At coarser resolution, contacts that occupied separate fine bins can share a
coarse bin. A populated 500 bp diagonal therefore agrees with an empty band in
the fine map.

The [GEO sample's processing description](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM9302472)
records HiC-Pro valid-pair processing followed by Cooler generation. It does not
specify the exact cause of these empty short-range cells. The source comparison
establishes their presence before HiCT import; it does not identify a particular
filter or biological explanation.

For a visual check, use **Show Observed** and **Normalization → None**. File
weights can additionally mask excluded bins; O/E is a derived signal. Palettes,
thresholds and other pipeline transforms also affect appearance. The numeric
comparison above uses raw counts before these display transformations.

The reproducible checks and source hashes are recorded in
[reinforcement-validation.md](reinforcement-validation.md). This result qualifies
the stated dataset and windows; it does not replace validation of other datasets.
