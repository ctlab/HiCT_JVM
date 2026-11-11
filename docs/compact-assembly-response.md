# Optional compact assembly snapshots

The six structural edit endpoints accept an optional JSON request member
`"assemblyFormat": "compact-v1"`: reverse, move, split, group, ungroup and
move-to-debris. All operation arguments, edits and version increments retain
their existing behavior. Missing or unrecognized formats return the existing
object representation. Previous servers ignore this optional request member;
new clients must continue to accept their legacy responses.

The response is still a **complete snapshot**, not a delta or a cache reference:

```json
{"version": 12, "assemblyInfo": {
  "format": "compact-v1",
  "resolutions": [250, 500],
  "contigs": [[7, "alias", "original", "source", 0, 1, 1000, [4, 2], [1, 1]]],
  "scaffolds": [[1, "alias", "original", 1000, [0, 1000]]]
}}
```

Contig row columns, in fixed order:

1. `contigId`
2. `contigName` (current display alias)
3. `contigOriginalName`
4. `contigSourceName`
5. `contigOffsetInSource`
6. `contigDirection` (existing enum ordinal)
7. `contigLengthBp`
8. `contigLengthBins` values in `resolutions` order
9. `contigPresenceAtResolution` values in `resolutions` order

Scaffold row columns: `scaffoldId`, `scaffoldName`, `scaffoldOriginalName`,
`spacerLength`, and either `[startBP, endBP]` or `null`. The legacy derived
border property `lengthBp` is exactly `endBP - startBP`. There is no zero/BP
entry in `resolutions`, matching the legacy resolution maps. Empty lists and
zero-bin contigs are retained. IDs, names, source offsets, order, orientation,
all resolutions, hide flags and scaffold boundaries are retained.

The server copies primitive bin/presence arrays before returning the DTO.
Subsequent edits or name changes cannot modify an already constructed response.
The client builds its usual domain descriptors and maps without first creating
the legacy intermediate JSON objects. Unsupported format versions and malformed
rows are rejected before replacing application state.

This removes repeated JSON property/resolution names and per-contig hash-map
construction from this response path. It does not cache matrices, skip edits,
change the data model or remove the O(number of contigs × resolutions) snapshot
cost. File opening and name-management endpoints retain their legacy responses.
Changing the row schema requires a new format version.
