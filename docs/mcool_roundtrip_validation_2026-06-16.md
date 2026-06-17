# `.mcool -> .hict.hdf5 -> .mcool -> .hict.hdf5` Validation

Date: 2026-06-16

Source file:

- `/mnt/Models/HiCT/data/arabiensis.0.hic.mcool`

## Scope

The goal was to validate whether:

1. `.mcool -> .hict.hdf5 -> .mcool` preserves the matrix
2. `.mcool -> .hict.hdf5 -> .mcool -> .hict.hdf5 -> .mcool` preserves the matrix again

## Commands used

From `HiCT_JVM`:

```bash
./gradlew compileJava
./gradlew shadowJar

java -jar build/libs/hict_server-1.0.178-eb2770f-webui_09b8ce7-fat.jar \
  convert mcool-to-hict \
  --input=/mnt/Models/HiCT/data/arabiensis.0.hic.mcool \
  --output=/tmp/arabiensis_roundtrip_1.hict.hdf5 \
  --parallelism=1

java -jar build/libs/hict_server-1.0.178-eb2770f-webui_09b8ce7-fat.jar \
  convert hict-to-mcool \
  --input=/tmp/arabiensis_roundtrip_1.hict.hdf5 \
  --output=/tmp/arabiensis_roundtrip_1_fix.mcool \
  --parallelism=1

java -jar build/libs/hict_server-1.0.178-eb2770f-webui_09b8ce7-fat.jar \
  convert mcool-to-hict \
  --input=/tmp/arabiensis_roundtrip_1_fix.mcool \
  --output=/tmp/arabiensis_roundtrip_2_fix.hict.hdf5 \
  --parallelism=1

java -jar build/libs/hict_server-1.0.178-eb2770f-webui_09b8ce7-fat.jar \
  convert hict-to-mcool \
  --input=/tmp/arabiensis_roundtrip_2_fix.hict.hdf5 \
  --output=/tmp/arabiensis_roundtrip_2_fix.mcool \
  --parallelism=1
```

Validation helpers:

```bash
build/resources/main/toolchains/linux_x86_64/bin/hictk metadata /tmp/arabiensis_roundtrip_1_fix.mcool
build/resources/main/toolchains/linux_x86_64/bin/hictk metadata /tmp/arabiensis_roundtrip_2_fix.mcool

java -cp /tmp:build/libs/hict_server-1.0.178-eb2770f-webui_09b8ce7-fat.jar \
  RoundTripMatrixCompare \
  /mnt/Models/HiCT/data/arabiensis.0.hic.mcool \
  /tmp/arabiensis_roundtrip_1_fix.mcool

java -cp /tmp:build/libs/hict_server-1.0.178-eb2770f-webui_09b8ce7-fat.jar \
  RoundTripMatrixCompare \
  /mnt/Models/HiCT/data/arabiensis.0.hic.mcool \
  /tmp/arabiensis_roundtrip_2_fix.mcool

java -cp /tmp:build/libs/hict_server-1.0.178-eb2770f-webui_09b8ce7-fat.jar \
  RoundTripMatrixCompare \
  /tmp/arabiensis_roundtrip_1_fix.mcool \
  /tmp/arabiensis_roundtrip_2_fix.mcool
```

## Result

`hictk metadata` accepts both exported `.mcool` files as structurally valid:

- `/tmp/arabiensis_roundtrip_1_fix.mcool`
- `/tmp/arabiensis_roundtrip_2_fix.mcool`

However, the matrix content is **not preserved**.

### Source vs first round-trip

Comparison fails immediately at:

```text
/resolutions/1000/indexes/bin1_offset differs at offset 1: 0 vs 42837843
```

### Source vs second round-trip

Comparison fails immediately at:

```text
/resolutions/1000/indexes/bin1_offset differs at offset 1: 0 vs 42838203
```

### First vs second round-trip

Comparison also fails:

```text
/resolutions/1000/indexes/bin1_offset differs at offset 1: 42837843 vs 42838203
```

So the export is not only different from the source, it is also not stable across a second round-trip.

## Root cause

The current `hict-to-mcool` exporter copies HiCT internal sparse COO arrays directly:

- `treap_coo/block_rows -> pixels/bin1_id`
- `treap_coo/block_cols -> pixels/bin2_id`
- `treap_coo/block_vals -> pixels/count`

This is not sufficient to produce a correct Cooler file.

Cooler expects:

- `pixels/bin1_id` sorted globally by `(bin1_id, bin2_id)` in row-major order
- `indexes/bin1_offset` to match that sorted order

The HiCT internal COO vectors are **not globally row-sorted**. They are stored in HiCT internal block/stripe order. Because of that:

- `indexes/bin1_offset` generated from those raw vectors becomes invalid
- `mcool-to-hict` re-import then consumes invalid row offsets
- the second round-trip produces visible corruption, including zero-valued leading rows in exported pixel coordinates

## Concrete evidence

At resolution `1000`:

### Original source (`arabiensis.0.hic.mcool`)

First offsets:

```text
0 -> 0
1 -> 0
2 -> 0
3 -> 0
4 -> 0
5 -> 1
...
```

First pixels:

```text
0: (4,246461)
1: (11,11)
2: (11,1568)
...
```

### First exported `.mcool`

First offsets:

```text
0 -> 0
1 -> 42837843
2 -> 42838145
3 -> 42838145
...
```

First pixels:

```text
0: (11,11)
1: (71,71)
2: (92,92)
...
```

### Second exported `.mcool`

First offsets:

```text
0 -> 0
1 -> 42838203
2 -> 42838203
3 -> 42838203
...
```

First pixels:

```text
0: (0,0)
1: (0,0)
2: (0,0)
...
```

## Conclusion

Current status:

- `.mcool -> .hict.hdf5` works on this file
- `.hict.hdf5 -> .mcool` produces a file that is structurally readable as `.mcool`
- but the resulting matrix is **not semantically equivalent** to the input `.mcool`
- double round-trip makes the corruption worse

## Required fix direction

`HictToMcoolConverter` must not export raw `treap_coo` vectors directly as Cooler `pixels/*`.

It needs one of these:

1. A proper row-major sparse export path that reconstructs/sorts all sparse pixels by `(bin1_id, bin2_id)` before writing `pixels/*` and `indexes/bin1_offset`
2. A reuse of an existing repository path that already materializes HiCT sparse data in Cooler-compatible row order
3. An external sort / merge-sort export step if full in-memory sort is too expensive for large matrices

Without that, `.hict.hdf5 -> .mcool` cannot be considered round-trip safe.
