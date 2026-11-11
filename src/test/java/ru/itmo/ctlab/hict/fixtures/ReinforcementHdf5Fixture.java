package ru.itmo.ctlab.hict.fixtures;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/** Small real Cooler -> HiCT inputs. No downloaded data or native converter executable. */
public final class ReinforcementHdf5Fixture {
  public static final long LENGTH = 17;
  // Each row is one upper-triangle contact at source bp coordinates, then count.
  public static final long[][] CONTACTS = {{0, 0, 1}, {1, 5, 3}, {6, 9, 7}, {16, 16, 11}};

  private ReinforcementHdf5Fixture() { }

  public static Path create(Path directory, String name, List<Long> resolutions) throws Exception {
    return create(directory, name, resolutions, LENGTH, CONTACTS);
  }

  public static Path create(Path directory, String name, List<Long> resolutions, long length, long[][] contacts) throws Exception {
    Files.createDirectories(directory);
    var input = directory.resolve(name + ".mcool");
    var output = directory.resolve(name + ".hict.hdf5");
    if (Files.exists(input) || Files.exists(output))
      throw new IllegalArgumentException("Fixture output already exists: " + name);
    HDF5LibraryInitializer.initializeHDF5Library();
    try (var writer = HDF5Factory.open(input.toFile())) {
      writer.string().writeArray("/chroms/name", new String[]{"tiny"});
      writer.int64().writeArray("/chroms/length", new long[]{length});
      for (long resolution : resolutions) {
        int bins = Math.toIntExact(Math.ceilDiv(length, resolution));
        String root = "/resolutions/" + resolution + "/";
        long[] starts = new long[bins], ends = new long[bins], offsets = new long[bins + 1];
        var pixels = new TreeMap<Long, Long>();
        for (var contact : contacts) {
          long key = (contact[0] / resolution) * bins + contact[1] / resolution;
          pixels.merge(key, contact[2], Long::sum);
        }
        var rows = new ArrayList<Long>();
        var cols = new ArrayList<Long>();
        var counts = new ArrayList<Long>();
        for (int row = 0; row < bins; row++) {
          starts[row] = row * resolution;
          ends[row] = Math.min(length, starts[row] + resolution);
          offsets[row] = rows.size();
          for (var entry : pixels.subMap((long) row * bins, (long) (row + 1) * bins).entrySet()) {
            rows.add((long) row); cols.add(entry.getKey() % bins); counts.add(entry.getValue());
          }
        }
        offsets[bins] = rows.size();
        writer.int64().writeArray(root + "indexes/chrom_offset", new long[]{0, bins});
        writer.int64().writeArray(root + "indexes/bin1_offset", offsets);
        writer.int64().writeArray(root + "bins/chrom", new long[bins]);
        writer.int64().writeArray(root + "bins/start", starts);
        writer.int64().writeArray(root + "bins/end", ends);
        writer.int64().writeArray(root + "pixels/bin1_id", rows.stream().mapToLong(Long::longValue).toArray());
        writer.int64().writeArray(root + "pixels/bin2_id", cols.stream().mapToLong(Long::longValue).toArray());
        writer.int64().writeArray(root + "pixels/count", counts.stream().mapToLong(Long::longValue).toArray());
      }
    }
    new McoolToHictConverter().convert(new ConversionOptions(input, output, resolutions, 64, 1,
      ConversionOptions.CompressionAlgorithm.DEFLATE, ConversionOptions.NO_AGP, false, 1, false,
      ConversionOptions.ExportMode.INTERNAL), ignored -> { });
    return output;
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 2 && args[0].equals("mixed-pyramids")) {
      final var directory = Path.of(args[1]);
      final long[][] landmarks = {{101, 211, 500}, {420, 537, 700}, {960, 1001, 900}, {12016, 12016, 1100}};
      create(directory, "mixed-primary", List.of(4L, 10L), 12017, landmarks);
      create(directory, "mixed-fine", List.of(1L), 12017, landmarks);
      create(directory, "mixed-coarse", List.of(25L), 12017, landmarks);
      create(directory, "mixed-disjoint", List.of(3L, 7L), 12017, landmarks);
      create(directory, "mixed-interleaved", List.of(2L, 4L, 6L, 10L, 15L), 12017, landmarks);
      return;
    }
    if (args.length != 1) throw new IllegalArgumentException("Supply a fresh fixture output directory");
    Path directory = Path.of(args[0]);
    create(directory, "tiny-primary", List.of(4L, 10L));
    create(directory, "tiny-disjoint", List.of(3L, 7L));
    create(directory, "tiny-unrelated", List.of(3L, 7L), LENGTH + 1, CONTACTS);
    create(directory, "tiny-fine", List.of(1L));
    create(directory, "tiny-coarse", List.of(25L));
    Files.writeString(directory.resolve("tiny.fa"), ">tiny synthetic 17 bp fixture\nACGTRYMKSWBDHVNAC\n");
  }
}
