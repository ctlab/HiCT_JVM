package ru.itmo.ctlab.hict.hict_library.assembly;

import ch.systemsx.cisd.hdf5.HDF5DataClass;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import io.vertx.core.json.Json;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;

/** The source metadata digest shared with overlay_store.py. Matrix values are never read. */
public final class SequenceSourceDigest {
  private static final int BLOCK_ROWS = 8192;

  private SequenceSourceDigest() { }

  public static String digest(IHDF5Reader file, List<Long> resolutions) {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
    // Missing-resolution markers precede the dataset visits in the adapter.
    for (long resolution : resolutions) {
      if (!file.object().isGroup("/resolutions/" + resolution)) {
        add(digest, "sequence-grid-v1/" + resolution + "/256\n");
      }
    }
    visit(file, digest, "contig_info");
    for (long resolution : resolutions) {
      var root = "resolutions/" + resolution;
      if (!file.object().isGroup("/" + root)) continue;
      for (var child : List.of("contigs", "atl", "stripes/stripe_length_bins")) {
        visit(file, digest, root + "/" + child);
      }
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static void visit(IHDF5Reader file, MessageDigest digest, String name) {
    add(digest, name);
    String path = "/" + name;
    if (file.object().isGroup(path)) {
      for (var child : file.object().getGroupMembers(path).stream().sorted().toList()) {
        visit(file, digest, name + "/" + child);
      }
      return;
    }
    var information = file.object().getDataSetInformation(path);
    var dimensions = information.getDimensions();
    String shape = Arrays.stream(dimensions).mapToObj(Long::toString).collect(Collectors.joining(", "));
    add(digest, "(" + shape + (dimensions.length == 1 ? "," : "") + ")");
    var type = information.getTypeInformation();
    long rows = dimensions.length == 0 ? 1 : dimensions[0];
    int[] block = Arrays.stream(dimensions).mapToInt(Math::toIntExact).toArray();
    long[] offset = new long[dimensions.length];
    for (long row = 0; row < rows; row += BLOCK_ROWS) {
      if (dimensions.length > 0) {
        block[0] = (int) Math.min(rows - row, BLOCK_ROWS);
        offset[0] = row;
      }
      if (type.getDataClass() == HDF5DataClass.STRING) {
        var strings = dimensions.length == 0 ? new String[]{file.string().read(path)}
          : file.string().readMDArrayBlockWithOffset(path, block, offset).getAsFlatArray();
        add(digest, Json.encode(strings));
      } else if (type.getDataClass() == HDF5DataClass.INTEGER) {
        var integers = type.isSigned() ? file.int64() : file.uint64();
        long[] values = dimensions.length == 0 ? new long[]{integers.read(path)}
          : integers.readMDArrayBlockWithOffset(path, block, offset).getAsFlatArray();
        var bytes = ByteBuffer.allocate(Math.multiplyExact(values.length, type.getElementSize())).order(ByteOrder.LITTLE_ENDIAN);
        for (long value : values) switch (type.getElementSize()) {
          case 1 -> bytes.put((byte) value);
          case 2 -> bytes.putShort((short) value);
          case 4 -> bytes.putInt((int) value);
          case 8 -> bytes.putLong(value);
          default -> throw new IllegalArgumentException("Unsupported integer in sequence source metadata");
        }
        digest.update(bytes.array());
      } else {
        throw new IllegalArgumentException("Unsupported dataset in sequence source metadata: " + name);
      }
    }
  }

  private static void add(MessageDigest digest, String value) {
    digest.update(value.getBytes(StandardCharsets.UTF_8));
  }
}
