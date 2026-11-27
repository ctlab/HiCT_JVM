package ru.itmo.ctlab.hict.hict_server.normalization;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.NormalizationWeights;
import ru.itmo.ctlab.hict.hict_library.visualization.CoverageNormalizer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/** Source-bin coverage vectors stored independently of the current assembly layout. */
final class CoverageCache {
  private static final byte[] MAGIC = "HICTCOV1".getBytes(StandardCharsets.US_ASCII);
  private static final String VERSION = CoverageNormalizer.ALGORITHM_VERSION;

  private CoverageCache() {}

  static String key(ChunkedFile file) throws IOException {
    var source = file.getHdfFilePath().toRealPath();
    var stamp = Files.readAttributes(source, BasicFileAttributes.class);
    var identity = String.join("\0", source.toString(), Long.toString(stamp.size()),
      stamp.lastModifiedTime().toString(), file.getSourceMetadata().toString(), VERSION);
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
        .digest(identity.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  static int[] expectedBins(ChunkedFile file) {
    var stripes = file.getStripeCount();
    var bins = new int[stripes.length];
    for (int i = 0; i < stripes.length; i++) {
      if (stripes[i] > 0) bins[i] = Math.toIntExact(file.getMatrixQueries().sourceAxisBins(i));
    }
    return bins;
  }

  static NormalizationWeights.Prepared read(Path processed, String key, int[] expectedBins) throws IOException {
    if (!Files.isDirectory(processed)) return null;
    var root = processed.toRealPath().resolve("coverage");
    if (Files.notExists(root, LinkOption.NOFOLLOW_LINKS)) return null;
    requireCoverageDirectory(root);
    var entry = root.resolve(key + ".bin");
    if (Files.notExists(entry, LinkOption.NOFOLLOW_LINKS)) return null;
    try (var stream = new BufferedInputStream(Files.newInputStream(entry, LinkOption.NOFOLLOW_LINKS))) {
      var checksum = new CRC32();
      var in = new DataInputStream(new CheckedInputStream(stream, checksum));
      if (!Arrays.equals(MAGIC, in.readNBytes(MAGIC.length)) ||
          !VERSION.equals(in.readUTF()) || in.readInt() != expectedBins.length) {
        throw new IOException("Coverage cache header does not match the input map");
      }
      var valid = new long[expectedBins.length];
      for (int level = 0; level < expectedBins.length; level++) {
        final int bins = in.readInt();
        valid[level] = in.readLong();
        if (bins != expectedBins[level] || valid[level] < 0 || valid[level] > bins) {
          throw new IOException("Coverage cache level does not match the input map");
        }
      }
      var weights = new double[expectedBins.length][];
      for (int level = 0; level < expectedBins.length; level++) {
        var values = new double[expectedBins[level]];
        for (int bin = 0; bin < values.length; bin++) {
          values[bin] = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
        }
        weights[level] = values;
      }
      var trailer = new DataInputStream(stream);
      if (Integer.toUnsignedLong(trailer.readInt()) != checksum.getValue() || trailer.read() != -1) {
        throw new IOException("Coverage cache checksum or length is invalid");
      }
      return new NormalizationWeights.Prepared(VERSION, weights, valid);
    }
  }

  static void write(Path processed, String key, NormalizationWeights.Prepared prepared) throws IOException {
    Files.createDirectories(processed);
    var root = processed.toRealPath().resolve("coverage");
    Files.createDirectories(root);
    requireCoverageDirectory(root);
    var temporary = Files.createTempFile(root, key + "-", ".tmp");
    try {
      try (var stream = new BufferedOutputStream(Files.newOutputStream(temporary))) {
        var checksum = new CRC32();
        var out = new DataOutputStream(new CheckedOutputStream(stream, checksum));
        out.write(MAGIC);
        out.writeUTF(VERSION);
        final var weights = prepared.weightsByResolution();
        out.writeInt(weights.length);
        for (int level = 0; level < weights.length; level++) {
          out.writeInt(weights[level].length);
          out.writeLong(prepared.validBinsByResolution()[level]);
        }
        for (var values : weights) {
          for (double value : values) out.writeLong(Long.reverseBytes(Double.doubleToRawLongBits(value)));
        }
        new DataOutputStream(stream).writeInt((int) checksum.getValue());
      }
      Files.move(temporary, root.resolve(key + ".bin"), StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void requireCoverageDirectory(Path root) throws IOException {
    if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS) || !root.equals(root.toRealPath())) {
      throw new IOException("Saved coverage directory must stay inside processed data");
    }
  }
}
