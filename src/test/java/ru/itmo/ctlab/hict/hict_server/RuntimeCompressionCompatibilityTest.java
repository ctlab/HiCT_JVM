package ru.itmo.ctlab.hict.hict_server;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** Exercise delivered transitive codecs, including Snappy's platform native loader. */
class RuntimeCompressionCompatibilityTest {
  private static final byte[] SOURCE = (">contig-α\nACGTNNacgt\n".repeat(4096))
    .getBytes(StandardCharsets.UTF_8);

  private static void roundTrip(String writerName, String readerName) throws Exception {
    var encoded = new ByteArrayOutputStream();
    try (var writer = (OutputStream) Class.forName(writerName).getConstructor(OutputStream.class).newInstance(encoded)) {
      writer.write(SOURCE, 0, 17);
      writer.write(SOURCE, 17, SOURCE.length - 17);
    }
    try (var reader = (InputStream) Class.forName(readerName).getConstructor(InputStream.class)
      .newInstance(new ByteArrayInputStream(encoded.toByteArray()))) {
      assertArrayEquals(SOURCE, reader.readAllBytes());
    }
  }

  @Test void deliveredSnappySupportsNativeArraysAndStreamingRoundTrips() throws Exception {
    var snappy = Class.forName("org.xerial.snappy.Snappy");
    byte[] compressed = (byte[]) snappy.getMethod("compress", byte[].class).invoke(null, (Object) SOURCE);
    assertArrayEquals(SOURCE, (byte[]) snappy.getMethod("uncompress", byte[].class).invoke(null, (Object) compressed));
    roundTrip("org.xerial.snappy.SnappyOutputStream", "org.xerial.snappy.SnappyInputStream");
  }

  @Test void deliveredCommonsCompressPreservesSequenceBytes() throws Exception {
    roundTrip("org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream",
      "org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream");
  }
}
