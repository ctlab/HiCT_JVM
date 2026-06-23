package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HictToMcoolExportPipelineMergeTest {
  @TempDir
  Path tempDir;

  @Test
  void boundedMultiPassMergeAggregatesRecordsAndCleansTemporaryChunks() throws Exception {
    final var chunks = new ArrayList<Path>();
    for (int i = 0; i < 10; i++) {
      final var path = tempDir.resolve(String.format("pixels-r250-%05d.bin", i));
      writeChunk(
        path,
        new long[][]{
          {i % 3L, 7L, 1L},
          {100L + i, 3L, i + 1L}
        }
      );
      chunks.add(path);
    }

    final var output = tempDir.resolve("pixels.coo.gz");
    final var messages = new ArrayList<String>();
    final var previousFanIn = System.getProperty("hict.export.mergeFanIn");
    System.setProperty("hict.export.mergeFanIn", "3");
    try {
      invokeMergeSortedChunks(chunks, output, messages::add, () -> false);
    } finally {
      if (previousFanIn == null) {
        System.clearProperty("hict.export.mergeFanIn");
      } else {
        System.setProperty("hict.export.mergeFanIn", previousFanIn);
      }
    }

    final var rows = readGzippedLines(output);
    assertEquals("0\t7\t4", rows.get(0));
    assertEquals("1\t7\t3", rows.get(1));
    assertEquals("2\t7\t3", rows.get(2));
    assertEquals("100\t3\t1", rows.get(3));
    assertEquals("109\t3\t10", rows.get(rows.size() - 1));
    assertEquals(13, rows.size());
    assertTrue(messages.stream().anyMatch(message -> message.contains("COO merge pass")));

    for (final var chunk : chunks) {
      assertFalse(Files.exists(chunk), "Source chunk was not removed: " + chunk);
    }
    try (final var stream = Files.list(tempDir)) {
      assertTrue(stream.noneMatch(path -> path.getFileName().toString().endsWith(".bin")));
    }
  }

  private static void invokeMergeSortedChunks(final List<Path> chunks,
                                              final Path output,
                                              final Consumer<String> logger,
                                              final BooleanSupplier cancellationRequested) throws Exception {
    final var method = HictToMcoolExportPipeline.class.getDeclaredMethod(
      "mergeSortedChunks",
      List.class,
      Path.class,
      HictToMcoolExportPipeline.CooTextCompression.class,
      Consumer.class,
      BooleanSupplier.class
    );
    method.setAccessible(true);
    try {
      method.invoke(null, chunks, output, HictToMcoolExportPipeline.CooTextCompression.GZIP, logger, cancellationRequested);
    } catch (InvocationTargetException e) {
      final var cause = e.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw e;
    }
  }

  private static void writeChunk(final Path path, final long[][] records) throws IOException {
    try (final var out = new DataOutputStream(Files.newOutputStream(path))) {
      for (final var record : records) {
        out.writeLong(record[0]);
        out.writeLong(record[1]);
        out.writeLong(record[2]);
      }
    }
  }

  private static List<String> readGzippedLines(final Path path) throws IOException {
    try (final var raw = Files.newInputStream(path);
         final var gzip = new GZIPInputStream(raw);
         final var reader = new BufferedReader(new java.io.InputStreamReader(gzip, StandardCharsets.UTF_8))) {
      return reader.lines().toList();
    }
  }
}
