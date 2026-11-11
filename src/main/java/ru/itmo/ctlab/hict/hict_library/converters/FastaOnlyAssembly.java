/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.SourceMetadata;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleConsumer;
import java.util.zip.GZIPInputStream;

/**
 * Builds an assembly from a FASTA file alone, with no contact data.
 *
 * <p>A map needs contigs, and a FASTA file has them: every record is one contig. {@link #scan} reads the
 * file once, as a stream, and returns each record with its length and its runs of {@code N}. {@link #write}
 * turns that into a real {@code .hict.hdf5} with the usual contig, scaffold and resolution structure, an
 * empty contact store and the declared state {@code SEQUENCE_ONLY} / {@code ABSENT}, so nothing drawn
 * from it is ever a contact. Splitting at runs of {@code N} is described as an AGP layout
 * ({@link #agpLayout}) that the normal AGP import applies, so split pieces keep their original sequence
 * coordinates and the gaps between them keep their lengths.
 */
public final class FastaOnlyAssembly {
  /** Default shortest run of N that splits a record. */
  public static final int DEFAULT_MIN_N_RUN = 100;
  /** Bins above which a finer resolution is not used, so the empty map stays small. */
  private static final long MAX_BINS = 5_000_000L;

  private FastaOnlyAssembly() {
  }

  /** One FASTA record. {@code gaps} are the half-open runs of N at least {@code minNRun} long. */
  public record Record(@NotNull String id, @NotNull String description, long length, @NotNull List<long[]> gaps,
                       long firstLine) {
  }

  public record Scan(@NotNull List<Record> records, long totalBases, int skippedEmpty, int skippedAllGap,
                     @NotNull List<String> warnings) {
  }

  /** A stretch of a record that becomes one contig, in zero-based half-open source coordinates. */
  public record Segment(@NotNull String sourceId, long start, long end) {
  }

  /** Reads a plain or gzip FASTA once. Duplicate names are refused, since sequence lookup is by name. */
  public static @NotNull Scan scan(final @NotNull Path fasta,
                                   final int minNRun,
                                   final @NotNull BooleanSupplier cancelled,
                                   final @Nullable DoubleConsumer progress) throws IOException {
    if (minNRun < 1) {
      throw new IllegalArgumentException("The minimum run of N must be at least 1");
    }
    final long fileSize = Math.max(1L, Files.size(fasta));
    final var records = new ArrayList<Record>();
    final var seen = new HashMap<String, Long>();
    final var warnings = new ArrayList<String>();
    long total = 0L;
    int skippedEmpty = 0;
    int skippedAllGap = 0;
    final var countingStream = new CountingInputStream(Files.newInputStream(fasta));
    InputStream decoded = new BufferedInputStream(countingStream, 1 << 16);
    final var name = fasta.getFileName().toString().toLowerCase();
    if (name.endsWith(".gz") || name.endsWith(".bgz")) {
      decoded = new BufferedInputStream(new GZIPInputStream(decoded, 1 << 16), 1 << 16);
    }
    try (final var reader = new BufferedReader(new InputStreamReader(decoded, StandardCharsets.ISO_8859_1), 1 << 16)) {
      String id = null;
      String description = "";
      long headerLine = 0L;
      long length = 0L;
      long runStart = -1L;
      final var gaps = new ArrayList<long[]>();
      long lineNo = 0L;
      String line;
      while (true) {
        line = reader.readLine();
        final boolean end = line == null;
        if (!end) {
          lineNo++;
        }
        if (end || (!line.isEmpty() && line.charAt(0) == '>')) {
          if (id != null) {
            if (runStart >= 0 && length - runStart >= minNRun) {
              gaps.add(new long[]{runStart, length});
            }
            if (length == 0L) {
              skippedEmpty++;
              warnings.add("Record " + id + " (line " + headerLine + ") is empty and was skipped.");
            } else if (gaps.size() == 1 && gaps.get(0)[0] == 0L && gaps.get(0)[1] == length) {
              skippedAllGap++;
              warnings.add("Record " + id + " (line " + headerLine + ") is all N and was skipped.");
            } else {
              records.add(new Record(id, description, length, List.copyOf(gaps), headerLine));
              total += length;
            }
          }
          if (end) {
            break;
          }
          final var header = line.substring(1).trim();
          final int space = firstWhitespace(header);
          id = space < 0 ? header : header.substring(0, space);
          description = space < 0 ? "" : header.substring(space).trim();
          if (id.isEmpty()) {
            throw new IllegalArgumentException("Line " + lineNo + " has a record with no name");
          }
          final var previous = seen.put(id, lineNo);
          if (previous != null) {
            throw new IllegalArgumentException("Sequence name " + id + " appears twice (lines " + previous + " and "
              + lineNo + "). Names must be unique.");
          }
          headerLine = lineNo;
          length = 0L;
          runStart = -1L;
          gaps.clear();
          if (cancelled.getAsBoolean()) {
            throw new java.util.concurrent.CancellationException("Reading the FASTA was cancelled");
          }
          if (progress != null) {
            progress.accept(Math.min(0.99d, countingStream.count() / (double) fileSize));
          }
          continue;
        }
        if (id == null) {
          if (line.isBlank()) {
            continue;
          }
          throw new IllegalArgumentException("Line " + lineNo + " has sequence before the first '>' header");
        }
        for (int i = 0; i < line.length(); i++) {
          final char c = line.charAt(i);
          if (c == ' ' || c == '\t' || c == '\r') {
            continue;
          }
          if (c == 'N' || c == 'n') {
            if (runStart < 0) {
              runStart = length;
            }
          } else if (runStart >= 0) {
            if (length - runStart >= minNRun) {
              gaps.add(new long[]{runStart, length});
            }
            runStart = -1L;
          }
          length++;
        }
      }
    }
    if (records.isEmpty()) {
      throw new IllegalArgumentException("The FASTA file has no sequence to make contigs from.");
    }
    if (progress != null) {
      progress.accept(1.0d);
    }
    return new Scan(List.copyOf(records), total, skippedEmpty, skippedAllGap, List.copyOf(warnings));
  }

  private static int firstWhitespace(final String text) {
    for (int i = 0; i < text.length(); i++) {
      if (Character.isWhitespace(text.charAt(i))) {
        return i;
      }
    }
    return -1;
  }

  /** The pieces of a record: whole, or the stretches between its N runs. Never empty, never overlapping. */
  public static @NotNull List<Segment> segments(final @NotNull Record record, final boolean split) {
    if (!split || record.gaps().isEmpty()) {
      return List.of(new Segment(record.id(), 0L, record.length()));
    }
    final var result = new ArrayList<Segment>();
    long cursor = 0L;
    for (final var gap : record.gaps()) {
      if (gap[0] > cursor) {
        result.add(new Segment(record.id(), cursor, gap[0]));
      }
      cursor = gap[1];
    }
    if (record.length() > cursor) {
      result.add(new Segment(record.id(), cursor, record.length()));
    }
    return List.copyOf(result);
  }

  /**
   * AGP (version 2) that splits records at their N runs. One object per record, components in order, a
   * gap line of the true length wherever a run was removed. Returns null when nothing would be split.
   */
  public static @Nullable String agpLayout(final @NotNull Scan scan) {
    boolean any = false;
    final var out = new StringBuilder("##agp-version\t2.0\n");
    for (final var record : scan.records()) {
      final var pieces = segments(record, true);
      if (pieces.size() == 1 && record.gaps().isEmpty()) {
        out.append(record.id()).append("\t1\t").append(record.length()).append("\t1\tW\t").append(record.id())
          .append("\t1\t").append(record.length()).append("\t+\n");
        continue;
      }
      any = true;
      long position = 0L;
      int part = 1;
      long previousEnd = -1L;
      for (final var piece : pieces) {
        if (previousEnd >= 0L && piece.start() > previousEnd) {
          final long gapLength = piece.start() - previousEnd;
          out.append(record.id()).append('\t').append(position + 1).append('\t').append(position + gapLength)
            .append('\t').append(part++).append("\tN\t").append(gapLength).append("\tcontig\tno\tna\n");
          position += gapLength;
        }
        final long length = piece.end() - piece.start();
        out.append(record.id()).append('\t').append(position + 1).append('\t').append(position + length).append('\t')
          .append(part++).append("\tW\t").append(piece.sourceId()).append('\t').append(piece.start() + 1).append('\t')
          .append(piece.end()).append("\t+\n");
        position += length;
        previousEnd = piece.end();
      }
    }
    return any ? out.toString() : null;
  }

  /** Smallest power-of-ten resolution that keeps the empty map under {@link #MAX_BINS} bins. */
  public static long baseResolution(final long totalBases) {
    long resolution = 1L;
    while (totalBases / resolution > MAX_BINS) {
      resolution *= 10L;
    }
    return resolution;
  }

  /** A short pyramid, so the empty map can be zoomed out like any other: R, 10R, 100R ... */
  public static @NotNull List<Long> resolutions(final long totalBases) {
    final var result = new ArrayList<Long>();
    long resolution = baseResolution(totalBases);
    result.add(resolution);
    while (result.size() < 6 && totalBases / resolution > 2_000L) {
      resolution *= 10L;
      result.add(resolution);
    }
    return List.copyOf(result);
  }

  /**
   * Writes {@code output} (a {@code .hict.hdf5}) with one contig per record. The temporary Cooler-shaped
   * input lives next to it and is removed.
   */
  public static void write(final @NotNull Scan scan,
                           final @NotNull Path output,
                           final @NotNull BooleanSupplier cancelled,
                           final @Nullable DoubleConsumer progress) throws Exception {
    final var resolutions = resolutions(scan.totalBases());
    Files.createDirectories(output.toAbsolutePath().getParent());
    final var temporary = output.resolveSibling(output.getFileName() + ".input.mcool");
    final var partial = output.resolveSibling(output.getFileName() + ".partial");
    Files.deleteIfExists(temporary);
    Files.deleteIfExists(partial);
    HDF5LibraryInitializer.initializeHDF5Library();
    try {
      final var names = scan.records().stream().map(Record::id).toArray(String[]::new);
      final var lengths = scan.records().stream().mapToLong(Record::length).toArray();
      try (final var writer = HDF5Factory.open(temporary.toFile())) {
        writer.string().writeArray("/chroms/name", names);
        writer.int64().writeArray("/chroms/length", lengths);
        writer.int32().setAttr("/", SourceMetadata.VERSION, 1);
        writer.string().setAttr("/", SourceMetadata.UNITS, SourceMetadata.Units.SEQUENCE_ONLY.name());
        writer.string().setAttr("/", SourceMetadata.CONTACT_STATE, SourceMetadata.ContactDataState.ABSENT.name());
        for (final long resolution : resolutions) {
          writeEmptyResolution(writer, resolution, lengths);
        }
      }
      if (cancelled.getAsBoolean()) {
        throw new java.util.concurrent.CancellationException("Building the assembly was cancelled");
      }
      new McoolToHictConverter().convert(
        new ConversionOptions(temporary, partial, resolutions, 64, 1, ConversionOptions.CompressionAlgorithm.DEFLATE,
          ConversionOptions.NO_AGP, false, 1, false, ConversionOptions.ExportMode.INTERNAL),
        ignored -> { }
      );
      Files.move(partial, output, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
      if (progress != null) {
        progress.accept(1.0d);
      }
    } finally {
      Files.deleteIfExists(temporary);
      Files.deleteIfExists(partial);
    }
  }

  private static void writeEmptyResolution(final ch.systemsx.cisd.hdf5.IHDF5Writer writer,
                                           final long resolution,
                                           final long[] chromosomeLengths) {
    final var starts = new ArrayList<Long>();
    final var ends = new ArrayList<Long>();
    final var chromosomes = new ArrayList<Long>();
    final long[] chromOffset = new long[chromosomeLengths.length + 1];
    for (int c = 0; c < chromosomeLengths.length; c++) {
      chromOffset[c] = starts.size();
      for (long start = 0; start < chromosomeLengths[c]; start += resolution) {
        starts.add(start);
        ends.add(Math.min(chromosomeLengths[c], start + resolution));
        chromosomes.add((long) c);
      }
    }
    chromOffset[chromosomeLengths.length] = starts.size();
    final String root = "/resolutions/" + resolution + "/";
    final long[] bin1Offset = new long[starts.size() + 1];
    writer.int64().writeArray(root + "indexes/chrom_offset", chromOffset);
    writer.int64().writeArray(root + "indexes/bin1_offset", bin1Offset);
    writer.int64().writeArray(root + "bins/chrom", chromosomes.stream().mapToLong(Long::longValue).toArray());
    writer.int64().writeArray(root + "bins/start", starts.stream().mapToLong(Long::longValue).toArray());
    writer.int64().writeArray(root + "bins/end", ends.stream().mapToLong(Long::longValue).toArray());
    writer.int64().writeArray(root + "pixels/bin1_id", new long[0]);
    writer.int64().writeArray(root + "pixels/bin2_id", new long[0]);
    writer.int64().writeArray(root + "pixels/count", new long[0]);
  }

  private static final class CountingInputStream extends InputStream {
    private final InputStream delegate;
    private long count;

    private CountingInputStream(final InputStream delegate) {
      this.delegate = delegate;
    }

    long count() {
      return count;
    }

    @Override
    public int read() throws IOException {
      final int b = delegate.read();
      if (b >= 0) {
        count++;
      }
      return b;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
      final int n = delegate.read(buffer, offset, length);
      if (n > 0) {
        count += n;
      }
      return n;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
