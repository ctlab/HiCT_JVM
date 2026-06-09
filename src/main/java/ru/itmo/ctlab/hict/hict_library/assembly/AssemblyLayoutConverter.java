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

package ru.itmo.ctlab.hict.hict_library.assembly;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class AssemblyLayoutConverter {
  private static final @NotNull Pattern JUICEBOX_CONTIG_HEADER = Pattern.compile("^>(\\S+)\\s+(\\d+)\\s+(\\d+)$");

  private AssemblyLayoutConverter() {
  }

  public static void convertToAgp(final @NotNull Path sourcePath,
                                  final @NotNull Path outputPath) throws IOException, NoSuchFieldException {
    final var parent = outputPath.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    final List<AGPProcessor.AGPFileRecord> records;
    if (isJuiceboxAssemblyFilename(sourcePath.getFileName().toString())) {
      records = parseJuiceboxAssembly(sourcePath);
    } else {
      try (final var reader = Files.newBufferedReader(sourcePath, StandardCharsets.UTF_8)) {
        records = AGPProcessor.parseRecordsFromReader(reader);
      }
    }

    AGPProcessor.writeRecordsAsAgp(records, outputPath);
  }

  public static @NotNull List<AGPProcessor.AGPFileRecord> loadAgpRecords(final @NotNull Path sourcePath) throws IOException, NoSuchFieldException {
    if (isJuiceboxAssemblyFilename(sourcePath.getFileName().toString())) {
      return parseJuiceboxAssembly(sourcePath);
    }
    try (final var reader = Files.newBufferedReader(sourcePath, StandardCharsets.UTF_8)) {
      return AGPProcessor.parseRecordsFromReader(reader);
    }
  }

  public static @NotNull List<AGPProcessor.AGPFileRecord> parseJuiceboxAssembly(final @NotNull Path sourcePath) throws IOException {
    try (final var reader = Files.newBufferedReader(sourcePath, StandardCharsets.UTF_8)) {
      return parseJuiceboxAssembly(reader);
    }
  }

  public static @NotNull List<AGPProcessor.AGPFileRecord> parseJuiceboxAssembly(final @NotNull Reader reader) throws IOException {
    final var contigs = parseJuiceboxContigs(reader);
    return toAgpRecords(contigs);
  }

  public static @NotNull List<JuiceboxAssemblyContig> parseJuiceboxContigs(final @NotNull Reader reader) throws IOException {
    final List<String> lines = new ArrayList<>();
    try (final var bufferedReader = new BufferedReader(reader)) {
      bufferedReader.lines()
        .map(String::trim)
        .filter(line -> !line.isBlank() && !line.startsWith("#"))
        .forEachOrdered(lines::add);
    }

    if (lines.isEmpty()) {
      throw new IOException("Juicebox assembly file is empty");
    }
    if ((lines.size() & 1) != 0) {
      throw new IOException("Juicebox assembly file must contain an even number of non-empty lines");
    }

    final int contigCount = lines.size() / 2;
    final List<JuiceboxAssemblyContig> contigs = new ArrayList<>(contigCount);
    for (int i = 0; i < contigCount; i++) {
      final var header = parseContigHeader(lines.get(i), i + 1);
      final long scaffoldId = parseLongLine(lines.get(contigCount + i), i + 1);
      contigs.add(new JuiceboxAssemblyContig(
        i + 1,
        header.contigName(),
        header.lengthBp(),
        scaffoldId,
        inferOrientation(header.contigName())
      ));
    }
    return contigs;
  }

  public static @NotNull List<AGPProcessor.AGPFileRecord> toAgpRecords(final @NotNull List<JuiceboxAssemblyContig> contigs) {
    final Map<Long, List<JuiceboxAssemblyContig>> contigsByScaffold = new LinkedHashMap<>();
    for (final var contig : contigs) {
      contigsByScaffold.computeIfAbsent(contig.scaffoldId(), ignored -> new ArrayList<>()).add(contig);
    }

    final List<AGPProcessor.AGPFileRecord> records = new ArrayList<>(contigs.size());
    for (final var entry : contigsByScaffold.entrySet()) {
      final String scaffoldName = Long.toString(entry.getKey());
      long objectBeg = 1L;
      int partNumber = 1;
      for (final var contig : entry.getValue()) {
        final long objectEnd = objectBeg + contig.lengthBp() - 1L;
        records.add(new AGPProcessor.ContigAGPRecord(
          scaffoldName,
          objectBeg,
          objectEnd,
          partNumber,
          contig.contigName(),
          1L,
          contig.lengthBp(),
          contig.orientation()
        ));
        objectBeg = objectEnd + 1L;
        ++partNumber;
      }
    }
    return records;
  }

  private static boolean isJuiceboxAssemblyFilename(final @NotNull String filename) {
    return filename.toLowerCase().endsWith(".assembly");
  }

  private static @NotNull JuiceboxAssemblyContigHeader parseContigHeader(final @NotNull String line, final int lineNumber) throws IOException {
    final var matcher = JUICEBOX_CONTIG_HEADER.matcher(line);
    if (!matcher.matches()) {
      throw new IOException("Invalid Juicebox assembly contig header at line " + lineNumber + ": " + line);
    }
    return new JuiceboxAssemblyContigHeader(matcher.group(1), Long.parseLong(matcher.group(3)));
  }

  private static long parseLongLine(final @NotNull String line, final int lineNumber) throws IOException {
    try {
      return Long.parseLong(line);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid Juicebox assembly scaffold id at line " + lineNumber + ": " + line, e);
    }
  }

  private static @NotNull AGPProcessor.AGPContigOrientation inferOrientation(final @NotNull String contigName) {
    final var upper = contigName.toUpperCase();
    if (upper.contains("R|")) {
      return AGPProcessor.AGPContigOrientation.MINUS;
    }
    return AGPProcessor.AGPContigOrientation.PLUS;
  }

  public record JuiceboxAssemblyContig(
    int index,
    @NotNull String contigName,
    long lengthBp,
    long scaffoldId,
    @NotNull AGPProcessor.AGPContigOrientation orientation
  ) {
  }

  private record JuiceboxAssemblyContigHeader(@NotNull String contigName, long lengthBp) {
  }
}
