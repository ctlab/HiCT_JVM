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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    int contigCount = 0;
    while (contigCount < lines.size() && lines.get(contigCount).startsWith(">")) {
      ++contigCount;
    }
    if (contigCount == 0) {
      throw new IOException("Juicebox assembly file contains no contig headers");
    }
    if (contigCount == lines.size()) {
      throw new IOException("Juicebox assembly file contains no scaffold layout lines");
    }

    final List<JuiceboxAssemblyContigHeader> headers = new ArrayList<>(contigCount);
    final Map<Long, JuiceboxAssemblyContigHeader> headersById = new LinkedHashMap<>();
    long sourceStartBp0 = 0L;
    for (int i = 0; i < contigCount; i++) {
      final var header = parseContigHeader(lines.get(i), i + 1, sourceStartBp0);
      if (headersById.put(header.contigId(), header) != null) {
        throw new IOException("Duplicate Juicebox assembly contig id " + header.contigId() + " at line " + (i + 1));
      }
      headers.add(header);
      sourceStartBp0 += header.lengthBp();
    }

    final var layoutLines = lines.subList(contigCount, lines.size());
    for (int i = 0; i < layoutLines.size(); i++) {
      if (layoutLines.get(i).startsWith(">")) {
        throw new IOException("Juicebox assembly contig header found after scaffold layout at line " + (contigCount + i + 1));
      }
    }

    final List<JuiceboxAssemblyContig> contigs = new ArrayList<>(contigCount);
    if (isLegacyPerContigScaffoldLayout(layoutLines, contigCount)) {
      for (int i = 0; i < contigCount; i++) {
        final var header = headers.get(i);
        final long scaffoldId = parseSingleLongToken(layoutLines.get(i), contigCount + i + 1, "scaffold id");
        contigs.add(new JuiceboxAssemblyContig(
          i + 1,
          header.contigName(),
          header.lengthBp(),
          header.sourceStartBp0(),
          scaffoldId,
          header.orientation()
        ));
      }
      return contigs;
    }

    final Set<Long> emittedContigIds = new HashSet<>();
    for (int scaffoldLineIndex = 0; scaffoldLineIndex < layoutLines.size(); scaffoldLineIndex++) {
      final var line = layoutLines.get(scaffoldLineIndex);
      final int lineNumber = contigCount + scaffoldLineIndex + 1;
      final String[] tokens = line.split("\\s+");
      if (tokens.length == 0) {
        continue;
      }
      final long scaffoldId = scaffoldLineIndex + 1L;
      for (final var token : tokens) {
        final long signedContigId = parseLongToken(token, lineNumber, "contig id");
        final long contigId = Math.abs(signedContigId);
        final var header = headersById.get(contigId);
        if (header == null) {
          throw new IOException("Unknown Juicebox assembly contig id " + contigId + " at line " + lineNumber);
        }
        if (!emittedContigIds.add(contigId)) {
          throw new IOException("Duplicate Juicebox assembly contig id " + contigId + " in scaffold layout at line " + lineNumber);
        }
        contigs.add(new JuiceboxAssemblyContig(
          contigs.size() + 1,
          header.contigName(),
          header.lengthBp(),
          header.sourceStartBp0(),
          scaffoldId,
          applyLayoutSign(header.orientation(), signedContigId < 0L)
        ));
      }
    }

    if (emittedContigIds.size() != headers.size()) {
      for (final var header : headers) {
        if (!emittedContigIds.contains(header.contigId())) {
          throw new IOException("Juicebox assembly scaffold layout does not place contig id " + header.contigId() + " (" + header.contigName() + ")");
        }
      }
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

  private static @NotNull JuiceboxAssemblyContigHeader parseContigHeader(final @NotNull String line,
                                                                         final int lineNumber,
                                                                         final long sourceStartBp0) throws IOException {
    final var matcher = JUICEBOX_CONTIG_HEADER.matcher(line);
    if (!matcher.matches()) {
      throw new IOException("Invalid Juicebox assembly contig header at line " + lineNumber + ": " + line);
    }
    final var contigName = matcher.group(1);
    return new JuiceboxAssemblyContigHeader(
      Long.parseLong(matcher.group(2)),
      contigName,
      Long.parseLong(matcher.group(3)),
      sourceStartBp0,
      inferOrientation(contigName)
    );
  }

  private static boolean isLegacyPerContigScaffoldLayout(final @NotNull List<String> layoutLines, final int contigCount) {
    if (layoutLines.size() != contigCount) {
      return false;
    }
    final Set<Long> values = new HashSet<>();
    for (int i = 0; i < layoutLines.size(); i++) {
      final var tokens = layoutLines.get(i).split("\\s+");
      if (tokens.length != 1) {
        return false;
      }
      try {
        final long value = Long.parseLong(tokens[0]);
        if (value < 1L || value > contigCount || !values.add(value)) {
          return true;
        }
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

  private static long parseSingleLongToken(final @NotNull String line,
                                           final int lineNumber,
                                           final @NotNull String description) throws IOException {
    final var tokens = line.split("\\s+");
    if (tokens.length != 1) {
      throw new IOException("Expected one Juicebox assembly " + description + " at line " + lineNumber + ": " + line);
    }
    return parseLongToken(tokens[0], lineNumber, description);
  }

  private static long parseLongToken(final @NotNull String token,
                                     final int lineNumber,
                                     final @NotNull String description) throws IOException {
    try {
      return Long.parseLong(token);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid Juicebox assembly " + description + " at line " + lineNumber + ": " + token, e);
    }
  }

  private static @NotNull AGPProcessor.AGPContigOrientation inferOrientation(final @NotNull String contigName) {
    final var upper = contigName.toUpperCase();
    if (upper.contains("R|")) {
      return AGPProcessor.AGPContigOrientation.MINUS;
    }
    return AGPProcessor.AGPContigOrientation.PLUS;
  }

  private static @NotNull AGPProcessor.AGPContigOrientation applyLayoutSign(
    final @NotNull AGPProcessor.AGPContigOrientation orientation,
    final boolean reverse
  ) {
    if (!reverse) {
      return orientation;
    }
    return switch (orientation) {
      case PLUS -> AGPProcessor.AGPContigOrientation.MINUS;
      case MINUS -> AGPProcessor.AGPContigOrientation.PLUS;
      case UNKNOWN, IRRELEVANT -> orientation;
    };
  }

  public record JuiceboxAssemblyContig(
    int index,
    @NotNull String contigName,
    long lengthBp,
    long sourceStartBp0,
    long scaffoldId,
    @NotNull AGPProcessor.AGPContigOrientation orientation
  ) {
  }

  private record JuiceboxAssemblyContigHeader(
    long contigId,
    @NotNull String contigName,
    long lengthBp,
    long sourceStartBp0,
    @NotNull AGPProcessor.AGPContigOrientation orientation
  ) {
  }
}
