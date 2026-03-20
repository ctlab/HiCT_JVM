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

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

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
import java.util.Locale;
import java.util.Map;
import java.util.Comparator;
import java.util.zip.GZIPInputStream;

@RequiredArgsConstructor
public class FASTAProcessor {
  private static final int MAX_MISMATCHES_IN_REPORT = 10;

  private final @NotNull ChunkedFile chunkedFile;

  public @NotNull FASTALinkCompatibilityReport analyzeLinkCandidate(final @NotNull Path fastaPath) {
    final var fastaRecords = readSequenceSummaries(fastaPath);
    final var assemblyEntries = currentAssemblyEntries();

    final boolean sameRecordCount = fastaRecords.size() == assemblyEntries.size();
    final int sharedCount = Math.min(fastaRecords.size(), assemblyEntries.size());

    boolean sameOrderAndLength = sameRecordCount;
    boolean sameOrderLengthAndCurrentNames = sameRecordCount;
    boolean sameOrderLengthAndOriginalNames = sameRecordCount;
    boolean sameOrderLengthAndSourceNames = sameRecordCount;

    final var mismatches = new ArrayList<FASTALinkCompatibilityReport.MismatchAtIndex>();
    for (int i = 0; i < sharedCount; ++i) {
      final var fasta = fastaRecords.get(i);
      final var assembly = assemblyEntries.get(i);
      final boolean sameLength = fasta.lengthBp() == assembly.lengthBp();
      final boolean anyNameMatch = fasta.name().equals(assembly.currentName())
        || fasta.name().equals(assembly.originalName())
        || fasta.name().equals(assembly.sourceName());
      sameOrderAndLength &= sameLength;
      sameOrderLengthAndCurrentNames &= sameLength && fasta.name().equals(assembly.currentName());
      sameOrderLengthAndOriginalNames &= sameLength && fasta.name().equals(assembly.originalName());
      sameOrderLengthAndSourceNames &= sameLength && fasta.name().equals(assembly.sourceName());
      if ((!sameLength || !anyNameMatch)
        && mismatches.size() < MAX_MISMATCHES_IN_REPORT) {
        mismatches.add(new FASTALinkCompatibilityReport.MismatchAtIndex(
          i,
          fasta.name(),
          fasta.lengthBp(),
          assembly.currentName(),
          assembly.originalName(),
          assembly.sourceName(),
          assembly.lengthBp()
        ));
      }
    }

    for (int i = sharedCount; i < fastaRecords.size() && mismatches.size() < MAX_MISMATCHES_IN_REPORT; ++i) {
      final var fasta = fastaRecords.get(i);
      mismatches.add(new FASTALinkCompatibilityReport.MismatchAtIndex(
        i,
        fasta.name(),
        fasta.lengthBp(),
        null,
        null,
        null,
        -1L
      ));
    }
    for (int i = sharedCount; i < assemblyEntries.size() && mismatches.size() < MAX_MISMATCHES_IN_REPORT; ++i) {
      final var assembly = assemblyEntries.get(i);
      mismatches.add(new FASTALinkCompatibilityReport.MismatchAtIndex(
        i,
        null,
        -1L,
        assembly.currentName(),
        assembly.originalName(),
        assembly.sourceName(),
        assembly.lengthBp()
      ));
    }

    final boolean sameLengthMultiset = sameRecordCount
      && fastaRecords.stream().map(FASTASequenceSummary::lengthBp).sorted().toList()
      .equals(assemblyEntries.stream().map(AssemblyContigEntry::lengthBp).sorted().toList());

    final var warnings = new ArrayList<String>();
    if (!sameRecordCount) {
      warnings.add(String.format(
        Locale.ROOT,
        "FASTA contains %d sequences, while the current Hi-C assembly contains %d contigs.",
        fastaRecords.size(),
        assemblyEntries.size()
      ));
    }
    if (!sameOrderAndLength) {
      if (sameLengthMultiset) {
        warnings.add("FASTA sequence lengths match as a set, but the current Hi-C assembly order differs.");
      } else {
        warnings.add("FASTA sequence order and lengths do not match the current Hi-C assembly.");
      }
    } else if (!(sameOrderLengthAndCurrentNames || sameOrderLengthAndOriginalNames || sameOrderLengthAndSourceNames)) {
      warnings.add("FASTA sequence lengths and order match the current Hi-C assembly, but contig names differ.");
    }

    return new FASTALinkCompatibilityReport(
      fastaPath.getFileName().toString(),
      fastaRecords.size(),
      assemblyEntries.size(),
      sameRecordCount,
      sameOrderAndLength,
      sameOrderLengthAndCurrentNames,
      sameOrderLengthAndOriginalNames,
      sameOrderLengthAndSourceNames,
      sameLengthMultiset,
      warnings,
      mismatches
    );
  }

  public @NotNull Map<String, String> buildSourceNameAliases(final @NotNull Path fastaPath) {
    final var fastaRecords = readSequenceSummaries(fastaPath);
    final var sourceDescriptors = this.chunkedFile.getOriginalDescriptors().values().stream()
      .sorted(Comparator.comparingInt(ContigDescriptor::getContigId))
      .toList();
    if (fastaRecords.size() != sourceDescriptors.size()) {
      return Map.of();
    }
    for (int i = 0; i < fastaRecords.size(); ++i) {
      if (fastaRecords.get(i).lengthBp() != sourceDescriptors.get(i).getLengthBp()) {
        return Map.of();
      }
    }
    final var aliases = new HashMap<String, String>();
    for (int i = 0; i < fastaRecords.size(); ++i) {
      aliases.put(sourceDescriptors.get(i).getContigNameInSourceFASTA(), fastaRecords.get(i).name());
    }
    return aliases;
  }

  public @NotNull String exportAssembly(final @NotNull Path fastaPath) {
    final var sequences = readSequenceContents(fastaPath);
    final var records = new ArrayList<FASTARecord>();
    final var contigs = this.chunkedFile.getAssemblyInfo().contigs();
    final var scaffolds = this.chunkedFile.getAssemblyInfo().scaffolds();

    int scaffoldIndex = 0;
    long assemblyPosition = 0L;
    final var currentUnscaffolded = new StringBuilder();
    String currentUnscaffoldedName = null;
    final var currentScaffold = new StringBuilder();
    String currentScaffoldName = null;
    long currentScaffoldEnd = -1L;
    long currentSpacerLength = 0L;
    boolean scaffoldHasContent = false;

    for (final var contig : contigs) {
      while (scaffoldIndex < scaffolds.size()
        && scaffolds.get(scaffoldIndex).scaffoldBordersBP().endBP() <= assemblyPosition) {
        scaffoldIndex++;
      }

      final var sequence = extractContigSequence(sequences, contig);
      final ScaffoldTree.ScaffoldTuple coveringScaffold =
        scaffoldIndex < scaffolds.size()
          && scaffolds.get(scaffoldIndex).scaffoldDescriptor() != null
          && scaffolds.get(scaffoldIndex).scaffoldBordersBP().startBP() <= assemblyPosition
          && assemblyPosition < scaffolds.get(scaffoldIndex).scaffoldBordersBP().endBP()
          ? scaffolds.get(scaffoldIndex)
          : null;

      if (coveringScaffold != null) {
        if (currentUnscaffoldedName != null && currentUnscaffolded.length() > 0) {
          records.add(new FASTARecord(currentUnscaffoldedName, currentUnscaffolded.toString()));
          currentUnscaffolded.setLength(0);
          currentUnscaffoldedName = null;
        }
        final ScaffoldDescriptor scaffoldDescriptor = coveringScaffold.scaffoldDescriptor();
        final String scaffoldName = this.chunkedFile.getScaffoldDisplayName(scaffoldDescriptor.scaffoldId());
        if (!scaffoldName.equals(currentScaffoldName)) {
          if (currentScaffoldName != null && currentScaffold.length() > 0) {
            records.add(new FASTARecord(currentScaffoldName, currentScaffold.toString()));
            currentScaffold.setLength(0);
          }
          currentScaffoldName = scaffoldName;
          currentScaffoldEnd = coveringScaffold.scaffoldBordersBP().endBP();
          currentSpacerLength = scaffoldDescriptor.spacerLength();
          scaffoldHasContent = false;
        }
        if (scaffoldHasContent && currentSpacerLength > 0) {
          currentScaffold.append("N".repeat((int) Math.min(Integer.MAX_VALUE, currentSpacerLength)));
        }
        currentScaffold.append(sequence);
        scaffoldHasContent = true;
        if (assemblyPosition + contig.descriptor().getLengthBp() >= currentScaffoldEnd) {
          records.add(new FASTARecord(currentScaffoldName, currentScaffold.toString()));
          currentScaffold.setLength(0);
          currentScaffoldName = null;
          currentScaffoldEnd = -1L;
          currentSpacerLength = 0L;
          scaffoldHasContent = false;
        }
      } else {
        if (currentScaffoldName != null && currentScaffold.length() > 0) {
          records.add(new FASTARecord(currentScaffoldName, currentScaffold.toString()));
          currentScaffold.setLength(0);
          currentScaffoldName = null;
          currentScaffoldEnd = -1L;
          currentSpacerLength = 0L;
          scaffoldHasContent = false;
        }
        currentUnscaffoldedName = this.chunkedFile.getContigDisplayName(contig.descriptor().getContigId());
        currentUnscaffolded.setLength(0);
        currentUnscaffolded.append(sequence);
        records.add(new FASTARecord(currentUnscaffoldedName, currentUnscaffolded.toString()));
        currentUnscaffolded.setLength(0);
        currentUnscaffoldedName = null;
      }

      assemblyPosition += contig.descriptor().getLengthBp();
    }

    if (currentScaffoldName != null && currentScaffold.length() > 0) {
      records.add(new FASTARecord(currentScaffoldName, currentScaffold.toString()));
    }
    if (records.isEmpty()) {
      throw new IllegalStateException("Current assembly does not contain any contigs");
    }
    return renderRecords(records);
  }

  public @NotNull String exportSelection(final @NotNull Path fastaPath,
                                         final long fromBpX,
                                         final long fromBpY,
                                         final long toBpX,
                                         final long toBpY) {
    final var sequences = readSequenceContents(fastaPath);
    final long selectionStart = Math.max(0L, Math.min(Math.min(fromBpX, fromBpY), Math.min(toBpX, toBpY)));
    final long selectionEnd = Math.max(selectionStart + 1L, Math.max(Math.max(fromBpX, fromBpY), Math.max(toBpX, toBpY)));
    final var builder = new StringBuilder();
    long assemblyPosition = 0L;
    for (final var contig : this.chunkedFile.getAssemblyInfo().contigs()) {
      final long contigStart = assemblyPosition;
      final long contigEnd = assemblyPosition + contig.descriptor().getLengthBp();
      final long overlapStart = Math.max(selectionStart, contigStart);
      final long overlapEnd = Math.min(selectionEnd, contigEnd);
      if (overlapEnd > overlapStart) {
        final long startInsideContig = overlapStart - contigStart;
        final long endInsideContig = overlapEnd - contigStart;
        builder.append(extractContigSlice(sequences, contig, startInsideContig, endInsideContig));
      }
      assemblyPosition = contigEnd;
      if (assemblyPosition >= selectionEnd) {
        break;
      }
    }
    if (builder.isEmpty()) {
      throw new IllegalArgumentException("Selected region does not intersect the current assembly");
    }
    return renderRecords(List.of(new FASTARecord(
      String.format(Locale.ROOT, "selection_%d_%d", selectionStart, selectionEnd),
      builder.toString()
    )));
  }

  private @NotNull List<AssemblyContigEntry> currentAssemblyEntries() {
    final var entries = new ArrayList<AssemblyContigEntry>();
    for (final ContigTree.ContigTuple tuple : this.chunkedFile.getAssemblyInfo().contigs()) {
      final var descriptor = tuple.descriptor();
      entries.add(new AssemblyContigEntry(
        this.chunkedFile.getContigDisplayName(descriptor.getContigId()),
        this.chunkedFile.getContigOriginalName(descriptor.getContigId()),
        descriptor.getContigNameInSourceFASTA(),
        descriptor.getLengthBp()
      ));
    }
    return entries;
  }

  public static @NotNull List<FASTASequenceSummary> readSequenceSummaries(final @NotNull Path fastaPath) {
    final var records = new ArrayList<FASTASequenceSummary>();
    try (final var stream = openPossiblyGzippedStream(fastaPath);
         final var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String currentName = null;
      long currentLength = 0L;
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(">")) {
          if (currentName != null) {
            records.add(new FASTASequenceSummary(currentName, currentLength));
          }
          currentName = parseHeaderName(line);
          currentLength = 0L;
          continue;
        }
        if (currentName == null || line.isBlank()) {
          continue;
        }
        currentLength += line.trim().length();
      }
      if (currentName != null) {
        records.add(new FASTASequenceSummary(currentName, currentLength));
      }
    } catch (final IOException e) {
      throw new RuntimeException("Failed to read FASTA file " + fastaPath, e);
    }
    if (records.isEmpty()) {
      throw new IllegalArgumentException("FASTA file " + fastaPath.getFileName() + " does not contain any sequences");
    }
    return records;
  }

  public static @NotNull Map<String, String> readSequenceContents(final @NotNull Path fastaPath) {
    final var sequences = new HashMap<String, StringBuilder>();
    final var order = new ArrayList<String>();
    try (final var stream = openPossiblyGzippedStream(fastaPath);
         final var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String currentName = null;
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(">")) {
          currentName = parseHeaderName(line);
          if (!sequences.containsKey(currentName)) {
            sequences.put(currentName, new StringBuilder());
            order.add(currentName);
          }
          continue;
        }
        if (currentName == null || line.isBlank()) {
          continue;
        }
        sequences.get(currentName).append(line.trim());
      }
    } catch (final IOException e) {
      throw new RuntimeException("Failed to read FASTA file " + fastaPath, e);
    }
    final var result = new HashMap<String, String>();
    for (final var name : order) {
      result.put(name, sequences.get(name).toString());
    }
    return result;
  }

  private static @NotNull InputStream openPossiblyGzippedStream(final @NotNull Path fastaPath) throws IOException {
    final var stream = Files.newInputStream(fastaPath);
    final var name = fastaPath.getFileName().toString().toLowerCase(Locale.ROOT);
    if (name.endsWith(".gz")) {
      return new GZIPInputStream(stream);
    }
    return stream;
  }

  private static @NotNull String parseHeaderName(final @NotNull String line) {
    final var header = line.substring(1).trim();
    if (header.isEmpty()) {
      throw new IllegalArgumentException("Encountered an empty FASTA header");
    }
    final int firstWhitespace = findFirstWhitespace(header);
    return (firstWhitespace >= 0 ? header.substring(0, firstWhitespace) : header).trim();
  }

  private static int findFirstWhitespace(final @NotNull String input) {
    for (int i = 0; i < input.length(); ++i) {
      if (Character.isWhitespace(input.charAt(i))) {
        return i;
      }
    }
    return -1;
  }

  private @NotNull String extractContigSequence(final @NotNull Map<String, String> sequences,
                                                final @NotNull ContigTree.ContigTuple contig) {
    return extractContigSlice(sequences, contig, 0L, contig.descriptor().getLengthBp());
  }

  private @NotNull String extractContigSlice(final @NotNull Map<String, String> sequences,
                                             final @NotNull ContigTree.ContigTuple contig,
                                             final long startInsideContig,
                                             final long endInsideContig) {
    final var descriptor = contig.descriptor();
    final var source = sequences.get(descriptor.getContigNameInSourceFASTA());
    if (source == null) {
      throw new IllegalArgumentException(
        "Linked FASTA does not contain sequence '" + descriptor.getContigNameInSourceFASTA() + "'"
      );
    }
    final int sourceStart = Math.toIntExact(descriptor.getOffsetInSourceFASTA() + startInsideContig);
    final int sourceEnd = Math.toIntExact(descriptor.getOffsetInSourceFASTA() + endInsideContig);
    if (sourceStart < 0 || sourceEnd > source.length() || sourceStart >= sourceEnd) {
      throw new IllegalArgumentException(
        "Requested source FASTA slice [" + sourceStart + ", " + sourceEnd + ") is outside sequence '"
          + descriptor.getContigNameInSourceFASTA() + "' of length " + source.length()
      );
    }
    final var subsequence = source.substring(sourceStart, sourceEnd);
    return switch (contig.direction()) {
      case FORWARD -> subsequence;
      case REVERSED -> reverseComplement(subsequence);
    };
  }

  private static @NotNull String reverseComplement(final @NotNull String sequence) {
    final var builder = new StringBuilder(sequence.length());
    for (int i = sequence.length() - 1; i >= 0; --i) {
      builder.append(complement(sequence.charAt(i)));
    }
    return builder.toString();
  }

  private static char complement(final char base) {
    return switch (Character.toUpperCase(base)) {
      case 'A' -> 'T';
      case 'T' -> 'A';
      case 'C' -> 'G';
      case 'G' -> 'C';
      case 'N' -> 'N';
      case 'R' -> 'Y';
      case 'Y' -> 'R';
      case 'S' -> 'S';
      case 'W' -> 'W';
      case 'K' -> 'M';
      case 'M' -> 'K';
      case 'B' -> 'V';
      case 'D' -> 'H';
      case 'H' -> 'D';
      case 'V' -> 'B';
      default -> 'N';
    };
  }

  private static @NotNull String renderRecords(final @NotNull List<FASTARecord> records) {
    final var builder = new StringBuilder();
    for (final var record : records) {
      builder.append('>').append(record.name()).append('\n');
      final var sequence = record.sequence();
      for (int i = 0; i < sequence.length(); i += 80) {
        builder.append(sequence, i, Math.min(sequence.length(), i + 80)).append('\n');
      }
    }
    return builder.toString();
  }

  private record AssemblyContigEntry(
    @NotNull String currentName,
    @NotNull String originalName,
    @NotNull String sourceName,
    long lengthBp
  ) {
  }

  public record FASTASequenceSummary(@NotNull String name, long lengthBp) {
  }

  private record FASTARecord(@NotNull String name, @NotNull String sequence) {
  }

  public record FASTALinkCompatibilityReport(
    @NotNull String fastaFilename,
    int fastaRecordCount,
    int assemblyContigCount,
    boolean sameRecordCount,
    boolean sameOrderAndLength,
    boolean sameOrderLengthAndCurrentNames,
    boolean sameOrderLengthAndOriginalNames,
    boolean sameOrderLengthAndSourceNames,
    boolean sameLengthMultiset,
    @NotNull List<@NotNull String> warnings,
    @NotNull List<@NotNull MismatchAtIndex> mismatches
  ) {
    public FASTALinkCompatibilityReport {
      warnings = List.copyOf(warnings);
      mismatches = List.copyOf(mismatches);
    }

    public boolean hasWarnings() {
      return !warnings.isEmpty();
    }

    public record MismatchAtIndex(
      int index,
      @Nullable String fastaName,
      long fastaLengthBp,
      @Nullable String assemblyCurrentName,
      @Nullable String assemblyOriginalName,
      @Nullable String assemblySourceName,
      long assemblyLengthBp
    ) {
    }
  }
}
