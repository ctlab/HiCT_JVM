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
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Comparator;
import java.util.zip.GZIPInputStream;

@RequiredArgsConstructor
public class FASTAProcessor implements AutoCloseable {
  private static final int MAX_MISMATCHES_IN_REPORT = 10;

  private final @NotNull ChunkedFile chunkedFile;

  public @NotNull FASTALinkCompatibilityReport analyzeLinkCandidate(final @NotNull Path fastaPath) {
    final var fastaRecords = sourceSummaries(fastaPath);
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

    // Export resolves source names and offsets, not current assembly order.
    final var sourceLengths = new HashMap<String, Long>();
    for (final var record : fastaRecords) sourceLengths.put(record.name(), record.lengthBp());
    final boolean sourceIntervalsCovered = assemblyEntries.stream().allMatch(entry -> {
      final var sourceLength = sourceLengths.get(entry.sourceName());
      return sourceLength != null && entry.sourceOffset() >= 0
        && entry.sourceOffset() <= sourceLength
        && entry.lengthBp() <= sourceLength - entry.sourceOffset();
    });
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

    if (sourceIntervalsCovered) warnings.clear();
    else if (warnings.isEmpty()) warnings.add("FASTA does not cover all source names and intervals required by the assembly.");

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
      sourceIntervalsCovered,
      warnings,
      mismatches
    );
  }

  public @NotNull Map<String, String> buildSourceNameAliases(final @NotNull Path fastaPath) {
    final var fastaRecords = sourceSummaries(fastaPath);
    final var sourceDescriptors = this.chunkedFile.getOriginalDescriptors().values().stream()
      .sorted(Comparator.comparingInt(ContigDescriptor::getContigId))
      .toList();
    // Exact source identifiers always win. Positional fallback is only safe when
    // no source identifier exists in the supplied FASTA; mixed mappings are ambiguous.
    final var names = fastaRecords.stream().map(FASTASequenceSummary::name).collect(java.util.stream.Collectors.toSet());
    if (sourceDescriptors.stream().anyMatch(d -> names.contains(d.getContigNameInSourceFASTA()))) return Map.of();
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

  private volatile IndexedFastaSource sourceIndex;
  private boolean closed;

  /** Bounded, gap-free current-assembly sequence for nucleotide-level tracks. */
  public String readRegion(Path path, long start, long end) throws IOException {
    if (start < 0 || end <= start || end - start > 4096)
      throw new IllegalArgumentException("Sequence track requests must contain 1 to 4096 bases");
    var selected = AssemblySequenceSnapshot.capture(chunkedFile).slice(start, end);
    try (var source = new SourceSession(path); var out = new java.io.ByteArrayOutputStream((int)(end-start))) {
      for (var piece : selected.fragments()) {
        source.validate(piece.source(), piece.start(), piece.end());
        source.copy(piece.source(), piece.start(), piece.end(), piece.reversed(), out);
      }
      return out.toString(StandardCharsets.US_ASCII);
    }
  }

  private synchronized IndexedFastaSource indexedSource(Path path) throws IOException {
    if(closed)throw new IOException("FASTA processor is closed");
    if (sourceIndex == null || !sourceIndex.matches(path)) {
      if(sourceIndex!=null)sourceIndex.close();
      sourceIndex=null;
      sourceIndex = new IndexedFastaSource(path);
    }
    return sourceIndex;
  }

  @Override public synchronized void close() throws IOException {
    closed=true;
    if(sourceIndex!=null)sourceIndex.close();
  }

  /** Compatibility wrappers retain the String API; HTTP uses bounded-memory writers. */
  public @NotNull String exportAssemblyPreservingGaps(final @NotNull Path path) {
    return renderToString(out -> writeAssembly(path, out, true, 0));
  }
  public @NotNull String exportAssembly(final @NotNull Path path) {
    return exportAssembly(path, AssemblyExportSettings.gapLengthBp());
  }
  public @NotNull String exportAssembly(final @NotNull Path path, final long gap) {
    return renderToString(out -> writeAssembly(path, out, false, gap));
  }
  @FunctionalInterface private interface OutputAction { void write(java.io.OutputStream out) throws IOException; }
  private static String renderToString(OutputAction action) {
    try (var out = new java.io.ByteArrayOutputStream()) { action.write(out); return out.toString(StandardCharsets.UTF_8); }
    catch (IOException e) { throw new java.io.UncheckedIOException(e); }
  }

  public void writeAssembly(Path path, java.io.OutputStream output, boolean preserveGaps, long gap) throws IOException {
    AssemblyExportSettings.requireGapLength(gap);
    var records = preserveGaps ? chunkedFile.getAgpProcessor().getAssemblyAGPRecords()
      : chunkedFile.getAgpProcessor().getAGPRecords(gap);
    if (records.isEmpty()) return;
    try (var source = new SourceSession(path)) {
      // Validate every interval before producing any output.
      for (var record : records) if (record instanceof AGPProcessor.ContigAGPRecord c)
        source.validate(c.getContigName(), c.getIntraContigStartBpIncl()-1, c.getIntraContigEndBpIncl());
      var writer = new WrappedOutput(output); String name = null;
      for (var record : records) {
        if (!record.getScaffoldName().equals(name)) {
          name = record.getScaffoldName();
          // Preserve legacy uniform-export naming of unplaced components.
          String exportedName = !preserveGaps && name.startsWith("unscaffolded_") ? name.substring(13) : name;
          writer.header(exportedName);
        }
        if (record instanceof AGPProcessor.GapAGPRecord g) writer.gap(g.getGapLength());
        else if (record instanceof AGPProcessor.ContigAGPRecord c)
          source.copy(c.getContigName(), c.getIntraContigStartBpIncl()-1, c.getIntraContigEndBpIncl(),
            c.getContigOrientation()==AGPProcessor.AGPContigOrientation.MINUS, writer);
      }
      writer.finish();
    }
  }

  public void writeInterval(Path path, java.io.OutputStream output, long from, long to, String name) throws IOException {
    long start=Math.max(0,Math.min(from,to)), end=Math.max(Math.addExact(start,1),Math.max(from,to));
    record Piece(String source,long from,long to,boolean reverse) { }
    var pieces=new ArrayList<Piece>();long position=0;
    for(var contig:chunkedFile.getAssemblyInfo().contigs()) {
      var d=contig.descriptor();long length=d.getLengthBp(),a=Math.max(start-position,0),b=Math.min(end-position,length);
      if(a<b) {
        boolean reverse=contig.direction()==ContigDirection.REVERSED;
        pieces.add(new Piece(d.getContigNameInSourceFASTA(),d.getOffsetInSourceFASTA()+(reverse?length-b:a),
          d.getOffsetInSourceFASTA()+(reverse?length-a:b),reverse));
      }
      position+=length;if(position>=end)break;
    }
    if(pieces.isEmpty())throw new IllegalArgumentException("Selected region does not intersect the current assembly");
    try(var source=new SourceSession(path)) {
      for(var piece:pieces)source.validate(piece.source,piece.from,piece.to);
      var writer=new WrappedOutput(output);writer.header(name);
      for(var piece:pieces)source.copy(piece.source,piece.from,piece.to,piece.reverse,writer);
      writer.finish();
    }
  }

  /** Export the captured sequence even if the user has edited the assembly since submission. */
  public void writeSnapshot(Path path, java.io.OutputStream output,
                            AssemblySequenceSnapshot snapshot, String name) throws IOException {
    if (snapshot.length() == 0) throw new IllegalArgumentException("Cannot export an empty sequence");
    if (name == null || name.isBlank() || name.indexOf('\n') >= 0 || name.indexOf('\r') >= 0)
      throw new IllegalArgumentException("Invalid FASTA record name");
    try(var source=new SourceSession(path)) {
      for(var piece:snapshot.fragments())source.validate(piece.source(),piece.start(),piece.end());
      var writer=new WrappedOutput(output);writer.header(name);
      for(var piece:snapshot.fragments())source.copy(piece.source(),piece.start(),piece.end(),piece.reversed(),writer);
      writer.finish();
    }
  }

  /** Selection coordinates stay gapless; emitted concatenated sequence preserves scaffold separators. */
  public void writeSelection(Path path, java.io.OutputStream output, long from, long to, String name) throws IOException {
    writeLayout(path, output, AssemblySequenceLayout.capture(chunkedFile, AssemblyExportSettings.gapLengthBp()), from, to, name);
  }

  /** Write a padded sequence interval without materializing nucleotides in memory. */
  public void writeLayout(Path path, java.io.OutputStream output, AssemblySequenceLayout layout,
                          long start, long end, String name) throws IOException {
    if (start < 0 || end <= start || end > layout.sequence().length()) throw new IllegalArgumentException("Invalid sequence interval");
    try (var source = new SourceSession(path)) {
      var selected = layout.sequence().slice(start, end);
      for (var piece : selected.fragments()) source.validate(piece.source(), piece.start(), piece.end());
      var writer = new WrappedOutput(output); writer.header(name); long position = start;
      for (var gap : layout.gaps()) {
        if (gap.position() <= start || gap.position() >= end) continue;
        for (var piece : layout.sequence().slice(position, gap.position()).fragments())
          source.copy(piece.source(),piece.start(),piece.end(),piece.reversed(),writer);
        writer.gap(gap.length()); position = gap.position();
      }
      for (var piece : layout.sequence().slice(position,end).fragments())
        source.copy(piece.source(),piece.start(),piece.end(),piece.reversed(),writer);
      writer.finish();
    }
  }

  /** Overlap chunks so matches crossing chunk/contig/scaffold joins are not lost. */
  public void writeSearchChunks(Path path, java.io.OutputStream output,
                                AssemblySequenceSnapshot snapshot, int chunkSize, int overlap) throws IOException {
    writeSearchChunks(path, output, new AssemblySequenceLayout(snapshot, List.of()), chunkSize, overlap);
  }
  public void writeSearchChunks(Path path, java.io.OutputStream output,
                                AssemblySequenceLayout layout, int chunkSize, int overlap) throws IOException {
    var snapshot = layout.sequence();
    if (chunkSize <= 0 || overlap < 0 || overlap >= chunkSize)
      throw new IllegalArgumentException("Search chunk size must exceed overlap");
    if (snapshot.length() == 0) throw new IllegalArgumentException("Empty search scope");
    try (var source = new SourceSession(path)) {
      for (var piece : snapshot.fragments()) source.validate(piece.source(), piece.start(), piece.end());
      var writer = new WrappedOutput(output);
      for (long start = 0; start < snapshot.length();) {
        long segmentEnd = layout.segmentEnd(start);
        long ownedEnd = start + Math.min(chunkSize, segmentEnd - start);
        long end = ownedEnd + Math.min(overlap, segmentEnd - ownedEnd);
        writer.header("hict_chunk_" + start + "_" + ownedEnd);
        for (var piece : snapshot.slice(start, end).fragments())
          source.copy(piece.source(), piece.start(), piece.end(), piece.reversed(), writer);
        writer.finish();
        start = ownedEnd;
      }
    }
  }

  private final class SourceSession implements AutoCloseable {
    final Map<String,Long> lengths; final Map<String,String> aliases;
    final IndexedFastaSource.Session indexed;
    SourceSession(Path path) throws IOException {
      synchronized(FASTAProcessor.this) {
        var index=indexedSource(path);lengths=index.lengths();indexed=index.open();
      }
      var originals=chunkedFile.getOriginalDescriptors().values().stream().sorted(Comparator.comparingInt(ContigDescriptor::getContigId)).toList();
      var result=new HashMap<String,String>();
      if(originals.stream().noneMatch(d->lengths.containsKey(d.getContigNameInSourceFASTA())) && originals.size()==lengths.size()) {
        var names=new ArrayList<>(lengths.keySet());boolean matched=true;
        for(int i=0;i<originals.size();i++)if(lengths.get(names.get(i))!=originals.get(i).getLengthBp()){matched=false;break;}
        if(matched)for(int i=0;i<originals.size();i++)result.put(originals.get(i).getContigNameInSourceFASTA(),names.get(i));
      }
      aliases=result;
    }
    void validate(String name,long start,long end) {
      name=aliases.getOrDefault(name,name);Long size=lengths.get(name);
      if(size==null||start<0||end<start||end>size)throw new IllegalArgumentException("Missing or out-of-bounds FASTA interval: "+name+" "+start+".."+end);
    }
    void copy(String name,long start,long end,boolean reverse,java.io.OutputStream output) throws IOException {
      name=aliases.getOrDefault(name,name);
      indexed.copy(name,start,end,reverse,output);
    }
    public void close() throws IOException {indexed.close();}
  }

  private static final class WrappedOutput extends java.io.OutputStream {
    final java.io.OutputStream out;int column;
    WrappedOutput(java.io.OutputStream out) {this.out=out;}
    void header(String name) throws IOException {finish();out.write(('>'+name+'\n').getBytes(StandardCharsets.UTF_8));}
    public void write(int value) throws IOException {out.write(value);if(++column==80){out.write('\n');column=0;}}
    public void write(byte[] bytes,int offset,int length) throws IOException {
      while(length>0){int n=Math.min(length,80-column);out.write(bytes,offset,n);column+=n;offset+=n;length-=n;if(column==80){out.write('\n');column=0;}}
    }
    void gap(long count) throws IOException {
      byte[] block=new byte[65536];java.util.Arrays.fill(block,(byte)'N');
      while(count>0){if(Thread.currentThread().isInterrupted())throw new java.io.InterruptedIOException("FASTA export cancelled");int n=(int)Math.min(block.length,count);write(block,0,n);count-=n;}
    }
    void finish() throws IOException {if(column!=0){out.write('\n');column=0;}}
  }

  public @NotNull String exportSelection(final @NotNull Path fastaPath,
                                         final long fromBpX,
                                         final long fromBpY,
                                         final long toBpX,
                                         final long toBpY) {
    final long selectionStart = Math.max(0L, Math.min(Math.min(fromBpX, fromBpY), Math.min(toBpX, toBpY)));
    final long selectionEnd = Math.max(selectionStart + 1L, Math.max(Math.max(fromBpX, fromBpY), Math.max(toBpX, toBpY)));
    return exportInterval(
      fastaPath,
      selectionStart,
      selectionEnd,
      String.format(Locale.ROOT, "selection_%d_%d", selectionStart, selectionEnd)
    );
  }

  public @NotNull String exportInterval(final @NotNull Path fastaPath,
                                        final long fromBp,
                                        final long toBp,
                                        final @NotNull String recordName) {
    return renderToString(out -> writeInterval(fastaPath, out, fromBp, toBp, recordName));
  }

  private @NotNull List<AssemblyContigEntry> currentAssemblyEntries() {
    final var entries = new ArrayList<AssemblyContigEntry>();
    for (final ContigTree.ContigTuple tuple : this.chunkedFile.getAssemblyInfo().contigs()) {
      final var descriptor = tuple.descriptor();
      entries.add(new AssemblyContigEntry(
        this.chunkedFile.getContigDisplayName(descriptor.getContigId()),
        this.chunkedFile.getContigOriginalName(descriptor.getContigId()),
        descriptor.getContigNameInSourceFASTA(),
        descriptor.getOffsetInSourceFASTA(),
        descriptor.getLengthBp()
      ));
    }
    return entries;
  }

  private List<FASTASequenceSummary> sourceSummaries(Path path) {
    try {return indexedSource(path).lengths().entrySet().stream().map(e->new FASTASequenceSummary(e.getKey(),e.getValue())).toList();}
    catch(IOException e){throw new java.io.UncheckedIOException(e);}
  }

  public static @NotNull List<FASTASequenceSummary> readSequenceSummaries(final @NotNull Path fastaPath) {
    final var records = new ArrayList<FASTASequenceSummary>();
    final var seenNames = new java.util.HashSet<String>();
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
          if (!seenNames.add(currentName)) throw new IllegalArgumentException("Duplicate FASTA identifier: " + currentName);
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
    final var sequences = new LinkedHashMap<String, String>();
    try (final var stream = openPossiblyGzippedStream(fastaPath);
         final var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String currentName = null; StringBuilder current = new StringBuilder(); String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(">")) {
          if(currentName!=null)sequences.put(currentName,current.toString());
          currentName=parseHeaderName(line);
          if(sequences.containsKey(currentName))throw new IllegalArgumentException("Duplicate FASTA identifier: "+currentName);
          current=new StringBuilder();
        } else if(currentName!=null && !line.isBlank()) current.append(line.trim());
      }
      if(currentName!=null)sequences.put(currentName,current.toString());
    } catch(IOException e) {throw new java.io.UncheckedIOException("Failed to read FASTA file "+fastaPath,e);}
    return sequences;
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

  private @NotNull Map<String, String> buildSourceNameAliases(final @NotNull Map<String, String> sequences) {
    final var sourceDescriptors = this.chunkedFile.getOriginalDescriptors().values().stream()
      .sorted(Comparator.comparingInt(ContigDescriptor::getContigId))
      .toList();
    if (sourceDescriptors.stream().anyMatch(d -> sequences.containsKey(d.getContigNameInSourceFASTA()))) return Map.of();
    if (sequences.size() != sourceDescriptors.size()) {
      return Map.of();
    }
    final var fastaEntries = new ArrayList<>(sequences.entrySet());
    for (int i = 0; i < fastaEntries.size(); ++i) {
      if (fastaEntries.get(i).getValue().length() != sourceDescriptors.get(i).getLengthBp()) {
        return Map.of();
      }
    }
    final var aliases = new HashMap<String, String>();
    for (int i = 0; i < fastaEntries.size(); ++i) {
      aliases.put(sourceDescriptors.get(i).getContigNameInSourceFASTA(), fastaEntries.get(i).getKey());
    }
    return aliases;
  }

  private @NotNull String extractContigSequence(final @NotNull Map<String, String> sequences,
                                                final @NotNull Map<String, String> sequenceNameAliases,
                                                final @NotNull ContigTree.ContigTuple contig) {
    return extractContigSlice(sequences, sequenceNameAliases, contig, 0L, contig.descriptor().getLengthBp());
  }

  private @NotNull String extractContigSlice(final @NotNull Map<String, String> sequences,
                                             final @NotNull Map<String, String> sequenceNameAliases,
                                             final @NotNull ContigTree.ContigTuple contig,
                                             final long startInsideContig,
                                             final long endInsideContig) {
    final var descriptor = contig.descriptor();
    final var sourceName = sequenceNameAliases.getOrDefault(
      descriptor.getContigNameInSourceFASTA(),
      descriptor.getContigNameInSourceFASTA()
    );
    final var source = sequences.get(sourceName);
    if (source == null) {
      throw new IllegalArgumentException(
        "Linked FASTA does not contain sequence '" + descriptor.getContigNameInSourceFASTA() + "'"
      );
    }
    return extractContigSliceFromSource(
      source,
      descriptor.getContigNameInSourceFASTA(),
      descriptor.getOffsetInSourceFASTA(),
      descriptor.getLengthBp(),
      contig.direction(),
      startInsideContig,
      endInsideContig
    );
  }

  static @NotNull String extractContigSliceFromSource(final @NotNull String source,
                                                      final @NotNull String sourceName,
                                                      final int sourceOffset,
                                                      final long contigLengthBp,
                                                      final @NotNull ContigDirection direction,
                                                      final long startInsideContig,
                                                      final long endInsideContig) {
    if (startInsideContig < 0L || endInsideContig > contigLengthBp || startInsideContig >= endInsideContig) {
      throw new IllegalArgumentException(
        "Requested contig slice [" + startInsideContig + ", " + endInsideContig + ") is outside contig of length " + contigLengthBp
      );
    }
    final long sourceStartLong;
    final long sourceEndLong;
    switch (direction) {
      case FORWARD -> {
        sourceStartLong = sourceOffset + startInsideContig;
        sourceEndLong = sourceOffset + endInsideContig;
      }
      case REVERSED -> {
        sourceStartLong = sourceOffset + (contigLengthBp - endInsideContig);
        sourceEndLong = sourceOffset + (contigLengthBp - startInsideContig);
      }
      default -> throw new IllegalStateException("Unexpected contig direction: " + direction);
    }
    final int sourceStart = Math.toIntExact(sourceStartLong);
    final int sourceEnd = Math.toIntExact(sourceEndLong);
    if (sourceStart < 0 || sourceEnd > source.length() || sourceStart >= sourceEnd) {
      throw new IllegalArgumentException(
        "Requested source FASTA slice [" + sourceStart + ", " + sourceEnd + ") is outside sequence '"
          + sourceName + "' of length " + source.length()
      );
    }
    final var subsequence = source.substring(sourceStart, sourceEnd);
    return switch (direction) {
      case FORWARD -> subsequence;
      case REVERSED -> reverseComplement(subsequence);
    };
  }

  private static @NotNull String reverseComplement(final @NotNull String sequence) {
    final var nativeResult = NativeProcessingService.getInstance().tryReverseComplementAscii(sequence);
    if (nativeResult != null) {
      return nativeResult;
    }
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
    long sourceOffset,
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
    boolean sourceIntervalsCovered,
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
