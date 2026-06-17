package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

/**
 * Packageable self-alignment dotplot pipeline.
 *
 * <p>minimap2-compatible aligners remain responsible for sequence alignment. HiCT replaces the legacy
 * Python/Cooler post-processing with Java PAF sampling, BG2 writing, hictk load/zoomify, and the existing
 * .mcool -> .hict.hdf5 importer.</p>
 */
public final class SelfDotplotPipeline {
  private static final int BUFFER_SIZE = 1 << 20;
  private static final long ALIGNER_HEARTBEAT_INTERVAL_MS = 60_000L;

  public @NotNull Path generate(final @NotNull Options options,
                                final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                final @NotNull Consumer<String> logger,
                                final @NotNull Consumer<Process> processSink,
                                final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    if (toolchain.hictkCommand() == null) {
      throw new IllegalStateException("Dotplot generation requires bundled or configured hictk.");
    }
    final var aligner = toolchain.selectedDotplotAlignerCommand(options.alignerPreference());
    if (aligner == null) {
      throw new IllegalStateException("Dotplot generation requires bundled or configured minimap2/mm2-plus aligner.");
    }
    final var hictk = Objects.requireNonNull(toolchain.hictkCommand());
    final var alignerName = toolchain.selectedDotplotAlignerName(options.alignerPreference());
    final var prefix = options.outputPrefix();
    final var chromSizes = options.outputDirectory().resolve(prefix + ".chrom.sizes");
    final var bg2 = options.outputDirectory().resolve(prefix + "." + options.binSize() + ".bg2");
    final var cool = options.outputDirectory().resolve(prefix + "." + options.binSize() + ".cool");
    final var mcool = options.outputDirectory().resolve(prefix + ".mcool");
    final var hict = options.outputDirectory().resolve(prefix + ".hict.hdf5");

    if (Files.exists(hict) && !options.overwrite()) {
      throw new IllegalArgumentException("Output file already exists: " + hict.getFileName());
    }
    Files.createDirectories(options.outputDirectory());
    if (options.overwrite()) {
      Files.deleteIfExists(bg2);
      Files.deleteIfExists(cool);
      Files.deleteIfExists(mcool);
      Files.deleteIfExists(hict);
    }

    final var tmp = Files.createTempDirectory(options.outputDirectory(), prefix + ".selfdot.");
    final var paf = tmp.resolve(prefix + ".paf");
    try {
      emitStage(logger, "fasta", 0.0d, 0.02d, "Reading FASTA and writing chrom sizes");
      final var alignmentFasta = prepareAlignmentFasta(options, tmp, logger, cancellationRequested);
      final var layout = writeChromSizes(options, alignmentFasta, chromSizes, logger, cancellationRequested);
      emitStage(logger, "fasta", 1.0d, 0.12d, "Parsed " + layout.chromosomes().size() + " sequence(s)");

      emitStage(logger, "align", 0.0d, 0.12d, "Running " + alignerName + " self-alignment");
      runAligner(buildAlignerCommand(aligner, options, alignmentFasta), alignerName, options.outputDirectory(), paf, logger, processSink, cancellationRequested);
      emitStage(logger, "align", 1.0d, 0.45d, alignerName + " PAF written: " + paf.getFileName());

      emitStage(logger, "paf_to_bg2", 0.0d, 0.45d, "Sampling PAF alignments into BG2 pixels");
      final var pixelCount = writeBg2FromPaf(options, layout, paf, bg2, logger, cancellationRequested);
      if (pixelCount == 0L) {
        throw new IllegalStateException(
          "Dotplot generation produced zero pixels. Try smaller k/window values, lower minimap2 -m, lower min-alignment length, or less diagonal filtering."
        );
      }
      emitStage(logger, "paf_to_bg2", 1.0d, 0.60d, "Wrote " + pixelCount + " BG2 pixel row(s)");

      emitStage(logger, "load_cool", 0.0d, 0.60d, "Loading BG2 into base .cool with hictk");
      runCommand(buildLoadCommand(hictk, bg2, chromSizes, cool, options), options.outputDirectory(), logger, processSink, cancellationRequested);
      emitStage(logger, "load_cool", 1.0d, 0.70d, "Created " + cool.getFileName());

      emitStage(logger, "zoomify", 0.0d, 0.70d, "Building multi-resolution .mcool with hictk");
      runCommand(
        buildZoomifyCommand(hictk, cool, mcool, resolveZoomResolutions(options, layout, logger), options),
        options.outputDirectory(),
        logger,
        processSink,
        cancellationRequested
      );
      emitStage(logger, "zoomify", 1.0d, 0.82d, "Created " + mcool.getFileName());

      emitStage(logger, "import_hict", 0.0d, 0.82d, "Importing generated .mcool into HiCT");
      new McoolToHictConverter().convert(
        new ConversionOptions(
          mcool,
          hict,
          List.of(),
          8192,
          6,
          ConversionOptions.CompressionAlgorithm.DEFLATE,
          ConversionOptions.NO_AGP,
          false,
          options.conversionThreads(),
          true,
          ConversionOptions.ExportMode.AUTO
        ),
        line -> {
          logger.accept(line);
          if (line.startsWith("Overall progress:")) {
            emitStage(logger, "import_hict", 0.5d, 0.92d, "Importing .mcool into HiCT");
          }
        }
      );
      emitStage(logger, "import_hict", 1.0d, 1.0d, "Created " + hict.getFileName());
      return hict;
    } finally {
      processSink.accept(null);
      if (!options.keepIntermediates()) {
        deleteIfExists(bg2, logger);
        deleteIfExists(cool, logger);
        deleteRecursively(tmp, logger);
      }
    }
  }

  private static @NotNull List<String> buildAlignerCommand(final @NotNull Path aligner,
                                                           final @NotNull Options options,
                                                           final @NotNull Path alignmentFasta) {
    final var command = new ArrayList<String>();
    command.add(aligner.toString());
    command.add("-t");
    command.add(Integer.toString(normalizeThreads(options.alignmentThreads())));
    command.add("-k");
    command.add(Integer.toString(options.minimizerK()));
    command.add("-w");
    command.add(Integer.toString(options.minimizerWindow()));
    command.add("-m");
    command.add(Integer.toString(options.minChainScore()));
    command.add("-v");
    command.add("4");
    command.add("-P");
    command.add("--dual=no");
    command.add("--no-long-join");
    if (options.skipDiagonal()) {
      command.add("-D");
    }
    command.addAll(parseExtraArguments(options.extraAlignerArgs()));
    command.add(alignmentFasta.toString());
    command.add(alignmentFasta.toString());
    return command;
  }

  private static @NotNull List<String> parseExtraArguments(final @NotNull String raw) {
    if (raw.isBlank()) {
      return List.of();
    }
    final var out = new ArrayList<String>();
    final var current = new StringBuilder();
    boolean quoted = false;
    char quote = '\0';
    for (int i = 0; i < raw.length(); i++) {
      final char ch = raw.charAt(i);
      if (quoted) {
        if (ch == quote) {
          quoted = false;
        } else {
          current.append(ch);
        }
        continue;
      }
      if (ch == '\'' || ch == '"') {
        quoted = true;
        quote = ch;
      } else if (Character.isWhitespace(ch)) {
        if (!current.isEmpty()) {
          out.add(current.toString());
          current.setLength(0);
        }
      } else {
        current.append(ch);
      }
    }
    if (quoted) {
      throw new IllegalArgumentException("Unterminated quote in extra aligner arguments.");
    }
    if (!current.isEmpty()) {
      out.add(current.toString());
    }
    return List.copyOf(out);
  }

  private static @NotNull List<String> buildLoadCommand(final @NotNull Path hictk,
                                                        final @NotNull Path bg2,
                                                        final @NotNull Path chromSizes,
                                                        final @NotNull Path cool,
                                                        final @NotNull Options options) {
    return List.of(
      hictk.toString(),
      "load",
      "--format",
      "bg2",
      "--chrom-sizes",
      chromSizes.toString(),
      "--bin-size",
      Integer.toString(options.binSize()),
      "--output-fmt",
      "cool",
      "--force",
      "--compression-lvl",
      "6",
      "--threads",
      Integer.toString(normalizeHictkLoadThreads(options.conversionThreads())),
      bg2.toString(),
      cool.toString()
    );
  }

  private static @NotNull List<String> buildZoomifyCommand(final @NotNull Path hictk,
                                                           final @NotNull Path cool,
                                                           final @NotNull Path mcool,
                                                           final @NotNull List<Long> zoomResolutions,
                                                           final @NotNull Options options) {
    final var command = new ArrayList<String>();
    command.add(hictk.toString());
    command.add("zoomify");
    command.add("--force");
    command.add("--copy-base-resolution");
    command.add("--compression-lvl");
    command.add("6");
    command.add("--threads");
    command.add(Integer.toString(normalizeThreads(options.conversionThreads())));
    if (zoomResolutions.isEmpty()) {
      command.add("--nice-steps");
    } else {
      command.add("--resolutions");
      zoomResolutions.stream().map(String::valueOf).forEach(command::add);
    }
    command.add(cool.toString());
    command.add(mcool.toString());
    return command;
  }

  private static void runAligner(final @NotNull List<String> command,
                                 final @NotNull String alignerName,
                                 final @NotNull Path workingDirectory,
                                 final @NotNull Path outputPaf,
                                 final @NotNull Consumer<String> logger,
                                 final @NotNull Consumer<Process> processSink,
                                 final @NotNull BooleanSupplier cancellationRequested) throws IOException, InterruptedException {
    logger.accept("Executing: " + String.join(" ", command) + " > " + outputPaf);
    final var process = new ProcessBuilder(command)
      .directory(workingDirectory.toFile())
      .redirectOutput(outputPaf.toFile())
      .redirectErrorStream(false)
      .start();
    processSink.accept(process);
    final long startedAt = System.currentTimeMillis();
    long lastHeartbeatAt = startedAt;
    try (final var reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
      while (process.isAlive()) {
        checkCancelled(cancellationRequested);
        String line;
        while (reader.ready() && (line = reader.readLine()) != null) {
          logger.accept(line);
        }
        final long now = System.currentTimeMillis();
        if (now - lastHeartbeatAt >= ALIGNER_HEARTBEAT_INTERVAL_MS) {
          emitStage(
            logger,
            "align",
            0.0d,
            0.12d,
            alignerName + " is still running; elapsed=" + formatDuration(now - startedAt) +
              ", PAF size=" + humanBytes(safeSize(outputPaf))
          );
          lastHeartbeatAt = now;
        }
        Thread.sleep(1000L);
      }
      String line;
      while ((line = reader.readLine()) != null) {
        logger.accept(line);
      }
    } finally {
      processSink.accept(null);
    }
    final int exit = process.waitFor();
    if (exit != 0) {
      throw new IllegalStateException(alignerName + " failed with exit code " + exit);
    }
    if (!Files.isRegularFile(outputPaf) || Files.size(outputPaf) == 0L) {
      throw new IllegalStateException(alignerName + " produced an empty PAF file.");
    }
  }

  private static void runCommand(final @NotNull List<String> command,
                                 final @NotNull Path workingDirectory,
                                 final @NotNull Consumer<String> logger,
                                 final @NotNull Consumer<Process> processSink,
                                 final @NotNull BooleanSupplier cancellationRequested) throws IOException, InterruptedException {
    logger.accept("Executing: " + String.join(" ", command));
    final var process = new ProcessBuilder(command)
      .directory(workingDirectory.toFile())
      .redirectErrorStream(true)
      .start();
    processSink.accept(process);
    try (final var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        checkCancelled(cancellationRequested);
        logger.accept(line);
      }
    } finally {
      processSink.accept(null);
    }
    final int exit = process.waitFor();
    if (exit != 0) {
      throw new IllegalStateException("Command failed with exit code " + exit + ": " + String.join(" ", command));
    }
  }

  private static @NotNull Path prepareAlignmentFasta(final @NotNull Options options,
                                                     final @NotNull Path tmp,
                                                     final @NotNull Consumer<String> logger,
                                                     final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    if (options.assemblyAgpPath() == null) {
      return options.fastaPath();
    }
    final var transformedFasta = tmp.resolve(options.outputPrefix() + ".agp-applied.fasta");
    logger.accept("Applying AGP before dotplot self-alignment: " + options.assemblyAgpPath().getFileName());
    applyAgpToFasta(options.fastaPath(), options.assemblyAgpPath(), transformedFasta, logger, cancellationRequested);
    return transformedFasta;
  }

  static @NotNull Path applyAgpToFasta(final @NotNull Path fasta,
                                       final @NotNull Path agp,
                                       final @NotNull Path outputFasta,
                                       final @NotNull Consumer<String> logger,
                                       final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    Files.createDirectories(outputFasta.getParent());
    final var sequenceStore = Files.createTempFile(outputFasta.getParent(), "hict-agp-fasta-sequences.", ".bin");
    try {
      final var sequenceIndex = indexFastaSequences(fasta, sequenceStore, cancellationRequested);
      final var agpObjects = parseAgpSegments(agp);
      if (agpObjects.isEmpty()) {
        throw new IllegalArgumentException("AGP file has no contig/component rows: " + agp.getFileName());
      }
      writeAgpTransformedFasta(sequenceStore, sequenceIndex, agpObjects, outputFasta, logger, cancellationRequested);
      return outputFasta;
    } finally {
      Files.deleteIfExists(sequenceStore);
    }
  }

  private static @NotNull Map<String, FastaSequenceIndex> indexFastaSequences(final @NotNull Path fasta,
                                                                              final @NotNull Path sequenceStore,
                                                                              final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    final var index = new java.util.LinkedHashMap<String, FastaSequenceIndex>();
    try (
      final var reader = fastaReader(fasta);
      final var writer = new BufferedOutputStream(Files.newOutputStream(sequenceStore), BUFFER_SIZE)
    ) {
      String name = null;
      long offset = 0L;
      long length = 0L;
      long position = 0L;
      String line;
      while ((line = reader.readLine()) != null) {
        checkCancelled(cancellationRequested);
        if (line.startsWith(">")) {
          if (name != null) {
            index.put(name, new FastaSequenceIndex(name, offset, length));
          }
          name = parseFastaName(line);
          if (index.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate FASTA sequence name: " + name);
          }
          offset = position;
          length = 0L;
        } else {
          final var sequence = line.trim();
          if (!sequence.isEmpty()) {
            final var bytes = sequence.getBytes(StandardCharsets.US_ASCII);
            writer.write(bytes);
            position += bytes.length;
            length += bytes.length;
          }
        }
      }
      if (name != null) {
        index.put(name, new FastaSequenceIndex(name, offset, length));
      }
    }
    if (index.isEmpty()) {
      throw new IllegalArgumentException("No FASTA records found in " + fasta.getFileName());
    }
    return Map.copyOf(index);
  }

  private static @NotNull Map<String, List<AgpSegment>> parseAgpSegments(final @NotNull Path agp) throws IOException {
    final var objects = new java.util.LinkedHashMap<String, List<AgpSegment>>();
    try (final var reader = Files.newBufferedReader(agp, StandardCharsets.UTF_8)) {
      String line;
      int lineNumber = 0;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        final var trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        final var fields = trimmed.split("\\s+");
        if (fields.length < 5) {
          throw new IllegalArgumentException("Malformed AGP row " + lineNumber + ": expected at least 5 columns");
        }
        final var objectName = fields[0];
        final var objectStart = parsePositiveAgpLong(fields[1], lineNumber, "object_beg");
        final var objectEnd = parsePositiveAgpLong(fields[2], lineNumber, "object_end");
        final var partNumber = (int) parsePositiveAgpLong(fields[3], lineNumber, "part_number");
        final var componentType = fields[4];
        if ("N".equalsIgnoreCase(componentType) || "U".equalsIgnoreCase(componentType)) {
          if (fields.length < 6) {
            throw new IllegalArgumentException("Malformed AGP gap row " + lineNumber + ": expected gap length");
          }
          final var gapLength = parsePositiveAgpLong(fields[5], lineNumber, "gap_length");
          objects.computeIfAbsent(objectName, ignored -> new ArrayList<>())
            .add(AgpSegment.gap(objectName, objectStart, objectEnd, partNumber, gapLength));
        } else {
          if (fields.length < 9) {
            throw new IllegalArgumentException("Malformed AGP component row " + lineNumber + ": expected 9 columns");
          }
          final var componentName = fields[5];
          final var componentStart = parsePositiveAgpLong(fields[6], lineNumber, "component_beg");
          final var componentEnd = parsePositiveAgpLong(fields[7], lineNumber, "component_end");
          final var orientation = fields[8];
          final var reverse = "-".equals(orientation);
          objects.computeIfAbsent(objectName, ignored -> new ArrayList<>())
            .add(AgpSegment.component(objectName, objectStart, objectEnd, partNumber, componentName, componentStart - 1L, componentEnd, reverse));
        }
      }
    }
    objects.values().forEach(segments -> segments.sort(
      Comparator.comparingLong(AgpSegment::objectStart).thenComparingInt(AgpSegment::partNumber)
    ));
    return objects;
  }

  private static long parsePositiveAgpLong(final @NotNull String value,
                                           final int lineNumber,
                                           final @NotNull String fieldName) {
    try {
      final var parsed = Long.parseLong(value);
      if (parsed < 1L) {
        throw new IllegalArgumentException("AGP row " + lineNumber + " has non-positive " + fieldName + ": " + value);
      }
      return parsed;
    } catch (final NumberFormatException ex) {
      throw new IllegalArgumentException("AGP row " + lineNumber + " has invalid " + fieldName + ": " + value, ex);
    }
  }

  private static void writeAgpTransformedFasta(final @NotNull Path sequenceStore,
                                               final @NotNull Map<String, FastaSequenceIndex> sequenceIndex,
                                               final @NotNull Map<String, List<AgpSegment>> agpObjects,
                                               final @NotNull Path outputFasta,
                                               final @NotNull Consumer<String> logger,
                                               final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    final var coveredRanges = new HashMap<String, List<long[]>>();
    long outputLength = 0L;
    long repeatedRanges = 0L;
    try (
      final var reader = new RandomAccessFile(sequenceStore.toFile(), "r");
      final var writer = new BufferedOutputStream(Files.newOutputStream(outputFasta), BUFFER_SIZE)
    ) {
      final var lineColumn = new int[]{0};
      for (final var entry : agpObjects.entrySet()) {
        checkCancelled(cancellationRequested);
        writeAscii(writer, ">" + entry.getKey() + "\n");
        lineColumn[0] = 0;
        for (final var segment : entry.getValue()) {
          if (segment.gap()) {
            outputLength += appendRepeatedBase(writer, 'N', segment.gapLength(), lineColumn);
            continue;
          }
          final var sequence = sequenceIndex.get(segment.componentName());
          if (sequence == null) {
            throw new IllegalArgumentException("AGP references FASTA sequence absent from input: " + segment.componentName());
          }
          if (segment.componentStart() < 0L || segment.componentEndExclusive() > sequence.length() || segment.componentStart() >= segment.componentEndExclusive()) {
            throw new IllegalArgumentException(
              "AGP component " + segment.componentName() + " has invalid range " +
                (segment.componentStart() + 1L) + "-" + segment.componentEndExclusive() +
                " for FASTA length " + sequence.length()
            );
          }
          if (isRepeatedRange(coveredRanges.computeIfAbsent(segment.componentName(), ignored -> new ArrayList<>()), segment.componentStart(), segment.componentEndExclusive())) {
            repeatedRanges++;
          }
          coveredRanges.get(segment.componentName()).add(new long[]{segment.componentStart(), segment.componentEndExclusive()});
          outputLength += appendSequenceSegment(reader, sequence, segment.componentStart(), segment.componentEndExclusive(), segment.reverse(), writer, lineColumn);
        }
        if (lineColumn[0] != 0) {
          writer.write('\n');
        }
      }
    }

    long inputLength = 0L;
    long coveredLength = 0L;
    long uncoveredSequences = 0L;
    for (final var sequence : sequenceIndex.values()) {
      inputLength += sequence.length();
      final var covered = mergedCoveredLength(coveredRanges.getOrDefault(sequence.name(), List.of()));
      coveredLength += covered;
      if (covered < sequence.length()) {
        uncoveredSequences++;
      }
    }
    if (uncoveredSequences > 0L) {
      logger.accept("AGP warning: " + uncoveredSequences + " FASTA sequence(s) are not fully covered by the AGP; dotplot will use only AGP-defined assembled sequence.");
    }
    if (repeatedRanges > 0L) {
      logger.accept("AGP warning: " + repeatedRanges + " repeated component range(s) were detected; repeated sequence is preserved in the scaffolded dotplot FASTA.");
    }
    if (outputLength != inputLength) {
      logger.accept("AGP warning: scaffolded FASTA length differs from original FASTA length: original=" + inputLength + " bp, scaffolded=" + outputLength + " bp, covered=" + coveredLength + " bp.");
    }
    logger.accept("AGP-applied FASTA written: " + outputFasta.getFileName() + " length=" + outputLength + " bp, scaffolds=" + agpObjects.size());
  }

  private static boolean isRepeatedRange(final @NotNull List<long[]> ranges,
                                         final long startInclusive,
                                         final long endExclusive) {
    for (final var range : ranges) {
      if (startInclusive < range[1] && endExclusive > range[0]) {
        return true;
      }
    }
    return false;
  }

  private static long mergedCoveredLength(final @NotNull List<long[]> ranges) {
    if (ranges.isEmpty()) {
      return 0L;
    }
    final var sorted = new ArrayList<>(ranges);
    sorted.sort(Comparator.comparingLong(range -> range[0]));
    long covered = 0L;
    long currentStart = sorted.get(0)[0];
    long currentEnd = sorted.get(0)[1];
    for (int i = 1; i < sorted.size(); i++) {
      final var range = sorted.get(i);
      if (range[0] <= currentEnd) {
        currentEnd = Math.max(currentEnd, range[1]);
      } else {
        covered += currentEnd - currentStart;
        currentStart = range[0];
        currentEnd = range[1];
      }
    }
    return covered + currentEnd - currentStart;
  }

  private static long appendSequenceSegment(final @NotNull RandomAccessFile reader,
                                            final @NotNull FastaSequenceIndex sequence,
                                            final long startInclusive,
                                            final long endExclusive,
                                            final boolean reverse,
                                            final @NotNull OutputStream writer,
                                            final int @NotNull [] lineColumn) throws IOException {
    final var buffer = new byte[BUFFER_SIZE];
    long emitted = 0L;
    if (reverse) {
      long cursor = sequence.offset() + endExclusive;
      long remaining = endExclusive - startInclusive;
      while (remaining > 0L) {
        final int chunk = (int) Math.min(buffer.length, remaining);
        cursor -= chunk;
        reader.seek(cursor);
        reader.readFully(buffer, 0, chunk);
        for (int i = chunk - 1; i >= 0; i--) {
          appendBase(writer, complement(buffer[i]), lineColumn);
        }
        remaining -= chunk;
        emitted += chunk;
      }
    } else {
      reader.seek(sequence.offset() + startInclusive);
      long remaining = endExclusive - startInclusive;
      while (remaining > 0L) {
        final int chunk = (int) Math.min(buffer.length, remaining);
        reader.readFully(buffer, 0, chunk);
        for (int i = 0; i < chunk; i++) {
          appendBase(writer, buffer[i], lineColumn);
        }
        remaining -= chunk;
        emitted += chunk;
      }
    }
    return emitted;
  }

  private static long appendRepeatedBase(final @NotNull OutputStream writer,
                                         final char base,
                                         final long count,
                                         final int @NotNull [] lineColumn) throws IOException {
    for (long i = 0L; i < count; i++) {
      appendBase(writer, (byte) base, lineColumn);
    }
    return count;
  }

  private static void appendBase(final @NotNull OutputStream writer,
                                 final byte base,
                                 final int @NotNull [] lineColumn) throws IOException {
    writer.write(base);
    lineColumn[0]++;
    if (lineColumn[0] >= 80) {
      writer.write('\n');
      lineColumn[0] = 0;
    }
  }

  private static byte complement(final byte base) {
    return switch (base) {
      case 'A' -> (byte) 'T';
      case 'a' -> (byte) 't';
      case 'C' -> (byte) 'G';
      case 'c' -> (byte) 'g';
      case 'G' -> (byte) 'C';
      case 'g' -> (byte) 'c';
      case 'T', 'U' -> (byte) 'A';
      case 't', 'u' -> (byte) 'a';
      case 'M' -> (byte) 'K';
      case 'm' -> (byte) 'k';
      case 'K' -> (byte) 'M';
      case 'k' -> (byte) 'm';
      case 'R' -> (byte) 'Y';
      case 'r' -> (byte) 'y';
      case 'Y' -> (byte) 'R';
      case 'y' -> (byte) 'r';
      case 'S', 's', 'W', 'w', 'N', 'n' -> base;
      case 'B' -> (byte) 'V';
      case 'b' -> (byte) 'v';
      case 'V' -> (byte) 'B';
      case 'v' -> (byte) 'b';
      case 'D' -> (byte) 'H';
      case 'd' -> (byte) 'h';
      case 'H' -> (byte) 'D';
      case 'h' -> (byte) 'd';
      default -> base;
    };
  }

  private static void writeAscii(final @NotNull OutputStream writer,
                                 final @NotNull String value) throws IOException {
    writer.write(value.getBytes(StandardCharsets.US_ASCII));
  }

  private static @NotNull GeneratedLayout writeChromSizes(final @NotNull Options options,
                                                          final @NotNull Path fastaPath,
                                                          final @NotNull Path chromSizes,
                                                          final @NotNull Consumer<String> logger,
                                                          final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    final var chromosomes = new ArrayList<Chromosome>();
    final var byName = new HashMap<String, Chromosome>();
    long totalBins = 0L;
    try (
      final var reader = fastaReader(fastaPath);
      final var writer = Files.newBufferedWriter(chromSizes, StandardCharsets.UTF_8)
    ) {
      String name = null;
      long length = 0L;
      String line;
      while ((line = reader.readLine()) != null) {
        checkCancelled(cancellationRequested);
        if (line.startsWith(">")) {
          if (name != null) {
            totalBins = appendChromosome(chromosomes, byName, writer, logger, name, length, totalBins, options.binSize());
          }
          name = parseFastaName(line);
          length = 0L;
        } else {
          length += line.trim().length();
        }
      }
      if (name != null) {
        appendChromosome(chromosomes, byName, writer, logger, name, length, totalBins, options.binSize());
      }
    }
    if (chromosomes.isEmpty()) {
      throw new IllegalArgumentException("No FASTA records found in " + fastaPath.getFileName());
    }
    return new GeneratedLayout(List.copyOf(chromosomes), Map.copyOf(byName));
  }

  private static long appendChromosome(final @NotNull List<Chromosome> chromosomes,
                                       final @NotNull Map<String, Chromosome> byName,
                                       final @NotNull BufferedWriter writer,
                                       final @NotNull Consumer<String> logger,
                                       final @NotNull String name,
                                       final long length,
                                       final long totalBins,
                                       final int binSize) throws IOException {
    if (byName.containsKey(name)) {
      throw new IllegalArgumentException("Duplicate FASTA sequence name: " + name);
    }
    final long bins = (length + (long) binSize - 1L) / binSize;
    final var chromosome = new Chromosome(name, length, totalBins, bins);
    chromosomes.add(chromosome);
    byName.put(name, chromosome);
    writer.write(name);
    writer.write('\t');
    writer.write(Long.toString(length));
    writer.newLine();
    logger.accept("Dotplot FASTA: " + name + " length=" + length + " bins=" + bins);
    return totalBins + bins;
  }

  private static @NotNull BufferedReader fastaReader(final @NotNull Path fasta) throws IOException {
    final var input = new BufferedInputStream(Files.newInputStream(fasta), BUFFER_SIZE);
    final var decompressed = fasta.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".gz")
      ? new GZIPInputStream(input)
      : input;
    return new BufferedReader(new InputStreamReader(decompressed, StandardCharsets.UTF_8), BUFFER_SIZE);
  }

  private static @NotNull String parseFastaName(final @NotNull String header) {
    final var name = header.substring(1).trim().split("\\s+", 2)[0];
    if (name.isBlank()) {
      throw new IllegalArgumentException("FASTA record has an empty name");
    }
    return name;
  }

  static long writeBg2FromPaf(final @NotNull Options options,
                              final @NotNull GeneratedLayout layout,
                              final @NotNull Path paf,
                              final @NotNull Path bg2,
                              final @NotNull Consumer<String> logger,
                              final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    final var counts = new HashMap<Long, Integer>();
    long lines = 0L;
    long kept = 0L;
    long points = 0L;
    long unknown = 0L;
    try (final var reader = Files.newBufferedReader(paf, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines++;
        if ((lines & 0x3ffffL) == 0) {
          checkCancelled(cancellationRequested);
          logger.accept("PAF converter: lines=" + lines + " kept=" + kept + " sampledPoints=" + points + " pixels=" + counts.size());
        }
        if (line.isBlank() || line.charAt(0) == '#') {
          continue;
        }
        final var record = PafRecord.parse(line);
        if (record == null || record.alignmentLength() < options.minAlignmentLength()) {
          continue;
        }
        final var query = layout.byName().get(record.queryName());
        final var target = layout.byName().get(record.targetName());
        if (query == null || target == null) {
          unknown++;
          continue;
        }
        kept++;
        points += samplePafRecord(options, query, target, record, counts);
      }
    }
    if (unknown > 0L) {
      logger.accept("PAF converter ignored " + unknown + " alignment(s) with contig names absent from FASTA sizes.");
    }
    final var entries = new ArrayList<>(counts.entrySet());
    entries.sort(Map.Entry.comparingByKey());
    try (final var writer = Files.newBufferedWriter(bg2, StandardCharsets.UTF_8)) {
      for (final var entry : entries) {
        final long key = entry.getKey();
        final long bin1 = key >>> 32;
        final long bin2 = key & 0xffff_ffffL;
        final var interval1 = layout.binInterval(bin1, options.binSize());
        final var interval2 = layout.binInterval(bin2, options.binSize());
        writer.write(interval1.chromosome().name());
        writer.write('\t');
        writer.write(Long.toString(interval1.start()));
        writer.write('\t');
        writer.write(Long.toString(interval1.end()));
        writer.write('\t');
        writer.write(interval2.chromosome().name());
        writer.write('\t');
        writer.write(Long.toString(interval2.start()));
        writer.write('\t');
        writer.write(Long.toString(interval2.end()));
        writer.write('\t');
        writer.write(Integer.toString(entry.getValue()));
        writer.newLine();
      }
    }
    logger.accept("PAF converter: lines=" + lines + " kept=" + kept + " sampledPoints=" + points + " pixels=" + entries.size());
    return entries.size();
  }

  private static long samplePafRecord(final @NotNull Options options,
                                      final @NotNull Chromosome query,
                                      final @NotNull Chromosome target,
                                      final @NotNull PafRecord record,
                                      final @NotNull Map<Long, Integer> counts) {
    final long querySpan = record.queryEnd() - record.queryStart();
    final long targetSpan = record.targetEnd() - record.targetStart();
    if (querySpan <= 0L || targetSpan <= 0L) {
      return 0L;
    }
    final long steps = Math.max(1L, record.alignmentLength() / (long) options.sampleBp());
    long sampled = 0L;
    for (long i = 0L; i <= steps; i++) {
      final double fraction = i / (double) steps;
      final long queryPos = record.queryStart() + Math.round(fraction * (querySpan - 1L));
      final long targetPos = record.forward()
        ? record.targetStart() + Math.round(fraction * (targetSpan - 1L))
        : record.targetEnd() - 1L - Math.round(fraction * (targetSpan - 1L));
      final long queryBin = query.binOffset() + (queryPos / options.binSize());
      final long targetBin = target.binOffset() + (targetPos / options.binSize());
      if (options.dropNearDiagonalBins() > 0 && query.name().equals(target.name())) {
        final long delta = Math.abs(queryBin - targetBin);
        if (delta <= options.dropNearDiagonalBins()) {
          continue;
        }
      }
      if (queryBin > 0xffff_ffffL || targetBin > 0xffff_ffffL) {
        throw new IllegalArgumentException("Dotplot has too many bins for the packed BG2 writer.");
      }
      final long lo = Math.min(queryBin, targetBin);
      final long hi = Math.max(queryBin, targetBin);
      final long key = (lo << 32) | hi;
      counts.merge(key, 1, Integer::sum);
      sampled++;
    }
    return sampled;
  }

  private static @NotNull List<Long> resolveZoomResolutions(final @NotNull Options options,
                                                            final @NotNull GeneratedLayout layout,
                                                            final @NotNull Consumer<String> logger) {
    if (!options.resolutions().isBlank()) {
      if (options.referenceMapPath() != null) {
        logger.accept("Reference map resolutions are ignored because explicit dotplot zoom resolutions were provided.");
      }
      return java.util.Arrays.stream(options.resolutions().split(","))
        .map(String::trim)
        .filter(token -> !token.isBlank())
        .map(Long::parseLong)
        .filter(resolution -> resolution > options.binSize())
        .distinct()
        .sorted()
        .toList();
    }
    if (options.referenceMapPath() != null) {
      return resolveReferenceZoomResolutions(options, logger);
    }
    final var out = new ArrayList<Long>();
    final long genomeLength = layout.chromosomes().stream().mapToLong(Chromosome::length).sum();
    long resolution = options.binSize();
    while ((genomeLength + resolution - 1L) / resolution > 500L) {
      resolution = nextNiceResolution(resolution);
      out.add(resolution);
      if (out.size() > 64) {
        break;
      }
    }
    return out;
  }

  private static @NotNull List<Long> resolveReferenceZoomResolutions(final @NotNull Options options,
                                                                     final @NotNull Consumer<String> logger) {
    final var referencePath = Objects.requireNonNull(options.referenceMapPath());
    try (final var reference = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(referencePath, 1, 2))) {
      final var referenceResolutions = Arrays.stream(reference.getResolutions())
        .filter(resolution -> resolution > 0L)
        .distinct()
        .sorted()
        .toArray();
      if (referenceResolutions.length == 0) {
        throw new IllegalArgumentException("Reference map has no usable bp resolutions: " + referencePath);
      }

      final var out = new LinkedHashSet<Long>();
      final long baseResolution = options.binSize();
      final long referenceFinestResolution = referenceResolutions[0];
      long resolution = baseResolution;
      while (resolution < referenceFinestResolution) {
        resolution = nextNiceResolution(resolution);
        if (resolution < referenceFinestResolution) {
          out.add(resolution);
        }
        if (out.size() > 64) {
          throw new IllegalArgumentException("Too many intermediate dotplot resolutions before reference finest resolution " + referenceFinestResolution);
        }
      }
      for (final long referenceResolution : referenceResolutions) {
        if (referenceResolution > baseResolution) {
          out.add(referenceResolution);
        }
      }
      logger.accept(
        "Dotplot zoom resolutions mirror reference map " + referencePath.getFileName() +
          " from base " + baseResolution + " bp/bin: " + out
      );
      return List.copyOf(out);
    }
  }

  private static long nextNiceResolution(final long current) {
    long pow10 = 1L;
    while (pow10 * 10L <= current) {
      pow10 *= 10L;
    }
    for (final long factor : List.of(1L, 2L, 5L, 10L)) {
      final long candidate = factor * pow10;
      if (candidate > current) {
        return candidate;
      }
    }
    return pow10 * 10L;
  }

  private static void emitStage(final @NotNull Consumer<String> logger,
                                final @NotNull String stage,
                                final double stageProgress,
                                final double overallProgress,
                                final @NotNull String detail) {
    logger.accept(
      "HICT_STAGE stage=" + stage +
        " progress=" + clamp(stageProgress) +
        " overall=" + clamp(overallProgress) +
        " detail=" + detail
    );
  }

  private static double clamp(final double value) {
    return Math.max(0.0d, Math.min(1.0d, value));
  }

  private static int normalizeThreads(final int threads) {
    return Math.max(1, Math.min(64, threads));
  }

  private static int normalizeHictkLoadThreads(final int threads) {
    return Math.max(2, Math.min(24, threads));
  }

  private static long safeSize(final @NotNull Path path) {
    try {
      return Files.exists(path) ? Files.size(path) : 0L;
    } catch (IOException ignored) {
      return 0L;
    }
  }

  private static @NotNull String humanBytes(final long bytes) {
    if (bytes < 1024L) {
      return bytes + " B";
    }
    final String[] units = {"KiB", "MiB", "GiB", "TiB"};
    double value = bytes;
    int unit = -1;
    do {
      value /= 1024.0d;
      unit++;
    } while (value >= 1024.0d && unit + 1 < units.length);
    return String.format(Locale.ROOT, "%.1f %s", value, units[unit]);
  }

  private static @NotNull String formatDuration(final long millis) {
    final var duration = Duration.ofMillis(Math.max(0L, millis));
    final long hours = duration.toHours();
    final long minutes = duration.toMinutesPart();
    final long seconds = duration.toSecondsPart();
    return hours > 0L
      ? String.format(Locale.ROOT, "%d:%02d:%02d", hours, minutes, seconds)
      : String.format(Locale.ROOT, "%d:%02d", minutes, seconds);
  }

  private static void checkCancelled(final @NotNull BooleanSupplier cancellationRequested) {
    if (cancellationRequested.getAsBoolean() || Thread.currentThread().isInterrupted()) {
      throw new IllegalStateException("Cancelled");
    }
  }

  private static void deleteIfExists(final @NotNull Path path, final @NotNull Consumer<String> logger) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      logger.accept("Unable to remove temporary file " + path + ": " + e.getMessage());
    }
  }

  private static void deleteRecursively(final @NotNull Path path, final @NotNull Consumer<String> logger) {
    if (!Files.exists(path)) {
      return;
    }
    try (final var stream = Files.walk(path)) {
      final var paths = stream.sorted(Comparator.reverseOrder()).toList();
      for (final var child : paths) {
        Files.deleteIfExists(child);
      }
    } catch (IOException e) {
      logger.accept("Unable to remove temporary directory " + path + ": " + e.getMessage());
    }
  }

  public record Options(
    @NotNull Path fastaPath,
    @NotNull Path outputDirectory,
    @NotNull String outputPrefix,
    int binSize,
    @NotNull String resolutions,
    @Nullable Path referenceMapPath,
    @Nullable Path assemblyAgpPath,
    int minimizerK,
    int minimizerWindow,
    int minChainScore,
    boolean skipDiagonal,
    int dropNearDiagonalBins,
    int alignmentThreads,
    int conversionThreads,
    boolean overwrite,
    boolean keepIntermediates,
    int sampleBp,
    int minAlignmentLength,
    @NotNull String extraAlignerArgs,
    @NotNull String alignerPreference
  ) {
    public Options {
      if (binSize <= 0) {
        throw new IllegalArgumentException("binSize must be positive");
      }
      if (minimizerK <= 0 || minimizerK > 31) {
        throw new IllegalArgumentException("minimizerK must be in [1, 31]");
      }
      if (minimizerWindow <= 0) {
        throw new IllegalArgumentException("minimizerWindow must be positive");
      }
      if (sampleBp <= 0) {
        throw new IllegalArgumentException("sampleBp must be positive");
      }
      if (minAlignmentLength < 0) {
        throw new IllegalArgumentException("minAlignmentLength cannot be negative");
      }
      extraAlignerArgs = extraAlignerArgs == null ? "" : extraAlignerArgs.trim();
      alignerPreference = alignerPreference == null || alignerPreference.isBlank() ? ExternalToolchainManager.dotplotAlignerPreference() : alignerPreference.trim();
    }
  }

  private record FastaSequenceIndex(@NotNull String name, long offset, long length) {
  }

  private record AgpSegment(@NotNull String objectName,
                            long objectStart,
                            long objectEnd,
                            int partNumber,
                            boolean gap,
                            @Nullable String componentName,
                            long componentStart,
                            long componentEndExclusive,
                            boolean reverse,
                            long gapLength) {
    private static @NotNull AgpSegment component(final @NotNull String objectName,
                                                 final long objectStart,
                                                 final long objectEnd,
                                                 final int partNumber,
                                                 final @NotNull String componentName,
                                                 final long componentStart,
                                                 final long componentEndExclusive,
                                                 final boolean reverse) {
      return new AgpSegment(objectName, objectStart, objectEnd, partNumber, false, componentName, componentStart, componentEndExclusive, reverse, 0L);
    }

    private static @NotNull AgpSegment gap(final @NotNull String objectName,
                                           final long objectStart,
                                           final long objectEnd,
                                           final int partNumber,
                                           final long gapLength) {
      return new AgpSegment(objectName, objectStart, objectEnd, partNumber, true, null, 0L, 0L, false, gapLength);
    }
  }

  record GeneratedLayout(@NotNull List<Chromosome> chromosomes, @NotNull Map<String, Chromosome> byName) {
    private @NotNull BinInterval binInterval(final long globalBin, final int binSize) {
      int lo = 0;
      int hi = chromosomes.size() - 1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        final var chrom = chromosomes.get(mid);
        if (globalBin < chrom.binOffset()) {
          hi = mid - 1;
        } else if (globalBin >= chrom.binOffset() + chrom.binCount()) {
          lo = mid + 1;
        } else {
          final long localBin = globalBin - chrom.binOffset();
          final long start = localBin * binSize;
          return new BinInterval(chrom, start, Math.min(start + binSize, chrom.length()));
        }
      }
      throw new IllegalArgumentException("Invalid bin id " + globalBin);
    }
  }

  record Chromosome(@NotNull String name, long length, long binOffset, long binCount) {
  }

  private record BinInterval(@NotNull Chromosome chromosome, long start, long end) {
  }

  record PafRecord(
    @NotNull String queryName,
    long queryStart,
    long queryEnd,
    boolean forward,
    @NotNull String targetName,
    long targetStart,
    long targetEnd,
    long alignmentLength
  ) {
    static PafRecord parse(final @NotNull String line) {
      final var fields = line.split("\t", 13);
      if (fields.length < 12) {
        return null;
      }
      try {
        final var strand = fields[4];
        return new PafRecord(
          fields[0],
          Long.parseLong(fields[2]),
          Long.parseLong(fields[3]),
          !"-".equals(strand),
          fields[5],
          Long.parseLong(fields[7]),
          Long.parseLong(fields[8]),
          Long.parseLong(fields[10])
        );
      } catch (RuntimeException ignored) {
        return null;
      }
    }
  }
}
