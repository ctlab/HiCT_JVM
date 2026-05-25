package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
 * <p>minimap2 remains the aligner. HiCT replaces the legacy Python/Cooler post-processing with Java PAF
 * sampling, BG2 writing, hictk load/zoomify, and the existing .mcool -> .hict.hdf5 importer.</p>
 */
public final class SelfDotplotPipeline {
  private static final int BUFFER_SIZE = 1 << 20;

  public @NotNull Path generate(final @NotNull Options options,
                                final @NotNull ExternalToolchainManager.ResolvedToolchain toolchain,
                                final @NotNull Consumer<String> logger,
                                final @NotNull Consumer<Process> processSink,
                                final @NotNull BooleanSupplier cancellationRequested) throws Exception {
    if (toolchain.hictkCommand() == null) {
      throw new IllegalStateException("Dotplot generation requires bundled or configured hictk.");
    }
    if (toolchain.minimap2Command() == null) {
      throw new IllegalStateException("Dotplot generation requires bundled or configured minimap2.");
    }
    final var hictk = Objects.requireNonNull(toolchain.hictkCommand());
    final var minimap2 = Objects.requireNonNull(toolchain.minimap2Command());
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
      final var layout = writeChromSizes(options, chromSizes, logger, cancellationRequested);
      emitStage(logger, "fasta", 1.0d, 0.12d, "Parsed " + layout.chromosomes().size() + " sequence(s)");

      emitStage(logger, "align", 0.0d, 0.12d, "Running minimap2 self-alignment");
      runMinimap2(buildMinimap2Command(minimap2, options), options.outputDirectory(), paf, logger, processSink, cancellationRequested);
      emitStage(logger, "align", 1.0d, 0.45d, "minimap2 PAF written: " + paf.getFileName());

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
        buildZoomifyCommand(hictk, cool, mcool, resolveZoomResolutions(options, layout), options),
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
          options.conversionThreads()
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

  private static @NotNull List<String> buildMinimap2Command(final @NotNull Path minimap2,
                                                            final @NotNull Options options) {
    final var command = new ArrayList<String>();
    command.add(minimap2.toString());
    command.add("-t");
    command.add(Integer.toString(normalizeThreads(options.alignmentThreads())));
    command.add("-k");
    command.add(Integer.toString(options.minimizerK()));
    command.add("-w");
    command.add(Integer.toString(options.minimizerWindow()));
    command.add("-m");
    command.add(Integer.toString(options.minChainScore()));
    command.add("-P");
    command.add("--dual=no");
    command.add("--no-long-join");
    if (options.skipDiagonal()) {
      command.add("-D");
    }
    command.addAll(parseExtraArguments(options.extraMinimap2Args()));
    command.add(options.fastaPath().toString());
    command.add(options.fastaPath().toString());
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
      throw new IllegalArgumentException("Unterminated quote in extra minimap2 arguments.");
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

  private static void runMinimap2(final @NotNull List<String> command,
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
    try (final var reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
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
      throw new IllegalStateException("minimap2 failed with exit code " + exit);
    }
    if (!Files.isRegularFile(outputPaf) || Files.size(outputPaf) == 0L) {
      throw new IllegalStateException("minimap2 produced an empty PAF file.");
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

  private static @NotNull GeneratedLayout writeChromSizes(final @NotNull Options options,
                                                          final @NotNull Path chromSizes,
                                                          final @NotNull Consumer<String> logger,
                                                          final @NotNull BooleanSupplier cancellationRequested) throws IOException {
    final var chromosomes = new ArrayList<Chromosome>();
    final var byName = new HashMap<String, Chromosome>();
    long totalBins = 0L;
    try (
      final var reader = fastaReader(options.fastaPath());
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
      throw new IllegalArgumentException("No FASTA records found in " + options.fastaPath().getFileName());
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
                                                            final @NotNull GeneratedLayout layout) {
    if (!options.resolutions().isBlank()) {
      return java.util.Arrays.stream(options.resolutions().split(","))
        .map(String::trim)
        .filter(token -> !token.isBlank())
        .map(Long::parseLong)
        .filter(resolution -> resolution > options.binSize())
        .distinct()
        .sorted()
        .toList();
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
    @NotNull String extraMinimap2Args
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
      extraMinimap2Args = extraMinimap2Args == null ? "" : extraMinimap2Args.trim();
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
