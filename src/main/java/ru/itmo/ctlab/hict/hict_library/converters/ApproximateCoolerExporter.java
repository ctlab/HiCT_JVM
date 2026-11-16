package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.*;
import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

import java.io.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Opt-in approximate float64 Cooler writer. Original HiCT contacts are opened read-only. */
public final class ApproximateCoolerExporter {
  private static final class Sum {
    double value, correction;
    void add(double term) {
      final double adjusted = term - correction, next = value + adjusted;
      correction = (next - value) - adjusted; value = next;
      if (!Double.isFinite(value)) throw new IllegalArgumentException("Contact total exceeds float64 range");
    }
  }

  public void export(ConversionOptions options, Consumer<String> logger, BooleanSupplier cancelled) throws IOException {
    if (!options.approximateRebin()) throw new IllegalArgumentException("Approximate rebinning must be explicitly enabled");
    if (!options.applyAgpBeforeExport() || options.agpPath().isBlank())
      throw new IllegalArgumentException("Approximate rebinning requires a current or selected AGP assembly");
    if (options.balanceExportedCoolers())
      throw new IllegalArgumentException("Approximate rebinning exports unbalanced float64 values; disable export balancing");
    final var requested = Path.of(options.agpPath());
    final var agp = requested.isAbsolute() ? requested : options.inputPath().resolveSibling(requested).normalize();
    try (var reader = Files.newBufferedReader(agp, StandardCharsets.UTF_8)) {
      export(options, AGPProcessor.parseRecordsFromReader(reader), logger, cancelled);
    } catch (NoSuchFieldException invalid) { throw new IOException("Invalid approximate export AGP", invalid); }
  }

  public void export(ConversionOptions options, List<AGPProcessor.AGPFileRecord> records,
      Consumer<String> logger, BooleanSupplier cancelled) throws IOException {
    ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer.initializeHDF5Library();
    final var output = options.outputPath().toAbsolutePath().normalize();
    final var input = options.inputPath().toAbsolutePath().normalize();
    if (input.equals(output) || Files.exists(output) && Files.isSameFile(input, output))
      throw new IllegalArgumentException("Approximate Cooler output must differ from the original HiCT file");
    if (Files.isSymbolicLink(output)) throw new IllegalArgumentException("Approximate output must not be a symbolic link");
    final var available = HictToMcoolConverter.readAvailableResolutions(input);
    final var selected = HictToMcoolConverter.normalizeSelectedResolutionsForOutput(output,
      HictToMcoolConverter.requireUsableSelectedResolutions(input,
        HictToMcoolConverter.resolveSelectedResolutions(available,
          options.resolutions(), options.exportAllResolutions()), available,
        options.resolutions(), options.exportAllResolutions()), logger);
    final boolean multi = output.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".mcool");
    if (!multi && !output.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".cool"))
      throw new IllegalArgumentException("Approximate output must be a .cool or .mcool file");
    Files.createDirectories(output.getParent());
    final var temporary = Files.createTempFile(output.getParent(), ".hict-approximate-", ".hdf5");
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(input, 1, 2, selected))) {
      final int chunk = Math.max(1, Math.min(65536, options.chunkSize()));
      final var integerStorage = HictToMcoolConverter.resolveIntStorageFeatures(options, logger);
      final var floatStorage = HictToMcoolConverter.resolveFloatStorageFeatures(options, logger);
      try (var writer = HDF5Factory.configure(temporary.toFile()).overwrite().writer()) {
        if (multi) {
          writer.string().setAttrVL("/", "format", "HDF5::MCOOL");
          writer.int64().setAttr("/", "format-version", 2);
        }
        ru.itmo.ctlab.hict.hict_library.chunkedfile.ApproximationProvenance.mark(writer,
          new ru.itmo.ctlab.hict.hict_library.chunkedfile.ApproximationProvenance.Summary(1,
            ApproximateContactProjection.METHOD, "independent_stored_levels", "Estimated values from sequence-overlap rebinning.",
            file.getApproximationProvenance() != null));
        writer.string().writeVL("/hict/approximate_export/agp", records.stream().map(Object::toString).collect(Collectors.joining("\n", "", "\n")));
        writer.string().setAttrVL("/hict/approximate_export", "method", ApproximateContactProjection.METHOD);
        int level = 0;
        for (var resolution : selected) {
          check(cancelled);
          final int currentLevel = level;
          logger.accept("HICT_STAGE stage=approximate_grid progress=0 overall=" + (level / (double) selected.size()) + " detail=Preparing " + resolution + " bp grid");
          logger.accept("Approximate rebinning at " + resolution + " bp (" + ApproximateContactProjection.METHOD + ")");
          final var projection = ApproximateContactProjection.prepare(file, records, resolution);
          final int levelChunk = (int) Math.max(1, Math.min(chunk, projection.targetBinCount()));
          final long budget = Math.min(HictToMcoolConverter.resolveExportMemoryLimitBytes(logger), Runtime.getRuntime().maxMemory() / 2);
          final long indexBytes = Math.multiplyExact(Math.addExact(projection.targetBinCount(), 1), Long.BYTES);
          if (indexBytes > budget / 2 || projection.targetBinCount() >= Integer.MAX_VALUE || budget < 16384)
            throw new IllegalArgumentException("Approximate export index exceeds the memory budget; use a coarser resolution or increase the export memory/Java heap limit");
          final int capacity = (int) Math.min(100000, Math.max(16, (budget - indexBytes) / 256));
          final String root = multi ? "/resolutions/" + resolution : "/";
          writeAxes(writer, root, projection, records, levelChunk, integerStorage, floatStorage, cancelled);
          final var sourceTotal = new Sum(); final var retainedTotal = new Sum(); final var absoluteTotal = new Sum();
          final int[] lastProgress = {-5};
          try (var sorter = new FractionalContactSorter(output.getParent(), capacity, 32, cancelled)) {
            // Bounded cache amortizes repeated endpoint projections without keeping an axis-sized map.
            // Contact scanning reads each unordered physical source pair exactly once.
            file.matrixQueries().scanStoredContacts(ResolutionDescriptor.fromBpResolution(resolution, file), (row, col, value) -> {
              check(cancelled); sourceTotal.add(value); absoluteTotal.add(Math.abs(value));
              retainedTotal.add(projection.projectContact(row, col, value, (r, c, v) -> {
                try { sorter.add(r, c, v); } catch (IOException error) { throw new UncheckedIOException(error); }
              }, cancelled));
            }, cancelled, progress -> {
              final int percent = (int) Math.round(100 * progress);
              if (percent >= lastProgress[0] + 5) {
                lastProgress[0] = percent;
                logger.accept("Approximate " + resolution + " bp source scan: " + percent + "%");
                logger.accept("HICT_STAGE stage=approximate_scan progress=" + progress + " overall="
                  + ((currentLevel + .65 * progress) / selected.size()) + " detail=Resolution " + resolution + " bp");
              }
            });
            final var pixels = new PixelWriter(writer, root, projection, levelChunk, integerStorage, floatStorage);
            logger.accept("HICT_STAGE stage=approximate_merge progress=0 overall=" + ((level + .65) / selected.size()) + " detail=Sorting fractional contacts at " + resolution + " bp");
            sorter.finish(pixels::accept); pixels.finish();
            final double tolerance = Math.max(128 * Math.ulp(absoluteTotal.value), 1e-10 * absoluteTotal.value);
            if (Math.abs(retainedTotal.value - pixels.total.value) > tolerance)
              throw new IllegalStateException("Approximate export failed contact conservation validation");
            final var metadata = new JsonObject().put("hict_approximate_export", true)
              .put("method", ApproximateContactProjection.METHOD)
              .put("resolution_policy", "independent_stored_levels")
              .put("assumption", "Independent uniform endpoint positions within each original bin's actual sequence interval")
              .put("source_contact_total", sourceTotal.value).put("expected_retained_contact_total", retainedTotal.value)
              .put("exported_contact_total", pixels.total.value).put("conservation_tolerance", tolerance)
              .put("native_contacts_modified", false).put("omitted_sequence_renormalized", false)
              .put("upstream_approximate", file.getApproximationProvenance() != null)
              .put("sequence_coverage_column", "hict_sequence_fraction");
            writeMetadata(writer, root, projection, pixels, metadata);
            logger.accept("Approximate " + resolution + " bp complete: " + pixels.count + " pixels, retained contacts=" + pixels.total.value);
            level++;
            logger.accept("HICT_STAGE stage=approximate_merge progress=1 overall=" + (level / (double) selected.size()) + " detail=Validated " + resolution + " bp output");
          } catch (UncheckedIOException error) { throw error.getCause(); }
          catch (ru.itmo.ctlab.hict.hict_library.visualization.CoverageNormalizer.CancelledException ignored) {
            throw new CancellationException("Approximate export was cancelled");
          }
        }
      }
      check(cancelled);
      try { Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
      catch (AtomicMoveNotSupportedException ignored) { Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING); }
      logger.accept("HICT_STAGE stage=approximate_export progress=1 overall=1 detail=Approximate Cooler validated and saved");
    } finally { Files.deleteIfExists(temporary); }
  }

  private static String path(String root, String child) { return root.equals("/") ? "/" + child : root + "/" + child; }
  private static void check(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new CancellationException();
  }
  private static void writeAxes(IHDF5Writer writer, String root, ApproximateContactProjection projection,
      List<AGPProcessor.AGPFileRecord> records, int chunk, HDF5IntStorageFeatures ints,
      HDF5FloatStorageFeatures floats, BooleanSupplier cancelled) {
    final var chroms = projection.chromosomes(); final long size = projection.targetBinCount();
    writer.string().writeArray(path(root, "chroms/name"), chroms.stream().map(ApproximateContactProjection.Chromosome::name).toArray(String[]::new));
    writer.int64().writeArray(path(root, "chroms/length"), chroms.stream().mapToLong(ApproximateContactProjection.Chromosome::lengthBp).toArray(), ints);
    final long[] chromOffsets = new long[chroms.size() + 1]; chromOffsets[chroms.size()] = size;
    for (String column : List.of("chrom", "start", "end")) writer.int64().createArray(path(root, "bins/" + column), size, chunk, ints);
    writer.float64().createArray(path(root, "bins/hict_sequence_fraction"), size, chunk, floats);
    final var intervals = new HashMap<String, List<long[]>>();
    for (var record : records) if (record instanceof AGPProcessor.ContigAGPRecord)
      intervals.computeIfAbsent(record.getScaffoldName(), ignored -> new ArrayList<>()).add(new long[]{record.getInterScaffoldStartIncl() - 1, record.getInterScaffoldEndIncl()});
    for (int c = 0; c < chroms.size(); c++) {
      final var chrom = chroms.get(c); chromOffsets[c] = chrom.firstBin();
      final var pieces = intervals.getOrDefault(chrom.name(), List.of()); int piece = 0;
      for (long offset = 0; offset < chrom.binCount(); offset += chunk) {
        check(cancelled); final int count = (int) Math.min(chunk, chrom.binCount() - offset);
        final var ids = new long[count]; final var starts = new long[count]; final var ends = new long[count]; final var fractions = new double[count];
        Arrays.fill(ids, c);
        for (int i = 0; i < count; i++) {
          final long start = (offset + i) * projection.resolution(), end = Math.min(chrom.lengthBp(), start + projection.resolution());
          starts[i] = start; ends[i] = end;
          while (piece < pieces.size() && pieces.get(piece)[1] <= start) piece++;
          long covered = 0;
          for (int p = piece; p < pieces.size() && pieces.get(p)[0] < end; p++)
            covered += Math.max(0, Math.min(end, pieces.get(p)[1]) - Math.max(start, pieces.get(p)[0]));
          fractions[i] = covered / (double) (end - start);
        }
        final long destination = chrom.firstBin() + offset;
        writer.int64().writeArrayBlockWithOffset(path(root, "bins/chrom"), ids, count, destination);
        writer.int64().writeArrayBlockWithOffset(path(root, "bins/start"), starts, count, destination);
        writer.int64().writeArrayBlockWithOffset(path(root, "bins/end"), ends, count, destination);
        writer.float64().writeArrayBlockWithOffset(path(root, "bins/hict_sequence_fraction"), fractions, count, destination);
      }
    }
    writer.int64().writeArray(path(root, "indexes/chrom_offset"), chromOffsets, ints);
  }

  private static final class PixelWriter {
    final IHDF5Writer writer; final String root; final ApproximateContactProjection projection;
    final long[] offsets, rows, cols; final double[] values; final Sum total = new Sum(), cis = new Sum();
    long count, written; int buffered, nextRow;
    PixelWriter(IHDF5Writer writer, String root, ApproximateContactProjection projection, int chunk,
        HDF5IntStorageFeatures ints, HDF5FloatStorageFeatures floats) {
      this.writer = writer; this.root = root; this.projection = projection;
      offsets = new long[Math.toIntExact(projection.targetBinCount() + 1)];
      rows = new long[chunk]; cols = new long[chunk]; values = new double[chunk];
      writer.int64().createArray(path(root, "pixels/bin1_id"), 0L, chunk, ints);
      writer.int64().createArray(path(root, "pixels/bin2_id"), 0L, chunk, ints);
      writer.float64().createArray(path(root, "pixels/count"), 0L, chunk, floats);
    }
    void accept(long row, long col, double value) {
      if (row < 0 || col < row || col >= projection.targetBinCount()) throw new IllegalArgumentException("Projected pixel outside output grid");
      while (nextRow <= row) offsets[nextRow++] = count;
      rows[buffered] = row; cols[buffered] = col; values[buffered++] = value; count++; total.add(value);
      if (chromosome(row) == chromosome(col)) cis.add(value);
      if (buffered == rows.length) flush();
    }
    int chromosome(long bin) {
      int lo = 0, hi = projection.chromosomes().size();
      while (lo < hi) { int mid = (lo + hi) >>> 1; if (projection.chromosomes().get(mid).firstBin() <= bin) lo = mid + 1; else hi = mid; }
      return lo - 1;
    }
    void flush() {
      if (buffered == 0) return;
      writer.int64().writeArrayBlockWithOffset(path(root, "pixels/bin1_id"), rows, buffered, written);
      writer.int64().writeArrayBlockWithOffset(path(root, "pixels/bin2_id"), cols, buffered, written);
      writer.float64().writeArrayBlockWithOffset(path(root, "pixels/count"), values, buffered, written);
      written += buffered; buffered = 0;
    }
    void finish() {
      flush(); while (nextRow < offsets.length) offsets[nextRow++] = count;
      writer.int64().writeArray(path(root, "indexes/bin1_offset"), offsets);
    }
  }

  private static void writeMetadata(IHDF5Writer writer, String root, ApproximateContactProjection projection,
      PixelWriter pixels, JsonObject metadata) {
    writer.string().setAttrVL(root, "format", "HDF5::Cooler"); writer.int64().setAttr(root, "format-version", 3);
    writer.string().setAttrVL(root, "format-url", "https://github.com/open2c/cooler");
    writer.string().setAttrVL(root, "generated-by", "HiCT approximate sequence-overlap exporter");
    writer.string().setAttrVL(root, "creation-date", OffsetDateTime.now(ZoneOffset.UTC).toString());
    writer.string().setAttrVL(root, "storage-mode", "symmetric-upper"); writer.string().setAttrVL(root, "bin-type", "fixed");
    writer.int64().setAttr(root, "bin-size", projection.resolution()); writer.int64().setAttr(root, "nbins", projection.targetBinCount());
    writer.int64().setAttr(root, "nchroms", projection.chromosomes().size()); writer.int64().setAttr(root, "nnz", pixels.count);
    writer.float64().setAttr(root, "sum", pixels.total.value); writer.float64().setAttr(root, "cis", pixels.cis.value);
    writer.string().setAttrVL(root, "metadata", metadata.encode());
  }
}
