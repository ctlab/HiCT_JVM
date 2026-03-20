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

package ru.itmo.ctlab.hict.hict_server.tracks;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.CloseableIterator;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.broad.igv.bbfile.BBFileReader;
import org.broad.igv.bbfile.BigWigIterator;
import org.broad.igv.bbfile.WigItem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;

@Slf4j
public class Track1DManager {
  private static final Set<String> SUPPORTED_EXTENSIONS = Set.of(
    ".bed", ".bed.gz", ".vcf", ".vcf.gz", ".bw", ".bigwig", ".bam"
  );
  private static final List<String> COLOR_PALETTE = List.of(
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ab"
  );
  private static final int MAX_FEATURES_PER_QUERY = 250_000;

  private final @NotNull Path dataDirectory;
  private final @NotNull ReadWriteLock lock = new ReentrantReadWriteLock();
  private final @NotNull LinkedHashMap<String, TrackState> tracks = new LinkedHashMap<>();
  private final @NotNull AtomicLong trackCounter = new AtomicLong(0L);
  private volatile @NotNull Map<String, String> linkedFastaAliasesBySource = Map.of();

  public Track1DManager(final @NotNull Path dataDirectory) {
    this.dataDirectory = dataDirectory.normalize().toAbsolutePath();
  }

  public @NotNull List<String> listTrackFiles() {
    try (final var stream = Files.walk(this.dataDirectory)) {
      return stream
        .filter(Files::isRegularFile)
        .map(this.dataDirectory::relativize)
        .map(Path::toString)
        .filter(this::isSupportedTrackPath)
        .sorted()
        .toList();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to list track files", e);
    }
  }

  public @NotNull TrackSummary openTrack(final @NotNull String relativeFilename,
                                         final String requestedName,
                                         final String requestedColor) {
    final var resolvedPath = resolveDataPath(relativeFilename);
    final var trackType = TrackType.fromPath(resolvedPath);
    if (trackType == TrackType.UNSUPPORTED) {
      throw new IllegalArgumentException(
        "Unsupported track format for " + relativeFilename + ". Supported: BED/VCF/BigWig/BAM."
      );
    }
    final var trackId = "trk_" + this.trackCounter.incrementAndGet();
    final var resolvedName = (requestedName == null || requestedName.isBlank())
      ? resolvedPath.getFileName().toString()
      : requestedName.trim();
    final var color = normalizeColor(requestedColor, colorForIndex((int) this.trackCounter.get() - 1));
    final TrackDataSource dataSource = switch (trackType) {
      case BED -> InMemoryTrackDataSource.fromBed(resolvedPath);
      case VCF -> InMemoryTrackDataSource.fromVcf(resolvedPath);
      case BIGWIG -> new BigWigTrackDataSource(resolvedPath);
      case BAM -> new BamTrackDataSource(resolvedPath);
      case UNSUPPORTED -> throw new IllegalStateException("Unexpected unsupported track type");
    };
    final var state = new TrackState(
      trackId,
      resolvedName,
      trackType,
      relativeFilename,
      color,
      true,
      dataSource,
      BamRenderMode.COVERAGE,
      BigWigAggregationMode.MAX
    );
    try {
      this.lock.writeLock().lock();
      this.tracks.put(trackId, state);
    } catch (final RuntimeException ex) {
      closeDataSourceQuietly(dataSource);
      throw ex;
    } finally {
      this.lock.writeLock().unlock();
    }
    return state.toSummary();
  }

  public @NotNull List<TrackSummary> listTracks() {
    try {
      this.lock.readLock().lock();
      return this.tracks.values().stream().map(TrackState::toSummary).toList();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public @NotNull TrackSummary updateTrack(final @NotNull String trackId,
                                           final Boolean visible,
                                           final String color,
                                           final String name,
                                           final String renderMode,
                                           final String aggregationMode) {
    try {
      this.lock.writeLock().lock();
      final var current = this.tracks.get(trackId);
      if (current == null) {
        throw new IllegalArgumentException("Unknown track id " + trackId);
      }
      final var updated = current.withUpdated(
        visible == null ? current.visible : visible,
        (color == null || color.isBlank()) ? current.color : normalizeColor(color, current.color),
        (name == null || name.isBlank()) ? current.name : name.trim(),
        parseBamRenderMode(renderMode, current.bamRenderMode()),
        parseBigWigAggregationMode(aggregationMode, current.bigWigAggregationMode())
      );
      this.tracks.put(trackId, updated);
      return updated.toSummary();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void removeTrack(final @NotNull String trackId) {
    try {
      this.lock.writeLock().lock();
      final var removed = this.tracks.remove(trackId);
      if (removed != null) {
        closeDataSourceQuietly(removed.dataSource());
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void close() {
    try {
      this.lock.writeLock().lock();
      this.tracks.values().forEach(track -> closeDataSourceQuietly(track.dataSource()));
      this.tracks.clear();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void setLinkedFastaAliasesBySource(final @Nullable Map<String, String> aliases) {
    this.linkedFastaAliasesBySource = aliases == null ? Map.of() : Map.copyOf(aliases);
  }

  public @NotNull QueryResult queryVisibleTracks(final @NotNull ChunkedFile chunkedFile,
                                                 final long startBp,
                                                 final long endBp,
                                                 final int widthPx) {
    final var queryStart = Math.max(0L, Math.min(startBp, endBp));
    final var queryEnd = Math.max(queryStart + 1L, Math.max(startBp, endBp));
    final var safeWidth = Math.max(1, widthPx);
    final Map<String, List<AssemblySegment>> sourceToAssemblySegments =
      buildSourceToAssemblySegments(chunkedFile, this.linkedFastaAliasesBySource);
    final List<TrackRender> trackRenders = new ArrayList<>();
    try {
      this.lock.readLock().lock();
      this.tracks.values().stream()
        .filter(track -> track.visible)
        .forEach(track -> {
          try {
            trackRenders.add(track.query(sourceToAssemblySegments, queryStart, queryEnd, safeWidth));
          } catch (final RuntimeException ex) {
            final var message = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
            log.error("Failed to query 1D track {} ({})", track.name(), track.trackId(), ex);
            trackRenders.add(track.toErrorRender(message));
          }
        });
    } finally {
      this.lock.readLock().unlock();
    }
    return new QueryResult(queryStart, queryEnd, safeWidth, trackRenders);
  }

  private @NotNull Map<String, List<AssemblySegment>> buildSourceToAssemblySegments(final @NotNull ChunkedFile chunkedFile,
                                                                                    final @NotNull Map<String, String> linkedFastaAliasesBySource) {
    final var sourceToAssemblySegments = new HashMap<String, List<AssemblySegment>>();
    final var contigs = chunkedFile.getAssemblyInfo().contigs();
    long assemblyCursor = 0L;
    for (int contigIndex = 0; contigIndex < contigs.size(); ++contigIndex) {
      final ContigTree.ContigTuple tuple = contigs.get(contigIndex);
      final var descriptor = tuple.descriptor();
      final var sourceName = descriptor.getContigNameInSourceFASTA();
      final var sourceStart = descriptor.getOffsetInSourceFASTA();
      final var sourceEnd = sourceStart + descriptor.getLengthBp();
      final var assemblyStart = assemblyCursor;
      final var assemblyEnd = assemblyCursor + descriptor.getLengthBp();
      sourceToAssemblySegments.computeIfAbsent(sourceName, key -> new ArrayList<>())
        .add(new AssemblySegment(sourceStart, sourceEnd, assemblyStart, assemblyEnd, tuple.direction() == ContigDirection.REVERSED));
      final var aliasName = linkedFastaAliasesBySource.get(sourceName);
      if (aliasName != null && !aliasName.equals(sourceName)) {
        sourceToAssemblySegments.computeIfAbsent(aliasName, key -> new ArrayList<>())
          .add(new AssemblySegment(sourceStart, sourceEnd, assemblyStart, assemblyEnd, tuple.direction() == ContigDirection.REVERSED));
      }
      assemblyCursor = assemblyEnd;
    }
    sourceToAssemblySegments.values().forEach(list -> list.sort(Comparator.comparingLong(AssemblySegment::sourceStart)));
    return sourceToAssemblySegments;
  }

  private boolean isSupportedTrackPath(final @NotNull String path) {
    final var lowered = path.toLowerCase(Locale.ROOT);
    return SUPPORTED_EXTENSIONS.stream().anyMatch(lowered::endsWith);
  }

  private @NotNull Path resolveDataPath(final @NotNull String relativePath) {
    final var resolved = this.dataDirectory.resolve(relativePath).normalize().toAbsolutePath();
    if (!resolved.startsWith(this.dataDirectory)) {
      throw new IllegalArgumentException("Path " + relativePath + " is outside DATA_DIR");
    }
    if (!Files.exists(resolved) || !Files.isRegularFile(resolved)) {
      throw new IllegalArgumentException("Track file " + relativePath + " does not exist");
    }
    return resolved;
  }

  private static @NotNull String normalizeColor(final String requestedColor, final @NotNull String fallback) {
    if (requestedColor == null) {
      return fallback;
    }
    final var trimmed = requestedColor.trim();
    if (trimmed.matches("^#[0-9a-fA-F]{6}$")) {
      return trimmed.toLowerCase(Locale.ROOT);
    }
    return fallback;
  }

  private static @NotNull String colorForIndex(final int index) {
    if (COLOR_PALETTE.isEmpty()) {
      return "#4e79a7";
    }
    return COLOR_PALETTE.get(Math.floorMod(index, COLOR_PALETTE.size()));
  }

  private static @NotNull BamRenderMode parseBamRenderMode(final String mode, final @NotNull BamRenderMode fallback) {
    if (mode == null || mode.isBlank()) {
      return fallback;
    }
    try {
      return BamRenderMode.valueOf(mode.trim().toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException ex) {
      throw new IllegalArgumentException("Unsupported BAM render mode: " + mode + ". Supported: COVERAGE, READ_DENSITY");
    }
  }

  private static @NotNull BigWigAggregationMode parseBigWigAggregationMode(final String mode,
                                                                            final @NotNull BigWigAggregationMode fallback) {
    if (mode == null || mode.isBlank()) {
      return fallback;
    }
    try {
      return BigWigAggregationMode.valueOf(mode.trim().toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException ex) {
      throw new IllegalArgumentException("Unsupported BigWig aggregation mode: " + mode + ". Supported: MAX, MEAN, SUM");
    }
  }

  private static void closeDataSourceQuietly(final @NotNull TrackDataSource dataSource) {
    try {
      dataSource.close();
    } catch (final Exception e) {
      log.warn("Failed to close track data source", e);
    }
  }

  private static long parseLongOrThrow(final @NotNull String value, final @NotNull String fieldName, final long lineNo) {
    try {
      return Long.parseLong(value);
    } catch (final NumberFormatException nfe) {
      throw new IllegalArgumentException("Cannot parse " + fieldName + " at line " + lineNo + ": " + value);
    }
  }

  private static double parseOptionalDouble(final String value, final double defaultValue) {
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(value);
    } catch (final NumberFormatException ignored) {
      return defaultValue;
    }
  }

  private static @NotNull BufferedReader openMaybeGzipReader(final @NotNull Path filePath) throws IOException {
    final InputStream baseStream = Files.newInputStream(filePath);
    final InputStream dataStream;
    final var lowered = filePath.getFileName().toString().toLowerCase(Locale.ROOT);
    if (lowered.endsWith(".gz")) {
      dataStream = new GZIPInputStream(baseStream);
    } else {
      dataStream = baseStream;
    }
    return new BufferedReader(new InputStreamReader(dataStream, StandardCharsets.UTF_8));
  }

  private static @NotNull Optional<ProjectedFeature> projectSourceIntervalOnSegment(final @NotNull AssemblySegment segment,
                                                                                     final long sourceStart,
                                                                                     final long sourceEnd,
                                                                                     final double value,
                                                                                     final String label,
                                                                                     final long queryStart,
                                                                                     final long queryEnd) {
    final var clippedSourceStart = Math.max(sourceStart, segment.sourceStart());
    final var clippedSourceEnd = Math.min(sourceEnd, segment.sourceEnd());
    if (clippedSourceEnd <= clippedSourceStart) {
      return Optional.empty();
    }
    final var segmentLength = segment.sourceEnd() - segment.sourceStart();
    final var localStart = clippedSourceStart - segment.sourceStart();
    final var localEnd = clippedSourceEnd - segment.sourceStart();

    final long assemblyStart;
    final long assemblyEnd;
    if (!segment.reversed()) {
      assemblyStart = segment.assemblyStart() + localStart;
      assemblyEnd = segment.assemblyStart() + localEnd;
    } else {
      assemblyStart = segment.assemblyStart() + (segmentLength - localEnd);
      assemblyEnd = segment.assemblyStart() + (segmentLength - localStart);
    }

    final var clippedAssemblyStart = Math.max(queryStart, Math.min(assemblyStart, assemblyEnd));
    final var clippedAssemblyEnd = Math.min(queryEnd, Math.max(assemblyStart, assemblyEnd));
    if (clippedAssemblyEnd <= clippedAssemblyStart) {
      return Optional.empty();
    }
    return Optional.of(new ProjectedFeature(clippedAssemblyStart, clippedAssemblyEnd, Math.max(0.0d, value), label));
  }

  private static @NotNull Optional<SourceInterval> mapAssemblyIntervalToSegmentSource(final @NotNull AssemblySegment segment,
                                                                                       final long assemblyStart,
                                                                                       final long assemblyEnd) {
    final var overlapAssemblyStart = Math.max(assemblyStart, segment.assemblyStart());
    final var overlapAssemblyEnd = Math.min(assemblyEnd, segment.assemblyEnd());
    if (overlapAssemblyEnd <= overlapAssemblyStart) {
      return Optional.empty();
    }
    final var localStart = overlapAssemblyStart - segment.assemblyStart();
    final var localEnd = overlapAssemblyEnd - segment.assemblyStart();
    final var segmentLength = segment.sourceEnd() - segment.sourceStart();
    if (!segment.reversed()) {
      return Optional.of(new SourceInterval(segment.sourceStart() + localStart, segment.sourceStart() + localEnd));
    }
    final var sourceStart = segment.sourceStart() + (segmentLength - localEnd);
    final var sourceEnd = segment.sourceStart() + (segmentLength - localStart);
    return Optional.of(new SourceInterval(sourceStart, sourceEnd));
  }

  private static @NotNull List<TrackBin> aggregateFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                           final long queryStart,
                                                           final long queryEnd,
                                                           final int widthPx) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEnd - queryStart);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final double[] maxValues = new double[bucketCount];
    final long[] counts = new long[bucketCount];
    Arrays.fill(maxValues, 0.0d);
    for (final var feature : projectedFeatures) {
      int left = (int) Math.floor((feature.start() - queryStart) / bucketSpan);
      int right = (int) Math.ceil((feature.end() - queryStart) / bucketSpan) - 1;
      left = Math.max(0, Math.min(left, bucketCount - 1));
      right = Math.max(0, Math.min(right, bucketCount - 1));
      for (int i = left; i <= right; i++) {
        maxValues[i] = Math.max(maxValues[i], feature.value());
        counts[i]++;
      }
    }
    final var bins = new ArrayList<TrackBin>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      if (counts[i] <= 0) {
        continue;
      }
      final var start = queryStart + (long) Math.floor(i * bucketSpan);
      final var end = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
      bins.add(new TrackBin(start, Math.max(start + 1, end), maxValues[i], counts[i], null));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateBigWigFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                 final long queryStart,
                                                                 final long queryEnd,
                                                                 final int widthPx,
                                                                 final @NotNull BigWigAggregationMode mode) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEnd - queryStart);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final double[] maxValues = new double[bucketCount];
    final double[] weightedSums = new double[bucketCount];
    final double[] overlapSums = new double[bucketCount];
    final long[] counts = new long[bucketCount];
    Arrays.fill(maxValues, 0.0d);
    for (final var feature : projectedFeatures) {
      int left = (int) Math.floor((feature.start() - queryStart) / bucketSpan);
      int right = (int) Math.ceil((feature.end() - queryStart) / bucketSpan) - 1;
      left = Math.max(0, Math.min(left, bucketCount - 1));
      right = Math.max(0, Math.min(right, bucketCount - 1));
      for (int i = left; i <= right; i++) {
        final var bucketStart = queryStart + (long) Math.floor(i * bucketSpan);
        final var bucketEnd = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
        final var overlap = Math.min(feature.end(), bucketEnd) - Math.max(feature.start(), bucketStart);
        if (overlap <= 0L) {
          continue;
        }
        maxValues[i] = Math.max(maxValues[i], feature.value());
        weightedSums[i] += feature.value() * overlap;
        overlapSums[i] += overlap;
        counts[i]++;
      }
    }
    final var bins = new ArrayList<TrackBin>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      if (counts[i] <= 0) {
        continue;
      }
      final var start = queryStart + (long) Math.floor(i * bucketSpan);
      final var end = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
      final var bucketWidth = Math.max(1.0d, end - start);
      final double value = switch (mode) {
        case MAX -> maxValues[i];
        case MEAN -> overlapSums[i] > 0.0d ? weightedSums[i] / overlapSums[i] : 0.0d;
        case SUM -> weightedSums[i] / bucketWidth;
      };
      bins.add(new TrackBin(start, Math.max(start + 1, end), value, counts[i], null));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateCoverageFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                   final long queryStart,
                                                                   final long queryEnd,
                                                                   final int widthPx) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEnd - queryStart);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final double[] coverage = new double[bucketCount];
    final long[] counts = new long[bucketCount];
    Arrays.fill(coverage, 0.0d);
    for (final var feature : projectedFeatures) {
      int left = (int) Math.floor((feature.start() - queryStart) / bucketSpan);
      int right = (int) Math.ceil((feature.end() - queryStart) / bucketSpan) - 1;
      left = Math.max(0, Math.min(left, bucketCount - 1));
      right = Math.max(0, Math.min(right, bucketCount - 1));
      for (int i = left; i <= right; i++) {
        final var bucketStart = queryStart + (long) Math.floor(i * bucketSpan);
        final var bucketEnd = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
        final var overlap = Math.min(feature.end(), bucketEnd) - Math.max(feature.start(), bucketStart);
        if (overlap <= 0L) {
          continue;
        }
        final var norm = overlap / Math.max(1.0d, (double) (bucketEnd - bucketStart));
        coverage[i] += norm;
        counts[i]++;
      }
    }
    final var bins = new ArrayList<TrackBin>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      if (counts[i] <= 0) {
        continue;
      }
      final var start = queryStart + (long) Math.floor(i * bucketSpan);
      final var end = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
      bins.add(new TrackBin(start, Math.max(start + 1, end), coverage[i], counts[i], null));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateReadDensityFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                      final long queryStart,
                                                                      final long queryEnd,
                                                                      final int widthPx) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEnd - queryStart);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final double[] values = new double[bucketCount];
    final long[] counts = new long[bucketCount];
    Arrays.fill(values, 0.0d);
    for (final var feature : projectedFeatures) {
      final var center = feature.start() + ((feature.end() - feature.start()) >>> 1);
      int idx = (int) Math.floor((center - queryStart) / bucketSpan);
      idx = Math.max(0, Math.min(idx, bucketCount - 1));
      values[idx] += 1.0d;
      counts[idx] += 1L;
    }
    final var bins = new ArrayList<TrackBin>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      if (counts[i] <= 0L) {
        continue;
      }
      final var start = queryStart + (long) Math.floor(i * bucketSpan);
      final var end = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
      bins.add(new TrackBin(start, Math.max(start + 1, end), values[i], counts[i], null));
    }
    return bins;
  }

  private static void accumulateBigWigValue(final long featureStart,
                                            final long featureEnd,
                                            final double featureValue,
                                            final long queryStart,
                                            final long queryEnd,
                                            final double bucketSpan,
                                            final double[] maxValues,
                                            final double[] weightedSums,
                                            final double[] overlapSums,
                                            final long[] counts) {
    int left = (int) Math.floor((featureStart - queryStart) / bucketSpan);
    int right = (int) Math.ceil((featureEnd - queryStart) / bucketSpan) - 1;
    left = Math.max(0, Math.min(left, counts.length - 1));
    right = Math.max(0, Math.min(right, counts.length - 1));
    for (int i = left; i <= right; i++) {
      final var bucketStart = queryStart + (long) Math.floor(i * bucketSpan);
      final var bucketEnd = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
      final var overlap = Math.min(featureEnd, bucketEnd) - Math.max(featureStart, bucketStart);
      if (overlap <= 0L) {
        continue;
      }
      maxValues[i] = Math.max(maxValues[i], featureValue);
      weightedSums[i] += featureValue * overlap;
      overlapSums[i] += overlap;
      counts[i]++;
    }
  }

  private static void accumulateCoverageValue(final long featureStart,
                                              final long featureEnd,
                                              final long queryStart,
                                              final long queryEnd,
                                              final double bucketSpan,
                                              final double[] coverage,
                                              final long[] counts) {
    int left = (int) Math.floor((featureStart - queryStart) / bucketSpan);
    int right = (int) Math.ceil((featureEnd - queryStart) / bucketSpan) - 1;
    left = Math.max(0, Math.min(left, counts.length - 1));
    right = Math.max(0, Math.min(right, counts.length - 1));
    for (int i = left; i <= right; i++) {
      final var bucketStart = queryStart + (long) Math.floor(i * bucketSpan);
      final var bucketEnd = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
      final var overlap = Math.min(featureEnd, bucketEnd) - Math.max(featureStart, bucketStart);
      if (overlap <= 0L) {
        continue;
      }
      coverage[i] += overlap / Math.max(1.0d, (double) (bucketEnd - bucketStart));
      counts[i]++;
    }
  }

  private static void accumulateReadDensityValue(final long featureStart,
                                                 final long featureEnd,
                                                 final long queryStart,
                                                 final double bucketSpan,
                                                 final double[] values,
                                                 final long[] counts) {
    final var center = featureStart + ((featureEnd - featureStart) >>> 1);
    int idx = (int) Math.floor((center - queryStart) / bucketSpan);
    idx = Math.max(0, Math.min(idx, counts.length - 1));
    values[idx] += 1.0d;
    counts[idx] += 1L;
  }

  private static @NotNull List<TrackBin> finalizeBigWigBins(final long queryStart,
                                                            final long queryEnd,
                                                            final double bucketSpan,
                                                            final double[] maxValues,
                                                            final double[] weightedSums,
                                                            final double[] overlapSums,
                                                            final long[] counts,
                                                            final @NotNull BigWigAggregationMode mode) {
    final var bins = new ArrayList<TrackBin>(counts.length);
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] <= 0L) {
        continue;
      }
      final var start = queryStart + (long) Math.floor(i * bucketSpan);
      final var end = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
      final var bucketWidth = Math.max(1.0d, end - start);
      final double value = switch (mode) {
        case MAX -> maxValues[i];
        case MEAN -> overlapSums[i] > 0.0d ? weightedSums[i] / overlapSums[i] : 0.0d;
        case SUM -> weightedSums[i] / bucketWidth;
      };
      bins.add(new TrackBin(start, Math.max(start + 1, end), value, counts[i], null));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> finalizeBins(final long queryStart,
                                                      final long queryEnd,
                                                      final double bucketSpan,
                                                      final double[] values,
                                                      final long[] counts) {
    final var bins = new ArrayList<TrackBin>(counts.length);
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] <= 0L) {
        continue;
      }
      final var start = queryStart + (long) Math.floor(i * bucketSpan);
      final var end = Math.min(queryEnd, queryStart + (long) Math.ceil((i + 1) * bucketSpan));
      bins.add(new TrackBin(start, Math.max(start + 1, end), values[i], counts[i], null));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> toBins(final @NotNull List<ProjectedFeature> projectedFeatures) {
    return projectedFeatures.stream()
      .map(f -> new TrackBin(f.start(), f.end(), f.value(), 1L, f.label()))
      .toList();
  }

  private enum TrackType {
    BED,
    VCF,
    BIGWIG,
    BAM,
    UNSUPPORTED;

    private static @NotNull TrackType fromPath(final @NotNull Path path) {
      final var lowered = path.getFileName().toString().toLowerCase(Locale.ROOT);
      if (lowered.endsWith(".bed") || lowered.endsWith(".bed.gz")) {
        return BED;
      }
      if (lowered.endsWith(".vcf") || lowered.endsWith(".vcf.gz")) {
        return VCF;
      }
      if (lowered.endsWith(".bw") || lowered.endsWith(".bigwig")) {
        return BIGWIG;
      }
      if (lowered.endsWith(".bam")) {
        return BAM;
      }
      return UNSUPPORTED;
    }
  }

  private enum BamRenderMode {
    COVERAGE,
    READ_DENSITY
  }

  private enum BigWigAggregationMode {
    MAX,
    MEAN,
    SUM
  }

  private interface TrackDataSource extends AutoCloseable {
    long featureCountHint();

    @NotNull
    List<ProjectedFeature> projectFeatures(@NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                           long queryStart,
                                           long queryEnd);

    @Override
    default void close() throws Exception {
      // no-op
    }
  }

  private record InMemoryTrackDataSource(@NotNull Map<String, List<FeatureRange>> featuresBySource,
                                         long featureCount) implements TrackDataSource {
    static @NotNull InMemoryTrackDataSource fromBed(final @NotNull Path filePath) {
      final var features = new HashMap<String, List<FeatureRange>>();
      long total = 0L;
      try (final BufferedReader reader = openMaybeGzipReader(filePath)) {
        String line;
        long lineNo = 0L;
        while ((line = reader.readLine()) != null) {
          lineNo++;
          if (line.isBlank() || line.startsWith("#") || line.startsWith("track ") || line.startsWith("browser ")) {
            continue;
          }
          final var fields = line.split("\t");
          if (fields.length < 3) {
            continue;
          }
          final var sourceName = fields[0];
          final long start = parseLongOrThrow(fields[1], "BED start", lineNo);
          final long end = parseLongOrThrow(fields[2], "BED end", lineNo);
          if (end <= start) {
            continue;
          }
          final var label = (fields.length >= 4 && !fields[3].isBlank()) ? fields[3] : null;
          final var value = parseOptionalDouble(fields.length >= 5 ? fields[4] : null, 1.0d);
          features.computeIfAbsent(sourceName, ignored -> new ArrayList<>())
            .add(new FeatureRange(start, end, Math.max(0.0d, value), label));
          total++;
        }
      } catch (final IOException e) {
        throw new RuntimeException("Failed to parse BED track " + filePath, e);
      }
      features.values().forEach(list -> list.sort(Comparator.comparingLong(FeatureRange::start)));
      return new InMemoryTrackDataSource(features, total);
    }

    static @NotNull InMemoryTrackDataSource fromVcf(final @NotNull Path filePath) {
      final var features = new HashMap<String, List<FeatureRange>>();
      long total = 0L;
      try (final BufferedReader reader = openMaybeGzipReader(filePath)) {
        String line;
        long lineNo = 0L;
        while ((line = reader.readLine()) != null) {
          lineNo++;
          if (line.isBlank() || line.startsWith("#")) {
            continue;
          }
          final var fields = line.split("\t");
          if (fields.length < 5) {
            continue;
          }
          final var sourceName = fields[0];
          final long pos1Based = parseLongOrThrow(fields[1], "VCF POS", lineNo);
          final long start = Math.max(0L, pos1Based - 1L);
          final var ref = fields[3];
          final long end = start + Math.max(1, ref.length());
          final var id = (fields[2] != null && !fields[2].isBlank() && !".".equals(fields[2])) ? fields[2] : null;
          final var alt = fields[4];
          final var label = (id != null) ? id : (ref + ">" + alt);
          features.computeIfAbsent(sourceName, ignored -> new ArrayList<>())
            .add(new FeatureRange(start, end, 1.0d, label));
          total++;
        }
      } catch (final IOException e) {
        throw new RuntimeException("Failed to parse VCF track " + filePath, e);
      }
      features.values().forEach(list -> list.sort(Comparator.comparingLong(FeatureRange::start)));
      return new InMemoryTrackDataSource(features, total);
    }

    @Override
    public long featureCountHint() {
      return this.featureCount;
    }

    @Override
    public @NotNull List<ProjectedFeature> projectFeatures(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                           final long queryStart,
                                                           final long queryEnd) {
      final var projected = new ArrayList<ProjectedFeature>();
      for (final var entry : this.featuresBySource.entrySet()) {
        final var sourceName = entry.getKey();
        final var sourceFeatures = entry.getValue();
        if (sourceFeatures.isEmpty()) {
          continue;
        }
        final var assemblySegments = sourceToAssemblySegments.get(sourceName);
        if (assemblySegments == null || assemblySegments.isEmpty()) {
          continue;
        }
        for (final var segment : assemblySegments) {
          int index = lowerBoundByStart(sourceFeatures, segment.sourceStart());
          if (index > 0) {
            index--;
          }
          for (int i = index; i < sourceFeatures.size(); i++) {
            final var feature = sourceFeatures.get(i);
            if (feature.start() >= segment.sourceEnd()) {
              break;
            }
            if (feature.end() <= segment.sourceStart()) {
              continue;
            }
            projectSourceIntervalOnSegment(
              segment,
              feature.start(),
              feature.end(),
              feature.value(),
              feature.label(),
              queryStart,
              queryEnd
            ).ifPresent(projected::add);
            if (projected.size() > MAX_FEATURES_PER_QUERY) {
              projected.sort(Comparator.comparingLong(ProjectedFeature::start));
              return projected;
            }
          }
        }
      }
      projected.sort(Comparator.comparingLong(ProjectedFeature::start));
      return projected;
    }

    private static int lowerBoundByStart(final @NotNull List<FeatureRange> features, final long targetStart) {
      int lo = 0;
      int hi = features.size();
      while (lo < hi) {
        final int mid = (lo + hi) >>> 1;
        if (features.get(mid).start() < targetStart) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      return lo;
    }
  }

  private static final class BigWigTrackDataSource implements TrackDataSource {
    private final @NotNull Path path;
    private final @NotNull BBFileReader reader;
    private final @NotNull Set<String> sourceNames;

    private BigWigTrackDataSource(final @NotNull Path path) {
      this.path = path;
      try {
        this.reader = new BBFileReader(path.toString());
        if (!this.reader.isBigWigFile()) {
          closeQuietly();
          throw new IllegalArgumentException("File " + path + " is not a BigWig");
        }
        this.sourceNames = new HashSet<>(this.reader.getChromosomeNames());
      } catch (final IOException e) {
        throw new RuntimeException("Failed to open BigWig " + path, e);
      }
    }

    @Override
    public long featureCountHint() {
      return -1L;
    }

    @Override
    public synchronized @NotNull List<ProjectedFeature> projectFeatures(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                                        final long queryStart,
                                                                        final long queryEnd) {
      final var projected = new ArrayList<ProjectedFeature>();
      for (final var entry : sourceToAssemblySegments.entrySet()) {
        final var sourceName = entry.getKey();
        if (!this.sourceNames.contains(sourceName)) {
          continue;
        }
        for (final var segment : entry.getValue()) {
          final var sourceIntervalOptional = mapAssemblyIntervalToSegmentSource(segment, queryStart, queryEnd);
          if (sourceIntervalOptional.isEmpty()) {
            continue;
          }
          final var sourceInterval = sourceIntervalOptional.get();
          final var queryStartClamped = clampToInt(sourceInterval.start());
          final var queryEndClamped = clampToInt(sourceInterval.end());
          if (queryEndClamped <= queryStartClamped) {
            continue;
          }
          final BigWigIterator iterator = this.reader.getBigWigIterator(
            sourceName,
            queryStartClamped,
            sourceName,
            queryEndClamped,
            false
          );
          if (iterator == null) {
            continue;
          }
          while (iterator.hasNext()) {
            final WigItem item = iterator.next();
            final var itemStart = item.getStartBase();
            final var itemEnd = item.getEndBase();
            projectSourceIntervalOnSegment(
              segment,
              itemStart,
              itemEnd,
              Math.abs(item.getWigValue()),
              null,
              queryStart,
              queryEnd
            ).ifPresent(projected::add);
            if (projected.size() > MAX_FEATURES_PER_QUERY) {
              projected.sort(Comparator.comparingLong(ProjectedFeature::start));
              return projected;
            }
          }
        }
      }
      projected.sort(Comparator.comparingLong(ProjectedFeature::start));
      return projected;
    }

    public synchronized @NotNull List<TrackBin> queryBins(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                          final long queryStart,
                                                          final long queryEnd,
                                                          final int widthPx,
                                                          final @NotNull BigWigAggregationMode mode) {
      final var bucketCount = Math.max(1, widthPx);
      final var span = Math.max(1L, queryEnd - queryStart);
      final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
      final double[] maxValues = new double[bucketCount];
      final double[] weightedSums = new double[bucketCount];
      final double[] overlapSums = new double[bucketCount];
      final long[] counts = new long[bucketCount];
      for (final var entry : sourceToAssemblySegments.entrySet()) {
        final var sourceName = entry.getKey();
        if (!this.sourceNames.contains(sourceName)) {
          continue;
        }
        for (final var segment : entry.getValue()) {
          final var sourceIntervalOptional = mapAssemblyIntervalToSegmentSource(segment, queryStart, queryEnd);
          if (sourceIntervalOptional.isEmpty()) {
            continue;
          }
          final var sourceInterval = sourceIntervalOptional.get();
          final var queryStartClamped = clampToInt(sourceInterval.start());
          final var queryEndClamped = clampToInt(sourceInterval.end());
          if (queryEndClamped <= queryStartClamped) {
            continue;
          }
          final BigWigIterator iterator = this.reader.getBigWigIterator(
            sourceName,
            queryStartClamped,
            sourceName,
            queryEndClamped,
            false
          );
          if (iterator == null) {
            continue;
          }
          while (iterator.hasNext()) {
            final WigItem item = iterator.next();
            final var projectedFeature = projectSourceIntervalOnSegment(
              segment,
              item.getStartBase(),
              item.getEndBase(),
              Math.abs(item.getWigValue()),
              null,
              queryStart,
              queryEnd
            );
            if (projectedFeature.isEmpty()) {
              continue;
            }
            final var feature = projectedFeature.get();
            accumulateBigWigValue(
              feature.start(),
              feature.end(),
              feature.value(),
              queryStart,
              queryEnd,
              bucketSpan,
              maxValues,
              weightedSums,
              overlapSums,
              counts
            );
          }
        }
      }
      return finalizeBigWigBins(
        queryStart,
        queryEnd,
        bucketSpan,
        maxValues,
        weightedSums,
        overlapSums,
        counts,
        mode
      );
    }

    @Override
    public void close() throws Exception {
      if (this.reader.getBBFis() != null) {
        this.reader.getBBFis().close();
      }
    }

    private void closeQuietly() {
      try {
        close();
      } catch (final Exception ignored) {
      }
    }

    private static int clampToInt(final long value) {
      if (value <= 0L) {
        return 0;
      }
      if (value >= Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      return (int) value;
    }
  }

  private static final class BamTrackDataSource implements TrackDataSource {
    private final @NotNull Path path;
    private final @NotNull SamReader reader;
    private final @NotNull Set<String> sequenceNames;

    private BamTrackDataSource(final @NotNull Path path) {
      this.path = path;
      final SamReaderFactory factory = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT);
      try {
        this.reader = factory.open(path.toFile());
        if (!this.reader.hasIndex()) {
          this.reader.close();
          throw new IllegalArgumentException(
            "Failed to open BAM " + path.getFileName() + ": BAM index (.bai) is required and must be readable."
          );
        }
      } catch (final RuntimeException ex) {
        throw new RuntimeException("Failed to open BAM " + path + ". Ensure BAM index (.bai) is present and readable.", ex);
      } catch (final IOException ex) {
        throw new RuntimeException("Failed to open BAM " + path + ". Ensure BAM index (.bai) is present and readable.", ex);
      }
      final SAMSequenceDictionary dictionary = this.reader.getFileHeader().getSequenceDictionary();
      final var names = new HashSet<String>();
      dictionary.getSequences().forEach(seq -> names.add(seq.getSequenceName()));
      this.sequenceNames = names;
    }

    @Override
    public long featureCountHint() {
      return -1L;
    }

    @Override
    public synchronized @NotNull List<ProjectedFeature> projectFeatures(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                                        final long queryStart,
                                                                        final long queryEnd) {
      final var projected = new ArrayList<ProjectedFeature>();
      for (final var entry : sourceToAssemblySegments.entrySet()) {
        final var sourceName = entry.getKey();
        if (!this.sequenceNames.contains(sourceName)) {
          continue;
        }
        for (final var segment : entry.getValue()) {
          final var sourceIntervalOptional = mapAssemblyIntervalToSegmentSource(segment, queryStart, queryEnd);
          if (sourceIntervalOptional.isEmpty()) {
            continue;
          }
          final var sourceInterval = sourceIntervalOptional.get();
          final int startInclusive1 = Math.max(1, clampToInt(sourceInterval.start()) + 1);
          final int endInclusive1 = Math.max(startInclusive1, clampToInt(sourceInterval.end()));
          try (final CloseableIterator<SAMRecord> iterator = this.reader.query(sourceName, startInclusive1, endInclusive1, false)) {
            while (iterator.hasNext()) {
              final SAMRecord record = iterator.next();
              if (record.getReadUnmappedFlag()) {
                continue;
              }
              final long recordStart = Math.max(0L, record.getAlignmentStart() - 1L);
              final long recordEnd = Math.max(recordStart + 1L, record.getAlignmentEnd());
              projectSourceIntervalOnSegment(
                segment,
                recordStart,
                recordEnd,
                1.0d,
                null,
                queryStart,
                queryEnd
              ).ifPresent(projected::add);
              if (projected.size() > MAX_FEATURES_PER_QUERY) {
                projected.sort(Comparator.comparingLong(ProjectedFeature::start));
                return projected;
              }
            }
          }
        }
      }
      projected.sort(Comparator.comparingLong(ProjectedFeature::start));
      return projected;
    }

    public synchronized @NotNull List<TrackBin> queryBins(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                          final long queryStart,
                                                          final long queryEnd,
                                                          final int widthPx,
                                                          final @NotNull BamRenderMode mode) {
      final var bucketCount = Math.max(1, widthPx);
      final var span = Math.max(1L, queryEnd - queryStart);
      final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
      final double[] values = new double[bucketCount];
      final long[] counts = new long[bucketCount];
      for (final var entry : sourceToAssemblySegments.entrySet()) {
        final var sourceName = entry.getKey();
        if (!this.sequenceNames.contains(sourceName)) {
          continue;
        }
        for (final var segment : entry.getValue()) {
          final var sourceIntervalOptional = mapAssemblyIntervalToSegmentSource(segment, queryStart, queryEnd);
          if (sourceIntervalOptional.isEmpty()) {
            continue;
          }
          final var sourceInterval = sourceIntervalOptional.get();
          final int startInclusive1 = Math.max(1, clampToInt(sourceInterval.start()) + 1);
          final int endInclusive1 = Math.max(startInclusive1, clampToInt(sourceInterval.end()));
          try (final CloseableIterator<SAMRecord> iterator = this.reader.query(sourceName, startInclusive1, endInclusive1, false)) {
            while (iterator.hasNext()) {
              final SAMRecord record = iterator.next();
              if (record.getReadUnmappedFlag()) {
                continue;
              }
              final var projectedFeature = projectSourceIntervalOnSegment(
                segment,
                Math.max(0L, record.getAlignmentStart() - 1L),
                Math.max(Math.max(0L, record.getAlignmentStart() - 1L) + 1L, record.getAlignmentEnd()),
                1.0d,
                null,
                queryStart,
                queryEnd
              );
              if (projectedFeature.isEmpty()) {
                continue;
              }
              final var feature = projectedFeature.get();
              if (mode == BamRenderMode.READ_DENSITY) {
                accumulateReadDensityValue(
                  feature.start(),
                  feature.end(),
                  queryStart,
                  bucketSpan,
                  values,
                  counts
                );
              } else {
                accumulateCoverageValue(
                  feature.start(),
                  feature.end(),
                  queryStart,
                  queryEnd,
                  bucketSpan,
                  values,
                  counts
                );
              }
            }
          }
        }
      }
      return finalizeBins(queryStart, queryEnd, bucketSpan, values, counts);
    }

    @Override
    public void close() throws Exception {
      this.reader.close();
    }

    private static int clampToInt(final long value) {
      if (value <= 0L) {
        return 0;
      }
      if (value >= Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      return (int) value;
    }
  }

  private record SourceInterval(long start, long end) {
  }

  private record AssemblySegment(long sourceStart, long sourceEnd, long assemblyStart, long assemblyEnd, boolean reversed) {
  }

  private record FeatureRange(long start, long end, double value, String label) {
  }

  private record ProjectedFeature(long start, long end, double value, String label) {
  }

  @Getter
  @RequiredArgsConstructor
  public static final class QueryResult {
    private final long startBp;
    private final long endBp;
    private final int widthPx;
    private final @NotNull List<TrackRender> tracks;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class TrackSummary {
    private final @NotNull String trackId;
    private final @NotNull String name;
    private final @NotNull String type;
    private final @NotNull String sourceFile;
    private final @NotNull String color;
    private final boolean visible;
    private final long featureCount;
    private final @NotNull String renderMode;
    private final @NotNull String aggregationMode;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class TrackRender {
    private final @NotNull String trackId;
    private final @NotNull String name;
    private final @NotNull String type;
    private final @NotNull String color;
    private final @NotNull List<TrackBin> bins;
    private final double maxValue;
    private final String error;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class TrackBin {
    private final long startBp;
    private final long endBp;
    private final double value;
    private final long count;
    private final String label;
  }

  private record TrackState(@NotNull String trackId,
                            @NotNull String name,
                            @NotNull TrackType type,
                            @NotNull String sourceFile,
                            @NotNull String color,
                            boolean visible,
                            @NotNull TrackDataSource dataSource,
                            @NotNull BamRenderMode bamRenderMode,
                            @NotNull BigWigAggregationMode bigWigAggregationMode) {
    private TrackSummary toSummary() {
      return new TrackSummary(
        trackId,
        name,
        type.name(),
        sourceFile,
        color,
        visible,
        dataSource.featureCountHint(),
        bamRenderMode.name(),
        bigWigAggregationMode.name()
      );
    }

    private TrackRender toErrorRender(final @NotNull String message) {
      return new TrackRender(
        trackId,
        name,
        type.name(),
        color,
        List.of(),
        0.0d,
        message
      );
    }

    private TrackState withUpdated(final boolean newVisible,
                                   final @NotNull String newColor,
                                   final @NotNull String newName,
                                   final @NotNull BamRenderMode newBamRenderMode,
                                   final @NotNull BigWigAggregationMode newBigWigAggregationMode) {
      return new TrackState(
        trackId,
        newName,
        type,
        sourceFile,
        newColor,
        newVisible,
        dataSource,
        newBamRenderMode,
        newBigWigAggregationMode
      );
    }

    private TrackRender query(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                              final long queryStart,
                              final long queryEnd,
                              final int widthPx) {
      final List<TrackBin> bins;
      if (type == TrackType.BAM && dataSource instanceof BamTrackDataSource bamDataSource) {
        bins = bamDataSource.queryBins(sourceToAssemblySegments, queryStart, queryEnd, widthPx, bamRenderMode);
      } else if (type == TrackType.BIGWIG && dataSource instanceof BigWigTrackDataSource bigWigDataSource) {
        bins = bigWigDataSource.queryBins(sourceToAssemblySegments, queryStart, queryEnd, widthPx, bigWigAggregationMode);
      } else {
        final var projectedFeatures = dataSource.projectFeatures(sourceToAssemblySegments, queryStart, queryEnd);
        final var maxFeatureCount = Math.max(widthPx * 8, 8192);
        if (projectedFeatures.size() > maxFeatureCount) {
          bins = aggregateFeatures(projectedFeatures, queryStart, queryEnd, widthPx);
        } else {
          bins = toBins(projectedFeatures);
        }
      }
      final var maxValue = bins.stream().mapToDouble(TrackBin::getValue).max().orElse(0.0d);
      return new TrackRender(trackId, name, type.name(), color, bins, maxValue, null);
    }
  }
}
