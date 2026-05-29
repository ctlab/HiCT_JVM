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

import ch.systemsx.cisd.hdf5.HDF5FloatStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
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
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_server.util.cache.FileFingerprint;
import ru.itmo.ctlab.hict.hict_server.util.cache.FileFingerprintService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;

@Slf4j
public class Track1DManager {
  private static final Set<String> SUPPORTED_EXTENSIONS = Set.of(
    ".bed", ".bed.gz",
    ".vcf", ".vcf.gz",
    ".gff", ".gff.gz", ".gff3", ".gff3.gz", ".gtf", ".gtf.gz",
    ".bw", ".bigwig", ".bam"
  );
  private static final List<String> COLOR_PALETTE = List.of(
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ab"
  );
  private static final int MAX_FEATURES_PER_QUERY = 250_000;
  private static final int FEATURE_DIRECT_RENDER_MIN_FEATURES = 1_024;
  private static final int FEATURE_DIRECT_RENDER_MAX_FEATURES = 12_000;
  private static final int FEATURE_DIRECT_RENDER_FEATURES_PER_PIXEL = 6;
  private static final int FEATURE_DOWNSAMPLED_FEATURES_PER_PIXEL = 2;
  private static final long BED_FEATURE_STYLE_MAX_FEATURES = 50_000L;
  private static final String COOLER_WEIGHTS_SOURCE_FILE = "__internal__/cooler_weights";
  private static final String COOLER_WEIGHTS_SOURCE_FILE_PRIMARY = "__internal__/cooler_weights/PRIMARY";
  private static final String COOLER_WEIGHTS_SOURCE_FILE_SECONDARY = "__internal__/cooler_weights/SECONDARY";
  private static final String PRECOMPUTE_CACHE_VERSION = "1";
  private static final String PRECOMPUTE_META_GROUP_PATH = "/cache_meta";
  private static final long MAX_PRECOMPUTE_VISIBLE_PIXELS = 2_000_000L;
  private static final int PRECOMPUTE_JOB_THREADS = resolveThreadCount("HICT_TRACK_PRECOMPUTE_JOB_THREADS", 2);
  private static final int PRECOMPUTE_WORKER_THREADS = resolveThreadCount(
    "HICT_TRACK_PRECOMPUTE_WORKER_THREADS",
    Math.max(2, Runtime.getRuntime().availableProcessors())
  );
  private static final int PRECOMPUTE_DATASET_CHUNK_SIZE = 8192;
  private static final int PRECOMPUTE_COMPRESSION_LEVEL = 4;
  private static final long PRECOMPUTE_STATUS_TTL_MS = 15L * 60_000L;

  private final @NotNull Path dataDirectory;
  private final @NotNull Path processedDirectory;
  private final @NotNull ReadWriteLock lock = new ReentrantReadWriteLock();
  private final @NotNull LinkedHashMap<String, TrackState> tracks = new LinkedHashMap<>();
  private final @NotNull AtomicLong trackCounter = new AtomicLong(0L);
  private final @NotNull ExecutorService precomputeJobExecutor;
  private final @NotNull ExecutorService precomputeWorkerExecutor;
  private final @NotNull ExecutorService precomputeWriterExecutor;
  private final @NotNull ConcurrentHashMap<PrecomputedSeriesKey, PrecomputedSeries> precomputedSeriesCache = new ConcurrentHashMap<>();
  private final @NotNull ConcurrentHashMap<String, TrackPrecomputeRuntime> precomputeRuntimeByTrackId = new ConcurrentHashMap<>();
  private final @NotNull FileFingerprintService fingerprintService = new FileFingerprintService();
  private volatile @NotNull Map<String, String> linkedFastaAliasesBySource = Map.of();

  public Track1DManager(final @NotNull Path dataDirectory) {
    this(dataDirectory, dataDirectory.resolve("processed"));
  }

  public Track1DManager(final @NotNull Path dataDirectory, final @Nullable Path processedDirectory) {
    this.dataDirectory = dataDirectory.normalize().toAbsolutePath();
    this.processedDirectory = (processedDirectory == null ? this.dataDirectory.resolve("processed") : processedDirectory)
      .normalize()
      .toAbsolutePath();
    try {
      Files.createDirectories(this.processedDirectory);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to create processed directory " + this.processedDirectory, e);
    }
    this.precomputeJobExecutor = Executors.newFixedThreadPool(
      PRECOMPUTE_JOB_THREADS,
      namedDaemonThreadFactory("hict-track-precompute-job")
    );
    this.precomputeWorkerExecutor = Executors.newFixedThreadPool(
      PRECOMPUTE_WORKER_THREADS,
      namedDaemonThreadFactory("hict-track-precompute-worker")
    );
    this.precomputeWriterExecutor = Executors.newSingleThreadExecutor(
      namedDaemonThreadFactory("hict-track-precompute-writer")
    );
  }

  private static @NotNull ThreadFactory namedDaemonThreadFactory(final @NotNull String prefix) {
    final var counter = new AtomicLong(0L);
    return runnable -> {
      final var thread = new Thread(runnable);
      thread.setDaemon(true);
      thread.setName(prefix + "-" + counter.incrementAndGet());
      return thread;
    };
  }

  private static int resolveThreadCount(final @NotNull String envKey, final int defaultValue) {
    final String env = System.getenv(envKey);
    if (env != null && !env.isBlank()) {
      try {
        return Math.max(1, Integer.parseInt(env.trim()));
      } catch (final NumberFormatException ignored) {
        // Fallback to default.
      }
    }
    final String property = System.getProperty(envKey);
    if (property != null && !property.isBlank()) {
      try {
        return Math.max(1, Integer.parseInt(property.trim()));
      } catch (final NumberFormatException ignored) {
        // Fallback to default.
      }
    }
    return Math.max(1, defaultValue);
  }

  public @NotNull List<String> listTrackFiles() {
    final var files = new ArrayList<Path>();
    try {
      Files.walkFileTree(this.dataDirectory, new SimpleFileVisitor<>() {
        @Override
        public @NotNull FileVisitResult preVisitDirectory(final @NotNull Path dir,
                                                          final @NotNull BasicFileAttributes attrs) {
          if (!dir.equals(dataDirectory) && (Files.isSymbolicLink(dir) || attrs.isOther())) {
            log.debug("Skipping non-standard directory while listing track files: {}", dir);
            return FileVisitResult.SKIP_SUBTREE;
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public @NotNull FileVisitResult visitFile(final @NotNull Path file,
                                                  final @NotNull BasicFileAttributes attrs) {
          if (attrs.isRegularFile()) {
            files.add(file);
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public @NotNull FileVisitResult visitFileFailed(final @NotNull Path file,
                                                        final @NotNull IOException exc) {
          log.warn("Skipping inaccessible filesystem path while listing track files: {} ({})", file, exc.toString());
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (final IOException e) {
      throw new RuntimeException("Failed to list track files under " + this.dataDirectory, e);
    }
    return files.stream()
      .map(this.dataDirectory::relativize)
      .map(Path::toString)
      .filter(this::isSupportedTrackPath)
      .sorted()
      .toList();
  }

  public @NotNull TrackSummary openTrack(final @NotNull String relativeFilename,
                                         final String requestedName,
                                         final String requestedColor) {
    final var resolvedPath = resolveDataPath(relativeFilename);
    final var trackType = TrackType.fromPath(resolvedPath);
    if (trackType == TrackType.UNSUPPORTED) {
      throw new IllegalArgumentException(
        "Unsupported track format for " + relativeFilename + ". Supported: BED/VCF/GFF/GTF/BigWig/BAM."
      );
    }
    final var trackId = "trk_" + this.trackCounter.incrementAndGet();
    final var resolvedName = (requestedName == null || requestedName.isBlank())
      ? resolvedPath.getFileName().toString()
      : requestedName.trim();
    final var color = normalizeColor(requestedColor, colorForIndex((int) this.trackCounter.get() - 1));
    final TrackDataSource dataSource = createDataSource(trackType, resolvedPath);
    final var state = new TrackState(
      trackId,
      resolvedName,
      trackType,
      relativeFilename,
      color,
      true,
      dataSource,
      BamRenderMode.COVERAGE,
      BigWigAggregationMode.MAX,
      false,
      true,
      0.0d,
      1.0d
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

  public @NotNull TrackSummary openCoolerWeightsTrack(final String requestedName,
                                                      final String requestedColor) {
    return openCoolerWeightsTrack(requestedName, requestedColor, "PRIMARY");
  }

  public @NotNull TrackSummary openCoolerWeightsTrack(final String requestedName,
                                                      final String requestedColor,
                                                      final String requestedSource) {
    final var source = normalizeSourceName(requestedSource);
    final var trackId = "trk_" + this.trackCounter.incrementAndGet();
    final var resolvedName = (requestedName == null || requestedName.isBlank())
      ? ("SECONDARY".equals(source) ? "Cooler weights - Secondary" : "Cooler weights - Primary")
      : requestedName.trim();
    final var color = normalizeColor(requestedColor, colorForIndex((int) this.trackCounter.get() - 1));
    final TrackDataSource dataSource = new CoolerWeightsTrackDataSource(source);
    final var state = new TrackState(
      trackId,
      resolvedName,
      TrackType.COOLER_WEIGHTS,
      coolerWeightsSourceFile(source),
      color,
      true,
      dataSource,
      BamRenderMode.COVERAGE,
      BigWigAggregationMode.MAX,
      false,
      true,
      0.0d,
      1.0d
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

  public @NotNull TrackCompatibilityReport probeTrackCompatibility(final @NotNull ChunkedFile chunkedFile,
                                                                   final @NotNull String relativeFilename) {
    final var resolvedPath = resolveDataPath(relativeFilename);
    final var trackType = TrackType.fromPath(resolvedPath);
    if (trackType == TrackType.UNSUPPORTED) {
      throw new IllegalArgumentException(
        "Unsupported track format for " + relativeFilename + ". Supported: BED/VCF/GFF/GTF/BigWig/BAM."
      );
    }
    final var sourceNameSet = buildSourceNameSet(chunkedFile, this.linkedFastaAliasesBySource);
    final var assemblyNameSet = buildAssemblyNameSet(chunkedFile);
    final TrackDataSource dataSource = createDataSource(trackType, resolvedPath);
    try {
      final var trackNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
      trackNames.addAll(dataSource.sourceNames());
      final int total = trackNames.size();
      int matchedSource = 0;
      int matchedAssembly = 0;
      int matchedAny = 0;
      final var unknownNames = new ArrayList<String>();
      for (final var name : trackNames) {
        final var inSource = sourceNameSet.contains(name);
        final var inAssembly = assemblyNameSet.contains(name);
        if (inSource) {
          matchedSource++;
        }
        if (inAssembly) {
          matchedAssembly++;
        }
        if (inSource || inAssembly) {
          matchedAny++;
        } else if (unknownNames.size() < 32) {
          unknownNames.add(name);
        }
      }
      final var status = resolveCompatibilityStatus(total, matchedAny);
      final var recommendation = matchedSource >= matchedAssembly ? "SOURCE" : "ASSEMBLY";
      final var message = buildCompatibilityMessage(trackType, total, matchedAny, unknownNames.size());
      return new TrackCompatibilityReport(
        relativeFilename,
        trackType.name(),
        status,
        total,
        matchedSource,
        matchedAssembly,
        matchedAny,
        unknownNames,
        recommendation,
        message
      );
    } finally {
      closeDataSourceQuietly(dataSource);
    }
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
                                           final String aggregationMode,
                                           final Boolean logScale) {
    return updateTrack(
      trackId,
      visible,
      color,
      name,
      renderMode,
      aggregationMode,
      logScale,
      null,
      null,
      null
    );
  }

  public @NotNull TrackSummary updateTrack(final @NotNull String trackId,
                                           final Boolean visible,
                                           final String color,
                                           final String name,
                                           final String renderMode,
                                           final String aggregationMode,
                                           final Boolean logScale,
                                           final Boolean rangeAuto,
                                           final Double rangeMin,
                                           final Double rangeMax) {
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
        parseBigWigAggregationMode(aggregationMode, current.bigWigAggregationMode()),
        logScale == null ? current.logScale() : logScale,
        rangeAuto == null ? current.rangeAuto() : rangeAuto,
        sanitizeTrackRangeValue(rangeMin, current.rangeMin()),
        sanitizeTrackRangeValue(rangeMax, current.rangeMax())
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
      this.precomputeRuntimeByTrackId.remove(trackId);
      this.precomputedSeriesCache.keySet().removeIf(key -> key.trackId().equals(trackId));
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public @NotNull List<TrackSummary> reorderTrack(final @NotNull String trackId,
                                                  final int targetIndex) {
    try {
      this.lock.writeLock().lock();
      final var entries = new ArrayList<>(this.tracks.values());
      final var sourceIndex = findTrackIndex(entries, trackId);
      if (sourceIndex < 0) {
        throw new IllegalArgumentException("Unknown track id " + trackId);
      }
      final var moved = entries.remove(sourceIndex);
      final var clampedTarget = Math.max(0, Math.min(targetIndex, entries.size()));
      entries.add(clampedTarget, moved);
      this.tracks.clear();
      for (final var entry : entries) {
        this.tracks.put(entry.trackId(), entry);
      }
      return this.tracks.values().stream().map(TrackState::toSummary).toList();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void close() {
    try {
      this.lock.writeLock().lock();
      this.tracks.values().forEach(track -> closeDataSourceQuietly(track.dataSource()));
      this.tracks.clear();
      this.precomputeRuntimeByTrackId.clear();
      this.precomputedSeriesCache.clear();
    } finally {
      this.lock.writeLock().unlock();
    }
    this.precomputeJobExecutor.shutdownNow();
    this.precomputeWorkerExecutor.shutdownNow();
    this.precomputeWriterExecutor.shutdownNow();
  }

  public void setLinkedFastaAliasesBySource(final @Nullable Map<String, String> aliases) {
    this.linkedFastaAliasesBySource = aliases == null ? Map.of() : Map.copyOf(aliases);
  }

  public void invalidateInMemoryCache() {
    this.precomputedSeriesCache.clear();
  }

  public void clearPrecomputeStatus() {
    this.precomputeRuntimeByTrackId.clear();
  }

  public @NotNull TracksPrecomputeStatus getPrecomputeStatus() {
    final var now = System.currentTimeMillis();
    this.precomputeRuntimeByTrackId.entrySet().removeIf(entry ->
      now - entry.getValue().lastUpdatedMs() > PRECOMPUTE_STATUS_TTL_MS
    );
    final var statuses = this.precomputeRuntimeByTrackId.values().stream()
      .sorted(Comparator.comparing(TrackPrecomputeRuntime::trackName))
      .map(TrackPrecomputeRuntime::toStatus)
      .toList();
    final var runningCount = statuses.stream()
      .filter(status -> "queued".equals(status.getStatus()) || "running".equals(status.getStatus()))
      .count();
    return new TracksPrecomputeStatus(statuses, (int) runningCount, this.processedDirectory.toString());
  }

  public @NotNull TracksPrecomputeStatus startPrecompute(final @NotNull ChunkedFile chunkedFile,
                                                         final @Nullable String trackId,
                                                         final boolean force) {
    return startPrecompute(chunkedFile, null, trackId, force);
  }

  public @NotNull TracksPrecomputeStatus startPrecompute(final @NotNull ChunkedFile primaryChunkedFile,
                                                         final @Nullable ChunkedFile secondaryChunkedFile,
                                                         final @Nullable String trackId,
                                                         final boolean force) {
    final List<TrackState> selectedTracks;
    try {
      this.lock.readLock().lock();
      if (trackId != null && !trackId.isBlank()) {
        final var state = this.tracks.get(trackId);
        if (state == null) {
          throw new IllegalArgumentException("Unknown track id " + trackId);
        }
        selectedTracks = List.of(state);
      } else {
        selectedTracks = this.tracks.values().stream().toList();
      }
    } finally {
      this.lock.readLock().unlock();
    }

    for (final var state : selectedTracks) {
      scheduleTrackPrecompute(resolveChunkedFileForTrack(primaryChunkedFile, secondaryChunkedFile, state), state, force);
    }
    return getPrecomputeStatus();
  }

  public @NotNull QueryResult queryVisibleTracks(final @NotNull ChunkedFile chunkedFile,
                                                 final long start,
                                                 final long end,
                                                 final int widthPx,
                                                 final long bpResolution,
                                                 final @NotNull QueryLengthUnit units) {
    return queryVisibleTracks(chunkedFile, null, start, end, widthPx, bpResolution, units);
  }

  public @NotNull QueryResult queryVisibleTracks(final @NotNull ChunkedFile primaryChunkedFile,
                                                 final @Nullable ChunkedFile secondaryChunkedFile,
                                                 final long start,
                                                 final long end,
                                                 final int widthPx,
                                                 final long bpResolution,
                                                 final @NotNull QueryLengthUnit units) {
    final var safeWidth = Math.max(1, widthPx);
    final var segmentsBuildResult =
      buildSourceToAssemblySegments(primaryChunkedFile, this.linkedFastaAliasesBySource, bpResolution);
    final var totalVisiblePixels = segmentsBuildResult.totalVisiblePixels();
    if (totalVisiblePixels <= 0L) {
      return new QueryResult(0L, 1L, 0L, 1L, safeWidth, bpResolution, List.of());
    }
    final var queryPxRange = resolveQueryPxRange(
      primaryChunkedFile,
      start,
      end,
      bpResolution,
      units,
      segmentsBuildResult.orderedSegments(),
      totalVisiblePixels
    );
    return queryVisibleTracksInternal(
      primaryChunkedFile,
      secondaryChunkedFile,
      segmentsBuildResult,
      queryPxRange.startPx(),
      queryPxRange.endPx(),
      safeWidth,
      bpResolution
    );
  }

  public @NotNull QueryResult queryVisibleTracks(final @NotNull ChunkedFile chunkedFile,
                                                 final long startPx,
                                                 final long endPx,
                                                 final int widthPx,
                                                 final long bpResolution) {
    return queryVisibleTracks(chunkedFile, null, startPx, endPx, widthPx, bpResolution);
  }

  public @NotNull QueryResult queryVisibleTracks(final @NotNull ChunkedFile primaryChunkedFile,
                                                 final @Nullable ChunkedFile secondaryChunkedFile,
                                                 final long startPx,
                                                 final long endPx,
                                                 final int widthPx,
                                                 final long bpResolution) {
    final var safeWidth = Math.max(1, widthPx);
    final var segmentsBuildResult =
      buildSourceToAssemblySegments(primaryChunkedFile, this.linkedFastaAliasesBySource, bpResolution);
    final var totalVisiblePixels = segmentsBuildResult.totalVisiblePixels();
    if (totalVisiblePixels <= 0L) {
      return new QueryResult(0L, 1L, 0L, 1L, safeWidth, bpResolution, List.of());
    }
    final var queryStartPx = Math.max(0L, Math.min(Math.min(startPx, endPx), totalVisiblePixels - 1L));
    final var queryEndPx = Math.max(queryStartPx + 1L, Math.min(Math.max(startPx, endPx), totalVisiblePixels));
    return queryVisibleTracksInternal(primaryChunkedFile, secondaryChunkedFile, segmentsBuildResult, queryStartPx, queryEndPx, safeWidth, bpResolution);
  }

  public @NotNull FeatureSearchResponse searchFeatures(final @NotNull ChunkedFile chunkedFile,
                                                       final @Nullable String query,
                                                       final int limit,
                                                       final int offset,
                                                       final @Nullable String trackId) {
    final var normalizedQuery = query == null ? "" : query.trim();
    if (normalizedQuery.length() < 2) {
      return new FeatureSearchResponse(normalizedQuery, 0, 0, false, List.of());
    }
    final var safeLimit = Math.max(1, Math.min(500, limit));
    final var safeOffset = Math.max(0, offset);
    final var queryLower = normalizedQuery.toLowerCase(Locale.ROOT);
    final var sourceToAssemblySegments = buildSourceToAssemblyBpSegments(
      chunkedFile,
      this.linkedFastaAliasesBySource
    );
    final List<TrackState> snapshot;
    try {
      this.lock.readLock().lock();
      if (trackId != null && !trackId.isBlank()) {
        final var state = this.tracks.get(trackId);
        snapshot = state == null ? List.of() : List.of(state);
      } else {
        snapshot = this.tracks.values().stream().toList();
      }
    } finally {
      this.lock.readLock().unlock();
    }

    final var hits = new ArrayList<FeatureSearchHit>(safeLimit);
    int skipped = 0;
    boolean hasMore = false;
    outer:
    for (final var track : snapshot) {
      if (!(track.dataSource() instanceof InMemoryTrackDataSource inMemoryDataSource)) {
        continue;
      }
      final var trackNameMatches = track.name().toLowerCase(Locale.ROOT).contains(queryLower);
      for (final var sourceEntry : inMemoryDataSource.featuresBySource().entrySet()) {
        final var sourceName = sourceEntry.getKey();
        final var assemblySegments = sourceToAssemblySegments.get(sourceName);
        if (assemblySegments == null || assemblySegments.isEmpty()) {
          continue;
        }
        final var sourceNameMatches = sourceName.toLowerCase(Locale.ROOT).contains(queryLower);
        for (final var feature : sourceEntry.getValue()) {
          final var featureLabel = resolveFeatureSearchLabel(feature, sourceName);
          final var featureType = normalizeBlankToNull(feature.featureType());
          final var strand = normalizeBlankToNull(feature.strand());
          final var featureLabelMatches = featureLabel.toLowerCase(Locale.ROOT).contains(queryLower);
          final var featureTypeMatches = featureType != null
            && featureType.toLowerCase(Locale.ROOT).contains(queryLower);
          if (!(trackNameMatches || sourceNameMatches || featureLabelMatches || featureTypeMatches)) {
            continue;
          }
          for (final var segment : assemblySegments) {
            final var interval = projectSourceIntervalToAssemblyBp(segment, feature.start(), feature.end());
            if (interval.isEmpty()) {
              continue;
            }
            if (skipped < safeOffset) {
              skipped++;
              continue;
            }
            if (hits.size() >= safeLimit) {
              hasMore = true;
              break outer;
            }
            final var projected = interval.get();
            hits.add(new FeatureSearchHit(
              track.trackId(),
              track.name(),
              sourceName,
              featureLabel,
              featureType,
              strand,
              projected.startBp(),
              projected.endBp()
            ));
          }
        }
      }
    }
    return new FeatureSearchResponse(normalizedQuery, safeLimit, safeOffset, hasMore, hits);
  }

  public @NotNull FeatureContextResponse queryFeatureContext(final @NotNull ChunkedFile chunkedFile,
                                                             final long startBp,
                                                             final long endBp,
                                                             final int widthPx,
                                                             final long bpResolution,
                                                             final double marginScreens) {
    final var safeStartBp = Math.max(0L, Math.min(startBp, endBp));
    final var safeEndBp = Math.max(safeStartBp + 1L, Math.max(startBp, endBp));
    final var safeMarginScreens = Double.isFinite(marginScreens)
      ? Math.max(0.0d, Math.min(4.0d, marginScreens))
      : 1.0d;
    final var safeWidthPx = Math.max(64, Math.min(4096, widthPx));
    final var featureSpanBp = Math.max(1L, safeEndBp - safeStartBp);
    final var screenSpanBp = Math.max(featureSpanBp, safeWidthPx * Math.max(1L, bpResolution));
    final var marginBp = (long) Math.floor(screenSpanBp * safeMarginScreens);
    final var centerBp = safeStartBp + ((safeEndBp - safeStartBp) >>> 1);
    final var halfSpan = screenSpanBp >>> 1;
    final var contextStartBp = Math.max(0L, centerBp - halfSpan - marginBp);
    final var contextEndBp = Math.max(
      contextStartBp + 1L,
      centerBp + halfSpan + marginBp + 1L
    );
    final var contextWidthPx = Math.max(
      safeWidthPx,
      Math.min(8192, (int) Math.ceil(safeWidthPx * (1.0d + 2.0d * safeMarginScreens)))
    );
    final var contextQuery = queryVisibleTracks(
      chunkedFile,
      contextStartBp,
      contextEndBp,
      contextWidthPx,
      bpResolution,
      QueryLengthUnit.BASE_PAIRS
    );
    return new FeatureContextResponse(
      safeStartBp,
      safeEndBp,
      contextStartBp,
      contextEndBp,
      safeMarginScreens,
      contextWidthPx,
      bpResolution,
      contextQuery
    );
  }

  public double @NotNull [] sampleTrackValues(final @NotNull ChunkedFile chunkedFile,
                                               final @NotNull String trackId,
                                               final long start,
                                               final long end,
                                               final long bpResolution,
                                               final @NotNull QueryLengthUnit units) {
    final TrackState track;
    try {
      this.lock.readLock().lock();
      track = this.tracks.get(trackId);
    } finally {
      this.lock.readLock().unlock();
    }
    if (track == null) {
      return new double[0];
    }

    final var segmentsBuildResult =
      buildSourceToAssemblySegments(chunkedFile, this.linkedFastaAliasesBySource, bpResolution);
    final var totalVisiblePixels = segmentsBuildResult.totalVisiblePixels();
    if (totalVisiblePixels <= 0L) {
      return new double[0];
    }

    final var pxRange = resolveQueryPxRange(
      chunkedFile,
      start,
      end,
      bpResolution,
      units,
      segmentsBuildResult.orderedSegments(),
      totalVisiblePixels
    );
    final var queryStartPx = pxRange.startPx();
    final var queryEndPx = pxRange.endPx();
    final int widthPx = (int) Math.max(1L, Math.min(Integer.MAX_VALUE, queryEndPx - queryStartPx));

    final var bins = queryBinsForTrack(
      track.type(),
      chunkedFile,
      track.dataSource(),
      segmentsBuildResult.sourceToAssemblySegments(),
      segmentsBuildResult.orderedSegments(),
      queryStartPx,
      queryEndPx,
      widthPx,
      bpResolution,
      track.bamRenderMode(),
      track.bigWigAggregationMode()
    );

    final var values = new double[widthPx];
    Arrays.fill(values, Double.NaN);
    for (final var bin : bins) {
      final long rawStartPx = bin.getStartPx() != null
        ? bin.getStartPx()
        : mapAssemblyBpToVisiblePx(bin.getStartBp(), segmentsBuildResult.orderedSegments(), bpResolution, totalVisiblePixels);
      final long rawEndPx = bin.getEndPx() != null
        ? bin.getEndPx()
        : mapAssemblyBpToVisiblePx(Math.max(bin.getStartBp(), bin.getEndBp() - bpResolution), segmentsBuildResult.orderedSegments(), bpResolution, totalVisiblePixels) + 1L;
      final int from = (int) Math.max(0L, Math.min(rawStartPx - queryStartPx, widthPx - 1L));
      final int to = (int) Math.max(from + 1L, Math.min(rawEndPx - queryStartPx, widthPx));
      final var value = bin.getValue();
      for (int idx = from; idx < to; idx++) {
        if (!Double.isFinite(value)) {
          continue;
        }
        if (Double.isNaN(values[idx])) {
          values[idx] = value;
        } else {
          values[idx] = Math.max(values[idx], value);
        }
      }
    }
    for (int i = 0; i < values.length; i++) {
      if (Double.isNaN(values[i]) || !Double.isFinite(values[i])) {
        values[i] = 0.0d;
      }
    }
    return values;
  }

  private static @NotNull ChunkedFile resolveChunkedFileForTrack(final @NotNull ChunkedFile primaryChunkedFile,
                                                                  final @Nullable ChunkedFile secondaryChunkedFile,
                                                                  final @NotNull TrackState track) {
    if (track.dataSource() instanceof CoolerWeightsTrackDataSource coolerWeightsTrackDataSource
      && "SECONDARY".equals(coolerWeightsTrackDataSource.source())) {
      if (secondaryChunkedFile == null) {
        throw new IllegalStateException("Secondary source is not attached");
      }
      return secondaryChunkedFile;
    }
    return primaryChunkedFile;
  }

  private @NotNull QueryResult queryVisibleTracksInternal(final @NotNull ChunkedFile primaryChunkedFile,
                                                          final @Nullable ChunkedFile secondaryChunkedFile,
                                                          final @NotNull SegmentBuildResult segmentsBuildResult,
                                                          final long queryStartPx,
                                                          final long queryEndPx,
                                                          final int safeWidth,
                                                          final long bpResolution) {
    final Map<String, List<AssemblySegment>> sourceToAssemblySegments =
      segmentsBuildResult.sourceToAssemblySegments();
    final var assemblySignature = computeAssemblySignature(segmentsBuildResult.orderedSegments(), bpResolution);
    final List<TrackRender> trackRenders = new ArrayList<>();
    try {
      this.lock.readLock().lock();
      this.tracks.values().stream()
        .filter(track -> track.visible)
        .forEach(track -> {
          try {
            final var trackChunkedFile = resolveChunkedFileForTrack(primaryChunkedFile, secondaryChunkedFile, track);
            final var precomputeRuntime = maybeScheduleTrackPrecomputeFromQuery(trackChunkedFile, track);
            final var maybePrecomputed = getPrecomputedBinsIfReady(
              trackChunkedFile,
              track,
              sourceToAssemblySegments,
              segmentsBuildResult.orderedSegments(),
              queryStartPx,
              queryEndPx,
              safeWidth,
              bpResolution,
              assemblySignature
            );
            if (maybePrecomputed != null) {
              trackRenders.add(maybePrecomputed);
            } else if (precomputeRuntime != null
              && precomputeRuntime.isActive()
              && track.dataSource().renderStyle() != RenderStyle.FEATURE) {
              trackRenders.add(track.toErrorRender("Optimizing 1D track index..."));
            } else {
              trackRenders.add(track.query(
                trackChunkedFile,
                sourceToAssemblySegments,
                segmentsBuildResult.orderedSegments(),
                queryStartPx,
                queryEndPx,
                safeWidth,
                bpResolution
              ));
            }
          } catch (final RuntimeException ex) {
            final var message = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
            log.error("Failed to query 1D track {} ({})", track.name(), track.trackId(), ex);
            trackRenders.add(track.toErrorRender(message));
          }
        });
    } finally {
      this.lock.readLock().unlock();
    }
    final var startBp = mapVisiblePxToAssemblyBp(queryStartPx, segmentsBuildResult.orderedSegments(), bpResolution);
    final var endBp = mapVisiblePxToAssemblyBp(
      Math.max(queryStartPx, queryEndPx - 1L),
      segmentsBuildResult.orderedSegments(),
      bpResolution
    ) + bpResolution;
    return new QueryResult(startBp, endBp, queryStartPx, queryEndPx, safeWidth, bpResolution, trackRenders);
  }

  private static @NotNull QueryPxRange resolveQueryPxRange(final @NotNull ChunkedFile chunkedFile,
                                                           final long start,
                                                           final long end,
                                                           final long bpResolution,
                                                           final @NotNull QueryLengthUnit units,
                                                           final @NotNull List<AssemblySegment> orderedSegments,
                                                           final long totalVisiblePixels) {
    final var resolutionOrder = chunkedFile.getResolutionToIndex().get(bpResolution);
    if (resolutionOrder == null) {
      throw new IllegalArgumentException("Unsupported resolution for 1D track query: " + bpResolution);
    }
    final var minCoord = Math.min(start, end);
    final var maxCoord = Math.max(start, end);
    if (units == QueryLengthUnit.PIXELS) {
      final var startPx = Math.max(0L, Math.min(minCoord, totalVisiblePixels - 1L));
      final var endPx = Math.max(startPx + 1L, Math.min(maxCoord, totalVisiblePixels));
      return new QueryPxRange(startPx, endPx);
    }

    final var bpResolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(resolutionOrder);
    final var bpResolution0 = ResolutionDescriptor.fromResolutionOrder(0);
    final var totalAssemblyBp = chunkedFile.getContigTree().getLengthInUnits(QueryLengthUnit.BASE_PAIRS, bpResolution0);
    if (totalAssemblyBp <= 0L) {
      return new QueryPxRange(0L, 1L);
    }

    final long startBp;
    final long endBpExcl;
    if (units == QueryLengthUnit.BASE_PAIRS) {
      startBp = Math.max(0L, Math.min(minCoord, totalAssemblyBp - 1L));
      endBpExcl = Math.max(startBp + 1L, Math.min(maxCoord, totalAssemblyBp));
    } else if (units == QueryLengthUnit.BINS) {
      final var totalBins = chunkedFile.getContigTree().getLengthInUnits(QueryLengthUnit.BINS, bpResolutionDescriptor);
      if (totalBins <= 0L) {
        return new QueryPxRange(0L, 1L);
      }
      final var startBin = Math.max(0L, Math.min(minCoord, totalBins - 1L));
      final var endBinExcl = Math.max(startBin + 1L, Math.min(maxCoord, totalBins));
      startBp = chunkedFile.convertUnits(
        startBin,
        bpResolutionDescriptor,
        QueryLengthUnit.BINS,
        bpResolution0,
        QueryLengthUnit.BASE_PAIRS
      );
      if (endBinExcl >= totalBins) {
        endBpExcl = totalAssemblyBp;
      } else {
        endBpExcl = chunkedFile.convertUnits(
          endBinExcl,
          bpResolutionDescriptor,
          QueryLengthUnit.BINS,
          bpResolution0,
          QueryLengthUnit.BASE_PAIRS
        );
      }
    } else {
      throw new IllegalArgumentException("Unsupported query units for 1D track query: " + units);
    }

    final var clampedStartBp = Math.max(0L, Math.min(startBp, totalAssemblyBp - 1L));
    final var clampedEndBpExcl = Math.max(clampedStartBp + 1L, Math.min(endBpExcl, totalAssemblyBp));
    final var startPx = mapAssemblyBpToVisiblePx(clampedStartBp, orderedSegments, bpResolution, totalVisiblePixels);
    final var endPx = Math.min(
      totalVisiblePixels,
      mapAssemblyBpToVisiblePx(Math.max(clampedStartBp, clampedEndBpExcl - 1L), orderedSegments, bpResolution, totalVisiblePixels) + 1L
    );
    final var safeEndPx = Math.max(startPx + 1L, endPx);
    return new QueryPxRange(startPx, safeEndPx);
  }

  private @Nullable TrackPrecomputeRuntime maybeScheduleTrackPrecomputeFromQuery(final @NotNull ChunkedFile chunkedFile,
                                                                                 final @NotNull TrackState track) {
    final var runtime = this.precomputeRuntimeByTrackId.get(track.trackId());
    if (runtime != null && runtime.isActive()) {
      return runtime;
    }
    if (track.dataSource().renderStyle() == RenderStyle.FEATURE
      && hasCompatiblePrecomputeSidecar(precomputeCacheContextForTrack(chunkedFile, track))) {
      return runtime;
    }
    scheduleTrackPrecompute(chunkedFile, track, false);
    return this.precomputeRuntimeByTrackId.get(track.trackId());
  }

  private void scheduleTrackPrecompute(final @NotNull ChunkedFile chunkedFile,
                                       final @NotNull TrackState track,
                                       final boolean force) {
    final var runtime = this.precomputeRuntimeByTrackId.computeIfAbsent(
      track.trackId(),
      ignored -> new TrackPrecomputeRuntime(track.trackId(), track.name())
    );
    synchronized (runtime) {
      if (runtime.isActive()) {
        return;
      }
      runtime.markQueued();
    }
    log.info("Queued 1D track precompute: track={} name={} force={}", track.trackId(), track.name(), force);
    this.precomputeJobExecutor.submit(() -> runTrackPrecompute(chunkedFile, track, force, runtime));
  }

  private void runTrackPrecompute(final @NotNull ChunkedFile chunkedFile,
                                  final @NotNull TrackState track,
                                  final boolean force,
                                  final @NotNull TrackPrecomputeRuntime runtime) {
    try {
      final var cacheContext = precomputeCacheContextForTrack(chunkedFile, track);
      final var sidecarPath = cacheContext.sidecarPath();
      log.info("Starting 1D track precompute: track={} name={} sidecar={}", track.trackId(), track.name(), sidecarPath);
      if (track.dataSource().renderStyle() == RenderStyle.FEATURE) {
        runtime.setTotalTasks(1);
        runtime.markRunning("Validating feature index", 0);
        if (force || !hasCompatiblePrecomputeSidecar(cacheContext)) {
          persistPrecomputeMetadataOnly(sidecarPath, cacheContext);
        }
        runtime.markTaskDone(1);
        runtime.markFinished();
        log.info("Finished 1D feature track index validation: track={} name={}", track.trackId(), track.name());
        return;
      }
      final var tasks = buildPrecomputeTasks(chunkedFile, track, sidecarPath, cacheContext, force);
      runtime.setTotalTasks(tasks.size());
      if (tasks.isEmpty()) {
        runtime.markFinished();
        log.info("Finished 1D track precompute with no pending tasks: track={} name={}", track.trackId(), track.name());
        return;
      }
      final CompletionService<ComputedPrecomputeTask> completionService =
        new ExecutorCompletionService<>(this.precomputeWorkerExecutor);
      for (final var task : tasks) {
        completionService.submit(() -> new ComputedPrecomputeTask(task, computePrecomputedSeries(chunkedFile, track, task)));
      }

      final var writeFutures = new ArrayList<Future<?>>(tasks.size());
      int completed = 0;
      for (int i = 0; i < tasks.size(); i++) {
        final var computed = completionService.take().get();
        final var task = computed.task();
        runtime.markRunning(task.bpResolution() + "bp/" + task.modeKey(), completed);
        final var key = new PrecomputedSeriesKey(track.trackId(), task.bpResolution(), task.assemblySignature(), task.modeKey());
        this.precomputedSeriesCache.put(key, computed.series());
        writeFutures.add(this.precomputeWriterExecutor.submit(() -> persistPrecomputedSeries(sidecarPath, task, computed.series(), cacheContext)));
        completed++;
        runtime.markTaskDone(completed);
      }
      for (final var writeFuture : writeFutures) {
        writeFuture.get();
      }
      runtime.markFinished();
      log.info("Finished 1D track precompute: track={} name={} tasks={}", track.trackId(), track.name(), tasks.size());
    } catch (final InterruptedException ex) {
      Thread.currentThread().interrupt();
      runtime.markFailed("Precompute interrupted");
      log.warn("Track precompute interrupted for track {}", track.trackId());
    } catch (final Exception ex) {
      runtime.markFailed(ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
      log.error("Failed to precompute track {}", track.trackId(), ex);
    }
  }

  private @NotNull List<PrecomputeTask> buildPrecomputeTasks(final @NotNull ChunkedFile chunkedFile,
                                                             final @NotNull TrackState track,
                                                             final @NotNull Path sidecarPath,
                                                             final @NotNull PrecomputeCacheContext cacheContext,
                                                             final boolean force) {
    if (track.dataSource().renderStyle() == RenderStyle.FEATURE) {
      return List.of();
    }
    final var tasks = new ArrayList<PrecomputeTask>();
    final var resolutions = Arrays.stream(chunkedFile.getResolutions()).boxed().sorted(Comparator.reverseOrder()).toList();
    final var modeKeys = modeKeysForTrack(track);
    for (final var bpResolution : resolutions) {
      final var segmentsBuildResult = buildSourceToAssemblySegments(chunkedFile, this.linkedFastaAliasesBySource, bpResolution);
      final var totalVisiblePixels = segmentsBuildResult.totalVisiblePixels();
      if (totalVisiblePixels <= 0L || totalVisiblePixels > MAX_PRECOMPUTE_VISIBLE_PIXELS) {
        continue;
      }
      final var assemblySignature = computeAssemblySignature(segmentsBuildResult.orderedSegments(), bpResolution);
      for (final var modeKey : modeKeys) {
        final var key = new PrecomputedSeriesKey(track.trackId(), bpResolution, assemblySignature, modeKey);
        if (!force && this.precomputedSeriesCache.containsKey(key)) {
          continue;
        }
        if (!force) {
          final var cached = loadPrecomputedSeriesFromSidecar(
            sidecarPath,
            bpResolution,
            modeKey,
            assemblySignature,
            totalVisiblePixels,
            cacheContext
          );
          if (cached.isPresent()) {
            this.precomputedSeriesCache.put(key, cached.get());
            continue;
          }
        }
        tasks.add(new PrecomputeTask(bpResolution, modeKey, totalVisiblePixels, assemblySignature));
      }
    }
    return tasks;
  }

  private @Nullable TrackRender getPrecomputedBinsIfReady(final @NotNull ChunkedFile chunkedFile,
                                                          final @NotNull TrackState track,
                                                          final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                          final @NotNull List<AssemblySegment> orderedSegments,
                                                          final long queryStartPx,
                                                          final long queryEndPx,
                                                          final int widthPx,
                                                          final long bpResolution,
                                                          final @NotNull String assemblySignature) {
    if (track.dataSource().renderStyle() == RenderStyle.FEATURE) {
      return null;
    }
    final var totalVisiblePixels = orderedSegments.isEmpty() ? 0L : orderedSegments.get(orderedSegments.size() - 1).visiblePxEnd();
    if (totalVisiblePixels <= 0L || totalVisiblePixels > MAX_PRECOMPUTE_VISIBLE_PIXELS) {
      return null;
    }
    final var modeKey = activeModeKey(track);
    final var key = new PrecomputedSeriesKey(track.trackId(), bpResolution, assemblySignature, modeKey);
    var series = this.precomputedSeriesCache.get(key);
    if (series == null) {
      final var cacheContext = precomputeCacheContextForTrack(chunkedFile, track);
      final var sidecar = cacheContext.sidecarPath();
      series = loadPrecomputedSeriesFromSidecar(
        sidecar,
        bpResolution,
        modeKey,
        assemblySignature,
        totalVisiblePixels,
        cacheContext
      ).orElse(null);
      if (series != null) {
        this.precomputedSeriesCache.put(key, series);
      }
    }
    if (series == null) {
      return null;
    }
    final var strategy = aggregationStrategy(track);
    final var bins = aggregatePrecomputedSeries(
      series,
      queryStartPx,
      queryEndPx,
      widthPx,
      strategy,
      orderedSegments,
      bpResolution
    );
    final var maxValue = bins.stream().mapToDouble(TrackBin::getValue).max().orElse(0.0d);
    return new TrackRender(
      track.trackId(),
      track.name(),
      track.type().name(),
      track.color(),
      track.dataSource().renderStyle().name(),
      bins,
      maxValue,
      null
    );
  }

  private @NotNull PrecomputedSeries computePrecomputedSeries(final @NotNull ChunkedFile chunkedFile,
                                                              final @NotNull TrackState track,
                                                              final @NotNull PrecomputeTask task) {
    final var segmentsBuildResult = buildSourceToAssemblySegments(chunkedFile, this.linkedFastaAliasesBySource, task.bpResolution());
    final var totalVisiblePixels = (int) Math.max(1L, Math.min(Integer.MAX_VALUE, task.totalVisiblePixels()));
    final var bamRenderMode = bamModeForKey(track.bamRenderMode(), task.modeKey());
    final var bigWigAggregationMode = bigWigModeForKey(track.bigWigAggregationMode(), task.modeKey());
    final var bins = queryBinsForTrack(
      track.type(),
      chunkedFile,
      track.dataSource(),
      segmentsBuildResult.sourceToAssemblySegments(),
      segmentsBuildResult.orderedSegments(),
      0L,
      task.totalVisiblePixels(),
      totalVisiblePixels,
      task.bpResolution(),
      bamRenderMode,
      bigWigAggregationMode
    );
    final var values = new double[totalVisiblePixels];
    final var support = new long[totalVisiblePixels];
    for (final var bin : bins) {
      final var rawStart = bin.getStartPx() == null ? bin.getStartBp() : bin.getStartPx();
      final var rawEnd = bin.getEndPx() == null ? bin.getEndBp() : bin.getEndPx();
      final int start = (int) Math.max(0L, Math.min(rawStart, totalVisiblePixels - 1L));
      final int end = (int) Math.max(start + 1L, Math.min(rawEnd, totalVisiblePixels));
      for (int i = start; i < end; i++) {
        values[i] = bin.getValue();
        support[i] = Math.max(support[i], Math.max(1L, bin.getCount()));
      }
    }
    return new PrecomputedSeries(values, support);
  }

  private @NotNull List<TrackBin> aggregatePrecomputedSeries(final @NotNull PrecomputedSeries series,
                                                             final long queryStartPx,
                                                             final long queryEndPx,
                                                             final int widthPx,
                                                             final @NotNull PrecomputeAggregationStrategy strategy,
                                                             final @NotNull List<AssemblySegment> orderedSegments,
                                                             final long bpResolution) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final var nativeAggregation = NativeProcessingService.getInstance().tryAggregatePrecomputedSeries(
      series.values(),
      series.support(),
      queryStartPx,
      queryEndPx,
      bucketCount,
      nativeStrategyCode(strategy)
    );
    if (nativeAggregation != null) {
      return finalizeNativeAggregationBins(
        nativeAggregation,
        queryStartPx,
        queryEndPx,
        bucketSpan,
        orderedSegments,
        bpResolution
      );
    }
    final var bins = new ArrayList<TrackBin>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      final int from = (int) Math.max(0L, Math.min(startPx, series.values().length - 1L));
      final int to = (int) Math.max(from + 1L, Math.min(safeEndPx, series.values().length));
      double maxValue = 0.0d;
      double sumValue = 0.0d;
      long supportSum = 0L;
      long supportCount = 0L;
      for (int idx = from; idx < to; idx++) {
        final var value = series.values()[idx];
        final var support = series.support()[idx];
        maxValue = Math.max(maxValue, value);
        sumValue += value;
        supportSum += support;
        if (support > 0L) {
          supportCount++;
        }
      }
      final double value = switch (strategy) {
        case MAX -> maxValue;
        case MEAN_ALL_PIXELS -> sumValue / Math.max(1.0d, to - from);
        case MEAN_PRESENT_PIXELS -> supportCount > 0L ? (sumValue / supportCount) : 0.0d;
        case SUM -> sumValue;
      };
      if (supportSum <= 0L && value <= 0.0d) {
        continue;
      }
      bins.add(aggregateSignalBin(
        orderedSegments,
        bpResolution,
        startPx,
        safeEndPx,
        value,
        Math.max(1L, supportSum),
        null
      ));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> finalizeNativeAggregationBins(
    final @NotNull NativeProcessingService.NativeAggregationResult aggregation,
    final long queryStartPx,
    final long queryEndPx,
    final double bucketSpan,
    final @NotNull List<AssemblySegment> orderedSegments,
    final long bpResolution
  ) {
    final var values = aggregation.values();
    final var counts = aggregation.counts();
    final var bins = new ArrayList<TrackBin>(counts.length);
    for (int i = 0; i < counts.length; i++) {
      final var value = values[i];
      final var support = counts[i];
      if (support <= 0L && value <= 0.0d) {
        continue;
      }
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      bins.add(aggregateSignalBin(
        orderedSegments,
        bpResolution,
        startPx,
        Math.max(startPx + 1L, endPx),
        value,
        Math.max(1L, support),
        null
      ));
    }
    return bins;
  }

  private @NotNull Optional<PrecomputedSeries> loadPrecomputedSeriesFromSidecar(final @NotNull Path sidecarPath,
                                                                                 final long bpResolution,
                                                                                 final @NotNull String modeKey,
                                                                                 final @NotNull String assemblySignature,
                                                                                 final long expectedLength,
                                                                                 final @NotNull PrecomputeCacheContext cacheContext) {
    if (!Files.exists(sidecarPath) || !Files.isRegularFile(sidecarPath)) {
      return Optional.empty();
    }
    final var groupPath = precomputeGroupPath(bpResolution, modeKey, assemblySignature);
    final var valuesPath = groupPath + "/values";
    final var supportPath = groupPath + "/support";
    try (final var reader = HDF5Factory.openForReading(sidecarPath.toFile())) {
      if (!sidecarMetadataMatches(reader, cacheContext)) {
        return Optional.empty();
      }
      if (!reader.object().isDataSet(valuesPath) || !reader.object().isDataSet(supportPath)) {
        return Optional.empty();
      }
      final var valuesDims = reader.object().getDataSetInformation(valuesPath).getDimensions();
      if (valuesDims.length != 1 || valuesDims[0] != expectedLength) {
        return Optional.empty();
      }
      final var values = reader.float64().readArray(valuesPath);
      final var support = reader.int64().readArray(supportPath);
      if (values.length != support.length) {
        return Optional.empty();
      }
      return Optional.of(new PrecomputedSeries(values, support));
    } catch (final Exception e) {
      log.debug("Could not load sidecar precompute {}: {}", sidecarPath, e.getMessage());
      return Optional.empty();
    }
  }

  private void persistPrecomputedSeries(final @NotNull Path sidecarPath,
                                        final @NotNull PrecomputeTask task,
                                        final @NotNull PrecomputedSeries series,
                                        final @NotNull PrecomputeCacheContext cacheContext) {
    try {
      Files.createDirectories(sidecarPath.getParent());
      final var groupPath = precomputeGroupPath(task.bpResolution(), task.modeKey(), task.assemblySignature());
      final var valuesPath = groupPath + "/values";
      final var supportPath = groupPath + "/support";
      final var chunkLen = Math.max(
        1,
        Math.min(PRECOMPUTE_DATASET_CHUNK_SIZE, Math.max(series.values().length, series.support().length))
      );
      try (final var writer = HDF5Factory.open(sidecarPath.toFile())) {
        writePrecomputeMetadata(writer, cacheContext);
        ensureGroupPath(writer, groupPath);
        writer.string().setAttr(groupPath, "version", PRECOMPUTE_CACHE_VERSION);
        writer.int64().setAttr(groupPath, "bpResolution", task.bpResolution());
        writer.string().setAttr(groupPath, "mode", task.modeKey());
        writer.string().setAttr(groupPath, "assemblySignature", task.assemblySignature());
        writer.int64().setAttr(groupPath, "length", series.values().length);
        if (!writer.object().isDataSet(valuesPath)) {
          writer.float64().createArray(
            valuesPath,
            series.values().length,
            chunkLen,
            HDF5FloatStorageFeatures.createDeflation(PRECOMPUTE_COMPRESSION_LEVEL)
          );
        }
        if (!writer.object().isDataSet(supportPath)) {
          writer.int64().createArray(
            supportPath,
            series.support().length,
            chunkLen,
            HDF5IntStorageFeatures.createDeflation(PRECOMPUTE_COMPRESSION_LEVEL)
          );
        }
        writer.float64().writeArrayBlockWithOffset(
          valuesPath,
          series.values(),
          series.values().length,
          0L
        );
        writer.int64().writeArrayBlockWithOffset(
          supportPath,
          series.support(),
          series.support().length,
          0L
        );
      }
    } catch (final Exception e) {
      log.warn("Failed to write precomputed sidecar {}", sidecarPath, e);
    }
  }

  private void persistPrecomputeMetadataOnly(final @NotNull Path sidecarPath,
                                             final @NotNull PrecomputeCacheContext cacheContext) {
    try {
      Files.createDirectories(sidecarPath.getParent());
      try (final var writer = HDF5Factory.open(sidecarPath.toFile())) {
        writePrecomputeMetadata(writer, cacheContext);
      }
    } catch (final Exception e) {
      log.warn("Failed to write track precompute metadata {}", sidecarPath, e);
    }
  }

  private static void writePrecomputeMetadata(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer writer,
                                              final @NotNull PrecomputeCacheContext cacheContext) {
    ensureGroupPath(writer, PRECOMPUTE_META_GROUP_PATH);
    writer.string().setAttr(PRECOMPUTE_META_GROUP_PATH, "cacheVersion", PRECOMPUTE_CACHE_VERSION);
    writer.string().setAttr(PRECOMPUTE_META_GROUP_PATH, "trackType", cacheContext.trackType().name());
    writer.string().setAttr(PRECOMPUTE_META_GROUP_PATH, "sourceIdentity", cacheContext.sourceIdentity());
    if (cacheContext.sourceFingerprint() != null) {
      writeFingerprintAttrs(writer, PRECOMPUTE_META_GROUP_PATH, "source", cacheContext.sourceFingerprint());
    }
    writeFingerprintAttrs(writer, PRECOMPUTE_META_GROUP_PATH, "hict", cacheContext.hictFingerprint());
  }

  private @NotNull Path sidecarPathForTrackCache(final @NotNull ChunkedFile chunkedFile,
                                                 final @NotNull TrackState track) {
    final var cacheContext = precomputeCacheContextForTrack(chunkedFile, track);
    return cacheContext.sidecarPath();
  }

  public @NotNull TrackPrecomputeCacheProbe probePrecomputeCache(final @NotNull ChunkedFile chunkedFile,
                                                                 final @NotNull String relativeFilename) {
    final var resolvedPath = resolveDataPath(relativeFilename);
    final var trackType = TrackType.fromPath(resolvedPath);
    if (trackType == TrackType.UNSUPPORTED) {
      throw new IllegalArgumentException(
        "Unsupported track format for " + relativeFilename + ". Supported: BED/VCF/GFF/GTF/BigWig/BAM."
      );
    }
    final var cacheContext = precomputeCacheContextForTrackSource(chunkedFile, relativeFilename, trackType);
    final var cacheAvailable = hasCompatiblePrecomputeSidecar(cacheContext);
    final var warnings = new ArrayList<String>();
    if (!cacheAvailable) {
      warnings.add("No valid precomputed cache exists for the selected track and current HiCT source.");
    }
    return new TrackPrecomputeCacheProbe(
      relativeFilename,
      trackType.name(),
      true,
      cacheAvailable,
      cacheAvailable,
      cacheContext.sidecarPath().toString(),
      warnings,
      cacheContext.sourceFingerprint(),
      cacheContext.hictFingerprint()
    );
  }

  private boolean hasCompatiblePrecomputeSidecar(final @NotNull PrecomputeCacheContext cacheContext) {
    final var sidecarPath = cacheContext.sidecarPath();
    if (!Files.exists(sidecarPath) || !Files.isRegularFile(sidecarPath)) {
      return false;
    }
    try (final var reader = HDF5Factory.openForReading(sidecarPath.toFile())) {
      return sidecarMetadataMatches(reader, cacheContext);
    } catch (final Exception e) {
      return false;
    }
  }

  private @NotNull PrecomputeCacheContext precomputeCacheContextForTrack(final @NotNull ChunkedFile chunkedFile,
                                                                         final @NotNull TrackState track) {
    if (track.type() == TrackType.COOLER_WEIGHTS) {
      final var hictPath = chunkedFile.getHdfFilePath().normalize().toAbsolutePath();
      final var hictFingerprint = this.fingerprintService.fingerprint(hictPath);
      return new PrecomputeCacheContext(
        sidecarPathForTrackCache(COOLER_WEIGHTS_SOURCE_FILE, track.type(), hictPath),
        track.type(),
        COOLER_WEIGHTS_SOURCE_FILE,
        null,
        hictFingerprint
      );
    }
    return precomputeCacheContextForTrackSource(chunkedFile, track.sourceFile(), track.type());
  }

  private @NotNull PrecomputeCacheContext precomputeCacheContextForTrackSource(final @NotNull ChunkedFile chunkedFile,
                                                                               final @NotNull String relativeFilename,
                                                                               final @NotNull TrackType trackType) {
    final var trackSource = resolveDataPath(relativeFilename).normalize().toAbsolutePath();
    final var hictPath = chunkedFile.getHdfFilePath().normalize().toAbsolutePath();
    return new PrecomputeCacheContext(
      sidecarPathForTrackCache(trackSource.toString(), trackType, hictPath),
      trackType,
      trackSource.toString(),
      this.fingerprintService.fingerprint(trackSource),
      this.fingerprintService.fingerprint(hictPath)
    );
  }

  private @NotNull Path sidecarPathForTrackCache(final @NotNull String sourceIdentity,
                                                 final @NotNull TrackType trackType,
                                                 final @NotNull Path hictPath) {
    final var fingerprint = String.join("|",
      PRECOMPUTE_CACHE_VERSION,
      trackType.name(),
      sourceIdentity,
      hictPath.toString()
    );
    final var fileName = sha256Hex(fingerprint) + ".h5";
    return this.processedDirectory.resolve("track_precompute").resolve(fileName);
  }

  private static void writeFingerprintAttrs(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer writer,
                                            final @NotNull String groupPath,
                                            final @NotNull String prefix,
                                            final @NotNull FileFingerprint fingerprint) {
    writer.int64().setAttr(groupPath, prefix + "_sizeBytes", fingerprint.sizeBytes());
    writer.int64().setAttr(groupPath, prefix + "_modifiedAtMs", fingerprint.modifiedAtMs());
    writer.string().setAttr(groupPath, prefix + "_sha256", fingerprint.sha256());
    writer.string().setAttr(groupPath, prefix + "_sha512", fingerprint.sha512());
  }

  private static boolean sidecarMetadataMatches(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
                                                final @NotNull PrecomputeCacheContext cacheContext) {
    try {
      if (!reader.object().isGroup(PRECOMPUTE_META_GROUP_PATH)) {
        return false;
      }
      final var cacheVersion = reader.string().getAttr(PRECOMPUTE_META_GROUP_PATH, "cacheVersion");
      final var trackType = reader.string().getAttr(PRECOMPUTE_META_GROUP_PATH, "trackType");
      final var sourceIdentity = reader.string().getAttr(PRECOMPUTE_META_GROUP_PATH, "sourceIdentity");
      if (!PRECOMPUTE_CACHE_VERSION.equals(cacheVersion)
        || !cacheContext.trackType().name().equals(trackType)
        || !cacheContext.sourceIdentity().equals(sourceIdentity)) {
        return false;
      }
      if (cacheContext.sourceFingerprint() != null) {
        final var storedSourceFingerprint = readFingerprintAttrs(reader, PRECOMPUTE_META_GROUP_PATH, "source");
        if (!storedSourceFingerprint.matches(cacheContext.sourceFingerprint())) {
          return false;
        }
      }
      final var storedHictFingerprint = readFingerprintAttrs(reader, PRECOMPUTE_META_GROUP_PATH, "hict");
      return storedHictFingerprint.matches(cacheContext.hictFingerprint());
    } catch (final RuntimeException e) {
      return false;
    }
  }

  private static @NotNull FileFingerprint readFingerprintAttrs(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
                                                               final @NotNull String groupPath,
                                                               final @NotNull String prefix) {
    return new FileFingerprint(
      reader.int64().getAttr(groupPath, prefix + "_sizeBytes"),
      reader.int64().getAttr(groupPath, prefix + "_modifiedAtMs"),
      reader.string().getAttr(groupPath, prefix + "_sha256"),
      reader.string().getAttr(groupPath, prefix + "_sha512")
    );
  }

  private static @NotNull String precomputeGroupPath(final long bpResolution,
                                                     final @NotNull String modeKey,
                                                     final @NotNull String assemblySignature) {
    return "/cache/resolutions/" + bpResolution + "/modes/" + modeKey + "/assemblies/" + assemblySignature;
  }

  private static void ensureGroupPath(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Writer writer,
                                      final @NotNull String groupPath) {
    final var parts = groupPath.split("/");
    final var current = new StringBuilder();
    for (final var part : parts) {
      if (part == null || part.isBlank()) {
        continue;
      }
      current.append('/').append(part);
      final var path = current.toString();
      if (!writer.object().isGroup(path)) {
        writer.object().createGroup(path);
      }
    }
  }

  private static @NotNull String computeAssemblySignature(final @NotNull List<AssemblySegment> orderedSegments,
                                                          final long bpResolution) {
    long hash = 1469598103934665603L;
    hash = fnv1a(hash, bpResolution);
    hash = fnv1a(hash, orderedSegments.size());
    for (final var segment : orderedSegments) {
      hash = fnv1a(hash, segment.sourceStart());
      hash = fnv1a(hash, segment.sourceEnd());
      hash = fnv1a(hash, segment.assemblyStart());
      hash = fnv1a(hash, segment.assemblyEnd());
      hash = fnv1a(hash, segment.visiblePxStart());
      hash = fnv1a(hash, segment.visiblePxEnd());
      hash = fnv1a(hash, segment.reversed() ? 1L : 0L);
    }
    return Long.toUnsignedString(hash, 16);
  }

  private static long fnv1a(long hash, final long value) {
    hash ^= value;
    hash *= 1099511628211L;
    return hash;
  }

  private static @NotNull String sha256Hex(final @NotNull String input) {
    try {
      final var digest = MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8));
      final var sb = new StringBuilder(digest.length * 2);
      for (final byte b : digest) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 is not available", e);
    }
  }

  private static @NotNull List<String> modeKeysForTrack(final @NotNull TrackState track) {
    return switch (track.type()) {
      case BIGWIG -> List.of("MAX", "MEAN", "SUM");
      case BAM -> List.of("COVERAGE", "READ_DENSITY");
      case BED, VCF, GFF_GTF, COOLER_WEIGHTS -> List.of("DEFAULT");
      case UNSUPPORTED -> List.of("DEFAULT");
    };
  }

  private static @NotNull String activeModeKey(final @NotNull TrackState track) {
    return switch (track.type()) {
      case BIGWIG -> track.bigWigAggregationMode().name();
      case BAM -> track.bamRenderMode().name();
      case BED, VCF, GFF_GTF, COOLER_WEIGHTS, UNSUPPORTED -> "DEFAULT";
    };
  }

  private static @NotNull BamRenderMode bamModeForKey(final @NotNull BamRenderMode fallback,
                                                       final @NotNull String modeKey) {
    return switch (modeKey) {
      case "READ_DENSITY" -> BamRenderMode.READ_DENSITY;
      case "COVERAGE" -> BamRenderMode.COVERAGE;
      default -> fallback;
    };
  }

  private static @NotNull BigWigAggregationMode bigWigModeForKey(final @NotNull BigWigAggregationMode fallback,
                                                                  final @NotNull String modeKey) {
    return switch (modeKey) {
      case "MAX" -> BigWigAggregationMode.MAX;
      case "MEAN" -> BigWigAggregationMode.MEAN;
      case "SUM" -> BigWigAggregationMode.SUM;
      default -> fallback;
    };
  }

  private static @NotNull PrecomputeAggregationStrategy aggregationStrategy(final @NotNull TrackState track) {
    return switch (track.type()) {
      case BIGWIG -> switch (track.bigWigAggregationMode()) {
        case MAX -> PrecomputeAggregationStrategy.MAX;
        case MEAN -> PrecomputeAggregationStrategy.MEAN_PRESENT_PIXELS;
        case SUM -> PrecomputeAggregationStrategy.MEAN_ALL_PIXELS;
      };
      case BAM -> switch (track.bamRenderMode()) {
        case COVERAGE -> PrecomputeAggregationStrategy.MEAN_ALL_PIXELS;
        case READ_DENSITY -> PrecomputeAggregationStrategy.SUM;
      };
      case BED, VCF, GFF_GTF, COOLER_WEIGHTS, UNSUPPORTED -> PrecomputeAggregationStrategy.MAX;
    };
  }

  private static int nativeStrategyCode(final @NotNull PrecomputeAggregationStrategy strategy) {
    return switch (strategy) {
      case MAX -> 1;
      case MEAN_ALL_PIXELS -> 2;
      case MEAN_PRESENT_PIXELS -> 3;
      case SUM -> 4;
    };
  }

  private @NotNull SegmentBuildResult buildSourceToAssemblySegments(final @NotNull ChunkedFile chunkedFile,
                                                                    final @NotNull Map<String, String> linkedFastaAliasesBySource,
                                                                    final long bpResolution) {
    final var resolutionOrder = chunkedFile.getResolutionToIndex().get(bpResolution);
    if (resolutionOrder == null) {
      throw new IllegalArgumentException("Unsupported resolution for 1D track query: " + bpResolution);
    }
    final var sourceToAssemblySegments = new HashMap<String, List<AssemblySegment>>();
    final var orderedSegments = new ArrayList<AssemblySegment>();
    final var contigs = chunkedFile.getAssemblyInfo().contigs();
    long assemblyCursor = 0L;
    long visiblePxCursor = 0L;
    for (int contigIndex = 0; contigIndex < contigs.size(); ++contigIndex) {
      final ContigTree.ContigTuple tuple = contigs.get(contigIndex);
      final var descriptor = tuple.descriptor();
      final var sourceName = descriptor.getContigNameInSourceFASTA();
      final var originalName = descriptor.getContigName();
      final var displayName = chunkedFile.getContigDisplayName(descriptor.getContigId());
      final var sourceStart = descriptor.getOffsetInSourceFASTA();
      final var sourceEnd = sourceStart + descriptor.getLengthBp();
      final var assemblyStart = assemblyCursor;
      final var assemblyEnd = assemblyCursor + descriptor.getLengthBp();
      final var visibleAtResolution = descriptor.getPresenceAtResolution().get(resolutionOrder) == ContigHideType.SHOWN;
      final var lengthPxAtResolution = descriptor.getLengthBinsAtResolution()[resolutionOrder];
      if (visibleAtResolution && lengthPxAtResolution > 0L) {
        final var segment = new AssemblySegment(
          sourceStart,
          sourceEnd,
          assemblyStart,
          assemblyEnd,
          tuple.direction() == ContigDirection.REVERSED,
          visiblePxCursor,
          visiblePxCursor + lengthPxAtResolution
        );
        orderedSegments.add(segment);
        sourceToAssemblySegments.computeIfAbsent(sourceName, key -> new ArrayList<>()).add(segment);
        if (originalName != null && !originalName.isBlank() && !originalName.equals(sourceName)) {
          sourceToAssemblySegments.computeIfAbsent(originalName, key -> new ArrayList<>()).add(segment);
        }
        if (displayName != null && !displayName.isBlank()
          && !displayName.equals(sourceName)
          && !displayName.equals(originalName)) {
          sourceToAssemblySegments.computeIfAbsent(displayName, key -> new ArrayList<>()).add(segment);
        }
        final var aliasName = linkedFastaAliasesBySource.get(sourceName);
        if (aliasName != null && !aliasName.equals(sourceName)) {
          sourceToAssemblySegments.computeIfAbsent(aliasName, key -> new ArrayList<>()).add(segment);
        }
        visiblePxCursor += lengthPxAtResolution;
      }
      assemblyCursor = assemblyEnd;
    }
    sourceToAssemblySegments.values().forEach(list -> list.sort(Comparator.comparingLong(AssemblySegment::sourceStart)));
    return new SegmentBuildResult(sourceToAssemblySegments, orderedSegments, visiblePxCursor);
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

  private static double sanitizeTrackRangeValue(final @Nullable Double value, final double fallback) {
    if (value == null || !Double.isFinite(value)) {
      return fallback;
    }
    return value;
  }

  private static @NotNull SignalRange normalizeTrackRange(final double min, final double max) {
    if (!Double.isFinite(min) || !Double.isFinite(max) || max <= min) {
      return new SignalRange(0.0d, 1.0d);
    }
    return new SignalRange(min, max);
  }

  private static int findTrackIndex(final @NotNull List<TrackState> entries,
                                    final @NotNull String trackId) {
    for (int i = 0; i < entries.size(); i++) {
      if (entries.get(i).trackId().equals(trackId)) {
        return i;
      }
    }
    return -1;
  }

  private static @NotNull String colorForIndex(final int index) {
    if (COLOR_PALETTE.isEmpty()) {
      return "#4e79a7";
    }
    return COLOR_PALETTE.get(Math.floorMod(index, COLOR_PALETTE.size()));
  }

  private static @NotNull String normalizeSourceName(final String requestedSource) {
    if (requestedSource != null && "SECONDARY".equalsIgnoreCase(requestedSource.trim())) {
      return "SECONDARY";
    }
    return "PRIMARY";
  }

  private static @NotNull String coolerWeightsSourceFile(final @NotNull String source) {
    return "SECONDARY".equals(source)
      ? COOLER_WEIGHTS_SOURCE_FILE_SECONDARY
      : COOLER_WEIGHTS_SOURCE_FILE_PRIMARY;
  }

  private static @NotNull TrackDataSource createDataSource(final @NotNull TrackType trackType,
                                                           final @NotNull Path resolvedPath) {
    return switch (trackType) {
      case BED -> InMemoryTrackDataSource.fromBed(resolvedPath);
      case VCF -> InMemoryTrackDataSource.fromVcf(resolvedPath);
      case GFF_GTF -> InMemoryTrackDataSource.fromGffOrGtf(resolvedPath);
      case BIGWIG -> new BigWigTrackDataSource(resolvedPath);
      case BAM -> new BamTrackDataSource(resolvedPath);
      case COOLER_WEIGHTS -> new CoolerWeightsTrackDataSource("PRIMARY");
      case UNSUPPORTED -> throw new IllegalStateException("Unexpected unsupported track type");
    };
  }

  private static @NotNull Set<String> buildSourceNameSet(final @NotNull ChunkedFile chunkedFile,
                                                          final @NotNull Map<String, String> linkedFastaAliasesBySource) {
    final var names = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    for (final var tuple : chunkedFile.getAssemblyInfo().contigs()) {
      final var sourceName = tuple.descriptor().getContigNameInSourceFASTA();
      if (sourceName != null && !sourceName.isBlank()) {
        names.add(sourceName);
      }
      final var alias = linkedFastaAliasesBySource.get(sourceName);
      if (alias != null && !alias.isBlank()) {
        names.add(alias);
      }
    }
    return names;
  }

  private static @NotNull Set<String> buildAssemblyNameSet(final @NotNull ChunkedFile chunkedFile) {
    final var names = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    for (final var tuple : chunkedFile.getAssemblyInfo().contigs()) {
      final var descriptor = tuple.descriptor();
      final var originalName = descriptor.getContigName();
      if (originalName != null && !originalName.isBlank()) {
        names.add(originalName);
      }
      final var displayName = chunkedFile.getContigDisplayName(descriptor.getContigId());
      if (displayName != null && !displayName.isBlank()) {
        names.add(displayName);
      }
    }
    return names;
  }

  private static @NotNull String resolveCompatibilityStatus(final int totalNames, final int matchedAny) {
    if (totalNames <= 0 || matchedAny >= totalNames) {
      return "ok";
    }
    final var ratio = matchedAny / (double) Math.max(1, totalNames);
    if (ratio >= 0.5d) {
      return "warning";
    }
    return "error";
  }

  private static @NotNull String buildCompatibilityMessage(final @NotNull TrackType trackType,
                                                           final int totalNames,
                                                           final int matchedAny,
                                                           final int unknownNamesCount) {
    if (totalNames <= 0) {
      return "Track has no contig/chromosome names to validate.";
    }
    if (matchedAny >= totalNames) {
      return "Track names are compatible with the current assembly.";
    }
    return "Track " + trackType.name() + " contains " + unknownNamesCount
      + " names that do not match current/source assembly names.";
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

  private static @Nullable Double parseNullableDouble(final String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Double.parseDouble(value);
    } catch (final NumberFormatException ignored) {
      return null;
    }
  }

  private static boolean isBedStrandToken(final String token) {
    if (token == null) {
      return false;
    }
    final var trimmed = token.trim();
    return "+".equals(trimmed) || "-".equals(trimmed) || ".".equals(trimmed);
  }

  private static @Nullable String normalizeStrand(final String token) {
    if (!isBedStrandToken(token)) {
      return null;
    }
    final var trimmed = token.trim();
    return ".".equals(trimmed) ? null : trimmed;
  }

  private static @NotNull Map<String, String> parseGffAttributes(final @Nullable String rawAttributes,
                                                                  final boolean gtfMode) {
    if (rawAttributes == null || rawAttributes.isBlank() || ".".equals(rawAttributes.trim())) {
      return Map.of();
    }
    final var parsed = new LinkedHashMap<String, String>();
    final var tokens = rawAttributes.split(";");
    for (final var token : tokens) {
      if (token == null || token.isBlank()) {
        continue;
      }
      final var trimmed = token.trim();
      if (gtfMode) {
        final int firstSpace = trimmed.indexOf(' ');
        if (firstSpace <= 0 || firstSpace >= trimmed.length() - 1) {
          continue;
        }
        final var key = trimmed.substring(0, firstSpace).trim();
        var value = trimmed.substring(firstSpace + 1).trim();
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
          value = value.substring(1, value.length() - 1);
        }
        if (!key.isBlank() && !value.isBlank()) {
          parsed.putIfAbsent(key, value);
        }
      } else {
        final int eqIndex = trimmed.indexOf('=');
        if (eqIndex <= 0 || eqIndex >= trimmed.length() - 1) {
          continue;
        }
        final var key = trimmed.substring(0, eqIndex).trim();
        final var value = trimmed.substring(eqIndex + 1).trim();
        if (!key.isBlank() && !value.isBlank()) {
          parsed.putIfAbsent(key, value);
        }
      }
    }
    return parsed;
  }

  private static @Nullable String firstNonBlank(final @Nullable String... candidates) {
    if (candidates == null) {
      return null;
    }
    for (final var candidate : candidates) {
      if (candidate != null && !candidate.isBlank()) {
        return candidate;
      }
    }
    return null;
  }

  private static boolean isGffBlockLikeFeature(final @NotNull String featureTypeLower) {
    return switch (featureTypeLower) {
      case "exon", "cds", "utr", "five_prime_utr", "three_prime_utr", "start_codon", "stop_codon" -> true;
      default -> false;
    };
  }

  private static boolean isGffTranscriptLikeFeature(final @NotNull String featureTypeLower) {
    return switch (featureTypeLower) {
      case "transcript",
        "mrna",
        "ncrna",
        "trna",
        "rrna",
        "snrna",
        "snorna",
        "lncrna",
        "mirna",
        "pirna",
        "guide_rna",
        "primary_transcript",
        "pseudogenic_transcript" -> true;
      default -> false;
    };
  }

  private static boolean isGffGeneLikeFeature(final @NotNull String featureTypeLower) {
    return "gene".equals(featureTypeLower) || "pseudogene".equals(featureTypeLower);
  }

  private static boolean isGffCodingFeature(final @NotNull String featureTypeLower) {
    return "cds".equals(featureTypeLower) || "start_codon".equals(featureTypeLower) || "stop_codon".equals(featureTypeLower);
  }

  private static @Nullable String sanitizeGffGroupToken(final @Nullable String token) {
    if (token == null || token.isBlank()) {
      return null;
    }
    final var primary = token.split(",")[0].trim();
    if (primary.isBlank()) {
      return null;
    }
    if (primary.startsWith("\"") && primary.endsWith("\"") && primary.length() > 1) {
      return primary.substring(1, primary.length() - 1);
    }
    return primary;
  }

  private static @Nullable String resolveGffGroupKey(final @NotNull Map<String, String> attributes,
                                                      final @NotNull String featureTypeLower) {
    final var transcriptLike = sanitizeGffGroupToken(firstNonBlank(
      attributes.get("transcript_id"),
      attributes.get("transcriptId"),
      attributes.get("Parent"),
      attributes.get("ID")
    ));
    if (transcriptLike != null) {
      return "tx:" + transcriptLike;
    }
    if (isGffGeneLikeFeature(featureTypeLower) || isGffTranscriptLikeFeature(featureTypeLower)) {
      final var geneLike = sanitizeGffGroupToken(firstNonBlank(
        attributes.get("gene_id"),
        attributes.get("gene"),
        attributes.get("gene_name"),
        attributes.get("Name"),
        attributes.get("ID")
      ));
      if (geneLike != null) {
        return "gene:" + geneLike;
      }
    }
    return null;
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

  private static long localPxToAssemblyBp(final @NotNull AssemblySegment segment,
                                          final long localPx,
                                          final long bpResolution) {
    final var segmentLengthBp = segment.assemblyEnd() - segment.assemblyStart();
    if (segmentLengthBp <= 0L) {
      return segment.assemblyStart();
    }
    final var segmentLengthPx = Math.max(1L, segment.visiblePxEnd() - segment.visiblePxStart());
    final var clampedLocalPx = Math.max(0L, Math.min(localPx, segmentLengthPx - 1L));
    final long localBp;
    if (!segment.reversed()) {
      localBp = clampedLocalPx * bpResolution;
    } else {
      final var firstBinLengthBp = segmentLengthBp % bpResolution;
      if (clampedLocalPx <= 0L) {
        localBp = 0L;
      } else {
        localBp = firstBinLengthBp + (clampedLocalPx - 1L) * bpResolution;
      }
    }
    return Math.max(segment.assemblyStart(), Math.min(segment.assemblyEnd() - 1L, segment.assemblyStart() + localBp));
  }

  private static long assemblyBpToLocalPx(final @NotNull AssemblySegment segment,
                                          final long assemblyBp,
                                          final long bpResolution) {
    final var segmentLengthBp = segment.assemblyEnd() - segment.assemblyStart();
    if (segmentLengthBp <= 0L) {
      return 0L;
    }
    final var segmentLengthPx = Math.max(1L, segment.visiblePxEnd() - segment.visiblePxStart());
    final var clampedBp = Math.max(segment.assemblyStart(), Math.min(assemblyBp, segment.assemblyEnd() - 1L));
    final var inContigOffsetBp = clampedBp - segment.assemblyStart();
    final long localPx;
    if (!segment.reversed()) {
      localPx = inContigOffsetBp / bpResolution;
    } else {
      final var firstBinLengthBp = segmentLengthBp % bpResolution;
      if (inContigOffsetBp < firstBinLengthBp) {
        localPx = 0L;
      } else {
        localPx = 1L + ((inContigOffsetBp - firstBinLengthBp) / bpResolution);
      }
    }
    return Math.max(0L, Math.min(localPx, segmentLengthPx - 1L));
  }

  private static long mapVisiblePxToAssemblyBp(final long px,
                                               final @NotNull List<AssemblySegment> orderedSegments,
                                               final long bpResolution) {
    if (orderedSegments.isEmpty()) {
      return 0L;
    }
    int lo = 0;
    int hi = orderedSegments.size();
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (orderedSegments.get(mid).visiblePxEnd() <= px) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    final int idx = Math.max(0, Math.min(lo, orderedSegments.size() - 1));
    AssemblySegment segment = orderedSegments.get(idx);
    if (px < segment.visiblePxStart() && idx > 0) {
      segment = orderedSegments.get(idx - 1);
    }
    final var localPx = Math.max(0L, px - segment.visiblePxStart());
    return localPxToAssemblyBp(segment, localPx, bpResolution);
  }

  private static long mapAssemblyBpToVisiblePx(final long assemblyBp,
                                               final @NotNull List<AssemblySegment> orderedSegments,
                                               final long bpResolution,
                                               final long totalVisiblePixels) {
    if (orderedSegments.isEmpty() || totalVisiblePixels <= 0L) {
      return 0L;
    }
    if (assemblyBp <= orderedSegments.get(0).assemblyStart()) {
      return 0L;
    }
    if (assemblyBp >= orderedSegments.get(orderedSegments.size() - 1).assemblyEnd()) {
      return totalVisiblePixels;
    }
    int lo = 0;
    int hi = orderedSegments.size();
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (orderedSegments.get(mid).assemblyEnd() <= assemblyBp) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    final int idx = Math.max(0, Math.min(lo, orderedSegments.size() - 1));
    final var segment = orderedSegments.get(idx);
    if (assemblyBp < segment.assemblyStart()) {
      return segment.visiblePxStart();
    }
    final var localPx = assemblyBpToLocalPx(segment, assemblyBp, bpResolution);
    return Math.max(0L, Math.min(totalVisiblePixels, segment.visiblePxStart() + localPx));
  }

  private static @NotNull Optional<SourceInterval> mapVisiblePxIntervalToSegmentSource(final @NotNull AssemblySegment segment,
                                                                                        final long queryStartPx,
                                                                                        final long queryEndPx,
                                                                                        final long bpResolution) {
    final var overlapStartPx = Math.max(queryStartPx, segment.visiblePxStart());
    final var overlapEndPx = Math.min(queryEndPx, segment.visiblePxEnd());
    if (overlapEndPx <= overlapStartPx) {
      return Optional.empty();
    }
    final var localStartPx = overlapStartPx - segment.visiblePxStart();
    final var localEndPx = overlapEndPx - segment.visiblePxStart();
    final var assemblyStart = localPxToAssemblyBp(segment, localStartPx, bpResolution);
    final var assemblyEnd = Math.min(
      segment.assemblyEnd(),
      localPxToAssemblyBp(segment, Math.max(localStartPx, localEndPx - 1L), bpResolution) + bpResolution
    );
    if (assemblyEnd <= assemblyStart) {
      return Optional.empty();
    }
    return mapAssemblyIntervalToSegmentSource(segment, assemblyStart, assemblyEnd);
  }

  private static @NotNull Optional<ProjectedFeature> projectSourceIntervalOnSegment(final @NotNull AssemblySegment segment,
                                                                                     final long sourceStart,
                                                                                     final long sourceEnd,
                                                                                     final double value,
                                                                                     final String label,
                                                                                     final long queryStartPx,
                                                                                     final long queryEndPx,
                                                                                     final long bpResolution) {
    return projectSourceIntervalOnSegmentRaw(
      segment,
      sourceStart,
      sourceEnd,
      value,
      label,
      queryStartPx,
      queryEndPx,
      bpResolution
    );
  }

  private static @NotNull Optional<ProjectedFeature> projectSourceFeatureOnSegment(final @NotNull AssemblySegment segment,
                                                                                    final @NotNull FeatureRange feature,
                                                                                    final long queryStartPx,
                                                                                    final long queryEndPx,
                                                                                    final long bpResolution) {
    final var projectedBase = projectSourceIntervalOnSegmentRaw(
      segment,
      feature.start(),
      feature.end(),
      feature.value(),
      feature.label(),
      queryStartPx,
      queryEndPx,
      bpResolution
    );
    if (projectedBase.isEmpty()) {
      return Optional.empty();
    }
    Long thickStartBp = null;
    Long thickEndBp = null;
    Long thickStartPx = null;
    Long thickEndPx = null;
    if (feature.thickStart() != null && feature.thickEnd() != null && feature.thickEnd() > feature.thickStart()) {
      final var projectedThick = projectSourceIntervalOnSegmentRaw(
        segment,
        feature.thickStart(),
        feature.thickEnd(),
        feature.value(),
        null,
        queryStartPx,
        queryEndPx,
        bpResolution
      );
      if (projectedThick.isPresent()) {
        thickStartBp = projectedThick.get().startBp();
        thickEndBp = projectedThick.get().endBp();
        thickStartPx = projectedThick.get().startPx();
        thickEndPx = projectedThick.get().endPx();
      }
    }
    final var projectedBlocks = new ArrayList<ProjectedBlock>();
    for (final var block : feature.blocks()) {
      if (block == null || block.end() <= block.start()) {
        continue;
      }
      final var projectedBlock = projectSourceIntervalOnSegmentRaw(
        segment,
        block.start(),
        block.end(),
        feature.value(),
        null,
        queryStartPx,
        queryEndPx,
        bpResolution
      );
      if (projectedBlock.isPresent()) {
        final var interval = projectedBlock.get();
        projectedBlocks.add(new ProjectedBlock(
          interval.startBp(),
          interval.endBp(),
          interval.startPx(),
          interval.endPx(),
          block.coding()
        ));
      }
    }
    projectedBlocks.sort(Comparator.comparingLong(ProjectedBlock::startPx));
    final var projected = projectedBase.get();
    return Optional.of(new ProjectedFeature(
      projected.startBp(),
      projected.endBp(),
      projected.startPx(),
      projected.endPx(),
      projected.value(),
      projected.label(),
      feature.strand(),
      thickStartBp,
      thickEndBp,
      thickStartPx,
      thickEndPx,
      feature.featureType(),
      projectedBlocks,
      feature.attributes()
    ));
  }

  private static @NotNull Optional<ProjectedFeature> projectSourceIntervalOnSegmentRaw(final @NotNull AssemblySegment segment,
                                                                                        final long sourceStart,
                                                                                        final long sourceEnd,
                                                                                        final double value,
                                                                                        final String label,
                                                                                        final long queryStartPx,
                                                                                        final long queryEndPx,
                                                                                        final long bpResolution) {
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

    final var clippedAssemblyStart = Math.max(segment.assemblyStart(), Math.min(assemblyStart, assemblyEnd));
    final var clippedAssemblyEnd = Math.min(segment.assemblyEnd(), Math.max(assemblyStart, assemblyEnd));
    if (clippedAssemblyEnd <= clippedAssemblyStart) {
      return Optional.empty();
    }
    final var localStartPx = assemblyBpToLocalPx(segment, clippedAssemblyStart, bpResolution);
    final var localEndPx = assemblyBpToLocalPx(
      segment,
      Math.max(clippedAssemblyStart, clippedAssemblyEnd - 1L),
      bpResolution
    ) + 1L;
    final var featureStartPx = segment.visiblePxStart() + localStartPx;
    final var featureEndPx = segment.visiblePxStart() + localEndPx;
    final var clippedStartPx = Math.max(queryStartPx, featureStartPx);
    final var clippedEndPx = Math.min(queryEndPx, featureEndPx);
    if (clippedEndPx <= clippedStartPx) {
      return Optional.empty();
    }
    return Optional.of(new ProjectedFeature(
      clippedAssemblyStart,
      clippedAssemblyEnd,
      clippedStartPx,
      clippedEndPx,
      Math.max(0.0d, value),
      label,
      null,
      null,
      null,
      null,
      null,
      null,
      List.of(),
      Map.of()
    ));
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

  private static @NotNull Optional<AssemblyBpInterval> projectSourceIntervalToAssemblyBp(final @NotNull AssemblyBpSegment segment,
                                                                                          final long sourceStart,
                                                                                          final long sourceEnd) {
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
    final var safeStart = Math.min(assemblyStart, assemblyEnd);
    final var safeEnd = Math.max(assemblyStart, assemblyEnd);
    if (safeEnd <= safeStart) {
      return Optional.empty();
    }
    return Optional.of(new AssemblyBpInterval(safeStart, safeEnd));
  }

  private @NotNull Map<String, List<AssemblyBpSegment>> buildSourceToAssemblyBpSegments(final @NotNull ChunkedFile chunkedFile,
                                                                                          final @NotNull Map<String, String> linkedFastaAliasesBySource) {
    final var sourceToAssemblySegments = new HashMap<String, List<AssemblyBpSegment>>();
    final var contigs = chunkedFile.getAssemblyInfo().contigs();
    long assemblyCursor = 0L;
    for (int contigIndex = 0; contigIndex < contigs.size(); ++contigIndex) {
      final ContigTree.ContigTuple tuple = contigs.get(contigIndex);
      final var descriptor = tuple.descriptor();
      final var sourceName = descriptor.getContigNameInSourceFASTA();
      final var originalName = descriptor.getContigName();
      final var displayName = chunkedFile.getContigDisplayName(descriptor.getContigId());
      final var sourceStart = descriptor.getOffsetInSourceFASTA();
      final var sourceEnd = sourceStart + descriptor.getLengthBp();
      final var assemblyStart = assemblyCursor;
      final var assemblyEnd = assemblyCursor + descriptor.getLengthBp();
      final var segment = new AssemblyBpSegment(
        sourceStart,
        sourceEnd,
        assemblyStart,
        assemblyEnd,
        tuple.direction() == ContigDirection.REVERSED
      );
      sourceToAssemblySegments.computeIfAbsent(sourceName, key -> new ArrayList<>()).add(segment);
      if (originalName != null && !originalName.isBlank() && !originalName.equals(sourceName)) {
        sourceToAssemblySegments.computeIfAbsent(originalName, key -> new ArrayList<>()).add(segment);
      }
      if (displayName != null && !displayName.isBlank()
        && !displayName.equals(sourceName)
        && !displayName.equals(originalName)) {
        sourceToAssemblySegments.computeIfAbsent(displayName, key -> new ArrayList<>()).add(segment);
      }
      final var aliasName = linkedFastaAliasesBySource.get(sourceName);
      if (aliasName != null && !aliasName.equals(sourceName)) {
        sourceToAssemblySegments.computeIfAbsent(aliasName, key -> new ArrayList<>()).add(segment);
      }
      assemblyCursor = assemblyEnd;
    }
    sourceToAssemblySegments.values().forEach(list -> list.sort(Comparator.comparingLong(AssemblyBpSegment::sourceStart)));
    return sourceToAssemblySegments;
  }

  private static @NotNull String resolveFeatureSearchLabel(final @NotNull FeatureRange feature,
                                                           final @NotNull String sourceName) {
    final var preferredLabel = normalizeBlankToNull(feature.label());
    if (preferredLabel != null) {
      return preferredLabel;
    }
    final var featureType = normalizeBlankToNull(feature.featureType());
    if (featureType != null) {
      return featureType;
    }
    return sourceName + ":" + feature.start() + "-" + feature.end();
  }

  private static @Nullable String normalizeBlankToNull(final @Nullable String value) {
    if (value == null) {
      return null;
    }
    final var trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static @NotNull List<TrackBin> aggregateFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                           final long queryStartPx,
                                                           final long queryEndPx,
                                                           final int widthPx,
                                                           final @NotNull List<AssemblySegment> orderedSegments,
                                                           final long bpResolution) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final double[] maxValues = new double[bucketCount];
    final long[] counts = new long[bucketCount];
    Arrays.fill(maxValues, 0.0d);
    for (final var feature : projectedFeatures) {
      int left = (int) Math.floor((feature.startPx() - queryStartPx) / bucketSpan);
      int right = (int) Math.ceil((feature.endPx() - queryStartPx) / bucketSpan) - 1;
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
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      bins.add(aggregateSignalBin(
        orderedSegments,
        bpResolution,
        startPx,
        safeEndPx,
        maxValues[i],
        counts[i],
        null
      ));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateBigWigFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                 final long queryStartPx,
                                                                 final long queryEndPx,
                                                                 final int widthPx,
                                                                 final @NotNull BigWigAggregationMode mode,
                                                                 final @NotNull List<AssemblySegment> orderedSegments,
                                                                 final long bpResolution) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final var nativeAggregation = tryAggregateProjectedFeaturesNative(
      projectedFeatures,
      queryStartPx,
      queryEndPx,
      bucketCount,
      nativeIntervalModeCode(mode),
      true
    );
    if (nativeAggregation != null) {
      return finalizeNativeAggregationBins(
        nativeAggregation,
        queryStartPx,
        queryEndPx,
        bucketSpan,
        orderedSegments,
        bpResolution
      );
    }
    final double[] maxValues = new double[bucketCount];
    final double[] weightedSums = new double[bucketCount];
    final double[] overlapSums = new double[bucketCount];
    final long[] counts = new long[bucketCount];
    Arrays.fill(maxValues, 0.0d);
    for (final var feature : projectedFeatures) {
      int left = (int) Math.floor((feature.startPx() - queryStartPx) / bucketSpan);
      int right = (int) Math.ceil((feature.endPx() - queryStartPx) / bucketSpan) - 1;
      left = Math.max(0, Math.min(left, bucketCount - 1));
      right = Math.max(0, Math.min(right, bucketCount - 1));
      for (int i = left; i <= right; i++) {
        final var bucketStart = queryStartPx + (long) Math.floor(i * bucketSpan);
        final var bucketEnd = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
        final var overlap = Math.min(feature.endPx(), bucketEnd) - Math.max(feature.startPx(), bucketStart);
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
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      final var bucketWidth = Math.max(1.0d, safeEndPx - startPx);
      final double value = switch (mode) {
        case MAX -> maxValues[i];
        case MEAN -> overlapSums[i] > 0.0d ? weightedSums[i] / overlapSums[i] : 0.0d;
        case SUM -> weightedSums[i] / bucketWidth;
      };
      bins.add(aggregateSignalBin(
        orderedSegments,
        bpResolution,
        startPx,
        safeEndPx,
        value,
        counts[i],
        null
      ));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateCoverageFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                   final long queryStartPx,
                                                                   final long queryEndPx,
                                                                   final int widthPx,
                                                                   final @NotNull List<AssemblySegment> orderedSegments,
                                                                   final long bpResolution) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final var nativeAggregation = tryAggregateProjectedFeaturesNative(
      projectedFeatures,
      queryStartPx,
      queryEndPx,
      bucketCount,
      4,
      false
    );
    if (nativeAggregation != null) {
      return finalizeNativeAggregationBins(
        nativeAggregation,
        queryStartPx,
        queryEndPx,
        bucketSpan,
        orderedSegments,
        bpResolution
      );
    }
    final double[] coverage = new double[bucketCount];
    final long[] counts = new long[bucketCount];
    Arrays.fill(coverage, 0.0d);
    for (final var feature : projectedFeatures) {
      int left = (int) Math.floor((feature.startPx() - queryStartPx) / bucketSpan);
      int right = (int) Math.ceil((feature.endPx() - queryStartPx) / bucketSpan) - 1;
      left = Math.max(0, Math.min(left, bucketCount - 1));
      right = Math.max(0, Math.min(right, bucketCount - 1));
      for (int i = left; i <= right; i++) {
        final var bucketStart = queryStartPx + (long) Math.floor(i * bucketSpan);
        final var bucketEnd = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
        final var overlap = Math.min(feature.endPx(), bucketEnd) - Math.max(feature.startPx(), bucketStart);
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
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      bins.add(aggregateSignalBin(
        orderedSegments,
        bpResolution,
        startPx,
        safeEndPx,
        coverage[i],
        counts[i],
        null
      ));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateReadDensityFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                      final long queryStartPx,
                                                                      final long queryEndPx,
                                                                      final int widthPx,
                                                                      final @NotNull List<AssemblySegment> orderedSegments,
                                                                      final long bpResolution) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    final var nativeAggregation = tryAggregateProjectedFeaturesNative(
      projectedFeatures,
      queryStartPx,
      queryEndPx,
      bucketCount,
      5,
      false
    );
    if (nativeAggregation != null) {
      return finalizeNativeAggregationBins(
        nativeAggregation,
        queryStartPx,
        queryEndPx,
        bucketSpan,
        orderedSegments,
        bpResolution
      );
    }
    final double[] values = new double[bucketCount];
    final long[] counts = new long[bucketCount];
    Arrays.fill(values, 0.0d);
    for (final var feature : projectedFeatures) {
      final var center = feature.startPx() + ((feature.endPx() - feature.startPx()) >>> 1);
      int idx = (int) Math.floor((center - queryStartPx) / bucketSpan);
      idx = Math.max(0, Math.min(idx, bucketCount - 1));
      values[idx] += 1.0d;
      counts[idx] += 1L;
    }
    final var bins = new ArrayList<TrackBin>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      if (counts[i] <= 0L) {
        continue;
      }
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      bins.add(aggregateSignalBin(
        orderedSegments,
        bpResolution,
        startPx,
        safeEndPx,
        values[i],
        counts[i],
        null
      ));
    }
    return bins;
  }

  private static @Nullable NativeProcessingService.NativeAggregationResult tryAggregateProjectedFeaturesNative(
    final @NotNull List<ProjectedFeature> projectedFeatures,
    final long queryStartPx,
    final long queryEndPx,
    final int bucketCount,
    final int modeCode,
    final boolean includeValues
  ) {
    if (projectedFeatures.isEmpty() || !NativeProcessingService.getInstance().status().enabled()) {
      return null;
    }
    final var starts = new long[projectedFeatures.size()];
    final var ends = new long[projectedFeatures.size()];
    final var values = includeValues ? new double[projectedFeatures.size()] : null;
    for (int i = 0; i < projectedFeatures.size(); i++) {
      final var feature = projectedFeatures.get(i);
      starts[i] = feature.startPx();
      ends[i] = feature.endPx();
      if (values != null) {
        values[i] = feature.value();
      }
    }
    return NativeProcessingService.getInstance().tryAggregateIntervals(
      starts,
      ends,
      values,
      queryStartPx,
      queryEndPx,
      bucketCount,
      modeCode
    );
  }

  private static int nativeIntervalModeCode(final @NotNull BigWigAggregationMode mode) {
    return switch (mode) {
      case MAX -> 1;
      case MEAN -> 2;
      case SUM -> 3;
    };
  }

  private static void accumulateBigWigValue(final long featureStart,
                                            final long featureEnd,
                                            final double featureValue,
                                            final long queryStartPx,
                                            final long queryEndPx,
                                            final double bucketSpan,
                                            final double[] maxValues,
                                            final double[] weightedSums,
                                            final double[] overlapSums,
                                            final long[] counts) {
    int left = (int) Math.floor((featureStart - queryStartPx) / bucketSpan);
    int right = (int) Math.ceil((featureEnd - queryStartPx) / bucketSpan) - 1;
    left = Math.max(0, Math.min(left, counts.length - 1));
    right = Math.max(0, Math.min(right, counts.length - 1));
    for (int i = left; i <= right; i++) {
      final var bucketStart = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var bucketEnd = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
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
                                              final long queryStartPx,
                                              final long queryEndPx,
                                              final double bucketSpan,
                                              final double[] coverage,
                                              final long[] counts) {
    int left = (int) Math.floor((featureStart - queryStartPx) / bucketSpan);
    int right = (int) Math.ceil((featureEnd - queryStartPx) / bucketSpan) - 1;
    left = Math.max(0, Math.min(left, counts.length - 1));
    right = Math.max(0, Math.min(right, counts.length - 1));
    for (int i = left; i <= right; i++) {
      final var bucketStart = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var bucketEnd = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
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
                                                 final long queryStartPx,
                                                 final double bucketSpan,
                                                 final double[] values,
                                                 final long[] counts) {
    final var center = featureStart + ((featureEnd - featureStart) >>> 1);
    int idx = (int) Math.floor((center - queryStartPx) / bucketSpan);
    idx = Math.max(0, Math.min(idx, counts.length - 1));
    values[idx] += 1.0d;
    counts[idx] += 1L;
  }

  private static @NotNull List<TrackBin> finalizeBigWigBins(final long queryStartPx,
                                                            final long queryEndPx,
                                                            final double bucketSpan,
                                                            final double[] maxValues,
                                                            final double[] weightedSums,
                                                            final double[] overlapSums,
                                                            final long[] counts,
                                                            final @NotNull BigWigAggregationMode mode,
                                                            final @NotNull List<AssemblySegment> orderedSegments,
                                                            final long bpResolution) {
    final var bins = new ArrayList<TrackBin>(counts.length);
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] <= 0L) {
        continue;
      }
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      final var bucketWidth = Math.max(1.0d, safeEndPx - startPx);
      final double value = switch (mode) {
        case MAX -> maxValues[i];
        case MEAN -> overlapSums[i] > 0.0d ? weightedSums[i] / overlapSums[i] : 0.0d;
        case SUM -> weightedSums[i] / bucketWidth;
      };
      bins.add(aggregateSignalBin(
        orderedSegments,
        bpResolution,
        startPx,
        safeEndPx,
        value,
        counts[i],
        null
      ));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> finalizeBins(final long queryStartPx,
                                                      final long queryEndPx,
                                                      final double bucketSpan,
                                                      final double[] values,
                                                      final long[] counts,
                                                      final @NotNull List<AssemblySegment> orderedSegments,
                                                      final long bpResolution) {
    final var bins = new ArrayList<TrackBin>(counts.length);
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] <= 0L) {
        continue;
      }
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      bins.add(aggregateSignalBin(
        orderedSegments,
        bpResolution,
        startPx,
        safeEndPx,
        values[i],
        counts[i],
        null
      ));
    }
    return bins;
  }

  private static @NotNull TrackBin aggregateSignalBin(final @NotNull List<AssemblySegment> orderedSegments,
                                                      final long bpResolution,
                                                      final long startPx,
                                                      final long safeEndPx,
                                                      final double value,
                                                      final long count,
                                                      final @Nullable String label) {
    final long totalVisiblePixels = orderedSegments.isEmpty()
      ? Math.max(safeEndPx, startPx + 1L)
      : Math.max(safeEndPx, orderedSegments.get(orderedSegments.size() - 1).visiblePxEnd());
    final long startBp = mapVisiblePxToAssemblyBp(
      Math.max(0L, Math.min(startPx, Math.max(0L, totalVisiblePixels - 1L))),
      orderedSegments,
      bpResolution
    );
    final long endBp = mapVisiblePxToAssemblyBp(
      Math.max(0L, Math.min(Math.max(startPx, safeEndPx - 1L), Math.max(0L, totalVisiblePixels - 1L))),
      orderedSegments,
      bpResolution
    ) + bpResolution;
    return new TrackBin(startBp, Math.max(startBp + 1L, endBp), value, count, label, startPx, safeEndPx);
  }

  private static @NotNull List<TrackBin> toBins(final @NotNull List<ProjectedFeature> projectedFeatures) {
    return projectedFeatures.stream()
      .map(f -> new TrackBin(
        f.startBp(),
        f.endBp(),
        f.value(),
        1L,
        f.label(),
        f.startPx(),
        f.endPx(),
        f.strand(),
        f.thickStartBp(),
        f.thickEndBp(),
        f.thickStartPx(),
        f.thickEndPx(),
        f.featureType(),
        f.blocks().stream()
          .map(block -> new TrackBin.TrackBinBlock(
            block.startBp(),
            block.endBp(),
            block.startPx(),
            block.endPx(),
            block.coding()
          ))
          .toList(),
        f.attributes()
      ))
      .toList();
  }

  private static @NotNull List<TrackBin> queryBinsForTrack(final @NotNull TrackType type,
                                                           final @NotNull ChunkedFile chunkedFile,
                                                           final @NotNull TrackDataSource dataSource,
                                                           final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                           final @NotNull List<AssemblySegment> orderedSegments,
                                                           final long queryStartPx,
                                                           final long queryEndPx,
                                                           final int widthPx,
                                                           final long bpResolution,
                                                           final @NotNull BamRenderMode bamRenderMode,
                                                           final @NotNull BigWigAggregationMode bigWigAggregationMode) {
    if (type == TrackType.BAM && dataSource instanceof BamTrackDataSource bamDataSource) {
      return bamDataSource.queryBins(
        sourceToAssemblySegments,
        orderedSegments,
        queryStartPx,
        queryEndPx,
        widthPx,
        bpResolution,
        bamRenderMode
      );
    }
    if (type == TrackType.BIGWIG && dataSource instanceof BigWigTrackDataSource bigWigDataSource) {
      return bigWigDataSource.queryBins(
        sourceToAssemblySegments,
        orderedSegments,
        queryStartPx,
        queryEndPx,
        widthPx,
        bpResolution,
        bigWigAggregationMode
      );
    }
    if (type == TrackType.COOLER_WEIGHTS && dataSource instanceof CoolerWeightsTrackDataSource coolerWeightsTrackDataSource) {
      return coolerWeightsTrackDataSource.queryBins(
        chunkedFile,
        orderedSegments,
        queryStartPx,
        queryEndPx,
        widthPx,
        bpResolution
      );
    }
    if (type == TrackType.BED || type == TrackType.GFF_GTF) {
      if (dataSource instanceof InMemoryTrackDataSource inMemoryTrackDataSource) {
        return inMemoryTrackDataSource.queryBins(
          sourceToAssemblySegments,
          orderedSegments,
          queryStartPx,
          queryEndPx,
          widthPx,
          bpResolution
        );
      }
      final var projectedFeatures = dataSource.projectFeatures(
        sourceToAssemblySegments,
        queryStartPx,
        queryEndPx,
        bpResolution
      );
      return aggregateCoverageFeatures(
        projectedFeatures,
        queryStartPx,
        queryEndPx,
        widthPx,
        orderedSegments,
        bpResolution
      );
    }
    final var projectedFeatures = dataSource.projectFeatures(
      sourceToAssemblySegments,
      queryStartPx,
      queryEndPx,
      bpResolution
    );
    final var maxFeatureCount = Math.max(widthPx * 8, 8192);
    if (projectedFeatures.size() > maxFeatureCount) {
      return aggregateFeatures(
        projectedFeatures,
        queryStartPx,
        queryEndPx,
        widthPx,
        orderedSegments,
        bpResolution
      );
    }
    return toBins(projectedFeatures);
  }

  private enum TrackType {
    BED,
    VCF,
    GFF_GTF,
    BIGWIG,
    BAM,
    COOLER_WEIGHTS,
    UNSUPPORTED;

    private static @NotNull TrackType fromPath(final @NotNull Path path) {
      final var lowered = path.getFileName().toString().toLowerCase(Locale.ROOT);
      if (lowered.endsWith(".bed") || lowered.endsWith(".bed.gz")) {
        return BED;
      }
      if (lowered.endsWith(".vcf") || lowered.endsWith(".vcf.gz")) {
        return VCF;
      }
      if (lowered.endsWith(".gff")
        || lowered.endsWith(".gff.gz")
        || lowered.endsWith(".gff3")
        || lowered.endsWith(".gff3.gz")
        || lowered.endsWith(".gtf")
        || lowered.endsWith(".gtf.gz")) {
        return GFF_GTF;
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

  private enum RenderStyle {
    SIGNAL,
    FEATURE
  }

  private interface TrackDataSource extends AutoCloseable {
    long featureCountHint();

    @NotNull
    Set<String> sourceNames();

    @NotNull
    default RenderStyle renderStyle() {
      return RenderStyle.SIGNAL;
    }

    @NotNull
    List<ProjectedFeature> projectFeatures(@NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                           long queryStartPx,
                                           long queryEndPx,
                                           long bpResolution);

    @Override
    default void close() throws Exception {
      // no-op
    }
  }

  private record InMemoryTrackDataSource(@NotNull Map<String, List<FeatureRange>> featuresBySource,
                                         long featureCount,
                                         boolean hasSignalValues,
                                         boolean hasStructuredFeatures,
                                         @NotNull RenderStyle preferredRenderStyle) implements TrackDataSource {
    static @NotNull InMemoryTrackDataSource fromBed(final @NotNull Path filePath) {
      final var features = new HashMap<String, List<FeatureRange>>();
      long total = 0L;
      boolean hasSignalValues = false;
      boolean hasStructuredFeatures = false;
      boolean hasStrandFeatures = false;
      boolean hasThickFeatures = false;
      boolean hasBed12Rows = false;
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
          final var col4Numeric = fields.length >= 4 ? parseNullableDouble(fields[3]) : null;
          final var col5Numeric = fields.length >= 5 ? parseNullableDouble(fields[4]) : null;
          final var hasStrand = fields.length >= 6 && isBedStrandToken(fields[5]);
          final String strand = hasStrand ? normalizeStrand(fields[5]) : null;
          final String label = (fields.length >= 4 && !fields[3].isBlank()) ? fields[3] : null;
          Long thickStart = null;
          Long thickEnd = null;
          if (fields.length >= 8) {
            try {
              final var parsedThickStart = Long.parseLong(fields[6]);
              final var parsedThickEnd = Long.parseLong(fields[7]);
              final var clampedThickStart = Math.max(start, Math.min(parsedThickStart, end));
              final var clampedThickEnd = Math.max(clampedThickStart, Math.min(parsedThickEnd, end));
              if (clampedThickEnd > clampedThickStart) {
                thickStart = clampedThickStart;
                thickEnd = clampedThickEnd;
                hasThickFeatures = true;
              }
            } catch (final NumberFormatException ignored) {
              // Optional BED fields, ignore malformed thickStart/thickEnd.
            }
          }
          final double value;
          if (fields.length == 4 && col4Numeric != null) {
            // BEDGraph row: chrom start end value
            value = Math.max(0.0d, col4Numeric);
          } else if (hasStrand) {
            // BED6/BED12 annotation-like rows are rendered as features.
            value = 1.0d;
          } else {
            value = Math.max(0.0d, col5Numeric != null ? col5Numeric : 1.0d);
          }
          final var featureType = (fields.length >= 12) ? "BED12" : (hasStrand ? "BED6" : "BED");
          final var attributes = new LinkedHashMap<String, String>();
          attributes.put("chrom", sourceName);
          attributes.put("chromStart", Long.toString(start));
          attributes.put("chromEnd", Long.toString(end));
          if (fields.length >= 4 && !fields[3].isBlank()) {
            attributes.put("name", fields[3]);
          }
          if (fields.length >= 5 && !fields[4].isBlank()) {
            attributes.put("score", fields[4]);
          }
          if (hasStrand) {
            attributes.put("strand", Objects.requireNonNull(strand));
          }
          if (thickStart != null && thickEnd != null) {
            attributes.put("thickStart", Long.toString(thickStart));
            attributes.put("thickEnd", Long.toString(thickEnd));
          }
          if (fields.length >= 9 && !fields[8].isBlank()) {
            attributes.put("itemRgb", fields[8]);
          }
          if (fields.length >= 12) {
            attributes.put("blockCount", fields[9]);
            attributes.put("blockSizes", fields[10]);
            attributes.put("blockStarts", fields[11]);
          }
          for (int fieldIndex = 12; fieldIndex < fields.length; fieldIndex++) {
            attributes.put("field" + (fieldIndex + 1), fields[fieldIndex]);
          }
          if (fields.length >= 12) {
            hasBed12Rows = true;
          }
          features.computeIfAbsent(sourceName, ignored -> new ArrayList<>())
            .add(new FeatureRange(start, end, value, label, strand, thickStart, thickEnd, featureType, List.of(), attributes));
          if (Math.abs(value - 1.0d) > 1e-9) {
            hasSignalValues = true;
          }
          if (strand != null || (thickStart != null && thickEnd != null)) {
            hasStructuredFeatures = true;
            if (strand != null) {
              hasStrandFeatures = true;
            }
          }
          total++;
        }
      } catch (final IOException e) {
        throw new RuntimeException("Failed to parse BED track " + filePath, e);
      }
      features.values().forEach(list -> list.sort(Comparator.comparingLong(FeatureRange::start)));
      return new InMemoryTrackDataSource(
        features,
        total,
        hasSignalValues,
        hasStructuredFeatures,
        resolveBedRenderStyle(total, hasStrandFeatures, hasThickFeatures, hasBed12Rows)
      );
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
            .add(new FeatureRange(start, end, 1.0d, label, null, null, null, "VCF", List.of(), Map.of()));
          total++;
        }
      } catch (final IOException e) {
        throw new RuntimeException("Failed to parse VCF track " + filePath, e);
      }
      features.values().forEach(list -> list.sort(Comparator.comparingLong(FeatureRange::start)));
      return new InMemoryTrackDataSource(features, total, false, true, RenderStyle.FEATURE);
    }

    static @NotNull InMemoryTrackDataSource fromGffOrGtf(final @NotNull Path filePath) {
      final var features = new HashMap<String, List<FeatureRange>>();
      final var groupedFeatures = new HashMap<String, Map<String, GffTranscriptFeatureBuilder>>();
      long total = 0L;
      boolean hasSignalValues = false;
      final var lowered = filePath.getFileName().toString().toLowerCase(Locale.ROOT);
      final var gtfMode = lowered.endsWith(".gtf") || lowered.endsWith(".gtf.gz");
      try (final BufferedReader reader = openMaybeGzipReader(filePath)) {
        String line;
        long lineNo = 0L;
        while ((line = reader.readLine()) != null) {
          lineNo++;
          if (line.isBlank() || line.startsWith("#")) {
            continue;
          }
          final var fields = line.split("\t");
          if (fields.length < 9) {
            continue;
          }
          final var sourceName = fields[0];
          final var featureType = (fields[2] == null || fields[2].isBlank()) ? "feature" : fields[2];
          final var featureTypeLower = featureType.trim().toLowerCase(Locale.ROOT);
          final long start1Based = parseLongOrThrow(fields[3], "GFF/GTF start", lineNo);
          final long end1Based = parseLongOrThrow(fields[4], "GFF/GTF end", lineNo);
          final long start = Math.max(0L, start1Based - 1L);
          final long end = Math.max(start + 1L, end1Based);
          final var score = parseNullableDouble(fields[5]);
          final double value = Math.max(0.0d, score != null ? score : 1.0d);
          final var strand = normalizeStrand(fields[6]);
          final var attributes = parseGffAttributes(fields[8], gtfMode);
          final var label = firstNonBlank(
            attributes.get("Name"),
            attributes.get("gene_name"),
            attributes.get("gene_id"),
            attributes.get("transcript_id"),
            attributes.get("ID"),
            attributes.get("Parent"),
            featureType
          );
          final var groupKey = resolveGffGroupKey(attributes, featureTypeLower);
          final boolean groupable =
            groupKey != null &&
              (isGffBlockLikeFeature(featureTypeLower) ||
                isGffTranscriptLikeFeature(featureTypeLower));
          if (groupable) {
            final var bySource = groupedFeatures.computeIfAbsent(sourceName, ignored -> new LinkedHashMap<>());
            final var builder = bySource.computeIfAbsent(
              groupKey,
              ignored -> new GffTranscriptFeatureBuilder(groupKey)
            );
            builder.accept(
              start,
              end,
              value,
              label,
              strand,
              featureType,
              featureTypeLower,
              attributes
            );
          } else {
            features.computeIfAbsent(sourceName, ignored -> new ArrayList<>())
              .add(new FeatureRange(start, end, value, label, strand, null, null, featureType, List.of(), attributes));
          }
          if (Math.abs(value - 1.0d) > 1e-9) {
            hasSignalValues = true;
          }
          total++;
        }
      } catch (final IOException e) {
        throw new RuntimeException("Failed to parse GFF/GTF track " + filePath, e);
      }
      for (final var entry : groupedFeatures.entrySet()) {
        final var sourceName = entry.getKey();
        final var destination = features.computeIfAbsent(sourceName, ignored -> new ArrayList<>());
        entry.getValue().values().forEach(builder -> {
          final var aggregated = builder.toFeatureRange();
          if (aggregated != null) {
            destination.add(aggregated);
          }
        });
      }
      features.values().forEach(list -> list.sort(Comparator.comparingLong(FeatureRange::start)));
      return new InMemoryTrackDataSource(features, total, hasSignalValues, true, RenderStyle.FEATURE);
    }

    @Override
    public long featureCountHint() {
      return this.featureCount;
    }

    @Override
    public @NotNull Set<String> sourceNames() {
      return this.featuresBySource.keySet();
    }

    @Override
    public @NotNull RenderStyle renderStyle() {
      return this.preferredRenderStyle;
    }

    @Override
    public @NotNull List<ProjectedFeature> projectFeatures(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                           final long queryStartPx,
                                                           final long queryEndPx,
                                                           final long bpResolution) {
      final var projected = new ArrayList<ProjectedFeature>();
      forEachProjectedFeature(
        sourceToAssemblySegments,
        queryStartPx,
        queryEndPx,
        bpResolution,
        feature -> projected.add(feature),
        MAX_FEATURES_PER_QUERY + 1
      );
      projected.sort(Comparator.comparingLong(ProjectedFeature::startPx));
      if (projected.size() > MAX_FEATURES_PER_QUERY) {
        return projected.subList(0, MAX_FEATURES_PER_QUERY);
      }
      return projected;
    }

    public @NotNull List<TrackBin> queryBins(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                             final @NotNull List<AssemblySegment> orderedSegments,
                                             final long queryStartPx,
                                             final long queryEndPx,
                                             final int widthPx,
                                             final long bpResolution) {
      final var bucketCount = Math.max(1, widthPx);
      final var span = Math.max(1L, queryEndPx - queryStartPx);
      final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);

      if (this.renderStyle() == RenderStyle.FEATURE) {
        return queryFeatureBins(
          sourceToAssemblySegments,
          queryStartPx,
          queryEndPx,
          widthPx,
          bpResolution
        );
      }

      if (this.hasSignalValues()) {
        final double[] maxValues = new double[bucketCount];
        final double[] weightedSums = new double[bucketCount];
        final double[] overlapSums = new double[bucketCount];
        final long[] counts = new long[bucketCount];
        forEachProjectedFeature(
          sourceToAssemblySegments,
          queryStartPx,
          queryEndPx,
          bpResolution,
          feature -> accumulateBigWigValue(
            feature.startPx(),
            feature.endPx(),
            feature.value(),
            queryStartPx,
            queryEndPx,
            bucketSpan,
            maxValues,
            weightedSums,
            overlapSums,
            counts
          ),
          Integer.MAX_VALUE
        );
        return finalizeBigWigBins(
          queryStartPx,
          queryEndPx,
          bucketSpan,
          maxValues,
          weightedSums,
          overlapSums,
          counts,
          BigWigAggregationMode.MAX,
          orderedSegments,
          bpResolution
        );
      }

      final double[] values = new double[bucketCount];
      final long[] counts = new long[bucketCount];
      forEachProjectedFeature(
        sourceToAssemblySegments,
        queryStartPx,
        queryEndPx,
        bpResolution,
        feature -> accumulateCoverageValue(
          feature.startPx(),
          feature.endPx(),
          queryStartPx,
          queryEndPx,
          bucketSpan,
          values,
          counts
        ),
        Integer.MAX_VALUE
      );
      return finalizeBins(
        queryStartPx,
        queryEndPx,
        bucketSpan,
        values,
        counts,
        orderedSegments,
        bpResolution
      );
    }

    private @NotNull List<TrackBin> queryFeatureBins(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                     final long queryStartPx,
                                                     final long queryEndPx,
                                                     final int widthPx,
                                                     final long bpResolution) {
      final int safeWidth = Math.max(1, widthPx);
      final int maxDirectFeatures = Math.max(
        FEATURE_DIRECT_RENDER_MIN_FEATURES,
        Math.min(
          Math.min(MAX_FEATURES_PER_QUERY, FEATURE_DIRECT_RENDER_MAX_FEATURES),
          safeWidth * FEATURE_DIRECT_RENDER_FEATURES_PER_PIXEL
        )
      );
      final var projected = new ArrayList<ProjectedFeature>(Math.min(maxDirectFeatures, 8192));
      forEachProjectedFeature(
        sourceToAssemblySegments,
        queryStartPx,
        queryEndPx,
        bpResolution,
        projected::add,
        maxDirectFeatures + 1
      );
      projected.sort(
        Comparator.comparingLong(ProjectedFeature::startPx)
          .thenComparingLong(ProjectedFeature::endPx)
      );
      if (projected.size() <= maxDirectFeatures) {
        return toBins(projected);
      }
      return downsampleFeatureBins(projected, queryStartPx, queryEndPx, safeWidth);
    }

    private static @NotNull List<TrackBin> downsampleFeatureBins(final @NotNull List<ProjectedFeature> projected,
                                                                 final long queryStartPx,
                                                                 final long queryEndPx,
                                                                 final int widthPx) {
      if (projected.isEmpty()) {
        return List.of();
      }
      final int bucketCount = Math.max(1, widthPx);
      final double bucketSpan = Math.max(1.0d, Math.max(1L, queryEndPx - queryStartPx) / (double) bucketCount);
      final int maxFeaturesPerBucket = FEATURE_DOWNSAMPLED_FEATURES_PER_PIXEL;
      @SuppressWarnings("unchecked")
      final ArrayList<ProjectedFeature>[] byBucket = new ArrayList[bucketCount];
      for (final var feature : projected) {
        if (feature.endPx() <= queryStartPx || feature.startPx() >= queryEndPx) {
          continue;
        }
        final var centerPx = feature.startPx() + Math.max(0L, (feature.endPx() - feature.startPx()) / 2L);
        final int bucket = Math.max(
          0,
          Math.min(
            bucketCount - 1,
            (int) Math.floor((centerPx - queryStartPx) / bucketSpan)
          )
        );
        var list = byBucket[bucket];
        if (list == null) {
          list = new ArrayList<>(maxFeaturesPerBucket + 1);
          byBucket[bucket] = list;
        }
        list.add(feature);
      }

      final var selected = new ArrayList<ProjectedFeature>(bucketCount * maxFeaturesPerBucket);
      for (final var bucketFeatures : byBucket) {
        if (bucketFeatures == null || bucketFeatures.isEmpty()) {
          continue;
        }
        bucketFeatures.sort(InMemoryTrackDataSource::compareProjectedFeaturesForDisplay);
        for (int idx = 0; idx < Math.min(maxFeaturesPerBucket, bucketFeatures.size()); idx++) {
          selected.add(bucketFeatures.get(idx));
        }
      }
      selected.sort(
        Comparator.comparingLong(ProjectedFeature::startPx)
          .thenComparingInt(feature -> featureHierarchyDepth(feature.featureType()))
          .thenComparingLong(ProjectedFeature::endPx)
      );
      return toBins(selected);
    }

    private static int compareProjectedFeaturesForDisplay(final @NotNull ProjectedFeature left,
                                                          final @NotNull ProjectedFeature right) {
      final int depthCmp = Integer.compare(
        featureHierarchyDepth(right.featureType()),
        featureHierarchyDepth(left.featureType())
      );
      if (depthCmp != 0) {
        return depthCmp;
      }
      final int blockCmp = Integer.compare(right.blocks().size(), left.blocks().size());
      if (blockCmp != 0) {
        return blockCmp;
      }
      final long leftSpan = Math.max(1L, left.endPx() - left.startPx());
      final long rightSpan = Math.max(1L, right.endPx() - right.startPx());
      final int spanCmp = Long.compare(rightSpan, leftSpan);
      if (spanCmp != 0) {
        return spanCmp;
      }
      final int startCmp = Long.compare(left.startPx(), right.startPx());
      if (startCmp != 0) {
        return startCmp;
      }
      return Long.compare(left.endPx(), right.endPx());
    }

    private static int featureHierarchyDepth(final @Nullable String featureType) {
      if (featureType == null || featureType.isBlank()) {
        return 1;
      }
      final var normalized = featureType.trim().toLowerCase(Locale.ROOT);
      if (isGffGeneLikeFeature(normalized)) {
        return 0;
      }
      if (isGffTranscriptLikeFeature(normalized)) {
        return 1;
      }
      if (isGffBlockLikeFeature(normalized)) {
        return 2;
      }
      return 1;
    }

    private static @NotNull RenderStyle resolveBedRenderStyle(final long featureCount,
                                                              final boolean hasStrandFeatures,
                                                              final boolean hasThickFeatures,
                                                              final boolean hasBed12Rows) {
      if (hasThickFeatures || hasBed12Rows) {
        return RenderStyle.FEATURE;
      }
      if (!hasStrandFeatures) {
        return RenderStyle.SIGNAL;
      }
      return featureCount <= BED_FEATURE_STYLE_MAX_FEATURES ? RenderStyle.FEATURE : RenderStyle.SIGNAL;
    }

    private void forEachProjectedFeature(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                         final long queryStartPx,
                                         final long queryEndPx,
                                         final long bpResolution,
                                         final @NotNull java.util.function.Consumer<ProjectedFeature> consumer,
                                         final int maxFeatures) {
      int emitted = 0;
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
          final var sourceIntervalOptional = mapVisiblePxIntervalToSegmentSource(
            segment,
            queryStartPx,
            queryEndPx,
            bpResolution
          );
          if (sourceIntervalOptional.isEmpty()) {
            continue;
          }
          final var sourceInterval = sourceIntervalOptional.get();
          int index = lowerBoundByStart(sourceFeatures, sourceInterval.start());
          if (index > 0) {
            index--;
          }
          for (int i = index; i < sourceFeatures.size(); i++) {
            final var feature = sourceFeatures.get(i);
            if (feature.start() >= sourceInterval.end()) {
              break;
            }
            if (feature.end() <= sourceInterval.start()) {
              continue;
            }
            final var projected = projectSourceFeatureOnSegment(
              segment,
              feature,
              queryStartPx,
              queryEndPx,
              bpResolution
            );
            if (projected.isEmpty()) {
              continue;
            }
            consumer.accept(projected.get());
            emitted++;
            if (emitted >= maxFeatures) {
              return;
            }
          }
        }
      }
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

  private static final class CoolerWeightsTrackDataSource implements TrackDataSource {
    private final @NotNull String source;

    private CoolerWeightsTrackDataSource(final @NotNull String source) {
      this.source = normalizeSourceName(source);
    }

    private @NotNull String source() {
      return this.source;
    }

    @Override
    public long featureCountHint() {
      return -1L;
    }

    @Override
    public @NotNull Set<String> sourceNames() {
      return Set.of();
    }

    @Override
    public @NotNull List<ProjectedFeature> projectFeatures(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                           final long queryStartPx,
                                                           final long queryEndPx,
                                                           final long bpResolution) {
      return List.of();
    }

    public @NotNull List<TrackBin> queryBins(final @NotNull ChunkedFile chunkedFile,
                                             final @NotNull List<AssemblySegment> orderedSegments,
                                             final long queryStartPx,
                                             final long queryEndPx,
                                             final int widthPx,
                                             final long bpResolution) {
      final var resolutionOrder = chunkedFile.getResolutionToIndex().get(bpResolution);
      if (resolutionOrder == null || resolutionOrder <= 0) {
        return List.of();
      }
      final var resolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(resolutionOrder);
      final var atus = chunkedFile.matrixQueries().getATUsForRange(
        resolutionDescriptor,
        queryStartPx,
        queryEndPx,
        true
      );
      if (atus.isEmpty()) {
        return List.of();
      }

      final int bucketCount = Math.max(1, widthPx);
      final long span = Math.max(1L, queryEndPx - queryStartPx);
      final double bucketSpan = Math.max(1.0d, span / (double) bucketCount);
      final double[] maxValues = new double[bucketCount];
      final long[] counts = new long[bucketCount];

      long pxCursor = queryStartPx;
      for (final var atu : atus) {
        final var weights = atu.getStripeDescriptor().bin_weights();
        final var start = atu.getStartIndexInStripeIncl();
        final var end = atu.getEndIndexInStripeExcl();
        if (start < 0 || end <= start || end > weights.length) {
          continue;
        }
        if (atu.getDirection() == ATUDirection.FORWARD) {
          for (int i = start; i < end; i++) {
            accumulateCoolerWeightValue(
              pxCursor,
              weights[i],
              queryStartPx,
              bucketSpan,
              maxValues,
              counts
            );
            pxCursor++;
          }
        } else {
          for (int i = end - 1; i >= start; i--) {
            accumulateCoolerWeightValue(
              pxCursor,
              weights[i],
              queryStartPx,
              bucketSpan,
              maxValues,
              counts
            );
            pxCursor++;
          }
        }
      }
      return finalizeCoolerWeightBins(
        queryStartPx,
        queryEndPx,
        bucketSpan,
        orderedSegments,
        bpResolution,
        maxValues,
        counts
      );
    }

    private static void accumulateCoolerWeightValue(final long valuePx,
                                                    final double value,
                                                    final long queryStartPx,
                                                    final double bucketSpan,
                                                    final double[] maxValues,
                                                    final long[] counts) {
      if (!Double.isFinite(value)) {
        return;
      }
      final var safeValue = Math.max(0.0d, value);
      int idx = (int) Math.floor((valuePx - queryStartPx) / bucketSpan);
      idx = Math.max(0, Math.min(idx, maxValues.length - 1));
      maxValues[idx] = Math.max(maxValues[idx], safeValue);
      counts[idx]++;
    }

    private static @NotNull List<TrackBin> finalizeCoolerWeightBins(final long queryStartPx,
                                                                     final long queryEndPx,
                                                                     final double bucketSpan,
                                                                     final @NotNull List<AssemblySegment> orderedSegments,
                                                                     final long bpResolution,
                                                                     final double[] maxValues,
                                                                     final long[] counts) {
      final var bins = new ArrayList<TrackBin>(counts.length);
      for (int i = 0; i < counts.length; i++) {
        if (counts[i] <= 0L) {
          continue;
        }
        final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
        final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
        final var safeEndPx = Math.max(startPx + 1L, endPx);
        final var startBp = mapVisiblePxToAssemblyBp(startPx, orderedSegments, bpResolution);
        final var endBp = mapVisiblePxToAssemblyBp(Math.max(startPx, safeEndPx - 1L), orderedSegments, bpResolution) + bpResolution;
        bins.add(new TrackBin(startBp, endBp, maxValues[i], counts[i], null, startPx, safeEndPx));
      }
      return bins;
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
    public @NotNull Set<String> sourceNames() {
      return this.sourceNames;
    }

    @Override
    public synchronized @NotNull List<ProjectedFeature> projectFeatures(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                                        final long queryStartPx,
                                                                        final long queryEndPx,
                                                                        final long bpResolution) {
      final var projected = new ArrayList<ProjectedFeature>();
      for (final var entry : sourceToAssemblySegments.entrySet()) {
        final var sourceName = entry.getKey();
        if (!this.sourceNames.contains(sourceName)) {
          continue;
        }
        for (final var segment : entry.getValue()) {
          final var sourceIntervalOptional = mapVisiblePxIntervalToSegmentSource(
            segment,
            queryStartPx,
            queryEndPx,
            bpResolution
          );
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
              queryStartPx,
              queryEndPx,
              bpResolution
            ).ifPresent(projected::add);
            if (projected.size() > MAX_FEATURES_PER_QUERY) {
              projected.sort(Comparator.comparingLong(ProjectedFeature::startPx));
              return projected;
            }
          }
        }
      }
      projected.sort(Comparator.comparingLong(ProjectedFeature::startPx));
      return projected;
    }

    public synchronized @NotNull List<TrackBin> queryBins(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                          final @NotNull List<AssemblySegment> orderedSegments,
                                                          final long queryStartPx,
                                                          final long queryEndPx,
                                                          final int widthPx,
                                                          final long bpResolution,
                                                          final @NotNull BigWigAggregationMode mode) {
      final var bucketCount = Math.max(1, widthPx);
      final var span = Math.max(1L, queryEndPx - queryStartPx);
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
          final var sourceIntervalOptional = mapVisiblePxIntervalToSegmentSource(
            segment,
            queryStartPx,
            queryEndPx,
            bpResolution
          );
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
              queryStartPx,
              queryEndPx,
              bpResolution
            );
            if (projectedFeature.isEmpty()) {
              continue;
            }
            final var feature = projectedFeature.get();
            accumulateBigWigValue(
              feature.startPx(),
              feature.endPx(),
              feature.value(),
              queryStartPx,
              queryEndPx,
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
        queryStartPx,
        queryEndPx,
        bucketSpan,
        maxValues,
        weightedSums,
        overlapSums,
        counts,
        mode,
        orderedSegments,
        bpResolution
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
    public @NotNull Set<String> sourceNames() {
      return this.sequenceNames;
    }

    @Override
    public synchronized @NotNull List<ProjectedFeature> projectFeatures(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                                        final long queryStartPx,
                                                                        final long queryEndPx,
                                                                        final long bpResolution) {
      final var projected = new ArrayList<ProjectedFeature>();
      for (final var entry : sourceToAssemblySegments.entrySet()) {
        final var sourceName = entry.getKey();
        if (!this.sequenceNames.contains(sourceName)) {
          continue;
        }
        for (final var segment : entry.getValue()) {
          final var sourceIntervalOptional = mapVisiblePxIntervalToSegmentSource(
            segment,
            queryStartPx,
            queryEndPx,
            bpResolution
          );
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
                queryStartPx,
                queryEndPx,
                bpResolution
              ).ifPresent(projected::add);
              if (projected.size() > MAX_FEATURES_PER_QUERY) {
                projected.sort(Comparator.comparingLong(ProjectedFeature::startPx));
                return projected;
              }
            }
          }
        }
      }
      projected.sort(Comparator.comparingLong(ProjectedFeature::startPx));
      return projected;
    }

    public synchronized @NotNull List<TrackBin> queryBins(final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                          final @NotNull List<AssemblySegment> orderedSegments,
                                                          final long queryStartPx,
                                                          final long queryEndPx,
                                                          final int widthPx,
                                                          final long bpResolution,
                                                          final @NotNull BamRenderMode mode) {
      final var bucketCount = Math.max(1, widthPx);
      final var span = Math.max(1L, queryEndPx - queryStartPx);
      final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
      final double[] values = new double[bucketCount];
      final long[] counts = new long[bucketCount];
      for (final var entry : sourceToAssemblySegments.entrySet()) {
        final var sourceName = entry.getKey();
        if (!this.sequenceNames.contains(sourceName)) {
          continue;
        }
        for (final var segment : entry.getValue()) {
          final var sourceIntervalOptional = mapVisiblePxIntervalToSegmentSource(
            segment,
            queryStartPx,
            queryEndPx,
            bpResolution
          );
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
                queryStartPx,
                queryEndPx,
                bpResolution
              );
              if (projectedFeature.isEmpty()) {
                continue;
              }
              final var feature = projectedFeature.get();
              if (mode == BamRenderMode.READ_DENSITY) {
                accumulateReadDensityValue(
                  feature.startPx(),
                  feature.endPx(),
                  queryStartPx,
                  bucketSpan,
                  values,
                  counts
                );
              } else {
                accumulateCoverageValue(
                  feature.startPx(),
                  feature.endPx(),
                  queryStartPx,
                  queryEndPx,
                  bucketSpan,
                  values,
                  counts
                );
              }
            }
          }
        }
      }
      return finalizeBins(
        queryStartPx,
        queryEndPx,
        bucketSpan,
        values,
        counts,
        orderedSegments,
        bpResolution
      );
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

  private record AssemblyBpInterval(long startBp, long endBp) {
  }

  private record QueryPxRange(long startPx, long endPx) {
  }

  private record SignalRange(double min, double max) {
  }

  private record PrecomputeTask(long bpResolution,
                                @NotNull String modeKey,
                                long totalVisiblePixels,
                                @NotNull String assemblySignature) {
  }

  private record PrecomputeCacheContext(@NotNull Path sidecarPath,
                                        @NotNull TrackType trackType,
                                        @NotNull String sourceIdentity,
                                        FileFingerprint sourceFingerprint,
                                        @NotNull FileFingerprint hictFingerprint) {
  }

  public record TrackPrecomputeCacheProbe(@NotNull String filename,
                                          @NotNull String trackType,
                                          boolean supported,
                                          boolean cacheAvailable,
                                          boolean cacheCurrent,
                                          @NotNull String cacheSidecarPath,
                                          @NotNull List<String> warnings,
                                          FileFingerprint sourceFingerprint,
                                          @NotNull FileFingerprint hictFingerprint) {
  }

  private record ComputedPrecomputeTask(@NotNull PrecomputeTask task,
                                        @NotNull PrecomputedSeries series) {
  }

  private record PrecomputedSeriesKey(@NotNull String trackId,
                                      long bpResolution,
                                      @NotNull String assemblySignature,
                                      @NotNull String modeKey) {
  }

  private record PrecomputedSeries(double @NotNull [] values,
                                   long @NotNull [] support) {
  }

  private enum PrecomputeAggregationStrategy {
    MAX,
    MEAN_ALL_PIXELS,
    MEAN_PRESENT_PIXELS,
    SUM
  }

  private record SegmentBuildResult(@NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                    @NotNull List<AssemblySegment> orderedSegments,
                                    long totalVisiblePixels) {
  }

  private record AssemblyBpSegment(long sourceStart,
                                   long sourceEnd,
                                   long assemblyStart,
                                   long assemblyEnd,
                                   boolean reversed) {
  }

  private record AssemblySegment(long sourceStart,
                                 long sourceEnd,
                                 long assemblyStart,
                                 long assemblyEnd,
                                 boolean reversed,
                                 long visiblePxStart,
                                 long visiblePxEnd) {
  }

  private static final class GffTranscriptFeatureBuilder {
    private final String groupKey;
    private final List<LongInterval> exonIntervals = new ArrayList<>();
    private final List<LongInterval> codingIntervals = new ArrayList<>();
    private long start = Long.MAX_VALUE;
    private long end = Long.MIN_VALUE;
    private String label;
    private String strand;
    private String featureType;
    private double maxValue = 1.0d;
    private boolean hasCustomValue = false;

    private GffTranscriptFeatureBuilder(final @NotNull String groupKey) {
      this.groupKey = groupKey;
    }

    private void accept(final long start,
                        final long end,
                        final double value,
                        final String fallbackLabel,
                        final String strand,
                        final String featureType,
                        final String featureTypeLower,
                        final @NotNull Map<String, String> attributes) {
      this.start = Math.min(this.start, start);
      this.end = Math.max(this.end, end);
      if (strand != null && this.strand == null) {
        this.strand = strand;
      }
      final var resolvedLabel = firstNonBlank(
        attributes.get("Name"),
        attributes.get("transcript_name"),
        attributes.get("gene_name"),
        attributes.get("gene_id"),
        attributes.get("transcript_id"),
        attributes.get("ID"),
        attributes.get("Parent"),
        fallbackLabel
      );
      if (shouldPreferLabel(this.label, resolvedLabel, this.featureType)) {
        this.label = resolvedLabel;
      }
      if (this.featureType == null || isGffTranscriptLikeFeature(featureTypeLower) || isGffGeneLikeFeature(featureTypeLower)) {
        this.featureType = featureType;
      }
      if (Math.abs(value - 1.0d) > 1e-9) {
        this.maxValue = Math.max(this.maxValue, value);
        this.hasCustomValue = true;
      }
      if (isGffBlockLikeFeature(featureTypeLower)) {
        final var interval = new LongInterval(start, end);
        this.exonIntervals.add(interval);
        if (isGffCodingFeature(featureTypeLower)) {
          this.codingIntervals.add(interval);
        }
      }
    }

    private @Nullable FeatureRange toFeatureRange() {
      final var mergedExons = mergeIntervals(this.exonIntervals);
      final var mergedCoding = mergeIntervals(this.codingIntervals);
      final var blockIntervals = mergedExons.isEmpty() ? mergedCoding : mergedExons;
      final var blocks = new ArrayList<FeatureBlock>(blockIntervals.size());
      for (final var interval : blockIntervals) {
        blocks.add(new FeatureBlock(
          interval.start(),
          interval.end(),
          overlapsAny(mergedCoding, interval)
        ));
      }
      long effectiveStart = this.start;
      long effectiveEnd = this.end;
      if (effectiveStart == Long.MAX_VALUE || effectiveEnd <= effectiveStart) {
        if (!blockIntervals.isEmpty()) {
          effectiveStart = blockIntervals.get(0).start();
          effectiveEnd = blockIntervals.get(blockIntervals.size() - 1).end();
        } else {
          return null;
        }
      }
      Long thickStart = null;
      Long thickEnd = null;
      if (!mergedCoding.isEmpty()) {
        thickStart = mergedCoding.get(0).start();
        thickEnd = mergedCoding.get(mergedCoding.size() - 1).end();
      }
      final var resolvedFeatureType = firstNonBlank(this.featureType, "transcript");
      final var resolvedLabel = firstNonBlank(this.label, this.groupKey, resolvedFeatureType);
      final var resolvedValue = this.hasCustomValue ? this.maxValue : 1.0d;
      return new FeatureRange(
        effectiveStart,
        Math.max(effectiveStart + 1L, effectiveEnd),
        resolvedValue,
        resolvedLabel,
        this.strand,
        thickStart,
        thickEnd,
        resolvedFeatureType,
        blocks,
        Map.of()
      );
    }

    private static @NotNull List<LongInterval> mergeIntervals(final @NotNull List<LongInterval> intervals) {
      if (intervals.isEmpty()) {
        return List.of();
      }
      final var sorted = new ArrayList<>(intervals);
      sorted.sort(Comparator.comparingLong(LongInterval::start).thenComparingLong(LongInterval::end));
      final var merged = new ArrayList<LongInterval>(sorted.size());
      long currentStart = sorted.get(0).start();
      long currentEnd = sorted.get(0).end();
      for (int i = 1; i < sorted.size(); i++) {
        final var interval = sorted.get(i);
        if (interval.start() <= currentEnd) {
          currentEnd = Math.max(currentEnd, interval.end());
          continue;
        }
        merged.add(new LongInterval(currentStart, currentEnd));
        currentStart = interval.start();
        currentEnd = interval.end();
      }
      merged.add(new LongInterval(currentStart, currentEnd));
      return merged;
    }

    private static boolean overlapsAny(final @NotNull List<LongInterval> codingIntervals,
                                       final @NotNull LongInterval target) {
      for (final var coding : codingIntervals) {
        if (coding.end() <= target.start()) {
          continue;
        }
        if (coding.start() >= target.end()) {
          break;
        }
        return true;
      }
      return false;
    }

    private static boolean shouldPreferLabel(final @Nullable String current,
                                             final @Nullable String candidate,
                                             final @Nullable String featureType) {
      if (candidate == null || candidate.isBlank()) {
        return false;
      }
      if (current == null || current.isBlank()) {
        return true;
      }
      final var currentLower = current.trim().toLowerCase(Locale.ROOT);
      final var candidateLower = candidate.trim().toLowerCase(Locale.ROOT);
      if (currentLower.equals(candidateLower)) {
        return false;
      }
      final var featureTypeLower = featureType == null ? "" : featureType.trim().toLowerCase(Locale.ROOT);
      final boolean currentGeneric =
        currentLower.equals(featureTypeLower) ||
          currentLower.startsWith("tx:") ||
          currentLower.startsWith("gene:");
      final boolean candidateGeneric =
        candidateLower.equals(featureTypeLower) ||
          candidateLower.startsWith("tx:") ||
          candidateLower.startsWith("gene:");
      return currentGeneric && !candidateGeneric;
    }
  }

  private record LongInterval(long start, long end) {
  }

  private record FeatureBlock(long start,
                              long end,
                              boolean coding) {
  }

  private record FeatureRange(long start,
                              long end,
                              double value,
                              String label,
                              String strand,
                              Long thickStart,
                              Long thickEnd,
                              String featureType,
                              @NotNull List<FeatureBlock> blocks,
                              @NotNull Map<String, String> attributes) {
    private FeatureRange {
      attributes = Map.copyOf(attributes);
    }
  }

  private record ProjectedBlock(long startBp,
                                long endBp,
                                long startPx,
                                long endPx,
                                boolean coding) {
  }

  private record ProjectedFeature(long startBp,
                                  long endBp,
                                  long startPx,
                                  long endPx,
                                  double value,
                                  String label,
                                  String strand,
                                  Long thickStartBp,
                                  Long thickEndBp,
                                  Long thickStartPx,
                                  Long thickEndPx,
                                  String featureType,
                                  @NotNull List<ProjectedBlock> blocks,
                                  @NotNull Map<String, String> attributes) {
    private ProjectedFeature {
      attributes = Map.copyOf(attributes);
    }
  }

  @Getter
  @RequiredArgsConstructor
  public static final class TrackPrecomputeStatus {
    private final @NotNull String trackId;
    private final @NotNull String trackName;
    private final @NotNull String status;
    private final int totalTasks;
    private final int completedTasks;
    private final double progress;
    private final @NotNull String currentTask;
    private final String error;
    private final long updatedAtMs;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class TracksPrecomputeStatus {
    private final @NotNull List<TrackPrecomputeStatus> tracks;
    private final int runningJobs;
    private final @NotNull String processedDirectory;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class TrackCompatibilityReport {
    private final @NotNull String filename;
    private final @NotNull String trackType;
    private final @NotNull String status;
    private final int totalNames;
    private final int matchedSourceNames;
    private final int matchedAssemblyNames;
    private final int matchedAnyNames;
    private final @NotNull List<String> unknownNames;
    private final @NotNull String recommendation;
    private final @NotNull String message;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class FeatureSearchResponse {
    private final @NotNull String query;
    private final int limit;
    private final int offset;
    private final boolean hasMore;
    private final @NotNull List<FeatureSearchHit> hits;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class FeatureSearchHit {
    private final @NotNull String trackId;
    private final @NotNull String trackName;
    private final @NotNull String sourceName;
    private final @NotNull String label;
    private final String featureType;
    private final String strand;
    private final long startBp;
    private final long endBp;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class FeatureContextResponse {
    private final long startBp;
    private final long endBp;
    private final long contextStartBp;
    private final long contextEndBp;
    private final double marginScreens;
    private final int contextWidthPx;
    private final long bpResolution;
    private final @NotNull QueryResult query;
  }

  private static final class TrackPrecomputeRuntime {
    private final String trackId;
    private volatile String trackName;
    private volatile String status;
    private volatile int totalTasks;
    private volatile int completedTasks;
    private volatile String currentTask;
    private volatile String error;
    private volatile long lastUpdatedMs;

    private TrackPrecomputeRuntime(final @NotNull String trackId, final @NotNull String trackName) {
      this.trackId = trackId;
      this.trackName = trackName;
      this.status = "idle";
      this.totalTasks = 0;
      this.completedTasks = 0;
      this.currentTask = "";
      this.error = null;
      this.lastUpdatedMs = System.currentTimeMillis();
    }

    private boolean isActive() {
      return "queued".equals(this.status) || "running".equals(this.status);
    }

    private void markQueued() {
      this.status = "queued";
      this.currentTask = "Waiting for precompute worker";
      this.error = null;
      this.completedTasks = 0;
      this.totalTasks = 0;
      this.lastUpdatedMs = System.currentTimeMillis();
    }

    private void setTotalTasks(final int totalTasks) {
      this.totalTasks = Math.max(0, totalTasks);
      if (this.totalTasks == 0 && !"finished".equals(this.status)) {
        this.currentTask = "No precomputable bins for this track style";
      }
      this.lastUpdatedMs = System.currentTimeMillis();
    }

    private void markRunning(final @NotNull String currentTask, final int completedTasks) {
      this.status = "running";
      this.currentTask = currentTask;
      this.completedTasks = Math.max(0, completedTasks);
      this.lastUpdatedMs = System.currentTimeMillis();
    }

    private void markTaskDone(final int completedTasks) {
      this.completedTasks = Math.max(0, completedTasks);
      this.lastUpdatedMs = System.currentTimeMillis();
    }

    private void markFinished() {
      this.status = "finished";
      this.currentTask = "";
      this.error = null;
      this.completedTasks = Math.max(this.completedTasks, this.totalTasks);
      this.lastUpdatedMs = System.currentTimeMillis();
    }

    private void markFailed(final @NotNull String error) {
      this.status = "failed";
      this.error = error;
      this.currentTask = "";
      this.lastUpdatedMs = System.currentTimeMillis();
    }

    private long lastUpdatedMs() {
      return this.lastUpdatedMs;
    }

    private String trackName() {
      return this.trackName;
    }

    private TrackPrecomputeStatus toStatus() {
      final var progress = switch (this.status) {
        case "finished" -> 1.0d;
        case "failed" -> this.totalTasks > 0 ? Math.min(1.0d, this.completedTasks / (double) this.totalTasks) : 0.0d;
        default -> this.totalTasks > 0 ? Math.min(1.0d, this.completedTasks / (double) this.totalTasks) : 0.0d;
      };
      return new TrackPrecomputeStatus(
        this.trackId,
        this.trackName,
        this.status,
        this.totalTasks,
        this.completedTasks,
        progress,
        this.currentTask == null ? "" : this.currentTask,
        this.error,
        this.lastUpdatedMs
      );
    }
  }

  @Getter
  @RequiredArgsConstructor
  public static final class QueryResult {
    private final long startBp;
    private final long endBp;
    private final long startPx;
    private final long endPx;
    private final int widthPx;
    private final long bpResolution;
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
    private final @NotNull String renderStyle;
    private final @NotNull String renderMode;
    private final @NotNull String aggregationMode;
    private final boolean logScale;
    private final boolean rangeAuto;
    private final double rangeMin;
    private final double rangeMax;
  }

  @Getter
  @RequiredArgsConstructor
  public static final class TrackRender {
    private final @NotNull String trackId;
    private final @NotNull String name;
    private final @NotNull String type;
    private final @NotNull String color;
    private final @NotNull String renderStyle;
    private final @NotNull List<TrackBin> bins;
    private final double maxValue;
    private final String error;
  }

  @Getter
  public static final class TrackBin {
    private final long startBp;
    private final long endBp;
    private final double value;
    private final long count;
    private final String label;
    private final Long startPx;
    private final Long endPx;
    private final String strand;
    private final Long thickStartBp;
    private final Long thickEndBp;
    private final Long thickStartPx;
    private final Long thickEndPx;
    private final String featureType;
    private final List<TrackBinBlock> blocks;
    private final Map<String, String> attributes;

    public TrackBin(final long startBp,
                    final long endBp,
                    final double value,
                    final long count,
                    final String label) {
      this(startBp, endBp, value, count, label, null, null, null, null, null, null, null, null, List.of(), Map.of());
    }

    public TrackBin(final long startBp,
                    final long endBp,
                    final double value,
                    final long count,
                    final String label,
                    final Long startPx,
                    final Long endPx) {
      this(startBp, endBp, value, count, label, startPx, endPx, null, null, null, null, null, null, List.of(), Map.of());
    }

    public TrackBin(final long startBp,
                    final long endBp,
                    final double value,
                    final long count,
                    final String label,
                    final Long startPx,
                    final Long endPx,
                    final String strand,
                    final Long thickStartBp,
                    final Long thickEndBp,
                    final Long thickStartPx,
                    final Long thickEndPx,
                    final String featureType) {
      this(
        startBp,
        endBp,
        value,
        count,
        label,
        startPx,
        endPx,
        strand,
        thickStartBp,
        thickEndBp,
        thickStartPx,
        thickEndPx,
        featureType,
        List.of(),
        Map.of()
      );
    }

    public TrackBin(final long startBp,
                    final long endBp,
                    final double value,
                    final long count,
                    final String label,
                    final Long startPx,
                    final Long endPx,
                    final String strand,
                    final Long thickStartBp,
                    final Long thickEndBp,
                    final Long thickStartPx,
                    final Long thickEndPx,
                    final String featureType,
                    final List<TrackBinBlock> blocks) {
      this(
        startBp,
        endBp,
        value,
        count,
        label,
        startPx,
        endPx,
        strand,
        thickStartBp,
        thickEndBp,
        thickStartPx,
        thickEndPx,
        featureType,
        blocks,
        Map.of()
      );
    }

    public TrackBin(final long startBp,
                    final long endBp,
                    final double value,
                    final long count,
                    final String label,
                    final Long startPx,
                    final Long endPx,
                    final String strand,
                    final Long thickStartBp,
                    final Long thickEndBp,
                    final Long thickStartPx,
                    final Long thickEndPx,
                    final String featureType,
                    final List<TrackBinBlock> blocks,
                    final Map<String, String> attributes) {
      this.startBp = startBp;
      this.endBp = endBp;
      this.value = value;
      this.count = count;
      this.label = label;
      this.startPx = startPx;
      this.endPx = endPx;
      this.strand = strand;
      this.thickStartBp = thickStartBp;
      this.thickEndBp = thickEndBp;
      this.thickStartPx = thickStartPx;
      this.thickEndPx = thickEndPx;
      this.featureType = featureType;
      this.blocks = blocks == null ? List.of() : blocks;
      this.attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    @Getter
    @RequiredArgsConstructor
    public static final class TrackBinBlock {
      private final long startBp;
      private final long endBp;
      private final long startPx;
      private final long endPx;
      private final boolean coding;
    }
  }

  private record TrackState(@NotNull String trackId,
                            @NotNull String name,
                            @NotNull TrackType type,
                            @NotNull String sourceFile,
                            @NotNull String color,
                            boolean visible,
                            @NotNull TrackDataSource dataSource,
                            @NotNull BamRenderMode bamRenderMode,
                            @NotNull BigWigAggregationMode bigWigAggregationMode,
                            boolean logScale,
                            boolean rangeAuto,
                            double rangeMin,
                            double rangeMax) {
    private TrackSummary toSummary() {
      final var safeRange = normalizeTrackRange(rangeMin, rangeMax);
      return new TrackSummary(
        trackId,
        name,
        type.name(),
        sourceFile,
        color,
        visible,
        dataSource.featureCountHint(),
        dataSource.renderStyle().name(),
        bamRenderMode.name(),
        bigWigAggregationMode.name(),
        logScale,
        rangeAuto,
        safeRange.min(),
        safeRange.max()
      );
    }

    private TrackRender toErrorRender(final @NotNull String message) {
      return new TrackRender(
        trackId,
        name,
        type.name(),
        color,
        dataSource.renderStyle().name(),
        List.of(),
        0.0d,
        message
      );
    }

    private TrackState withUpdated(final boolean newVisible,
                                   final @NotNull String newColor,
                                   final @NotNull String newName,
                                   final @NotNull BamRenderMode newBamRenderMode,
                                   final @NotNull BigWigAggregationMode newBigWigAggregationMode,
                                   final boolean newLogScale,
                                   final boolean newRangeAuto,
                                   final double newRangeMin,
                                   final double newRangeMax) {
      final var safeRange = normalizeTrackRange(newRangeMin, newRangeMax);
      return new TrackState(
        trackId,
        newName,
        type,
        sourceFile,
        newColor,
        newVisible,
        dataSource,
        newBamRenderMode,
        newBigWigAggregationMode,
        newLogScale,
        newRangeAuto,
        safeRange.min(),
        safeRange.max()
      );
    }

    private TrackRender query(final @NotNull ChunkedFile chunkedFile,
                              final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                              final @NotNull List<AssemblySegment> orderedSegments,
                              final long queryStartPx,
                              final long queryEndPx,
                              final int widthPx,
                              final long bpResolution) {
      final var bins = queryBinsForTrack(
        type,
        chunkedFile,
        dataSource,
        sourceToAssemblySegments,
        orderedSegments,
        queryStartPx,
        queryEndPx,
        widthPx,
        bpResolution,
        bamRenderMode,
        bigWigAggregationMode
      );
      final var maxValue = bins.stream().mapToDouble(TrackBin::getValue).max().orElse(0.0d);
      return new TrackRender(
        trackId,
        name,
        type.name(),
        color,
        dataSource.renderStyle().name(),
        bins,
        maxValue,
        null
      );
    }
  }
}
