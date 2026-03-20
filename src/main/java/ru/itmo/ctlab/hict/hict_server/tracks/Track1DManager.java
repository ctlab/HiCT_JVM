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

import ch.systemsx.cisd.hdf5.HDF5Factory;
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
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    ".bed", ".bed.gz", ".vcf", ".vcf.gz", ".bw", ".bigwig", ".bam"
  );
  private static final List<String> COLOR_PALETTE = List.of(
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ab"
  );
  private static final int MAX_FEATURES_PER_QUERY = 250_000;
  private static final String PRECOMPUTE_CACHE_VERSION = "1";
  private static final long MAX_PRECOMPUTE_VISIBLE_PIXELS = 2_000_000L;
  private static final int PRECOMPUTE_EXECUTOR_THREADS = 1;
  private static final long PRECOMPUTE_STATUS_TTL_MS = 15L * 60_000L;

  private final @NotNull Path dataDirectory;
  private final @NotNull Path processedDirectory;
  private final @NotNull ReadWriteLock lock = new ReentrantReadWriteLock();
  private final @NotNull LinkedHashMap<String, TrackState> tracks = new LinkedHashMap<>();
  private final @NotNull AtomicLong trackCounter = new AtomicLong(0L);
  private final @NotNull ExecutorService precomputeExecutor;
  private final @NotNull ConcurrentHashMap<PrecomputedSeriesKey, PrecomputedSeries> precomputedSeriesCache = new ConcurrentHashMap<>();
  private final @NotNull ConcurrentHashMap<String, TrackPrecomputeRuntime> precomputeRuntimeByTrackId = new ConcurrentHashMap<>();
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
    this.precomputeExecutor = Executors.newFixedThreadPool(
      PRECOMPUTE_EXECUTOR_THREADS,
      r -> {
        final var t = new Thread(r);
        t.setDaemon(true);
        t.setName("hict-track-precompute");
        return t;
      }
    );
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
      this.precomputeRuntimeByTrackId.remove(trackId);
      this.precomputedSeriesCache.keySet().removeIf(key -> key.trackId().equals(trackId));
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
    this.precomputeExecutor.shutdownNow();
  }

  public void setLinkedFastaAliasesBySource(final @Nullable Map<String, String> aliases) {
    this.linkedFastaAliasesBySource = aliases == null ? Map.of() : Map.copyOf(aliases);
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
      scheduleTrackPrecompute(chunkedFile, state, force);
    }
    return getPrecomputeStatus();
  }

  public @NotNull QueryResult queryVisibleTracks(final @NotNull ChunkedFile chunkedFile,
                                                 final long start,
                                                 final long end,
                                                 final int widthPx,
                                                 final long bpResolution,
                                                 final @NotNull QueryLengthUnit units) {
    final var safeWidth = Math.max(1, widthPx);
    final var segmentsBuildResult =
      buildSourceToAssemblySegments(chunkedFile, this.linkedFastaAliasesBySource, bpResolution);
    final var totalVisiblePixels = segmentsBuildResult.totalVisiblePixels();
    if (totalVisiblePixels <= 0L) {
      return new QueryResult(0L, 1L, 0L, 1L, safeWidth, bpResolution, List.of());
    }
    final var queryPxRange = resolveQueryPxRange(
      chunkedFile,
      start,
      end,
      bpResolution,
      units,
      segmentsBuildResult.orderedSegments(),
      totalVisiblePixels
    );
    return queryVisibleTracksInternal(
      chunkedFile,
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
    final var safeWidth = Math.max(1, widthPx);
    final var segmentsBuildResult =
      buildSourceToAssemblySegments(chunkedFile, this.linkedFastaAliasesBySource, bpResolution);
    final var totalVisiblePixels = segmentsBuildResult.totalVisiblePixels();
    if (totalVisiblePixels <= 0L) {
      return new QueryResult(0L, 1L, 0L, 1L, safeWidth, bpResolution, List.of());
    }
    final var queryStartPx = Math.max(0L, Math.min(Math.min(startPx, endPx), totalVisiblePixels - 1L));
    final var queryEndPx = Math.max(queryStartPx + 1L, Math.min(Math.max(startPx, endPx), totalVisiblePixels));
    return queryVisibleTracksInternal(chunkedFile, segmentsBuildResult, queryStartPx, queryEndPx, safeWidth, bpResolution);
  }

  private @NotNull QueryResult queryVisibleTracksInternal(final @NotNull ChunkedFile chunkedFile,
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
            maybeScheduleTrackPrecomputeFromQuery(chunkedFile, track);
            final var maybePrecomputed = getPrecomputedBinsIfReady(
              chunkedFile,
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
            } else {
              trackRenders.add(track.query(sourceToAssemblySegments, queryStartPx, queryEndPx, safeWidth, bpResolution));
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

  private void maybeScheduleTrackPrecomputeFromQuery(final @NotNull ChunkedFile chunkedFile,
                                                     final @NotNull TrackState track) {
    final var runtime = this.precomputeRuntimeByTrackId.get(track.trackId());
    if (runtime != null && runtime.isActive()) {
      return;
    }
    scheduleTrackPrecompute(chunkedFile, track, false);
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
    this.precomputeExecutor.submit(() -> runTrackPrecompute(chunkedFile, track, force, runtime));
  }

  private void runTrackPrecompute(final @NotNull ChunkedFile chunkedFile,
                                  final @NotNull TrackState track,
                                  final boolean force,
                                  final @NotNull TrackPrecomputeRuntime runtime) {
    try {
      final var tasks = buildPrecomputeTasks(chunkedFile, track, force);
      runtime.setTotalTasks(tasks.size());
      if (tasks.isEmpty()) {
        runtime.markFinished();
        return;
      }
      int completed = 0;
      for (final var task : tasks) {
        runtime.markRunning(task.bpResolution() + "bp/" + task.modeKey(), completed);
        final var key = new PrecomputedSeriesKey(track.trackId(), task.bpResolution(), task.assemblySignature(), task.modeKey());
        PrecomputedSeries series = this.precomputedSeriesCache.get(key);
        if (series == null || force) {
          series = loadPrecomputedSeriesFromSidecar(task.sidecarFile(), task.totalVisiblePixels()).orElse(null);
        }
        if (series == null || force) {
          series = computePrecomputedSeries(chunkedFile, track, task);
          persistPrecomputedSeries(task.sidecarFile(), series);
        }
        this.precomputedSeriesCache.put(key, series);
        completed++;
        runtime.markTaskDone(completed);
      }
      runtime.markFinished();
    } catch (final Exception ex) {
      runtime.markFailed(ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
      log.error("Failed to precompute track {}", track.trackId(), ex);
    }
  }

  private @NotNull List<PrecomputeTask> buildPrecomputeTasks(final @NotNull ChunkedFile chunkedFile,
                                                             final @NotNull TrackState track,
                                                             final boolean force) {
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
        final var sidecarPath = sidecarPathForVector(chunkedFile, track, bpResolution, assemblySignature, modeKey);
        if (!force && Files.exists(sidecarPath)) {
          continue;
        }
        tasks.add(new PrecomputeTask(bpResolution, modeKey, totalVisiblePixels, assemblySignature, sidecarPath));
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
    final var totalVisiblePixels = orderedSegments.isEmpty() ? 0L : orderedSegments.get(orderedSegments.size() - 1).visiblePxEnd();
    if (totalVisiblePixels <= 0L || totalVisiblePixels > MAX_PRECOMPUTE_VISIBLE_PIXELS) {
      return null;
    }
    final var modeKey = activeModeKey(track);
    final var key = new PrecomputedSeriesKey(track.trackId(), bpResolution, assemblySignature, modeKey);
    var series = this.precomputedSeriesCache.get(key);
    if (series == null) {
      final var sidecar = sidecarPathForVector(chunkedFile, track, bpResolution, assemblySignature, modeKey);
      series = loadPrecomputedSeriesFromSidecar(sidecar, totalVisiblePixels).orElse(null);
      if (series != null) {
        this.precomputedSeriesCache.put(key, series);
      }
    }
    if (series == null) {
      return null;
    }
    final var strategy = aggregationStrategy(track);
    final var bins = aggregatePrecomputedSeries(series, queryStartPx, queryEndPx, widthPx, strategy);
    final var maxValue = bins.stream().mapToDouble(TrackBin::getValue).max().orElse(0.0d);
    return new TrackRender(track.trackId(), track.name(), track.type().name(), track.color(), bins, maxValue, null);
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
      track.dataSource(),
      segmentsBuildResult.sourceToAssemblySegments(),
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
                                                             final @NotNull PrecomputeAggregationStrategy strategy) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
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
      bins.add(new TrackBin(startPx, safeEndPx, value, Math.max(1L, supportSum), null, startPx, safeEndPx));
    }
    return bins;
  }

  private @NotNull Optional<PrecomputedSeries> loadPrecomputedSeriesFromSidecar(final @NotNull Path sidecarPath,
                                                                                 final long expectedLength) {
    if (!Files.exists(sidecarPath) || !Files.isRegularFile(sidecarPath)) {
      return Optional.empty();
    }
    try (final var reader = HDF5Factory.openForReading(sidecarPath.toFile())) {
      if (!reader.object().isDataSet("/cache/values") || !reader.object().isDataSet("/cache/support")) {
        return Optional.empty();
      }
      final var valuesDims = reader.object().getDataSetInformation("/cache/values").getDimensions();
      if (valuesDims.length != 1 || valuesDims[0] != expectedLength) {
        return Optional.empty();
      }
      final var values = reader.float64().readArray("/cache/values");
      final var support = reader.int64().readArray("/cache/support");
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
                                        final @NotNull PrecomputedSeries series) {
    try {
      Files.createDirectories(sidecarPath.getParent());
      final var tmpPath = sidecarPath.resolveSibling(sidecarPath.getFileName() + ".tmp");
      try (final var writer = HDF5Factory.open(tmpPath.toFile())) {
        if (!writer.object().isGroup("/cache")) {
          writer.object().createGroup("/cache");
        }
        writer.string().setAttr("/cache", "version", PRECOMPUTE_CACHE_VERSION);
        writer.int64().setAttr("/cache", "length", series.values().length);
        writer.float64().writeArray("/cache/values", series.values());
        writer.int64().writeArray("/cache/support", series.support());
      }
      Files.move(tmpPath, sidecarPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (final Exception e) {
      log.warn("Failed to write precomputed sidecar {}", sidecarPath, e);
    }
  }

  private @NotNull Path sidecarPathForVector(final @NotNull ChunkedFile chunkedFile,
                                             final @NotNull TrackState track,
                                             final long bpResolution,
                                             final @NotNull String assemblySignature,
                                             final @NotNull String modeKey) {
    final var trackSource = resolveDataPath(track.sourceFile());
    final var hictPath = chunkedFile.getHdfFilePath();
    final String fingerprint;
    try {
      fingerprint = String.join("|",
        PRECOMPUTE_CACHE_VERSION,
        trackSource.toString(),
        String.valueOf(Files.size(trackSource)),
        String.valueOf(Files.getLastModifiedTime(trackSource).toMillis()),
        hictPath.toString(),
        String.valueOf(Files.size(hictPath)),
        String.valueOf(Files.getLastModifiedTime(hictPath).toMillis()),
        String.valueOf(bpResolution),
        assemblySignature,
        modeKey
      );
    } catch (final IOException e) {
      throw new RuntimeException("Cannot build precompute fingerprint for " + track.sourceFile(), e);
    }
    final var fileName = sha256Hex(fingerprint) + ".h5";
    return this.processedDirectory.resolve("track_precompute").resolve(fileName);
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
      case BED, VCF -> List.of("DEFAULT");
      case UNSUPPORTED -> List.of("DEFAULT");
    };
  }

  private static @NotNull String activeModeKey(final @NotNull TrackState track) {
    return switch (track.type()) {
      case BIGWIG -> track.bigWigAggregationMode().name();
      case BAM -> track.bamRenderMode().name();
      case BED, VCF, UNSUPPORTED -> "DEFAULT";
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
      case BED, VCF, UNSUPPORTED -> PrecomputeAggregationStrategy.MAX;
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
      label
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

  private static @NotNull List<TrackBin> aggregateFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                           final long queryStartPx,
                                                           final long queryEndPx,
                                                           final int widthPx) {
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
      bins.add(new TrackBin(startPx, safeEndPx, maxValues[i], counts[i], null, startPx, safeEndPx));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateBigWigFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                 final long queryStartPx,
                                                                 final long queryEndPx,
                                                                 final int widthPx,
                                                                 final @NotNull BigWigAggregationMode mode) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
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
      bins.add(new TrackBin(startPx, safeEndPx, value, counts[i], null, startPx, safeEndPx));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateCoverageFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                   final long queryStartPx,
                                                                   final long queryEndPx,
                                                                   final int widthPx) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
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
      bins.add(new TrackBin(startPx, safeEndPx, coverage[i], counts[i], null, startPx, safeEndPx));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> aggregateReadDensityFeatures(final @NotNull List<ProjectedFeature> projectedFeatures,
                                                                      final long queryStartPx,
                                                                      final long queryEndPx,
                                                                      final int widthPx) {
    final var bucketCount = Math.max(1, widthPx);
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
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
      bins.add(new TrackBin(startPx, safeEndPx, values[i], counts[i], null, startPx, safeEndPx));
    }
    return bins;
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
                                                            final @NotNull BigWigAggregationMode mode) {
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
      bins.add(new TrackBin(startPx, safeEndPx, value, counts[i], null, startPx, safeEndPx));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> finalizeBins(final long queryStartPx,
                                                      final long queryEndPx,
                                                      final double bucketSpan,
                                                      final double[] values,
                                                      final long[] counts) {
    final var bins = new ArrayList<TrackBin>(counts.length);
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] <= 0L) {
        continue;
      }
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      bins.add(new TrackBin(startPx, safeEndPx, values[i], counts[i], null, startPx, safeEndPx));
    }
    return bins;
  }

  private static @NotNull List<TrackBin> toBins(final @NotNull List<ProjectedFeature> projectedFeatures) {
    return projectedFeatures.stream()
      .map(f -> new TrackBin(f.startBp(), f.endBp(), f.value(), 1L, f.label(), f.startPx(), f.endPx()))
      .toList();
  }

  private static @NotNull List<TrackBin> queryBinsForTrack(final @NotNull TrackType type,
                                                           final @NotNull TrackDataSource dataSource,
                                                           final @NotNull Map<String, List<AssemblySegment>> sourceToAssemblySegments,
                                                           final long queryStartPx,
                                                           final long queryEndPx,
                                                           final int widthPx,
                                                           final long bpResolution,
                                                           final @NotNull BamRenderMode bamRenderMode,
                                                           final @NotNull BigWigAggregationMode bigWigAggregationMode) {
    if (type == TrackType.BAM && dataSource instanceof BamTrackDataSource bamDataSource) {
      return bamDataSource.queryBins(
        sourceToAssemblySegments,
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
        queryStartPx,
        queryEndPx,
        widthPx,
        bpResolution,
        bigWigAggregationMode
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
      return aggregateFeatures(projectedFeatures, queryStartPx, queryEndPx, widthPx);
    }
    return toBins(projectedFeatures);
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
                                           long queryStartPx,
                                           long queryEndPx,
                                           long bpResolution);

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
                                                           final long queryStartPx,
                                                           final long queryEndPx,
                                                           final long bpResolution) {
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
            projectSourceIntervalOnSegment(
              segment,
              feature.start(),
              feature.end(),
              feature.value(),
              feature.label(),
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
      return finalizeBins(queryStartPx, queryEndPx, bucketSpan, values, counts);
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

  private record QueryPxRange(long startPx, long endPx) {
  }

  private record PrecomputeTask(long bpResolution,
                                @NotNull String modeKey,
                                long totalVisiblePixels,
                                @NotNull String assemblySignature,
                                @NotNull Path sidecarFile) {
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

  private record AssemblySegment(long sourceStart,
                                 long sourceEnd,
                                 long assemblyStart,
                                 long assemblyEnd,
                                 boolean reversed,
                                 long visiblePxStart,
                                 long visiblePxEnd) {
  }

  private record FeatureRange(long start, long end, double value, String label) {
  }

  private record ProjectedFeature(long startBp,
                                  long endBp,
                                  long startPx,
                                  long endPx,
                                  double value,
                                  String label) {
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
      this.currentTask = "";
      this.error = null;
      this.completedTasks = 0;
      this.totalTasks = 0;
      this.lastUpdatedMs = System.currentTimeMillis();
    }

    private void setTotalTasks(final int totalTasks) {
      this.totalTasks = Math.max(0, totalTasks);
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
      final var progress = this.totalTasks > 0 ? Math.min(1.0d, this.completedTasks / (double) this.totalTasks) : 0.0d;
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
  public static final class TrackBin {
    private final long startBp;
    private final long endBp;
    private final double value;
    private final long count;
    private final String label;
    private final Long startPx;
    private final Long endPx;

    public TrackBin(final long startBp,
                    final long endBp,
                    final double value,
                    final long count,
                    final String label) {
      this(startBp, endBp, value, count, label, null, null);
    }

    public TrackBin(final long startBp,
                    final long endBp,
                    final double value,
                    final long count,
                    final String label,
                    final Long startPx,
                    final Long endPx) {
      this.startBp = startBp;
      this.endBp = endBp;
      this.value = value;
      this.count = count;
      this.label = label;
      this.startPx = startPx;
      this.endPx = endPx;
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
                              final long queryStartPx,
                              final long queryEndPx,
                              final int widthPx,
                              final long bpResolution) {
      final var bins = queryBinsForTrack(
        type,
        dataSource,
        sourceToAssemblySegments,
        queryStartPx,
        queryEndPx,
        widthPx,
        bpResolution,
        bamRenderMode,
        bigWigAggregationMode
      );
      final var maxValue = bins.stream().mapToDouble(TrackBin::getValue).max().orElse(0.0d);
      return new TrackRender(trackId, name, type.name(), color, bins, maxValue, null);
    }
  }
}
