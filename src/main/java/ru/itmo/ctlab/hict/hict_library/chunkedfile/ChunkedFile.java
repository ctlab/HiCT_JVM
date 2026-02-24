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

package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5FileDatasetsBundle;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5FileDatasetsBundleFactory;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.AssemblyInfo;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;
import ru.itmo.ctlab.hict.hict_library.visualization.TileVisualizationProcessor;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.LongStream;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBasisATUDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockColsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockLengthDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockOffsetDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockRowsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getBlockValuesDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigHideTypeDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigLengthBinsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getContigsATLDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getDenseBlockDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeBinWeightsDatasetPath;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeLengthsBinsDatasetPath;

@Getter
@Slf4j
public class ChunkedFile implements AutoCloseable {

  private final @NotNull Path hdfFilePath;
  //  private final long[] blockCount;
  private final int denseBlockSize;
  private final long @NotNull [] resolutions;
  private final Map<@NotNull Long, @NotNull Integer> resolutionToIndex;
  private final long @NotNull [] matrixSizeBins;
  private final int @NotNull [] stripeCount;
  private final double @NotNull [] resolutionScalingCoefficient, resolutionLinearScalingCoefficient;
  private final @NotNull ContigTree contigTree;
  private final @NotNull ScaffoldTree scaffoldTree;
  private final @NotNull MatrixQueries matrixQueries;
  private final @NotNull ScaffoldingOperations scaffoldingOperations;
  private final @NotNull List<ObjectPool<HDF5FileDatasetsBundle>> datasetBundlePools;
  private final @NotNull ExecutorService queryExecutor;
  private final boolean blockCacheEnabled;
  private final @NotNull AtomicReferenceArray<long[]> blockLengthCache;
  private final @NotNull AtomicReferenceArray<long[]> blockOffsetCache;
  private final @NotNull Object[] blockCacheLocks;
  private final @NotNull AGPProcessor agpProcessor;
  private final @NotNull Map<String, ContigDescriptor> originalDescriptors;
  private final @NotNull Map<Integer, String> contigNameOverrides = new ConcurrentHashMap<>();
  private final @NotNull Map<Long, String> scaffoldNameOverrides = new ConcurrentHashMap<>();
  private final @NotNull Object nameOverrideLock = new Object();
  private final @NotNull TileVisualizationProcessor tileVisualizationProcessor;
  private final @NotNull FASTAProcessor fastaProcessor;
  @Getter
  private final AtomicInteger parallelThreadCount = new AtomicInteger(4);


  public ChunkedFile(final @NotNull ChunkedFileOptions options) {
    this.hdfFilePath = options.hdfFilePath;


    try (final var reader = HDF5Factory.openForReading(this.hdfFilePath.toFile())) {
      final var parsedResolutions = reader.object().getAllGroupMembers("/resolutions").parallelStream().flatMap(s -> {
        try {
          log.debug("Trying to parse " + s + " as a resolution");
          final var parsed = Long.parseLong(s);
          log.debug("Found new resolution: " + s);
          return java.util.stream.Stream.of(parsed);
        } catch (final NumberFormatException nfe) {
          log.debug("Not a resolution: " + s);
          return java.util.stream.Stream.empty();
        }
      }).sorted().toList();

      final var validResolutions = parsedResolutions.stream()
        .filter(resolution -> isResolutionComplete(reader, resolution))
        .sorted()
        .toList();

      if (validResolutions.isEmpty()) {
        throw new IllegalStateException("No complete resolutions found in " + this.hdfFilePath);
      }

      this.resolutions = LongStream.concat(LongStream.of(0L), validResolutions.stream().mapToLong(Long::longValue)).sorted().toArray();

      this.denseBlockSize = (int) validResolutions.stream()
        .mapToLong(res -> reader.int64().getAttr(String.format("/resolutions/%d/treap_coo", res), "dense_submatrix_size"))
        .max()
        .orElse(256L);
      log.info("Dense block size: " + this.denseBlockSize);

      log.debug("Resolutions count: " + resolutions.length);

      this.stripeCount = new int[resolutions.length];

      this.resolutionToIndex = new ConcurrentHashMap<>();
      for (int i = 0; i < this.resolutions.length; i++) {
        this.resolutionToIndex.put(this.resolutions[i], i);
      }
    }
    this.contigTree = new ContigTree();
    Initializers.initializeContigTree(this);
    final var originalDescriptors = new ConcurrentHashMap<String, ContigDescriptor>();
    this.contigTree.getContigDescriptors().values().forEach(contigDescriptor -> originalDescriptors.put(contigDescriptor.getContigName(), contigDescriptor));
    this.originalDescriptors = originalDescriptors;
    this.matrixSizeBins = new long[this.resolutions.length];
    this.matrixSizeBins[0] = this.contigTree.getLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
    for (int i = 1; i < this.resolutions.length; ++i) {
      this.matrixSizeBins[i] = this.contigTree.getLengthInUnits(QueryLengthUnit.BINS, ResolutionDescriptor.fromResolutionOrder(i));
      log.debug("Matrix size at resolution order=" + i + " is " + this.matrixSizeBins[i]);
    }
    this.scaffoldTree = new ScaffoldTree(this.matrixSizeBins[0]);
    Initializers.initializeScaffoldTree(this);

    this.matrixQueries = new MatrixQueries(this);
    this.scaffoldingOperations = new ScaffoldingOperations(this);
    {
      this.datasetBundlePools = new CopyOnWriteArrayList<org.apache.commons.pool2.ObjectPool<HDF5FileDatasetsBundle>>();
      this.datasetBundlePools.add(null);
      final var poolConfig = new GenericObjectPoolConfig<HDF5FileDatasetsBundle>();
      final int cpuCount = Math.max(2, Runtime.getRuntime().availableProcessors());
      final int maxPool = Math.max(options.maxDatasetPoolSize(), cpuCount);
      final int minPool = Math.max(1, Math.min(options.minDatasetPoolSize(), maxPool));
      poolConfig.setMaxTotal(maxPool);
      poolConfig.setMinIdle(minPool);
      poolConfig.setBlockWhenExhausted(true);
      for (int i = 1; i < this.resolutions.length; ++i) {
        this.datasetBundlePools.add(new GenericObjectPool<HDF5FileDatasetsBundle>(
          new HDF5FileDatasetsBundleFactory(ResolutionDescriptor.fromResolutionOrder(i), this),
          poolConfig
        ));
      }
      log.info("Using dataset pools with minimum of " + minPool + " readily available bundles and maximum of " + maxPool + " readily available bundles.");
    }
    final int queryThreads = options.queryThreads() > 0
      ? options.queryThreads()
      : Math.max(2, Runtime.getRuntime().availableProcessors());
    this.queryExecutor = Executors.newWorkStealingPool(queryThreads);
    this.blockCacheEnabled = options.blockCacheEnabled();
    this.blockLengthCache = new AtomicReferenceArray<>(this.resolutions.length);
    this.blockOffsetCache = new AtomicReferenceArray<>(this.resolutions.length);
    this.blockCacheLocks = new Object[this.resolutions.length];
    for (int i = 0; i < this.blockCacheLocks.length; i++) {
      this.blockCacheLocks[i] = new Object();
    }
    this.agpProcessor = new AGPProcessor(this);
    this.tileVisualizationProcessor = new TileVisualizationProcessor(this);
    this.fastaProcessor = new FASTAProcessor(this);
    this.loadNameOverrides();

    this.resolutionScalingCoefficient = new double[this.resolutions.length];
    this.resolutionLinearScalingCoefficient = new double[this.resolutions.length];
    this.resolutionScalingCoefficient[0] = 1.0d;
    this.resolutionScalingCoefficient[1] = 1.0d;
    this.resolutionLinearScalingCoefficient[0] = 1.0d;
    this.resolutionLinearScalingCoefficient[1] = 1.0d;
    for (int i = 2; i < this.resolutions.length; ++i) {
      final var ratio = (this.resolutions[i] / this.resolutions[1]);
      this.resolutionScalingCoefficient[i] = 1.0d / ((double) (ratio * ratio));
      this.resolutionLinearScalingCoefficient[i] = 1.0d / ((double) ratio);
    }
  }

  private void loadNameOverrides() {
    // Name overrides are session-only and should not be loaded from the HDF5 file.
  }

  private static <K> void readOverrideMap(final @NotNull String encoded, final @NotNull Map<K, String> target, final @NotNull java.util.function.Function<String, K> keyParser) {
    if (encoded.isBlank()) {
      return;
    }
    for (final var line : encoded.split("\n")) {
      if (line.isBlank()) {
        continue;
      }
      final var parts = line.split("\t", 2);
      if (parts.length != 2) {
        continue;
      }
      final var key = keyParser.apply(parts[0]);
      final var value = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
      if (!value.isBlank()) {
        target.put(key, value);
      }
    }
  }

  private static <K> @NotNull String writeOverrideMap(final @NotNull Map<K, String> source) {
    final var sb = new StringBuilder();
    for (final var entry : source.entrySet()) {
      final var name = entry.getValue();
      if (name == null || name.isBlank()) {
        continue;
      }
      if (!sb.isEmpty()) {
        sb.append('\n');
      }
      sb.append(entry.getKey()).append('\t').append(URLEncoder.encode(name, StandardCharsets.UTF_8));
    }
    return sb.toString();
  }

  private void persistNameOverrides() {
    // Name overrides are session-only and should not be persisted to the HDF5 file.
  }

  public @NotNull String getContigOriginalName(final int contigId) {
    final var descriptor = this.contigTree.getContigDescriptors().get(contigId);
    if (descriptor == null) {
      throw new IllegalArgumentException("Unknown contig id " + contigId);
    }
    return descriptor.getContigName();
  }

  public @NotNull String getContigDisplayName(final int contigId) {
    return Optional.ofNullable(contigNameOverrides.get(contigId)).orElse(getContigOriginalName(contigId));
  }

  public @NotNull String getScaffoldOriginalName(final long scaffoldId) {
    return this.scaffoldTree.getScaffoldList().stream()
      .filter(tuple -> tuple.scaffoldDescriptor().scaffoldId() == scaffoldId)
      .findFirst()
      .map(tuple -> tuple.scaffoldDescriptor().scaffoldName())
      .orElseThrow(() -> new IllegalArgumentException("Unknown scaffold id " + scaffoldId));
  }

  public @NotNull String getScaffoldDisplayName(final long scaffoldId) {
    return Optional.ofNullable(scaffoldNameOverrides.get(scaffoldId)).orElse(getScaffoldOriginalName(scaffoldId));
  }

  public void setContigNameOverride(final int contigId, final @NotNull String newName) {
    if (newName.isBlank() || newName.equals(getContigOriginalName(contigId))) {
      contigNameOverrides.remove(contigId);
    } else {
      contigNameOverrides.put(contigId, newName);
    }
    persistNameOverrides();
  }

  public void setScaffoldNameOverride(final long scaffoldId, final @NotNull String newName) {
    if (newName.isBlank() || newName.equals(getScaffoldOriginalName(scaffoldId))) {
      scaffoldNameOverrides.remove(scaffoldId);
    } else {
      scaffoldNameOverrides.put(scaffoldId, newName);
    }
    persistNameOverrides();
  }

  public @NotNull Map<Integer, String> getContigNameOverrides() {
    return contigNameOverrides;
  }

  public @NotNull Map<Long, String> getScaffoldNameOverrides() {
    return scaffoldNameOverrides;
  }

  public @NotNull java.util.Set<String> getAllContigDisplayNames() {
    return this.contigTree.getContigDescriptors().keySet().stream().map(this::getContigDisplayName).collect(java.util.stream.Collectors.toSet());
  }

  public @NotNull java.util.Set<String> getAllScaffoldDisplayNames() {
    return this.scaffoldTree.getScaffoldList().stream()
      .map(tuple -> Optional.ofNullable(scaffoldNameOverrides.get(tuple.scaffoldDescriptor().scaffoldId())).orElse(tuple.scaffoldDescriptor().scaffoldName()))
      .collect(java.util.stream.Collectors.toSet());
  }

  public @NotNull ContigDescriptor resolveContigDescriptorByName(final @NotNull String contigName) {
    final var original = originalDescriptors.get(contigName);
    if (original != null) {
      return original;
    }
    for (final var entry : contigNameOverrides.entrySet()) {
      if (contigName.equals(entry.getValue())) {
        final var descriptor = this.contigTree.getContigDescriptors().get(entry.getKey());
        if (descriptor != null) {
          return descriptor;
        }
      }
    }
    throw new IllegalArgumentException("Unknown contig name " + contigName);
  }

  private static boolean isResolutionComplete(final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader, final long resolution) {
    final String base = "/resolutions/" + resolution;
    try {
      if (!reader.object().isGroup(base + "/treap_coo")) {
        log.warn("Skipping resolution {}: missing treap_coo group", resolution);
        return false;
      }
      if (!reader.object().isDataSet(getBlockLengthDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getBlockOffsetDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getBlockRowsDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getBlockColsDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getBlockValuesDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getDenseBlockDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getStripeLengthsBinsDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getStripeBinWeightsDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getContigLengthBinsDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getContigHideTypeDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getContigsATLDatasetPath(resolution))) return false;
      if (!reader.object().isDataSet(getBasisATUDatasetPath(resolution))) return false;
      return true;
    } catch (Exception e) {
      log.warn("Skipping resolution {} due to validation error: {}", resolution, e.getMessage());
      return false;
    }
  }

  public @NotNull MatrixQueries matrixQueries() {
    return this.matrixQueries;
  }

  public @NotNull ScaffoldingOperations scaffoldingOperations() {
    return this.scaffoldingOperations;
  }

  public @NotNull TileVisualizationProcessor tileVisualizationProcessor() {
    return this.tileVisualizationProcessor;
  }

  public long @NotNull [] getResolutions() {
    return this.resolutions;
  }

  public @NotNull List<@NotNull Long> getResolutionsList() {
    return Arrays.stream(this.resolutions).boxed().toList();
  }

  public @NotNull AssemblyInfo getAssemblyInfo() {
    return new AssemblyInfo(this.contigTree.getOrderedContigList(), this.scaffoldTree.getScaffoldList());
  }

  @Override
  public void close() {
    this.queryExecutor.shutdown();
    for (int i = 1; i < resolutions.length; ++i) {
      this.datasetBundlePools.get(i).close();
    }
  }

  public @NotNull ExecutorService getQueryExecutor() {
    return this.queryExecutor;
  }

  public long getBlockLengthAt(final int resolutionOrder, final long blockIndexInDatasets) {
    final var pool = datasetBundlePools.get(resolutionOrder);
    if (pool == null) {
      throw new IllegalStateException("No dataset pool for resolution order " + resolutionOrder);
    }
    if (blockCacheEnabled) {
      final var cached = getBlockLengthCache(resolutionOrder);
      if (cached.length > 0 && blockIndexInDatasets < cached.length) {
        return cached[(int) blockIndexInDatasets];
      }
    }
    HDF5FileDatasetsBundle bundle = null;
    try {
      bundle = pool.borrowObject();
      final var reader = bundle.getReader();
      final long[] buf = reader.int64().readArrayBlockWithOffset(
        bundle.getBlockLengthDataSet(),
        1,
        blockIndexInDatasets
      );
      return buf[0];
    } catch (Exception e) {
      throw new RuntimeException("Failed to read block length", e);
    } finally {
      if (bundle != null) {
        try {
          pool.returnObject(bundle);
        } catch (Exception ignored) {
        }
      }
    }
  }

  public long getBlockOffsetAt(final int resolutionOrder, final long blockIndexInDatasets) {
    final var pool = datasetBundlePools.get(resolutionOrder);
    if (pool == null) {
      throw new IllegalStateException("No dataset pool for resolution order " + resolutionOrder);
    }
    if (blockCacheEnabled) {
      final var cached = getBlockOffsetCache(resolutionOrder);
      if (cached.length > 0 && blockIndexInDatasets < cached.length) {
        return cached[(int) blockIndexInDatasets];
      }
    }
    HDF5FileDatasetsBundle bundle = null;
    try {
      bundle = pool.borrowObject();
      final var reader = bundle.getReader();
      final long[] buf = reader.int64().readArrayBlockWithOffset(
        bundle.getBlockOffsetDataSet(),
        1,
        blockIndexInDatasets
      );
      return buf[0];
    } catch (Exception e) {
      throw new RuntimeException("Failed to read block offset", e);
    } finally {
      if (bundle != null) {
        try {
          pool.returnObject(bundle);
        } catch (Exception ignored) {
        }
      }
    }
  }

  private long @NotNull [] getBlockLengthCache(final int resolutionOrder) {
    final var cached = blockLengthCache.get(resolutionOrder);
    if (cached != null) {
      return cached;
    }
    synchronized (blockCacheLocks[resolutionOrder]) {
      final var again = blockLengthCache.get(resolutionOrder);
      if (again != null) {
        return again;
      }
      final var pool = datasetBundlePools.get(resolutionOrder);
      if (pool == null) {
        throw new IllegalStateException("No dataset pool for resolution order " + resolutionOrder);
      }
      HDF5FileDatasetsBundle bundle = null;
      try {
        bundle = pool.borrowObject();
        final var reader = bundle.getReader();
        final var data = reader.int64().readArray(getBlockLengthDatasetPath(resolutions[resolutionOrder]));
        blockLengthCache.set(resolutionOrder, data);
        return data;
      } catch (Exception e) {
        throw new RuntimeException("Failed to cache block lengths", e);
      } finally {
        if (bundle != null) {
          try {
            pool.returnObject(bundle);
          } catch (Exception ignored) {
          }
        }
      }
    }
  }

  private long @NotNull [] getBlockOffsetCache(final int resolutionOrder) {
    final var cached = blockOffsetCache.get(resolutionOrder);
    if (cached != null) {
      return cached;
    }
    synchronized (blockCacheLocks[resolutionOrder]) {
      final var again = blockOffsetCache.get(resolutionOrder);
      if (again != null) {
        return again;
      }
      final var pool = datasetBundlePools.get(resolutionOrder);
      if (pool == null) {
        throw new IllegalStateException("No dataset pool for resolution order " + resolutionOrder);
      }
      HDF5FileDatasetsBundle bundle = null;
      try {
        bundle = pool.borrowObject();
        final var reader = bundle.getReader();
        final var data = reader.int64().readArray(getBlockOffsetDatasetPath(resolutions[resolutionOrder]));
        blockOffsetCache.set(resolutionOrder, data);
        return data;
      } catch (Exception e) {
        throw new RuntimeException("Failed to cache block offsets", e);
      } finally {
        if (bundle != null) {
          try {
            pool.returnObject(bundle);
          } catch (Exception ignored) {
          }
        }
      }
    }
  }

  public long convertUnits(final long position, final @NotNull @NonNull ResolutionDescriptor fromResolution, final @NotNull @NonNull QueryLengthUnit fromUnits, final @NotNull @NonNull ResolutionDescriptor toResolution, final @NotNull @NonNull QueryLengthUnit toUnits) {
    assert ((QueryLengthUnit.BASE_PAIRS.equals(fromUnits)) == (fromResolution.getResolutionOrderInArray() == 0)) : "If converting from base pairs, set fromResolution=0";
    assert ((QueryLengthUnit.BASE_PAIRS.equals(toUnits)) == (toResolution.getResolutionOrderInArray() == 0)) : "If converting from base pairs, set toResolution=0";

    final var contigTree = this.contigTree;
    final var lock = contigTree.getRootLock();
    try {
      lock.readLock().lock();
      final var es = contigTree.expose(fromResolution, position, 1 + position, fromUnits);

      final var fromBpResolution = this.resolutions[fromResolution.getResolutionOrderInArray()];
      final var toBpResolution = this.resolutions[toResolution.getResolutionOrderInArray()];

      final long leftFromUnits = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(fromUnits, fromResolution);
      final var leftToUnits = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(toUnits, toResolution);

      final var deltaFromUnits = position - leftFromUnits;
      final var deltaBp = switch (fromUnits) {
        case BASE_PAIRS -> deltaFromUnits;
        case BINS, PIXELS -> (deltaFromUnits * fromBpResolution);
      };

      final var deltaToUnits = switch (toUnits) {
        case BASE_PAIRS -> deltaBp;
        case BINS, PIXELS -> (deltaBp / toBpResolution);
      };

      return leftToUnits + deltaToUnits;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void importAGP(final @NotNull Reader tsvReader) throws IOException, NoSuchFieldException {
    final var agpFileRecords = this.agpProcessor.parseRecords(tsvReader);
    final var contigTreeLock = this.contigTree.getRootLock().writeLock();
    final var scaffoldTreeLock = this.scaffoldTree.getRootLock().writeLock();
    try {
      contigTreeLock.lock();
      scaffoldTreeLock.lock();

      this.agpProcessor.initializeContigTreeFromAGP(agpFileRecords);
      this.agpProcessor.initializeScaffoldTreeFromAGP(agpFileRecords);
    } finally {
      contigTreeLock.unlock();
      scaffoldTreeLock.unlock();
    }
  }

  public record ChunkedFileOptions(@NotNull Path hdfFilePath, int minDatasetPoolSize, int maxDatasetPoolSize,
                                   boolean blockCacheEnabled, int queryThreads) {

  }
}
