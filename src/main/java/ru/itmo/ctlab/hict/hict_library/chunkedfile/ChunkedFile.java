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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

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
  private final @NotNull AGPProcessor agpProcessor;
  private final @NotNull Map<String, ContigDescriptor> originalDescriptors;
  private final @NotNull TileVisualizationProcessor tileVisualizationProcessor;
  private final @NotNull FASTAProcessor fastaProcessor;
  @Getter
  private final AtomicInteger parallelThreadCount = new AtomicInteger(4);


  public ChunkedFile(final @NotNull ChunkedFileOptions options) {
    this.hdfFilePath = options.hdfFilePath;


    try (final var reader = HDF5Factory.openForReading(this.hdfFilePath.toFile())) {
      this.resolutions = LongStream.concat(LongStream.of(0L), reader.object().getAllGroupMembers("/resolutions").parallelStream().filter(s -> {
        try {
          log.debug("Trying to parse " + s + " as a resolution");
          Long.parseLong(s);
          log.debug("Found new resolution: " + s);
          return true;
        } catch (final NumberFormatException nfe) {
          log.debug("Not a resolution: " + s);
          return false;
        }
      }).mapToLong(Long::parseLong)).sorted().toArray();


      this.denseBlockSize = (int) Arrays.stream(resolutions).sorted().skip(1L).map(res -> reader.int64().getAttr(String.format("/resolutions/%d/treap_coo", res), "dense_submatrix_size")).max().orElse(256L);
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
      poolConfig.setMaxTotal(options.maxDatasetPoolSize());
      poolConfig.setMinIdle(options.minDatasetPoolSize());
      poolConfig.setBlockWhenExhausted(true);
      for (int i = 1; i < this.resolutions.length; ++i) {
        this.datasetBundlePools.add(new GenericObjectPool<HDF5FileDatasetsBundle>(
          new HDF5FileDatasetsBundleFactory(ResolutionDescriptor.fromResolutionOrder(i), this),
          poolConfig
        ));
      }
      log.info("Using dataset pools with minimum of " + options.minDatasetPoolSize() + " readily available bundles and maximum of " + options.maxDatasetPoolSize() + " readily available bundles.");
    }
    this.agpProcessor = new AGPProcessor(this);
    this.tileVisualizationProcessor = new TileVisualizationProcessor(this);
    this.fastaProcessor = new FASTAProcessor(this);

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
    for (int i = 1; i < resolutions.length; ++i) {
      this.datasetBundlePools.get(i).close();
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
      assert (this.validateTreeBordersCorrectness()) : "Contig and scaffold borders are not aligned";
    } finally {
      contigTreeLock.unlock();
      scaffoldTreeLock.unlock();
    }
  }

  public boolean validateTreeBordersCorrectness() {
    final var assemblyInfo = this.getAssemblyInfo();
    long leftContigBp = 0;
    long leftScaffoldBp = 0;
    int scaffoldIndex = 0;
    int contigIndex = 0;

//    throw new UnsupportedOperationException("Not yet implemented");

    while (contigIndex < assemblyInfo.contigs().size() && scaffoldIndex < assemblyInfo.scaffolds().size()) {
      while (scaffoldIndex < assemblyInfo.scaffolds().size()) {
        final var sct = assemblyInfo.scaffolds().get(scaffoldIndex);
        assert (sct.scaffoldBordersBP().endBP() - sct.scaffoldBordersBP().startBP() == sct.getLengthBp()) : "Wrong scaffold length in BP??";
        if (sct.scaffoldBordersBP().endBP() <= leftContigBp) {
          leftScaffoldBp += sct.getLengthBp();
          ++scaffoldIndex;
        } else {
          assert (sct.scaffoldBordersBP().startBP() == leftScaffoldBp) : "Start of scaffold not as expected";
          assert (leftScaffoldBp == leftContigBp) : "Scaffold should start with a contig";
          break;
        }
      }

      if (scaffoldIndex >= assemblyInfo.scaffolds().size()) {
        break;
      }
      final var currentScaffold = assemblyInfo.scaffolds().get(scaffoldIndex);
      assert (currentScaffold.scaffoldBordersBP().startBP() == leftContigBp) : "Scaffolds left border is not aligned to contigs?";

      while (contigIndex < assemblyInfo.contigs().size()) {
        final var ctgt = assemblyInfo.contigs().get(contigIndex);
        final var newLeftContigBp = leftContigBp + ctgt.descriptor().getLengthBp();
        if (newLeftContigBp >= currentScaffold.scaffoldBordersBP().endBP()) {
          assert (newLeftContigBp == currentScaffold.scaffoldBordersBP().endBP()) : "Contig has a start inside scaffold and ends outside of scaffold";
          leftContigBp = newLeftContigBp;
          ++contigIndex;
          break;
        }
        leftContigBp = newLeftContigBp;
        ++contigIndex;
      }
    }

    return true;
  }

  public record ChunkedFileOptions(@NotNull Path hdfFilePath, int minDatasetPoolSize, int maxDatasetPoolSize) {

  }
}
