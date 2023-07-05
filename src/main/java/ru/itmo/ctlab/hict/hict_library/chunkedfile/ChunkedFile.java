package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.AssemblyInfo;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.LongStream;

@Getter
@Slf4j
public class ChunkedFile {

  private final @NotNull Path hdfFilePath;
  //  private final long[] blockCount;
  private final int denseBlockSize;
  private final long @NotNull [] resolutions;
  private final Map<@NotNull Long, @NotNull Integer> resolutionToIndex;
  private final long @NotNull [] matrixSizeBins;
  private final int @NotNull [] stripeCount;
  private final @NotNull ContigTree contigTree;
  private final @NotNull ScaffoldTree scaffoldTree;
  private final @NotNull MatrixQueries matrixQueries;
  private final @NotNull ScaffoldingOperations scaffoldingOperations;
  private final @NotNull List<ObjectPool<HDF5FileDatasetsBundle>> datasetBundlePools;


  public ChunkedFile(final @NotNull Path hdfFilePath, final int denseBlockSize) {
    this.hdfFilePath = hdfFilePath;
    this.denseBlockSize = denseBlockSize;

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

      log.debug("Resolutions count: " + resolutions.length);

      this.stripeCount = new int[resolutions.length];

      this.resolutionToIndex = new ConcurrentHashMap<>();
      for (int i = 0; i < this.resolutions.length; i++) {
        this.resolutionToIndex.put(this.resolutions[i], i);
      }
    }
    this.contigTree = new ContigTree();
    Initializers.initializeContigTree(this);
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
    this.datasetBundlePools = new CopyOnWriteArrayList<org.apache.commons.pool2.ObjectPool<HDF5FileDatasetsBundle>>();
    this.datasetBundlePools.add(null);
    for (int i = 1; i < this.resolutions.length; ++i) {
      this.datasetBundlePools.add(new GenericObjectPool<HDF5FileDatasetsBundle>(new HDF5FileDatasetsBundleFactory(ResolutionDescriptor.fromResolutionOrder(i), this)));
    }
  }

  public @NotNull MatrixQueries matrixQueries() {
    return this.matrixQueries;
  }

  public @NotNull ScaffoldingOperations scaffoldingOperations() {
    return this.scaffoldingOperations;
  }

  public long @NotNull [] getResolutions() {
    return this.resolutions;
  }

  public @NotNull List<@NotNull Long> getResolutionsList() {
    return Arrays.stream(this.resolutions).boxed().toList();
  }

  public @NotNull AssemblyInfo getAssemblyInfo() {
    return new AssemblyInfo(this.contigTree.getContigList(), this.scaffoldTree.getScaffoldList());
  }

}
