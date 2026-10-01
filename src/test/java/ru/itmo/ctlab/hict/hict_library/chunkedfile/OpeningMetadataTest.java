package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5FloatStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5ObjectReadOnlyInfoProviderHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

class OpeningMetadataTest {
  @TempDir Path temp;

  @BeforeAll static void loadHdf5() {
    HDF5LibraryInitializer.initializeHDF5Library();
  }

  @Test void weightReadPlanAlignsChunksAndBoundsBuffers() {
    assertEquals(42629, Initializers.stripeWeightBlockRows(42629, 256, 42629, 8L << 30));
    assertEquals(8074, Initializers.stripeWeightBlockRows(11743, 256, 367, 8L << 30));
    assertEquals(8192, Initializers.stripeWeightBlockRows(42629, 256, 42629, 256L << 20));
    assertEquals(65536, Initializers.stripeWeightBlockRows(200000, 256, 200000, 8L << 30));
    assertEquals(1, Initializers.stripeWeightBlockRows(10, Integer.MAX_VALUE, 10, 8L << 30));
    assertEquals(1, Initializers.stripeWeightBlockRows(0, 256, 1, 8L << 30));
    assertThrows(IllegalArgumentException.class, () -> Initializers.stripeWeightBlockRows(4, 0, 1, 100));
    assertThrows(IllegalArgumentException.class, () -> Initializers.stripeWeightBlockRows(4, 1, 0, 100));
  }

  @ParameterizedTest
  @CsvSource({"12000,256,12000,256", "1025,8,367,3", "1025,8,256,8"})
  void compressedWeightsRetainValuesAndPartialStripes(int rows, int columns, int chunkRows, int chunkColumns) {
    var path = temp.resolve("weights.h5");
    var values = new double[rows * columns];
    for (int i = 0; i < values.length; i++) values[i] = i / 3.0;
    values[1] = Double.NaN;
    values[2] = Double.POSITIVE_INFINITY;
    values[3] = Double.NEGATIVE_INFINITY;
    values[4] = -0.0;
    var lengths = new long[rows];
    Arrays.fill(lengths, columns);
    lengths[rows - 2] = 0;
    lengths[rows - 1] = 3;
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.int64().writeArray(getStripeLengthsBinsDatasetPath(1000), lengths);
      writer.float64().createMDArray(getStripeBinWeightsDatasetPath(1000), new long[]{rows, columns},
        new int[]{chunkRows, chunkColumns}, HDF5FloatStorageFeatures.FLOAT_DEFLATE);
      writer.float64().writeMDArrayBlockWithOffset(getStripeBinWeightsDatasetPath(1000),
        new MDDoubleArray(values, new int[]{rows, columns}), new long[]{0, 0});
    }
    var progress = new ArrayList<Double>();
    try (var reader = HDF5Factory.openForReading(path.toFile())) {
      var stripes = Initializers.withProgressReporter((stage, value) -> progress.add(value),
        () -> Initializers.readStripeDescriptors(1000, reader));
      assertEquals(rows, stripes.size());
      for (int i = 0; i < rows; i++) {
        assertEquals(i, stripes.get(i).stripeId());
        assertArrayEquals(Arrays.copyOfRange(values, i * columns, i * columns + (int) lengths[i]), stripes.get(i).bin_weights());
      }
      assertEquals(0.0, progress.getFirst());
      assertEquals(1.0, progress.getLast());
      int blockRows = Initializers.stripeWeightBlockRows(rows, columns, chunkRows, Runtime.getRuntime().maxMemory());
      assertEquals(2 + (rows + blockRows - 1) / blockRows, progress.size());
    }
  }

  @Test void contiguousWeightsAndEmptyDatasetRemainSupported() {
    var path = temp.resolve("empty.h5");
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.int64().writeArray(getStripeLengthsBinsDatasetPath(1), new long[0]);
      writer.float64().createMatrix(getStripeBinWeightsDatasetPath(1), 0L, 8L, 1, 8);
      writer.int64().writeArray(getStripeLengthsBinsDatasetPath(2), new long[]{2});
      writer.float64().writeMatrix(getStripeBinWeightsDatasetPath(2), new double[][]{{1.5, 2.5}});
    }
    try (var reader = HDF5Factory.openForReading(path.toFile())) {
      assertTrue(Initializers.readStripeDescriptors(1, reader).isEmpty());
      assertArrayEquals(new double[]{1.5, 2.5}, Initializers.readStripeDescriptors(2, reader).getFirst().bin_weights());
    }
  }

  @Test void resolutionValidationUsesDataspacesNotStorageStatistics() {
    var path = temp.resolve("metadata.h5");
    try (var writer = HDF5Factory.open(path.toFile())) {
      writeResolution(writer, 1000);
    }
    try (var reader = HDF5Factory.openForReading(path.toFile())) {
      var objects = (IHDF5ObjectReadOnlyInfoProviderHandler) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[]{IHDF5ObjectReadOnlyInfoProviderHandler.class}, (proxy, method, args) -> {
          assertEquals("getSpaceDimensions", method.getName(), "Validation must not scan chunk indexes");
          try { return method.invoke(reader.object(), args); }
          catch (InvocationTargetException e) { throw e.getCause(); }
        });
      var guarded = (IHDF5Reader) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{IHDF5Reader.class},
        (proxy, method, args) -> {
          assertEquals("object", method.getName());
          return objects;
        });
      assertTrue(ChunkedFile.isResolutionComplete(guarded, 1000));
      assertFalse(ChunkedFile.isResolutionComplete(guarded, 2000));
    }
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.object().delete(getBlockValuesDatasetPath(1000));
      writer.object().createGroup(getBlockValuesDatasetPath(1000));
    }
    try (var reader = HDF5Factory.openForReading(path.toFile())) {
      assertFalse(ChunkedFile.isResolutionComplete(reader, 1000), "A group cannot replace a required dataset");
    }
  }

  @Test void selectedResolutionDoesNotLoadOtherWeightsAndMissingSelectionFails() {
    var path = temp.resolve("selected.h5");
    try (var writer = HDF5Factory.open(path.toFile())) {
      writeResolution(writer, 1000);
      writeResolution(writer, 2000);
      writer.object().delete(getStripeBinWeightsDatasetPath(2000));
      writer.string().writeArray(getStripeBinWeightsDatasetPath(2000), new String[]{"invalid unused weights"});
      writer.int64().writeArray(getContigLengthBpDatasetPath(), new long[]{3000});
      writer.int64().writeArray(getContigDirectionDatasetPath(), new long[]{0});
      writer.int64().writeArray(getContigOrderDatasetPath(), new long[]{0});
      writer.string().writeArray(getContigNameDatasetPath(), new String[]{"chr1"});
    }
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2, List.of(1000L)))) {
      assertArrayEquals(new long[]{0, 1000}, file.getResolutions());
      assertEquals(3000, file.getMatrixSizeBins()[0]);
      assertEquals(3, file.getMatrixSizeBins()[1]);
      assertEquals("chr1", file.getAssemblyInfo().contigs().getFirst().descriptor().getContigName());
    }
    assertThrows(IllegalArgumentException.class,
      () -> new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2, List.of(1000L, 9999L))));
  }

  private static void writeResolution(IHDF5Writer writer, long resolution) {
    for (var path : List.of(getBlockLengthDatasetPath(resolution), getBlockOffsetDatasetPath(resolution),
      getBlockRowsDatasetPath(resolution), getBlockColsDatasetPath(resolution),
      getBlockValuesDatasetPath(resolution), getDenseBlockDatasetPath(resolution))) {
      writer.int64().writeArray(path, new long[]{0});
    }
    writer.int64().setAttr("/resolutions/" + resolution + "/treap_coo", "dense_submatrix_size", 256);
    writer.int64().writeArray(getStripeLengthsBinsDatasetPath(resolution), new long[]{3});
    writer.float64().writeMatrix(getStripeBinWeightsDatasetPath(resolution), new double[][]{{1, 1, 1}});
    writer.int64().writeMatrix(getBasisATUDatasetPath(resolution), new long[][]{{0, 0, 3, 0}});
    writer.int64().writeMatrix(getContigsATLDatasetPath(resolution), new long[][]{{0, 0}});
    writer.int64().writeArray(getContigLengthBinsDatasetPath(resolution), new long[]{3});
    writer.int8().writeArray(getContigHideTypeDatasetPath(resolution), new byte[]{1});
  }
}
