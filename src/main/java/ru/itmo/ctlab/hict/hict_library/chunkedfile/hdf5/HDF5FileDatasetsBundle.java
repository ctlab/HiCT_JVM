package ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5;

import ch.systemsx.cisd.hdf5.HDF5DataSet;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import lombok.Getter;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

@Getter
public class HDF5FileDatasetsBundle implements AutoCloseable {
  private final @NotNull IHDF5Reader reader;
  private final @NotNull HDF5DataSet blockLengthDataSet;
  private final @NotNull HDF5DataSet blockOffsetDataSet;
  private final @NotNull HDF5DataSet blockRowsDataSet;
  private final @NotNull HDF5DataSet blockColsDataSet;
  private final @NotNull HDF5DataSet blockValuesDataSet;
  private final @NotNull HDF5DataSet denseBlockDataSet;

  private final @NotNull ChunkedFile chunkedFile;

  public HDF5FileDatasetsBundle(final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor, final @NotNull @NonNull ChunkedFile chunkedFile) {
    this.chunkedFile = chunkedFile;
    final var resolution = this.chunkedFile.getResolutions()[resolutionDescriptor.getResolutionOrderInArray()];
    this.reader = HDF5Factory.openForReading(this.chunkedFile.getHdfFilePath().toFile());
    this.blockLengthDataSet = reader.object().openDataSet(getBlockLengthDatasetPath(resolution));
    this.blockOffsetDataSet = reader.object().openDataSet(getBlockOffsetDatasetPath(resolution));
    this.blockRowsDataSet = reader.object().openDataSet(getBlockRowsDatasetPath(resolution));
    this.blockColsDataSet = reader.object().openDataSet(getBlockColsDatasetPath(resolution));
    this.blockValuesDataSet = reader.object().openDataSet(getBlockValuesDatasetPath(resolution));
    this.denseBlockDataSet = reader.object().openDataSet(getDenseBlockDatasetPath(resolution));
  }


  @Override
  public void close() {
    this.blockLengthDataSet.close();
    this.blockOffsetDataSet.close();
    this.blockRowsDataSet.close();
    this.blockColsDataSet.close();
    this.blockValuesDataSet.close();
    this.denseBlockDataSet.close();
    this.reader.close();
  }
}
