/*
 * MIT License
 *
 * Copyright (c) 2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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
