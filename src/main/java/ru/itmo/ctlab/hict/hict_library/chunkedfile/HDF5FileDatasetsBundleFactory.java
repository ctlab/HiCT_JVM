package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

@AllArgsConstructor
public class HDF5FileDatasetsBundleFactory extends BasePooledObjectFactory<HDF5FileDatasetsBundle> {
  private final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor;
  private final @NotNull @NonNull ChunkedFile chunkedFile;

  @Override
  public @NotNull HDF5FileDatasetsBundle create() {
    return new HDF5FileDatasetsBundle(this.resolutionDescriptor, this.chunkedFile);
  }

  @Override
  public PooledObject<HDF5FileDatasetsBundle> wrap(HDF5FileDatasetsBundle obj) {
    return new DefaultPooledObject<HDF5FileDatasetsBundle>(obj);
  }
}
