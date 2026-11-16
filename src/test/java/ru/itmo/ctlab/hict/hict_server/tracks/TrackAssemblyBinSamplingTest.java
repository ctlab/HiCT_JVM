package ru.itmo.ctlab.hict.hict_server.tracks;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeBinWeightsDatasetPath;

class TrackAssemblyBinSamplingTest {
  @TempDir Path directory;

  @Test void hiddenSharedBinsKeepTheirWeightsAndFeaturesWithoutChangingVisibleTrackQueries() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory,"tracks",List.of(4L,10L));
    Files.writeString(directory.resolve("first-base.bed"),"tiny\t0\t1\tfirst\t7\n");
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.float64().writeMatrix(getStripeBinWeightsDatasetPath(4),new double[][]{{2,3,4,5,6}});
    }
    final var tracks = new Track1DManager(directory,directory.resolve("processed"));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path,1,2))) {
      final var weights = tracks.openCoolerWeightsTrack(null,null);
      final var bed = tracks.openTrack("first-base.bed","First",null);
      file.scaffoldingOperations().splitContigAtBp(0,1);
      final var baseline = tracks.sampleTrackValues(file,bed.getTrackId(),0,6,4,QueryLengthUnit.BINS);
      assertEquals(6,baseline.length); assertTrue(baseline[0] > 0);
      final int id = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.getContigTree().mapDescriptors(d -> d.getContigId() != id ? d : new ContigDescriptor(
        d.getContigId(),d.getContigName(),d.getLengthBp(),Arrays.stream(d.getLengthBinsAtResolution()).skip(1).boxed().toList(),
        List.of(ContigHideType.HIDDEN,ContigHideType.HIDDEN),d.getAtus().subList(1,d.getAtus().size()),
        d.getContigNameInSourceFASTA(),d.getOffsetInSourceFASTA(),d.isSharedBinSlice()));
      assertArrayEquals(baseline,tracks.sampleTrackValues(file,bed.getTrackId(),0,6,4,QueryLengthUnit.BINS),0);
      assertArrayEquals(new double[]{0,2,2,3,4,5,6,0},
        tracks.sampleTrackValues(file,weights.getTrackId(),-1,7,4,QueryLengthUnit.BINS),0);
      assertEquals(5,tracks.sampleTrackValues(file,bed.getTrackId(),0,5,4,QueryLengthUnit.PIXELS).length);
      assertArrayEquals(new double[5],tracks.sampleTrackValues(file,bed.getTrackId(),0,5,4,QueryLengthUnit.PIXELS),0);
      assertArrayEquals(new double[3],tracks.sampleTrackValues(file,bed.getTrackId(),-4,-1,4,QueryLengthUnit.BINS),0);
      assertThrows(IllegalArgumentException.class,()->tracks.sampleTrackValues(file,bed.getTrackId(),0,1_048_577,4,QueryLengthUnit.BINS));
    } finally {
      tracks.close();
    }
  }
}
