package ru.itmo.ctlab.hict.hict_library.visualization;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.*;

import java.awt.image.BufferedImage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeBinWeightsDatasetPath;

class SequenceGridProjectorTest {
  @TempDir Path directory;

  @Test void everyInteriorCutOnBothStrandsRetainsUniformBaseWidthsAndOriginalColors() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory,"all-cuts",List.of(4L,10L));
    final var bytes = Files.readAllBytes(path);
    for (boolean reversed : List.of(false,true)) for (int cut = 1; cut < 17; cut++) {
      try (var file = open(path)) {
        final var originals = List.of(original(file,4,5),original(file,10,2));
        if (reversed) file.scaffoldingOperations().reverseSelectionRangeBp(0,17);
        file.scaffoldingOperations().splitContigAtBp(0,cut);
        for (int level = 0; level < 2; level++) {
          final long resolution = level == 0 ? 4 : 10;
          final var projected = SequenceGridProjector.prepare(file,resolution(file,resolution),0,17,0,17,()->false)
            .render(SequenceGridProjectorTest::colors);
          assertEquals(17,projected.getWidth()); assertEquals(17,projected.getHeight());
          for (int row = 0; row < 17; row++) for (int col = 0; col < 17; col++) {
            int a = reversed ? 16-row : row, b = reversed ? 16-col : col;
            assertEquals(color(originals.get(level)[(int)(a/resolution)][(int)(b/resolution)]), projected.getRGB(col,row),
              "cut="+cut+", reversed="+reversed+", resolution="+resolution+", bp="+row+","+col);
          }
        }
      }
    }
    assertArrayEquals(bytes,Files.readAllBytes(path));
  }

  @Test void nestedCutsReorderingPaddingAndWeightsFollowOriginalSourceBins() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory,"nested",List.of(4L,10L));
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.float64().writeMatrix(getStripeBinWeightsDatasetPath(4),new double[][]{{2,3,4,5,6}});
    }
    try (var file = open(path)) {
      final var source = original(file,4,5);
      file.scaffoldingOperations().splitContigAtBp(0,5);
      file.scaffoldingOperations().splitContigAtBp(file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId(),1);
      file.scaffoldingOperations().reverseSelectionRangeBp(0,17);
      file.scaffoldingOperations().moveSelectionRangeBp(0,12,17);
      final var sourceBases = bases(file);
      // All references, including a one-base child, occupy exactly their sequence width.
      final var image = SequenceGridProjector.prepare(file,resolution(file,4),-2,20,1,18,()->false).render(matrix -> {
        final var result = colors(matrix);
        for (int r = 0; r < result.getHeight(); r++) for (int c = 0; c < result.getWidth(); c++)
          result.setRGB(c,r,color(matrix.matrix().getAsDouble(r,c) * matrix.rowWeights()[r] * matrix.colWeights()[c]));
        return result;
      });
      for (int row = 0; row < 22; row++) for (int col = 0; col < 17; col++) {
        int r = row-2, c = col+1;
        if (r < 0 || r >= 17 || c >= 17) assertEquals(0,image.getRGB(col,row));
        else {
          int a = sourceBases.get(r)/4, b = sourceBases.get(c)/4;
          assertEquals(color(source[a][b]*(a+2)*(b+2)),image.getRGB(col,row));
        }
      }
    }
  }

  @Test void hiddenFragmentsRemainVisibleAtSequenceScaleAndMissingBinsAreTransparent() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory,"hidden",List.of(4L,10L));
    try (var file = open(path)) {
      final var source = original(file,4,5);
      file.scaffoldingOperations().splitContigAtBp(0,1);
      final int id = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.getContigTree().mapDescriptors(d -> d.getContigId() != id ? d : descriptor(d,false));
      assertEquals(5,file.getContigTree().getLengthInUnits(QueryLengthUnit.PIXELS,resolution(file,4)));
      final var image = SequenceGridProjector.prepare(file,resolution(file,4),0,17,0,17,()->false).render(SequenceGridProjectorTest::colors);
      for (int r = 0; r < 17; r++) for (int c = 0; c < 17; c++)
        assertEquals(color(source[r/4][c/4]),image.getRGB(c,r));
      file.getContigTree().mapDescriptors(d -> d.getContigId() != id ? d : descriptor(d,true));
      final var missing = SequenceGridProjector.prepare(file,resolution(file,4),0,17,0,17,()->false).render(SequenceGridProjectorTest::colors);
      for (int r = 0; r < 17; r++) for (int c = 0; c < 17; c++)
        assertEquals(r == 0 || c == 0 ? 0 : color(source[r/4][c/4]),missing.getRGB(c,r));
    }
  }

  @Test void longAssemblyPositionsDoNotNarrowToIntAndRespectExactCrossingBins() throws Exception {
    final long length = 3_000_000_017L, start = 2_500_000_000L;
    final var path = ReinforcementHdf5Fixture.create(directory,"long",List.of(1_000_000_000L),length,
      new long[][]{{0,0,1},{start,length-1,7},{length-1,length-1,11}});
    try (var file = open(path)) {
      final var original = original(file,1_000_000_000L,4);
      file.scaffoldingOperations().splitContigAtBp(0,start+3);
      final var image = SequenceGridProjector.prepare(file,resolution(file,1_000_000_000L),start,start+8,length-8,length,()->false)
        .render(SequenceGridProjectorTest::colors);
      for (int r = 0; r < 8; r++) for (int c = 0; c < 8; c++)
        assertEquals(color(original[(int)((start+r)/1_000_000_000L)][(int)((length-8+c)/1_000_000_000L)]),image.getRGB(c,r));
    }
  }

  @Test void boundsCancellationAndEmptyWindowsDoNotPublishInventedColors() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory,"bounds",List.of(4L));
    try (var file = open(path)) {
      final var resolution = resolution(file,4);
      assertThrows(IllegalArgumentException.class,()->SequenceGridProjector.prepare(file,resolution,0,1025,0,1,()->false));
      assertThrows(IllegalArgumentException.class,()->SequenceGridProjector.prepare(file,resolution,2,2,0,1,()->false));
      assertThrows(CancellationException.class,()->SequenceGridProjector.prepare(file,resolution,0,1,0,1,()->true));
      final var empty = SequenceGridProjector.prepare(file,resolution,-10,-5,100,105,()->false).render(matrix -> {
        fail("Out-of-assembly windows must not read or render native cells");return null;
      });
      assertEquals(0,empty.getRGB(0,0));
      final var cancelled = new AtomicBoolean();
      final var projection = SequenceGridProjector.prepare(file,resolution,0,17,0,17,cancelled::get);
      assertThrows(CancellationException.class,()->projection.render(matrix -> { cancelled.set(true);return colors(matrix); }));
      assertThrows(IllegalArgumentException.class,()->SequenceGridProjector.prepare(file,resolution,0,17,0,17,()->false)
        .render(matrix -> new BufferedImage(1,1,BufferedImage.TYPE_INT_ARGB)));
    }
  }

  @Test void upperAndLowerSourceChoicesSplitInsideNativeDiagonalCells() throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory,"triangles",List.of(10L));
    try (var file = open(path)) {
      final var image = SequenceGridProjector.prepare(file,resolution(file,10),2,9,4,11,()->false)
        .renderTriangles((matrix, upper) -> {
          final var result = colors(matrix);
          final int[] rgba = new int[result.getWidth()*result.getHeight()];
          Arrays.fill(rgba,upper ? 0xffff0000 : 0xff0000ff);
          result.setRGB(0,0,result.getWidth(),result.getHeight(),rgba,0,result.getWidth());
          return result;
        });
      for (int row = 0; row < 7; row++) for (int col = 0; col < 7; col++)
        assertEquals(row+2 <= col+4 ? 0xffff0000 : 0xff0000ff,image.getRGB(col,row));
    }
  }

  private ContigDescriptor descriptor(ContigDescriptor d, boolean removeBins) {
    return new ContigDescriptor(d.getContigId(),d.getContigName(),d.getLengthBp(),
      Arrays.stream(d.getLengthBinsAtResolution()).skip(1).map(value -> removeBins ? 0 : value).boxed().toList(),
      List.of(ContigHideType.HIDDEN,ContigHideType.HIDDEN),removeBins ? List.of(List.of(),List.of()) : d.getAtus().subList(1,d.getAtus().size()),
      d.getContigNameInSourceFASTA(),d.getOffsetInSourceFASTA(),d.isSharedBinSlice());
  }
  private static BufferedImage colors(MatrixWithWeights matrix) {
    assertEquals(QueryLengthUnit.BINS,matrix.units());
    final var image = new BufferedImage(matrix.matrix().cols(),matrix.matrix().rows(),BufferedImage.TYPE_INT_ARGB);
    for (int row = 0; row < image.getHeight(); row++) for (int col = 0; col < image.getWidth(); col++)
      image.setRGB(col,row,color(matrix.matrix().getAsDouble(row,col)));
    return image;
  }
  private static int color(double value) { return 0xff000000 | (int)Math.round(value); }
  private ChunkedFile open(Path path) { return new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path,1,2)); }
  private ResolutionDescriptor resolution(ChunkedFile file,long bp) { return ResolutionDescriptor.fromBpResolution(bp,file); }
  private double[][] original(ChunkedFile file,long bp,int bins) {
    final var matrix = file.matrixQueries().getSubmatrix(resolution(file,bp),0,0,bins,bins,false).matrix();
    final var values = new double[bins][bins];
    for (int r = 0; r < bins; r++) for (int c = 0; c < bins; c++) values[r][c] = matrix.getAsDouble(r,c);
    return values;
  }
  private List<Integer> bases(ChunkedFile file) {
    final var result = new ArrayList<Integer>();
    for (var tuple : file.getAssemblyInfo().contigs()) {
      final var d = tuple.descriptor();
      for (int i = 0; i < d.getLengthBp(); i++) result.add(Math.toIntExact(d.getOffsetInSourceFASTA()
        + (tuple.direction() == ContigDirection.REVERSED ? d.getLengthBp()-1-i : i)));
    }
    return result;
  }
}
