package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.*;

import java.nio.file.*;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

class FloatingPointCoolerExportTest {
  @TempDir Path directory;
  private static final double[] FLOATS = {.125,1.75,3.125,11.5};
  private static final double[] WEIGHTS = {.5,1,1.5,2,2.5};
  private static final long LARGE = 9_007_199_254_740_993L;

  @ParameterizedTest @ValueSource(strings={".cool",".mcool"})
  void ordinaryExportPreservesFloatAndInt64LevelsWithoutAnotherApproximation(String extension) throws Exception {
    final var input=mixedSource(true); final var bytes=Files.readAllBytes(input);
    final var output=directory.resolve("ordinary"+extension);
    new HictToMcoolConverter().convert(options(input,output,null),ignored->{});
    try(var reader=HDF5Factory.openForReading(output.toFile())) {
      final String root=extension.equals(".cool")?"":"/resolutions/4";
      assertEquals(HDF5DataClass.FLOAT,reader.object().getDataSetInformation(root+"/pixels/count").getTypeInformation().getDataClass());
      assertArrayEquals(FLOATS,reader.float64().readArray(root+"/pixels/count"),0);
      assertArrayEquals(WEIGHTS,reader.float64().readArray(root+"/bins/weight"),0);
      assertEquals(16.5,reader.float64().getAttr(root.isEmpty()?"/":root,"sum"),0);
      assertFalse(reader.object().hasAttribute("/",ApproximationProvenance.MARKER));
      final var source=SourceMetadata.read(reader,extension.equals(".cool")?List.of(4L):List.of(4L,10L));
      assertEquals(SourceMetadata.Units.CONTACT_COUNT,source.units());
      assertEquals(List.of(4L),source.importedWeightResolutions());
      if(extension.equals(".mcool")) {
        assertEquals(HDF5DataClass.INTEGER,reader.object().getDataSetInformation("/resolutions/10/pixels/count").getTypeInformation().getDataClass());
        assertArrayEquals(new long[]{LARGE,11},reader.int64().readArray("/resolutions/10/pixels/count"));
        assertEquals(LARGE+11,reader.int64().getAttr("/resolutions/10","sum"));
        assertFalse(reader.object().exists("/resolutions/10/bins/weight"));
      }
    }
    assertArrayEquals(bytes,Files.readAllBytes(input));assertNoTemporaryOutputs();
  }

  @ParameterizedTest @ValueSource(booleans={false,true})
  void reversalKeepsContactValuesAndGenuineWeightsInTheSameBinOrder(boolean useAgp) throws Exception {
    final var input=mixedSource(true);
    final double[][] original;
    try(var file=open(input)) {
      original=((MatrixQueries.DoubleMatrix)file.matrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(4,file),0,0,5,5,false).matrix()).values();
    }
    Path agp=null;
    if(useAgp) {agp=directory.resolve("reverse.agp");Files.writeString(agp,"reversed\t1\t17\t1\tW\ttiny\t1\t17\t-\n");}
    else try(var writer=HDF5Factory.open(input.toFile())) {writer.int64().writeArray(getContigDirectionDatasetPath(),new long[]{0});}
    final var output=directory.resolve("reverse.cool");
    new HictToMcoolConverter().convert(options(input,output,agp),ignored->{});
    try(var reader=HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(new double[]{2.5,2,1.5,1,.5},reader.float64().readArray("/bins/weight"),0);
      final var rows=reader.int64().readArray("/pixels/bin1_id"); final var cols=reader.int64().readArray("/pixels/bin2_id");
      final var values=reader.float64().readArray("/pixels/count"); final var actual=new double[5][5];
      for(int i=0;i<rows.length;i++)actual[(int)rows[i]][(int)cols[i]]=actual[(int)cols[i]][(int)rows[i]]=values[i];
      for(int r=0;r<5;r++)for(int c=0;c<5;c++)assertEquals(original[4-r][4-c],actual[r][c],0);
    }
  }

  @Test void autoModeWithoutBalancingExportsFloatValuesWithoutRequiringExternalTools() throws Exception {
    final var input=mixedSource(false);final var output=directory.resolve("auto.mcool");
    new HictToMcoolExportPipeline(new ExternalToolchainManager()).convert(options(input,output,null),ignored->{});
    try(var reader=HDF5Factory.openForReading(output.toFile())) {
      assertArrayEquals(FLOATS,reader.float64().readArray("/resolutions/4/pixels/count"),0);
      assertFalse(reader.object().exists("/resolutions/4/bins/weight"));
    }
  }

  @Test void cancellationAndInvalidValuesPreserveAnExistingDestinationAndSource() throws Exception {
    final var input=mixedSource(false);final var output=directory.resolve("existing.mcool");Files.writeString(output,"owner data");
    final var sourceBytes=Files.readAllBytes(input);final var cancel=new AtomicBoolean();
    assertThrows(CancellationException.class,()->new HictToMcoolConverter().convert(options(input,output,null),
      message->{if(message.contains("stage=export_float progress=0"))cancel.set(true);},cancel::get));
    assertTrue(cancel.get());assertEquals("owner data",Files.readString(output));assertArrayEquals(sourceBytes,Files.readAllBytes(input));
    try(var writer=HDF5Factory.open(input.toFile())) {
      final String path=getBlockValuesDatasetPath(4);final var values=writer.float64().readArray(path);values[0]=Double.NaN;writer.float64().writeArray(path,values);
    }
    final var badBytes=Files.readAllBytes(input);
    final var failure=assertThrows(IllegalArgumentException.class,()->new HictToMcoolConverter().convert(options(input,output,null),ignored->{}));
    assertTrue(failure.getMessage().contains("non-finite"));assertEquals("owner data",Files.readString(output));
    assertArrayEquals(badBytes,Files.readAllBytes(input));assertNoTemporaryOutputs();
  }

  @Test void cisTotalsExcludeTransContactsForBothNumericTypes() throws Exception {
    ReinforcementHdf5Fixture.create(directory,"init",List.of(4L));
    final var source=directory.resolve("two-chromosomes.mcool");
    try(var writer=HDF5Factory.open(source.toFile())) {
      writer.string().writeArray("/chroms/name",new String[]{"a","b"});writer.int64().writeArray("/chroms/length",new long[]{8,8});
      for(long r:List.of(4L,8L)) {
        String root="/resolutions/"+r;
        writer.int64().writeArray(root+"/bins/chrom",r==4?new long[]{0,0,1,1}:new long[]{0,1});
        writer.int64().writeArray(root+"/bins/start",r==4?new long[]{0,4,0,4}:new long[]{0,0});
        writer.int64().writeArray(root+"/bins/end",r==4?new long[]{4,8,4,8}:new long[]{8,8});
        writer.int64().writeArray(root+"/indexes/chrom_offset",r==4?new long[]{0,2,4}:new long[]{0,1,2});
        writer.int64().writeArray(root+"/indexes/bin1_offset",r==4?new long[]{0,2,2,3,3}:new long[]{0,2,3});
        writer.int64().writeArray(root+"/pixels/bin1_id",r==4?new long[]{0,0,2}:new long[]{0,0,1});
        writer.int64().writeArray(root+"/pixels/bin2_id",r==4?new long[]{0,2,3}:new long[]{0,1,1});
        if(r==4)writer.float64().writeArray(root+"/pixels/count",new double[]{.5,1.25,2.5});
        else writer.int64().writeArray(root+"/pixels/count",new long[]{3,7,11});
      }
    }
    final var input=directory.resolve("two-chromosomes.hict.hdf5");new McoolToHictConverter().convert(options(source,input,null),ignored->{});
    final var output=directory.resolve("two-export.mcool");new HictToMcoolConverter().convert(options(input,output,null),ignored->{});
    try(var reader=HDF5Factory.openForReading(output.toFile())) {
      assertEquals(4.25,reader.float64().getAttr("/resolutions/4","sum"),0);
      assertEquals(3,reader.float64().getAttr("/resolutions/4","cis"),0);
      assertEquals(21,reader.int64().getAttr("/resolutions/8","sum"));
      assertEquals(14,reader.int64().getAttr("/resolutions/8","cis"));
    }
  }

  @Test void integerCompactionRejectsOverflowInsteadOfWrappingNativeCounts() {
    assertThrows(ArithmeticException.class,()->HictToMcoolConverter.sortAndCompactCoolerRecordsRowMajor(
      new long[]{0,0},new long[]{0,0},new long[]{Long.MAX_VALUE,1}));
  }

  private Path mixedSource(boolean weighted) throws Exception {
    ReinforcementHdf5Fixture.create(directory,"fixture",List.of(4L,10L));
    final var cooler=directory.resolve("fixture.mcool");
    try(var writer=HDF5Factory.open(cooler.toFile())) {
      writer.object().delete("/resolutions/4/pixels/count");writer.float64().writeArray("/resolutions/4/pixels/count",FLOATS);
      writer.int64().writeArray("/resolutions/10/pixels/count",new long[]{LARGE,11});
      writer.int32().setAttr("/",SourceMetadata.VERSION,1);writer.string().setAttr("/",SourceMetadata.UNITS,"CONTACT_COUNT");
      writer.string().setAttr("/",SourceMetadata.CONTACT_STATE,"PRESENT");writer.float64().setAttr("/",SourceMetadata.SCALE,1);
      writer.string().setAttr("/resolutions/4",SourceMetadata.WEIGHTS,weighted?"PRESENT":"ABSENT");
      writer.string().setAttr("/resolutions/10",SourceMetadata.WEIGHTS,"ABSENT");
      if(weighted)writer.float64().writeArray("/resolutions/4/bins/weight",WEIGHTS);
    }
    final var nativeFile=directory.resolve("mixed.hict.hdf5");
    new McoolToHictConverter().convert(options(cooler,nativeFile,null),ignored->{});return nativeFile;
  }
  private ConversionOptions options(Path input,Path output,Path agp) {
    return new ConversionOptions(input,output,List.of(),2,1,ConversionOptions.CompressionAlgorithm.DEFLATE,
      agp==null?"":agp.toString(),agp!=null,1,true,false,false,false,ConversionOptions.ExportMode.AUTO);
  }
  private ChunkedFile open(Path path){return new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path,1,2));}
  private void assertNoTemporaryOutputs() throws Exception {try(var files=Files.list(directory)){assertFalse(files.anyMatch(p->p.getFileName().toString().startsWith(".hict-")));}}
}
