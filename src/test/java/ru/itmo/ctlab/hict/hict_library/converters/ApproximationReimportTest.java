package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.*;

import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ApproximationReimportTest {
  @TempDir Path directory;

  @Test void reimportPreservesEstimationProvenanceFractionalCellsAndSessionCapabilities() throws Exception {
    final var source = ReinforcementHdf5Fixture.create(directory, "native", List.of(4L, 10L));
    final var layout = directory.resolve("cuts.agp");
    try (var file = open(source)) {
      assertNull(file.getApproximationProvenance());
      assertFalse(JsonObject.mapFrom(SourceCapabilitiesDTO.fromFile(file)).containsKey("approximation"));
      file.scaffoldingOperations().splitContigAtBp(0,5);
      file.scaffoldingOperations().splitContigAtBp(file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId(),3);
      Files.writeString(layout, String.join("",file.getAgpProcessor().getAGPStream().toList()));
    }
    final var estimated = directory.resolve("estimated.mcool");
    new HictToMcoolConverter().convert(options(source,estimated,layout,true), ignored -> {});
    final var imported = directory.resolve("reopened.hict.hdf5");
    new McoolToHictConverter().convert(options(estimated,imported,null,false), ignored -> {});
    try (var file = open(imported); var cooler = HDF5Factory.openForReading(estimated.toFile());
         var reader = HDF5Factory.openForReading(imported.toFile())) {
      final var provenance = file.getApproximationProvenance();
      assertNotNull(provenance);
      assertEquals(ApproximateContactProjection.METHOD, provenance.method());
      assertEquals("independent_stored_levels", provenance.resolutionPolicy());
      assertFalse(provenance.upstreamApproximate());
      assertEquals(SourceMetadata.Units.UNKNOWN,file.getSourceMetadata().units(),"Provenance does not invent source units");
      assertEquals(Files.readString(layout),reader.string().read(ApproximationProvenance.GROUP+"/agp"));
      assertEquals(JsonObject.mapFrom(provenance),SessionSnapshotDTO.source(file,"reopened","")
        .getJsonObject("capabilities").getJsonObject("approximation"));
      for (long resolution : List.of(4L,10L)) {
        final String root = "/resolutions/"+resolution;
        final var original = new JsonObject(cooler.string().getAttr(root,"metadata"));
        assertEquals(original,new JsonObject(reader.string().read(ApproximationProvenance.GROUP+"/levels/"+resolution)));
        final int bins = (int)cooler.int64().getAttr(root,"nbins");
        final var matrix = file.matrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(resolution,file),0,0,bins,bins,false).matrix();
        assertInstanceOf(MatrixQueries.DoubleMatrix.class,matrix);
        final var expected = new double[bins][bins];
        final var rows = cooler.int64().readArray(root+"/pixels/bin1_id");
        final var cols = cooler.int64().readArray(root+"/pixels/bin2_id");
        final var counts = cooler.float64().readArray(root+"/pixels/count");
        for(int i=0;i<rows.length;i++) expected[(int)rows[i]][(int)cols[i]]=expected[(int)cols[i]][(int)rows[i]]=counts[i];
        for(int r=0;r<bins;r++)for(int c=0;c<bins;c++) assertEquals(expected[r][c],matrix.getAsDouble(r,c),0);
      }
      final var secondLayout = directory.resolve("second.agp");
      Files.writeString(secondLayout,String.join("",file.getAgpProcessor().getAGPStream().toList()));
      final var second = directory.resolve("second.mcool");
      new HictToMcoolConverter().convert(options(imported,second,secondLayout,true), ignored -> {});
      try(var output=HDF5Factory.openForReading(second.toFile())) {
        assertTrue(ApproximationProvenance.read(output,List.of(4L,10L)).upstreamApproximate());
        for(long r:List.of(4L,10L)) assertTrue(new JsonObject(output.string().getAttr("/resolutions/"+r,"metadata")).getBoolean("upstream_approximate"));
      }
    }
  }

  @Test void explicitButIncompleteProvenanceIsNotSilentlyErasedOnCopy() {
    ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer.initializeHDF5Library();
    final var input=directory.resolve("incomplete.hdf5"); final var output=directory.resolve("copy.hdf5");
    try(var writer=HDF5Factory.open(input.toFile())) { writer.bool().setAttr("/",ApproximationProvenance.MARKER,true); }
    try(var reader=HDF5Factory.openForReading(input.toFile()); var writer=HDF5Factory.open(output.toFile())) {
      assertNotNull(ApproximationProvenance.read(reader,List.of(4L)));
      ApproximationProvenance.copy(reader,writer,List.of(4L));
    }
    try(var reader=HDF5Factory.openForReading(output.toFile())) {
      assertNotNull(ApproximationProvenance.read(reader,List.of(4L)));
      assertEquals("unspecified",ApproximationProvenance.read(reader,List.of(4L)).method());
    }
  }

  @Test void sequenceScoresDoNotInheritContactEstimationFromTheirTemplate() {
    ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer.initializeHDF5Library();
    final var input = directory.resolve("identity.hdf5");
    try(var writer=HDF5Factory.open(input.toFile())) {
      ApproximationProvenance.mark(writer,new ApproximationProvenance.Summary(1,"uniform-independent-endpoints-v1","independent_stored_levels","Estimated contact values",false));
      writer.object().createGroup("/sequence_analysis");
      writer.string().setAttr("/sequence_analysis","score_unit","identity_percent");
      writer.float64().setAttr("/sequence_analysis","score_scale",10000);
    }
    try(var reader=HDF5Factory.openForReading(input.toFile())) {
      assertNull(ApproximationProvenance.read(reader,List.of(4L)));
      assertEquals(SourceMetadata.Units.IDENTITY_PERCENT,SourceMetadata.read(reader,List.of(4L)).units());
    }
  }

  @Test void originalSingleCoolerProvenanceSurvivesPreparationThatDropsRootAttributes() {
    ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer.initializeHDF5Library();
    final var original=directory.resolve("original.cool"); final var prepared=directory.resolve("prepared.hdf5");
    final String metadata=new JsonObject().put("hict_approximate_export",true).put("method","uniform-independent-endpoints-v1")
      .put("resolution_policy","independent_stored_levels").encode();
    try(var writer=HDF5Factory.open(original.toFile())) {
      writer.bool().setAttr("/",ApproximationProvenance.MARKER,true);
      writer.float64().writeArray("/pixels/count",new double[]{.5});
      writer.string().setAttrVL("/","metadata",metadata);
    }
    try(var writer=HDF5Factory.open(prepared.toFile())) { writer.object().createGroup("/resolutions/4"); }
    ApproximationProvenance.copyOriginalInput(original,prepared,List.of(4L));
    try(var reader=HDF5Factory.openForReading(prepared.toFile())) {
      final var provenance=ApproximationProvenance.read(reader,List.of(4L));
      assertNotNull(provenance);
      assertEquals("uniform-independent-endpoints-v1",provenance.method());
      assertEquals("independent_stored_levels",provenance.resolutionPolicy());
      assertEquals(new JsonObject(metadata),new JsonObject(reader.string().read(ApproximationProvenance.GROUP+"/levels/4")));
    }
  }

  private ChunkedFile open(Path input) { return new ChunkedFile(new ChunkedFile.ChunkedFileOptions(input,1,2)); }
  private ConversionOptions options(Path input,Path output,Path agp,boolean estimate) {
    return new ConversionOptions(input,output,List.of(),8192,1,ConversionOptions.CompressionAlgorithm.DEFLATE,
      agp==null?"":agp.toString(),agp!=null,1,true,false,false,false,ConversionOptions.ExportMode.INTERNAL,estimate);
  }
}
