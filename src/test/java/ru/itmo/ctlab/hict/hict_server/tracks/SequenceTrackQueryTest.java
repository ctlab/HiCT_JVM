package ru.itmo.ctlab.hict.hict_server.tracks;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import htsjdk.samtools.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeBinWeightsDatasetPath;

class SequenceTrackQueryTest {
  @TempDir Path directory;

  private Path fixture() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory,"sequence-tracks",List.of(4L,10L));
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.float64().writeMatrix(getStripeBinWeightsDatasetPath(4),new double[][]{{2,3,Double.NaN,5,6}});
      writer.float64().writeMatrix(getStripeBinWeightsDatasetPath(10),new double[][]{{11,12}});
    }
    Files.writeString(directory.resolve("signal.bed"),"tiny\t0\t1\t7\ntiny\t1\t3\t2\ntiny\t3\t6\t9\ntiny\t6\t17\t4\n");
    var genes = new StringBuilder();
    for(int i=0;i<17;i++) genes.append("tiny\tfixture\tgene\t").append(i+1).append('\t').append(i+1)
      .append("\t.\t+\t.\tgene_id \"base").append(i).append("\";\n");
    Files.writeString(directory.resolve("genes.gtf"),genes);
    var header = new SAMFileHeader(); header.setSortOrder(SAMFileHeader.SortOrder.coordinate);
    header.addSequence(new SAMSequenceRecord("tiny",17));
    try(var writer = new SAMFileWriterFactory().setCreateIndex(true).makeBAMWriter(header,true,directory.resolve("reads.bam").toFile())) {
      var read = new SAMRecord(header); read.setReadName("known");read.setReferenceName("tiny");
      read.setAlignmentStart(3);read.setCigarString("10M");read.setReadString("AAAAAAAAAA");read.setBaseQualityString("IIIIIIIIII");read.setMappingQuality(60);
      writer.addAlignment(read);
    }
    return path;
  }

  private static double[] values(Track1DManager.TrackRender track,int length) {
    assertNull(track.getError()); assertNull(track.getStatusMessage());
    var result = new double[length]; Arrays.fill(result,Double.NaN);
    for(var bin : track.getBins()) {
      assertTrue(bin.getStartBp()>=0 && bin.getEndBp()<=length);
      assertEquals(bin.getStartBp(),bin.getStartPx());assertEquals(bin.getEndBp(),bin.getEndPx());
      for(long p=bin.getStartBp();p<bin.getEndBp();p++) result[(int)p]=bin.getValue();
    }
    return result;
  }

  @Test void everyBaseIncludingHiddenAndBinlessFragmentsKeepsSourceFeaturesSignalsReadsAndWeights() throws Exception {
    var path = fixture();var original = Files.readAllBytes(path);
    try(var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path,1,2));
        var tracks = new Track1DManager(directory,directory.resolve("processed"))) {
      tracks.openTrack("signal.bed","Signal",null);tracks.openTrack("genes.gtf","Genes",null);
      tracks.openTrack("reads.bam","Reads",null);tracks.openCoolerWeightsTrack(null,null);
      file.scaffoldingOperations().splitContigAtBp(0,3);
      int left = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.scaffoldingOperations().splitContigAtBp(left,1);
      int binless = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      int hidden = file.getAssemblyInfo().contigs().get(1).descriptor().getContigId();
      file.getContigTree().mapDescriptors(d -> d.getContigId()!=binless && d.getContigId()!=hidden ? d : new ContigDescriptor(
        d.getContigId(),d.getContigName(),d.getLengthBp(),d.getContigId()==binless?List.of(0L,0L):Arrays.stream(d.getLengthBinsAtResolution()).skip(1).boxed().toList(),
        List.of(ContigHideType.HIDDEN,ContigHideType.HIDDEN),d.getContigId()==binless?List.of(List.of(),List.of()):d.getAtus().subList(1,d.getAtus().size()),
        d.getContigNameInSourceFASTA(),d.getOffsetInSourceFASTA(),d.isSharedBinSlice()));
      for(boolean reverse : List.of(false,true)) {
        if(reverse) file.scaffoldingOperations().reverseSelectionRangeBp(0,17);
        for(long resolution : List.of(4L,10L)) {
          var query=tracks.querySequenceTracks(file,null,0,17,17,resolution);
          assertEquals("SEQUENCE_BP",query.getDisplayGrid());assertEquals(resolution,query.getSourceBpResolution());
          assertEquals(1,query.getBpResolution());assertEquals(17,query.getEndBp());
          var signal=values(query.getTracks().get(0),17);var genes=query.getTracks().get(1);
          var reads=values(query.getTracks().get(2),17);var weights=values(query.getTracks().get(3),17);
          assertEquals(resolution,query.getTracks().get(3).getSamplingBpResolution());
          assertEquals(17,genes.getBins().size());
          for(int p=0;p<17;p++) {
            int source=reverse?16-p:p;
            assertEquals(source==0?7:source<3?2:source<6?9:4,signal[p],0,"signal source="+source);
            double coverage=Double.isNaN(reads[p])?0:reads[p];
            assertEquals(source>=2 && source<12?1:0,coverage,0,"read source="+source);
            double expectedWeight=source==0 || resolution==4 && source>=8 && source<12?Double.NaN:resolution==4?2+source/4:11+source/10;
            assertEquals(expectedWeight,weights[p],0,"weight source="+source);
            var feature=genes.getBins().stream().filter(b->b.getLabel().equals("base"+source)).findFirst().orElseThrow();
            assertEquals(p,feature.getStartBp());assertEquals(p+1,feature.getEndBp());assertEquals(reverse?"-":"+",feature.getStrand());
          }
          var context=tracks.querySequenceFeatureContext(file,null,reverse?16:0,reverse?17:1,64,resolution,0);
          assertEquals("SEQUENCE_BP",context.getQuery().getDisplayGrid());
          assertEquals(17,context.getQuery().getTracks().get(1).getBins().size());
        }
        // Even a single coarse display bucket must not invent a value for the
        // one base without native bins. Every other base takes the finite max.
        var coarse=values(tracks.querySequenceTracks(file,null,0,17,1,4).getTracks().get(3),17);
        for(int p=0;p<17;p++) {
          int source=reverse?16-p:p;
          assertEquals(source==0 || source>=8 && source<12?Double.NaN:6,coarse[p],0);
        }
      }
      assertTrue(tracks.querySequenceTracks(file,null,17,20,8,4).getTracks().stream().allMatch(t->t.getBins().isEmpty()));
      assertThrows(IllegalArgumentException.class,()->tracks.querySequenceTracks(file,null,0,1_048_577,512,4));
      assertThrows(IllegalArgumentException.class,()->tracks.querySequenceTracks(file,null,0,17,8193,4));
      assertThrows(IllegalArgumentException.class,()->tracks.querySequenceTracks(file,null,0,17,512,3));
      Thread.currentThread().interrupt();
      try { assertThrows(CancellationException.class,()->tracks.querySequenceTracks(file,null,0,17,17,4)); }
      finally { Thread.interrupted(); }
    }
    assertArrayEquals(original,Files.readAllBytes(path));
  }

  @Test void secondaryWeightsUseTheirOwnStoredLevelAndStrandWhenPyramidsAreDisjoint() throws Exception {
    var path=fixture();var other=ReinforcementHdf5Fixture.create(directory,"secondary",List.of(3L,7L));
    try(var writer=HDF5Factory.open(other.toFile())) {
      writer.float64().writeMatrix(getStripeBinWeightsDatasetPath(3),new double[][]{{10,11,12,13,14,15}});
    }
    try(var primary=new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path,1,2));
        var secondary=new ChunkedFile(new ChunkedFile.ChunkedFileOptions(other,1,2));
        var tracks=new Track1DManager(directory,directory.resolve("processed"))) {
      tracks.openCoolerWeightsTrack("Other weights",null,"SECONDARY");
      primary.scaffoldingOperations().reverseSelectionRangeBp(0,17);
      secondary.scaffoldingOperations().reverseSelectionRangeBp(0,17);
      var query=tracks.querySequenceTracks(primary,secondary,0,17,17,4);
      assertEquals(3,query.getTracks().getFirst().getSamplingBpResolution());
      var values=values(query.getTracks().getFirst(),17);
      for(int p=0;p<17;p++) assertEquals(10+(16-p)/3,values[p],0);
    }
  }

  @Test void aOneBaseFeatureBeyondTwoPointFiveGbDoesNotNeedANativeOneBpLevel() throws Exception {
    long position=2_500_000_003L;
    var path=ReinforcementHdf5Fixture.create(directory,"long",List.of(1_000_000_000L),3_000_000_017L,new long[][]{{0,0,1}});
    Files.writeString(directory.resolve("long.bed"),"tiny\t"+position+"\t"+(position+1)+"\tlandmark\t1\t+\n");
    try(var file=new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path,1,2));
        var tracks=new Track1DManager(directory,directory.resolve("processed"))) {
      tracks.openTrack("long.bed","Long",null);
      var query=tracks.querySequenceTracks(file,null,position,position+1,1,1_000_000_000L);
      var bin=query.getTracks().getFirst().getBins().getFirst();
      assertEquals(position,bin.getStartBp());assertEquals(position+1,bin.getEndBp());
      assertEquals(position,bin.getStartPx());assertEquals(1,query.getBpResolution());
    }
  }
}
