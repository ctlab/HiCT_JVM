import ch.systemsx.cisd.hdf5.HDF5Factory;
import java.nio.file.*;
import java.util.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.converters.*;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.*;

/** Three source sequences: binless (1 bp), hidden (2 bp), visible (4 kb). */
class SequenceTrackBrowserFixture {
  public static void main(String[] args) throws Exception {
    var directory=Path.of(args[0]).toRealPath();
    if(!directory.startsWith("/tmp")) throw new IllegalArgumentException("Use disposable /tmp data");
    var cooler=directory.resolve("tracks.mcool");var nativeFile=directory.resolve("tracks.hict.hdf5");
    if(Files.exists(cooler)||Files.exists(nativeFile)) throw new IllegalArgumentException("Use fresh fixture files");
    HDF5LibraryInitializer.initializeHDF5Library();
    try(var w=HDF5Factory.open(cooler.toFile())) {
      w.string().writeArray("/chroms/name",new String[]{"binless","hidden","visible"});
      w.int64().writeArray("/chroms/length",new long[]{1,2,4000});
      for(long r:List.of(100L,200L)) {
        String root="/resolutions/"+r;int count=(int)(4000/r)+2;
        long[] chrom=new long[count],start=new long[count],end=new long[count],offset=new long[count+1];
        start[0]=0;end[0]=1;chrom[1]=1;end[1]=2;
        for(int i=2;i<count;i++){chrom[i]=2;start[i]=(i-2)*r;end[i]=(i-1)*r;}
        long[] rows={1,2,count-1},cols={1,2,count-1},counts={5,11,17};
        int cursor=0;for(int row=0;row<count;row++){offset[row]=cursor;while(cursor<rows.length&&rows[cursor]==row)cursor++;}offset[count]=cursor;
        w.int64().writeArray(root+"/bins/chrom",chrom);w.int64().writeArray(root+"/bins/start",start);w.int64().writeArray(root+"/bins/end",end);
        w.int64().writeArray(root+"/indexes/chrom_offset",new long[]{0,1,2,count});w.int64().writeArray(root+"/indexes/bin1_offset",offset);
        w.int64().writeArray(root+"/pixels/bin1_id",rows);w.int64().writeArray(root+"/pixels/bin2_id",cols);w.int64().writeArray(root+"/pixels/count",counts);
      }
    }
    new McoolToHictConverter().convert(new ConversionOptions(cooler,nativeFile,List.of(100L,200L),64,1,
      ConversionOptions.CompressionAlgorithm.DEFLATE,"",false,1,true,false,false,false,ConversionOptions.ExportMode.INTERNAL),ignored->{});
    try(var w=HDF5Factory.open(nativeFile.toFile())) {
      for(long r:List.of(100L,200L)) {
        var mapping=w.int64().readMatrix(getContigsATLDatasetPath(r));var basis=w.int64().readMatrix(getBasisATUDatasetPath(r));
        var weights=w.float64().readMatrix(getStripeBinWeightsDatasetPath(r));
        for(var row:weights)Arrays.fill(row,Double.NaN);
        for(int c=0;c<3;c++) {
          int contig=c;
          var links=Arrays.stream(mapping).filter(row->row[0]==contig).toList();
          if(links.size()!=1)throw new IllegalStateException("Fixture requires one ATU per source sequence");
          var atu=basis[(int)links.getFirst()[1]];
          for(int bin=(int)atu[1];bin<atu[2];bin++)weights[(int)atu[0]][bin]=c==0?10:c==1?20:100+bin-atu[1];
        }
        w.float64().writeMatrix(getStripeBinWeightsDatasetPath(r),weights);
        w.string().setAttr("/resolutions/"+r,"hict_imported_weights","PRESENT");
        var lengths=w.int64().readArray(getContigLengthBinsDatasetPath(r));lengths[0]=0;
        w.int64().writeArray(getContigLengthBinsDatasetPath(r),lengths);
        w.int8().writeArray(getContigHideTypeDatasetPath(r),new byte[]{0,0,1});
        w.int64().writeMatrix(getContigsATLDatasetPath(r),Arrays.stream(mapping).filter(row->row[0]!=0).toArray(long[][]::new));
      }
    }
    Files.writeString(directory.resolve("tracks.fa"),">binless\nA\n>hidden\nCG\n>visible\n"+"ACGTRYMKSWBDHVNAC".repeat(235)+"ACGTR\n",StandardOpenOption.CREATE_NEW);
    Files.writeString(directory.resolve("signal.bed"),"binless\t0\t1\t7\nhidden\t0\t2\t9\nvisible\t0\t4000\t4\n",StandardOpenOption.CREATE_NEW);
    Files.writeString(directory.resolve("genes.gtf"),"binless\tfixture\tgene\t1\t1\t.\t+\t.\tgene_id \"binless\";\n"
      +"hidden\tfixture\tgene\t1\t1\t.\t+\t.\tgene_id \"hidden\";\n"
      +"visible\tfixture\tgene\t4\t6\t.\t+\t.\tgene_id \"visible\";\n",StandardOpenOption.CREATE_NEW);
  }
}
