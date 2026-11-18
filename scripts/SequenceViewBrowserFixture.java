import ch.systemsx.cisd.hdf5.HDF5Factory;
import java.nio.file.*;
import java.util.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.converters.*;

/** Deterministic 4 kb input by default; optional bounded larger input for export qualification. */
class SequenceViewBrowserFixture {
  public static void main(String[] args) throws Exception {
    final var directory = Path.of(args[0]).toRealPath();
    if (!directory.startsWith("/tmp")) throw new IllegalArgumentException("Use disposable /tmp data");
    final long length=args.length>1?Long.parseLong(args[1]):4000;
    final long baseResolution=length>4000?1_000_000L:100L;
    if(length<4000 || length%(2*baseResolution)!=0 || length>1_000_000_000L)
      throw new IllegalArgumentException("Fixture length must be a multiple of two bins, from 4 kb to 1 Gb");
    final var cooler = directory.resolve("sequence.mcool");
    final var nativeFile = directory.resolve("sequence.hict.hdf5");
    if (Files.exists(cooler) || Files.exists(nativeFile)) throw new IllegalArgumentException("Use fresh fixture data");
    HDF5LibraryInitializer.initializeHDF5Library();
    try (var writer = HDF5Factory.open(cooler.toFile())) {
      writer.string().writeArray("/chroms/name",new String[]{"tiny"});
      writer.int64().writeArray("/chroms/length",new long[]{length});
      for (long resolution : List.of(baseResolution,2*baseResolution)) {
        final String root = "/resolutions/"+resolution;
        final int bins = Math.toIntExact(length/resolution);
        final var pixels = new TreeMap<Long,Long>();
        for (var contact : new long[][]{{0,0,11},{1,253,3},{500,3900,7},{length-1,length-1,17}})
          pixels.merge(contact[0]/resolution*bins+contact[1]/resolution,contact[2],Long::sum);
        final var rows = new ArrayList<Long>();final var cols = new ArrayList<Long>();final var counts = new ArrayList<Long>();
        final var starts = new long[bins];final var ends = new long[bins];final var offsets = new long[bins+1];
        for (int row=0;row<bins;row++) {
          starts[row]=row*resolution;ends[row]=(row+1)*resolution;offsets[row]=rows.size();
          for(var entry:pixels.subMap((long)row*bins,(long)(row+1)*bins).entrySet()) {
            rows.add((long)row);cols.add(entry.getKey()%bins);counts.add(entry.getValue());
          }
        }
        offsets[bins]=rows.size();
        writer.int64().writeArray(root+"/bins/chrom",new long[bins]);
        writer.int64().writeArray(root+"/bins/start",starts);writer.int64().writeArray(root+"/bins/end",ends);
        writer.int64().writeArray(root+"/indexes/chrom_offset",new long[]{0,bins});writer.int64().writeArray(root+"/indexes/bin1_offset",offsets);
        writer.int64().writeArray(root+"/pixels/bin1_id",rows.stream().mapToLong(Long::longValue).toArray());
        writer.int64().writeArray(root+"/pixels/bin2_id",cols.stream().mapToLong(Long::longValue).toArray());
        writer.int64().writeArray(root+"/pixels/count",counts.stream().mapToLong(Long::longValue).toArray());
      }
    }
    new McoolToHictConverter().convert(new ConversionOptions(cooler,nativeFile,List.of(baseResolution,2*baseResolution),64,1,
      ConversionOptions.CompressionAlgorithm.DEFLATE,"",false,1,true,false,false,false,ConversionOptions.ExportMode.INTERNAL),ignored->{});
    try(var fasta=Files.newBufferedWriter(directory.resolve("sequence.fa"),StandardOpenOption.CREATE_NEW)) {
      fasta.write(">tiny\n");final String block="ACGTRYMKSWBDHVNAC".repeat(1024);
      long remaining=length;
      while(remaining>0) {int size=(int)Math.min(remaining,block.length());fasta.write(block,0,size);fasta.write('\n');remaining-=size;}
    }
    Files.writeString(directory.resolve("landmark.bed"),"tiny\t3\t6\tlandmark\t7\t+\n"+"tiny\t0\t1\ttiny-base\t3\t+\n",StandardOpenOption.CREATE_NEW);
  }
}
