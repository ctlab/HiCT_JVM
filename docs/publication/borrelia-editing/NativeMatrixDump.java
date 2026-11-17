import java.nio.file.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
class NativeMatrixDump { public static void main(String[] args) throws Exception {
 try(var f=new ChunkedFile(new ChunkedFile.ChunkedFileOptions(Path.of(args[0]),1,2));var out=Files.newBufferedWriter(Path.of(args[1]))) {
  var region=f.matrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(10000,f),0,0,155,155,false);var m=region.matrix();Files.writeString(Path.of(args[1]+".weights"),java.util.Arrays.toString(region.rowWeights())+"\n"+java.util.Arrays.toString(region.colWeights()));
  for(int r=0;r<155;r++){for(int c=0;c<155;c++){if(c>0)out.write("\t");out.write(Double.toString(m.getAsDouble(r,c)));}out.newLine();}
 }
}}
