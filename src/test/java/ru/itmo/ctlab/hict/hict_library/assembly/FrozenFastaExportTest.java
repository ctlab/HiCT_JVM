package ru.itmo.ctlab.hict.hict_library.assembly;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import java.io.ByteArrayOutputStream;
import java.nio.file.*;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
class FrozenFastaExportTest {
  @TempDir Path directory;
  @Test void recordAndSelectionWritersUseCapturedLayoutsAfterCutsAndReversal() throws Exception {
    var nativeFile=ReinforcementHdf5Fixture.create(directory,"frozen",List.of(4L,10L));
    var path=directory.resolve("source.fa");String original="ACGTRYMKSWBDHVNAC";Files.writeString(path,">tiny\n"+original+"\n");
    byte[] digest=java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(nativeFile));
    try(var file=new ChunkedFile(new ChunkedFile.ChunkedFileOptions(nativeFile,1,2))) {
      var records=List.copyOf(file.getAgpProcessor().getAssemblyAGPRecords());
      var layout=AssemblySequenceLayout.capture(file,1000);
      file.getScaffoldingOperations().splitContigAtBp(0,3);
      file.getScaffoldingOperations().reverseSelectionRangeBp(0,17);
      var output=new ByteArrayOutputStream();
      file.getFastaProcessor().writeAssemblyRecords(path,output,records,true);
      assertEquals(original,output.toString().lines().filter(line->!line.startsWith(">")).reduce("",String::concat));
      output.reset();file.getFastaProcessor().writeLayout(path,output,layout,2,8,"selection");
      assertEquals(original.substring(2,8),output.toString().lines().filter(line->!line.startsWith(">")).reduce("",String::concat));
      assertNotEquals(original.substring(2,8),file.getFastaProcessor().readRegion(path,2,8));
    }
    assertArrayEquals(digest,java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(nativeFile)));
  }
}
