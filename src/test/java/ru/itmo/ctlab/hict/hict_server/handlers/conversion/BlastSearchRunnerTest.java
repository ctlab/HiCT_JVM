package ru.itmo.ctlab.hict.hict_server.handlers.conversion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.assembly.*;
import ru.itmo.ctlab.hict.hict_server.tracks.SequenceSearchMetadata;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
class BlastSearchRunnerTest {
  @TempDir Path dir;
  @Test void partialGappedReverseHitAnchorsToSubjectRatherThanQueryLength() throws Exception {
    var s = new AssemblySequenceSnapshot(List.of(new AssemblySequenceSnapshot.Fragment("A",100,120,true),new AssemblySequenceSnapshot.Fragment("B",0,20,false)));
    var raw=dir.resolve("raw"); var bed=dir.resolve("out");
    Files.writeString(raw,"query_1\thict_chunk_0_40\t2\t10\t24\t18\t77.778\t9\t0\t1\t0.01\t20\t12\n");
    assertArrayEquals(new long[]{1,2},BlastSearchRunner.anchorHits(raw,bed,s,SequenceSearchQueries.parse("ACGTACGTACGT"),s.fingerprint()));
    var rows=Files.readAllLines(bed).stream().filter(x->!x.startsWith("#")).toList();
    var a=rows.getFirst().split("\t");var b=rows.getLast().split("\t");
    assertEquals("100",a[1]);assertEquals("103",a[2]);assertEquals("+",a[5]);
    assertEquals("0",b[1]);assertEquals("4",b[2]);assertEquals("-",b[5]);
    assertEquals("220,38,38",a[8]);assertEquals("37,99,235",b[8]);
    var metadata=SequenceSearchMetadata.decode(a[12]);assertEquals(7,metadata.path().length());
    assertTrue(metadata.description().contains("gap openings 1"));
  }
  @Test void realBlastFindsGappedAlignmentAndKeepsRawScores() throws Exception {
    String binary=System.getenv("HICT_BLASTN_BIN");assumeTrue(binary!=null&&Files.isExecutable(Path.of(binary)),"Set HICT_BLASTN_BIN for native test");
    var random=new Random(17);var query=new StringBuilder();for(int i=0;i<220;i++)query.append("ACGT".charAt(random.nextInt(4)));
    String subject=query.substring(0,100)+"AAAAA"+query.substring(100);
    var snapshot=new AssemblySequenceSnapshot(List.of(new AssemblySequenceSnapshot.Fragment("chr",0,subject.length(),false)));
    var input=dir.resolve("subject.fa");Files.writeString(input,">hict_chunk_0_"+subject.length()+"\n"+subject+"\n");
    var result=new BlastSearchRunner().run(Path.of(binary),dir,input,snapshot,SequenceSearchQueries.parse(query.toString()),
      new BlastSearchRunner.Options(true,10,80,80,500),snapshot.fingerprint(),ignored->{},ignored->{},()->false);
    assertTrue(result.hits()>0);
    var lines=Files.readAllLines(dir.resolve("blast-hits.tsv"));
    assertTrue(lines.stream().map(l->l.split("\t")).anyMatch(f->Integer.parseInt(f[9])>0 && Integer.parseInt(f[7])>=220));
    var metadata=Files.readAllLines(result.bed()).stream().filter(l->!l.startsWith("#")).map(l->SequenceSearchMetadata.decode(l.split("\t")[12])).toList();
    assertTrue(metadata.stream().anyMatch(m->m.path().length()==225));
  }
}
