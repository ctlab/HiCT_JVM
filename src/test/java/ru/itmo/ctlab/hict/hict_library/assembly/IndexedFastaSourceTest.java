package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class IndexedFastaSourceTest {
  @TempDir Path dir;
  private static String copy(IndexedFastaSource index,String name,int a,int b,boolean reverse) throws Exception {
    var out=new ByteArrayOutputStream();try(var session=index.open()){session.copy(name,a,b,reverse,out);}return out.toString();
  }
  private static String reverse(String s) {
    String input="ACGTRYMKSWBDHVNacgtrymkswbdhvn",output="TGCAYRKMSWVHDBNtgcayrkmswvhdbn";
    var result=new StringBuilder();for(int i=s.length()-1;i>=0;i--)result.append(output.charAt(input.indexOf(s.charAt(i))));return result.toString();
  }
  @Test void variableLinesCrLfWhitespaceNamesAndIupacMasking() throws Exception {
    var path=dir.resolve("in.fa");Files.writeString(path,">one description ü\r\n  aCGTry  \r\n\r\nMKSW\nBDHVN\n>two\nAA\nCC");
    var index=new IndexedFastaSource(path);String expected="aCGTryMKSWBDHVN";
    assertEquals(Map.of("one",15L,"two",4L),index.lengths());
    for(int a=0;a<expected.length();a++)for(int b=a+1;b<=expected.length();b++) {
      assertEquals(expected.substring(a,b),copy(index,"one",a,b,false));
      assertEquals(reverse(expected.substring(a,b)),copy(index,"one",a,b,true));
    }
    assertEquals("AACC",copy(index,"two",0,4,false));
    assertThrows(IllegalArgumentException.class,()->copy(index,"absent",0,1,false));
    assertThrows(IllegalArgumentException.class,()->copy(index,"one",0,16,false));
  }
  @Test void chunksAndRegularLineRunsMatchIndependentSlices() throws Exception {
    var random=new Random(2431);var text=new StringBuilder();String alphabet="ACGTRYMKSWBDHVNacgtrymkswbdhvn";
    for(int i=0;i<240003;i++)text.append(alphabet.charAt(random.nextInt(alphabet.length())));
    String sequence=text.toString();var lines=new StringBuilder(">x\n");
    for(int i=0;i<sequence.length();i+=80)lines.append(sequence,i,Math.min(i+80,sequence.length())).append("\r\n");
    var path=dir.resolve("long.fa");Files.writeString(path,lines);var index=new IndexedFastaSource(path);
    for(int i=0;i<80;i++){int a=random.nextInt(sequence.length()),b=a+1+random.nextInt(sequence.length()-a);assertEquals(sequence.substring(a,b),copy(index,"x",a,b,false));assertEquals(reverse(sequence.substring(a,b)),copy(index,"x",a,b,true));}
    assertEquals(sequence,copy(index,"x",0,sequence.length(),false));
    Files.writeString(path,">x\n"+sequence);index=new IndexedFastaSource(path);
    assertEquals(reverse(sequence),copy(index,"x",0,sequence.length(),true));
  }
  @Test void refusesDuplicateHeadersAndDetectsSourceReplacement() throws Exception {
    var path=dir.resolve("file.fa");Files.writeString(path,">x\nACT\n>x\nG");
    assertThrows(IllegalArgumentException.class,()->new IndexedFastaSource(path));
    Files.writeString(path,">x\nACT");var index=new IndexedFastaSource(path);
    var replacement=dir.resolve("new.fa");Files.writeString(replacement,">x\nTGA");Files.move(replacement,path,StandardCopyOption.REPLACE_EXISTING);
    assertFalse(index.matches(path));assertThrows(IOException.class,index::open);
  }

  private static void gzip(Path path,String text) throws IOException {
    try(var stream=new java.util.zip.GZIPOutputStream(Files.newOutputStream(path))) {
      stream.write(text.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }
  }
  private static Path indexedPath(IndexedFastaSource index) throws Exception {
    var field=IndexedFastaSource.class.getDeclaredField("indexedPath");field.setAccessible(true);
    return (Path)field.get(index);
  }
  @Test void gzipIsIndexedOnceAndRemovedAfterLastReaderCloses() throws Exception {
    var path=dir.resolve("input.fa.gz");String text="aCGTryMKSWBDHVN".repeat(20000);
    gzip(path,">one description\r\n"+text+"\n>two\nACGT\n");
    byte[] original=Files.readAllBytes(path);
    var index=new IndexedFastaSource(path);var temporary=indexedPath(index);
    assertNotEquals(path,temporary);assertTrue(Files.exists(temporary));
    assertEquals(Map.of("one",(long)text.length(),"two",4L),index.lengths());
    var random=new Random(245);
    for(int i=0;i<100;i++) {
      int start=random.nextInt(text.length()-4096),end=start+4096;
      assertEquals(text.substring(start,end),copy(index,"one",start,end,false));
      assertEquals(reverse(text.substring(start,end)),copy(index,"one",start,end,true));
      assertEquals(temporary,indexedPath(index));
    }
    var reader=index.open();index.close();
    assertTrue(Files.exists(temporary),"An active reader retains its backing file, including on Windows");
    assertThrows(IOException.class,index::open);
    var out=new ByteArrayOutputStream();reader.copy("two",0,4,false,out);
    assertEquals("ACGT",out.toString());reader.close();reader.close();index.close();
    assertFalse(Files.exists(temporary));assertArrayEquals(original,Files.readAllBytes(path));
    try(var files=Files.list(dir)) {assertEquals(1,files.count(),"No sidecars beside the source");}
  }
  @Test void gzipReplacementCannotServeStaleBases() throws Exception {
    var path=dir.resolve("input.fa.gz");gzip(path,">x\nACTG");
    try(var index=new IndexedFastaSource(path)) {
      var replacement=dir.resolve("replacement.gz");gzip(replacement,">x\nTGCA");
      Files.move(replacement,path,StandardCopyOption.REPLACE_EXISTING);
      assertFalse(index.matches(path));assertThrows(IOException.class,index::open);
    }
    try(var index=new IndexedFastaSource(path)) {assertEquals("TGCA",copy(index,"x",0,4,false));}
  }
}
