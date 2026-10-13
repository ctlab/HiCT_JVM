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
}
