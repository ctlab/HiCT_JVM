package ru.itmo.ctlab.hict.hict_library.assembly;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SequenceTrackProjectionTest {
  @Test void gapsHaveNoInventedSourceCoordinatesAndReversedSlicesKeepProvenance() {
    var components=List.of(new SequenceTrackProjection.Component(0,4,"left","scf_left","originalA",10,false),
      new SequenceTrackProjection.Component(4,9,"right","scf_right","originalB",20,true));
    var model=new SequenceTrackProjection(components,List.of(new AssemblySequenceLayout.Gap(4,1000)));
    assertEquals(1009,model.length());
    var pieces=model.window(2,1007); assertEquals(3,pieces.size());
    assertEquals(12,pieces.get(0).sourceStart()); assertEquals(14,pieces.get(0).sourceEnd());
    assertTrue(pieces.get(1).gap());assertNull(pieces.get(1).assemblyStart());assertNull(pieces.get(1).source());
    var reverse=pieces.get(2);assertTrue(reverse.reversed());assertEquals(22,reverse.sourceStart());assertEquals(25,reverse.sourceEnd());
    assertEquals(4,reverse.assemblyStart());assertEquals(7,reverse.assemblyEnd());
    assertEquals("right",reverse.contig());assertEquals("scf_right",reverse.scaffold());
    assertEquals(3,model.sequencePosition(3));
    assertEquals(1004,model.sequencePosition(4));
    assertEquals(1008,model.sequencePosition(8));
    assertThrows(IllegalArgumentException.class,()->model.sequencePosition(9));
    assertThrows(IllegalArgumentException.class,()->model.sequencePosition(-1));
    var plain=new SequenceTrackProjection(components,List.of());assertEquals(9,plain.length());
    assertEquals(20,plain.window(8,9).getFirst().sourceStart());
  }
  @Test void everyPaddedBaseMatchesIndependentExplicitOracle() {
    var random=new Random(90457);var components=new ArrayList<SequenceTrackProjection.Component>();
    var gaps=new ArrayList<AssemblySequenceLayout.Gap>();var oracle=new ArrayList<Long>();long position=0;
    for(int i=0;i<80;i++) {
      if(i>0) {int length=random.nextInt(6);if(length>0)gaps.add(new AssemblySequenceLayout.Gap(position,length));
        for(int j=0;j<length;j++)oracle.add(null);}
      int length=1+random.nextInt(19);boolean reversed=random.nextBoolean();
      components.add(new SequenceTrackProjection.Component(position,position+length,"c"+i,"s"+i,"source",i*100,reversed));
      for(int j=0;j<length;j++)oracle.add((long)i*100+(reversed?length-1-j:j));position+=length;
    }
    var model=new SequenceTrackProjection(components,gaps);assertEquals(oracle.size(),model.length());
    for(int i=0;i<oracle.size();i++) {
      var p=model.window(i,i+1).getFirst();assertEquals(oracle.get(i),p.sourceStart(),"base "+i);
      assertEquals(oracle.get(i)==null,p.gap());
    }
    assertThrows(IllegalArgumentException.class,()->model.window(-1,2));
    assertThrows(IllegalArgumentException.class,()->model.window(0,4097));
    assertThrows(IllegalArgumentException.class,()->model.window(model.length(),model.length()+1));
  }
}
