package ru.itmo.ctlab.hict.hict_library.assembly;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class AssemblySequenceLayoutTest {
  @Test void paddingDoesNotChangeContactCoordinatesAndBoundaryOwnership() {
    var snapshot = new AssemblySequenceSnapshot(List.of(new AssemblySequenceSnapshot.Fragment("a",0,10,false),new AssemblySequenceSnapshot.Fragment("b",0,20,true)));
    var layout = new AssemblySequenceLayout(snapshot,List.of(new AssemblySequenceLayout.Gap(10,1000), new AssemblySequenceLayout.Gap(10,100)));
    assertEquals(1130, layout.length()); assertEquals(30, snapshot.length());
    assertEquals(10, layout.padded(10,false)); assertEquals(1110,layout.padded(10,true));
    assertEquals(1111,layout.padded(11,true)); assertEquals(0,layout.segmentStart(9)); assertEquals(10,layout.segmentEnd(9));
    assertEquals(10,layout.segmentStart(10)); assertEquals(30,layout.segmentEnd(10));
    assertEquals(1110,layout.slice(5,15).length());
    assertEquals(20,layout.slice(10,30).length());
    assertEquals(10,layout.slice(0,10).length());
    assertThrows(IllegalArgumentException.class,()->layout.padded(31,true));
  }
}
