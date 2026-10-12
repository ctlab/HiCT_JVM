package ru.itmo.ctlab.hict.hict_library.history;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class EditJournalTest {
  @Test void navigationRetainsRedoUntilANewEditAndRejectsStaleClients() {
    var history = new EditJournal<>("original");
    var initial = history.summary();
    history.append("Reverse", "reversed"); history.append("Split", "split");
    history.movedTo(history.target("undo", null));
    assertEquals("reversed", history.current()); assertTrue(history.summary().canRedo());
    history.movedTo(history.target("restore", 0L));
    assertEquals("original", history.current()); assertEquals(3, history.summary().size());
    history.movedTo(history.target("redo", null)); assertEquals("reversed", history.current());
    assertThrows(IllegalStateException.class, () -> history.requireVersion(initial.session(), initial.revision()));
    history.append("New move", "moved");
    assertEquals(3, history.summary().size()); assertEquals(3, history.summary().currentId());
    assertFalse(history.summary().canRedo());
    assertThrows(IllegalArgumentException.class, () -> history.target("restore", 2L));
    assertEquals(List.of("Initial assembly", "Reverse", "New move"), history.page(0,100).stream().map(EditJournal.Item::label).toList());
  }

  @Test void mapCheckpointsRestoreOverwritesRemovalsAndBranchesWithoutCopyingOnRead() {
    var map = new VersionedMap<Integer,String>();
    map.put(1,"original"); var a=map.snapshot();
    map.put(1,"changed"); map.put(2,"child"); var b=map.snapshot();
    map.restore(a); assertEquals(Map.of(1,"original"), map);
    map.put(3,"new branch"); var c=map.snapshot();
    map.restore(b); assertEquals(Map.of(1,"changed",2,"child"),map);
    map.clear(); assertTrue(map.isEmpty()); map.restore(c);
    assertEquals(Map.of(1,"original",3,"new branch"),map);
    map.put(3,"new branch"); assertSame(c,map.snapshot());
    assertThrows(UnsupportedOperationException.class, ()->map.entrySet().iterator().next().setValue("leak"));
    var other=new VersionedMap<Integer,String>();
    assertThrows(IllegalArgumentException.class,()->map.restore(other.snapshot()));
  }

  @Test void randomMapRestoreMatchesIndependentFullCopies() {
    var random = new Random(20261012); var map = new VersionedMap<Integer,String>();
    var states = new ArrayList<VersionedMap.Version<Integer,String>>();
    var expected = new ArrayList<Map<Integer,String>>();
    for (int i=0;i<3000;i++) {
      if (!states.isEmpty() && random.nextInt(4)==0) {
        int index=random.nextInt(states.size()); map.restore(states.get(index)); assertEquals(expected.get(index),map);
      } else {
        int key=random.nextInt(80);
        if(random.nextBoolean())map.put(key,"v"+i);else map.remove(key);
        states.add(map.snapshot()); expected.add(Map.copyOf(map));
      }
    }
  }
}
