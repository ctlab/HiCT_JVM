package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_library.assembly.AssemblySequenceSnapshot.FeatureStatus.*;
import ru.itmo.ctlab.hict.hict_library.assembly.AssemblySequenceSnapshot.Fragment;

class AssemblySequenceSnapshotTest {
  private static AssemblySequenceSnapshot snapshot(Fragment... pieces) {
    return new AssemblySequenceSnapshot(List.of(pieces));
  }
  private static Fragment f(String name, long a, long b, boolean reverse) { return new Fragment(name, a, b, reverse); }

  @Test void slicingAcrossJoinsUsesOriginalCoordinatesAndOrientation() {
    var source = snapshot(f("A", 10, 20, true), f("B", 30, 38, false));
    var selected = source.slice(3, 14);
    assertEquals(List.of(f("A", 10, 17, true), f("B", 30, 34, false)), selected.fragments());
    assertEquals(11, selected.length());
    assertEquals(source.fingerprint(), source.slice(0, source.length()).fingerprint());
    assertNotEquals(source.fingerprint(), selected.fingerprint());
    assertThrows(IllegalArgumentException.class, () -> source.slice(-1, 2));
    assertThrows(IllegalArgumentException.class, () -> source.slice(4, 3));
    assertThrows(IllegalArgumentException.class, () -> source.slice(0, 19));
    assertEquals(0, source.slice(18, 18).length());
  }

  @Test void distinguishesInversionSplitDeletionAndRearrangedJoin() {
    var hit = snapshot(f("A", 2, 7, false), f("B", 0, 3, true));
    var unchanged = snapshot(f("A", 0, 7, false), f("B", 0, 3, true));
    assertEquals(INTACT, unchanged.project(hit).status());
    var inverted = snapshot(f("B", 0, 3, false), f("A", 0, 7, true));
    var result = inverted.project(hit);
    assertEquals(INTACT, result.status());
    assertTrue(result.sequencePreserved());
    assertTrue(result.pieces().stream().allMatch(AssemblySequenceSnapshot.ProjectedPiece::reversed));
    var split = snapshot(f("A", 0, 4, false), f("A", 4, 7, false), f("B", 0, 3, true));
    assertEquals(FRAGMENTED, split.project(hit).status());
    assertTrue(split.project(hit).sequencePreserved());
    var moved = snapshot(f("A", 0, 7, false), f("X", 0, 4, false), f("B", 0, 3, true));
    assertEquals(REARRANGED, moved.project(hit).status());
    var deleted = snapshot(f("A", 0, 4, false), f("A", 5, 7, false), f("B", 0, 3, true));
    assertEquals(PARTIAL, deleted.project(hit).status());
    assertEquals(7, deleted.project(hit).pieces().stream().mapToLong(p -> p.assemblyEnd() - p.assemblyStart()).sum());
    assertEquals(ABSENT, snapshot(f("other", 0, 5, false)).project(hit).status());
  }

  @Test void immutableInputsAndUnambiguousSourceOwnership() {
    var input = new ArrayList<>(List.of(f("A", 0, 10, false)));
    var source = new AssemblySequenceSnapshot(input);
    input.clear();
    assertEquals(10, source.length());
    assertThrows(UnsupportedOperationException.class, () -> source.fragments().clear());
    assertThrows(IllegalArgumentException.class, () -> snapshot(f("A", 0, 10, false), f("A", 9, 20, true)));
    assertThrows(ArithmeticException.class, () -> snapshot(f("A", 0, Long.MAX_VALUE, false), f("B", 0, 1, false)));
  }

  private record Base(String source, long coordinate, boolean reversed) { }
  private static List<Base> bases(AssemblySequenceSnapshot snapshot) {
    var out = new ArrayList<Base>();
    for (var f : snapshot.fragments()) for (long i = 0; i < f.length(); i++)
      out.add(new Base(f.source(), f.reversed() ? f.end() - i - 1 : f.start() + i, f.reversed()));
    return out;
  }

  @Test void randomizedProjectionMatchesIndependentPerBaseOracle() {
    var random = new Random(620261013);
    var original = snapshot(f("A", 0, 32, false), f("B", 0, 24, true));
    for (int trial = 0; trial < 300; trial++) {
      int from = random.nextInt(56), to = from + 1 + random.nextInt(56 - from);
      var hit = original.slice(from, to);
      var fragments = new ArrayList<Fragment>();
      for (String name : List.of("A", "B")) {
        int length = name.equals("A") ? 32 : 24;
        for (int i = 0; i < length; i += 4)
          if (random.nextInt(5) != 0) fragments.add(f(name, i, i + 4, random.nextBoolean()));
      }
      Collections.shuffle(fragments, random);
      var current = new AssemblySequenceSnapshot(fragments);
      var currentBases = bases(current); var originalBases = bases(hit);
      var result = current.project(hit);
      var projected = new HashMap<Integer, Integer>();
      for (var p : result.pieces()) {
        for (long i = 0; i < p.featureEnd() - p.featureStart(); i++) {
          int featurePosition = (int) (p.featureStart() + i);
          int currentPosition = (int) (p.reversed() ? p.assemblyEnd() - i - 1 : p.assemblyStart() + i);
          assertNull(projected.put(featurePosition, currentPosition));
          var expected = originalBases.get(featurePosition); var actual = currentBases.get(currentPosition);
          assertEquals(expected.source(), actual.source());
          assertEquals(expected.coordinate(), actual.coordinate());
          assertEquals(expected.reversed() != actual.reversed(), p.reversed());
        }
      }
      for (int i = 0; i < originalBases.size(); i++) {
        var base = originalBases.get(i);
        boolean present = currentBases.stream().anyMatch(b -> b.source().equals(base.source()) && b.coordinate() == base.coordinate());
        assertEquals(present, projected.containsKey(i));
      }
      if (result.sequencePreserved()) {
        assertEquals(originalBases.size(), projected.size());
        int direction = result.pieces().getFirst().reversed() ? -1 : 1;
        for (int i = 1; i < originalBases.size(); i++) assertEquals(direction, projected.get(i) - projected.get(i - 1));
      }
    }
  }
}
