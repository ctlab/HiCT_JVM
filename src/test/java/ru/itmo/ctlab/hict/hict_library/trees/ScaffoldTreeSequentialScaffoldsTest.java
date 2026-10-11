package ru.itmo.ctlab.hict.hict_library.trees;

import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;
import static org.junit.jupiter.api.Assertions.*;

class ScaffoldTreeSequentialScaffoldsTest {
  @Test void loadingAndEditingManySingletonScaffoldsPreservesOrderAndLength() {
    final int count = 100_000;
    final long length = count * 10L;
    var tree = new ScaffoldTree(length);
    for (int i = 0; i < count; i++) {
      final int id = i;
      tree.rescaffold(i * 10L, (i + 1L) * 10L,
        unused -> new ScaffoldDescriptor(id, "scaffold_" + id, 1000));
    }
    // A degenerate tree overflows on traversal or an edit near its first node.
    var original = tree.getScaffoldList();
    assertEquals(count, original.size());
    for (int i = 0; i < count; i++) {
      assertEquals(i, original.get(i).scaffoldDescriptor().scaffoldId());
      assertEquals(i * 10L, original.get(i).scaffoldBordersBP().startBP());
      assertEquals((i + 1L) * 10L, original.get(i).scaffoldBordersBP().endBP());
    }
    tree.reverseSelectionRange(0, 30);
    tree.moveSelectionRange(0, 30, length);
    var edited = tree.getScaffoldList();
    assertEquals(count, edited.size());
    for (int i = 0; i < count; i++) {
      assertEquals(i < count - 3 ? i + 3 : count - 1 - i,
        edited.get(i).scaffoldDescriptor().scaffoldId());
      assertEquals(i * 10L, edited.get(i).scaffoldBordersBP().startBP());
      assertEquals((i + 1L) * 10L, edited.get(i).scaffoldBordersBP().endBP());
    }
    tree.unscaffold(50, 100);
    assertEquals(count - 5, tree.getScaffoldList().size());
    var replacement = tree.rescaffold(50, 100, null);
    assertTrue(replacement.scaffoldId() >= count);
    assertEquals(count - 4, tree.getScaffoldList().size());
    assertEquals(length, tree.getScaffoldList().getLast().scaffoldBordersBP().endBP());
  }

  private static ScaffoldTree.Node node(int id, boolean reverse, ScaffoldTree.Node left, ScaffoldTree.Node right) {
    return ScaffoldTree.Node.builder().scaffoldDescriptor(new ScaffoldDescriptor(id, "s" + id, 1000))
      .nodeLengthBp(10).yPriority(10).needsChangingDirection(reverse).left(left).right(right).build().updateSizes();
  }

  @Test void ancestorReversalIsPropagatedExactlyOnceForOrderAndEndpoints() {
    var root = node(3, true,
      node(1, true, node(0, false, null, null), node(2, false, null, null)),
      node(5, true, node(4, false, null, null), node(6, false, null, null)));
    var actual = new java.util.ArrayList<Long>();
    ScaffoldTree.Node.traverseNode(root, n -> actual.add(n.scaffoldDescriptor.scaffoldId()));
    assertEquals(java.util.List.of(4L, 5L, 6L, 3L, 0L, 1L, 2L), actual);
    assertEquals(4, ScaffoldTree.Node.leftmost(root).scaffoldDescriptor.scaffoldId());
    assertEquals(2, ScaffoldTree.Node.rightmost(root).scaffoldDescriptor.scaffoldId());
    assertNull(ScaffoldTree.Node.leftmost(null));
    assertNull(ScaffoldTree.Node.rightmost(null));
  }

  @Test void splittingGapWithRightSubtreeDoesNotDuplicateItsLength() {
    var gap = ScaffoldTree.Node.builder().nodeLengthBp(40).yPriority(100)
      .left(node(1, false, null, null)).right(node(2, false, null, null)).build().updateSizes();
    var split = ScaffoldTree.Node.splitNodeBp(gap, 20, false);
    assertEquals(20, split.left().subtreeLengthBp);
    assertEquals(40, split.right().subtreeLengthBp);
    assertEquals(gap.subtreeLengthBp, split.left().subtreeLengthBp + split.right().subtreeLengthBp);
    assertEquals(100, split.right().yPriority);
  }

  @Test void repeatedReversalsAndMovesMatchIndependentListModel() {
    final int count = 64;
    var tree = new ScaffoldTree(count * 10L);
    var expected = new java.util.ArrayList<Long>();
    for (int i = 0; i < count; i++) {
      final int id = i;
      expected.add((long) id);
      tree.rescaffold(i * 10L, (i + 1L) * 10L, unused -> new ScaffoldDescriptor(id, "s" + id, 1000));
    }
    var random = new java.util.Random(20261012);
    for (int trial = 0; trial < 1000; trial++) {
      int first = random.nextInt(count), last = first + 1 + random.nextInt(count - first);
      if (random.nextBoolean()) {
        tree.reverseSelectionRange(first * 10L, last * 10L);
        java.util.Collections.reverse(expected.subList(first, last));
      } else {
        var moved = new java.util.ArrayList<>(expected.subList(first, last));
        expected.subList(first, last).clear();
        int target = random.nextInt(expected.size() + 1);
        tree.moveSelectionRange(first * 10L, last * 10L, target * 10L);
        expected.addAll(target, moved);
      }
      var actual = tree.getScaffoldList();
      assertEquals(count, actual.size());
      for (int i = 0; i < count; i++) {
        assertEquals(expected.get(i).longValue(), actual.get(i).scaffoldDescriptor().scaffoldId());
        assertEquals(expected.get(i).longValue(), tree.getScaffoldAtBp(i * 10L + 5).scaffoldId());
        assertEquals(i * 10L, actual.get(i).scaffoldBordersBP().startBP());
        assertEquals((i + 1L) * 10L, actual.get(i).scaffoldBordersBP().endBP());
      }
    }
  }

  public static void main(String[] args) {
    var test = new ScaffoldTreeSequentialScaffoldsTest();
    if (args.length == 0 || args[0].equals("large")) test.loadingAndEditingManySingletonScaffoldsPreservesOrderAndLength();
    if (args.length == 0 || args[0].equals("reversal")) test.ancestorReversalIsPropagatedExactlyOnceForOrderAndEndpoints();
    if (args.length == 0 || args[0].equals("gap")) test.splittingGapWithRightSubtreeDoesNotDuplicateItsLength();
    if (args.length == 0 || args[0].equals("model")) test.repeatedReversalsAndMovesMatchIndependentListModel();
  }
}
