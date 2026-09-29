package ru.itmo.ctlab.hict.hict_library.trees;

import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ContigTreeEndpointTest {
  private static final ResolutionDescriptor RESOLUTION = ResolutionDescriptor.fromResolutionOrder(1);

  private static ContigTree.Node tree(int lo, int hi, int reversals, int visible) {
    if (lo >= hi) return null;
    int id = (lo + hi) / 2;
    var descriptor = new ContigDescriptor(id, "c" + id, 1000, List.of(10L),
      List.of((visible & (1 << id)) != 0 ? ContigHideType.SHOWN : ContigHideType.HIDDEN),
      List.of(List.of()), null, 0);
    return ContigTree.Node.createNodeFromDescriptor(descriptor, ContigDirection.FORWARD).cloneBuilder()
      .left(tree(lo, id, reversals, visible)).right(tree(id + 1, hi, reversals, visible))
      .needsChangingDirection((reversals & (1 << id)) != 0).build().updateSizes();
  }

  private record Expected(int id, ContigDirection direction) {}
  private static void enumerate(ContigTree.Node node, boolean inherited, List<Expected> result, boolean visibleOnly) {
    if (node == null) return;
    boolean reversed = inherited ^ node.isNeedsChangingDirection();
    enumerate(reversed ? node.getRight() : node.getLeft(), reversed, result, visibleOnly);
    if (!visibleOnly || node.getContigDescriptor().getPresenceAtResolution().get(1) == ContigHideType.SHOWN)
      result.add(new Expected(node.getContigDescriptor().getContigId(), reversed ? node.getContigDirection().inverse() : node.getContigDirection()));
    enumerate(reversed ? node.getLeft() : node.getRight(), reversed, result, visibleOnly);
  }

  private static void check(Expected expected, ContigTree.Node actual) {
    if (expected == null) assertNull(actual);
    else {
      assertNotNull(actual);
      assertEquals(expected.id(), actual.getContigDescriptor().getContigId());
      assertEquals(expected.direction(), actual.getTrueDirection());
    }
  }

  @Test void endpointOrderAndDirectionIncludeAncestorReversals() {
    for (int flags = 0; flags < 128; flags++) for (int visible : new int[]{0, 1, 8, 65, 85, 127}) {
      var root = tree(0, 7, flags, visible);
      var left = root.getLeft(); var right = root.getRight();
      for (boolean filter : new boolean[]{false, true}) {
        var expected = new ArrayList<Expected>(); enumerate(root, false, expected, filter);
        check(expected.isEmpty() ? null : expected.getFirst(), filter ? root.leftmostVisibleNode(RESOLUTION) : root.leftmost());
        check(expected.isEmpty() ? null : expected.getLast(), filter ? root.rightmostVisibleNode(RESOLUTION) : root.rightmost());
      }
      assertSame(left, root.getLeft()); assertSame(right, root.getRight());
    }
    assertNull(ContigTree.Node.leftmost(null));
    assertNull(ContigTree.Node.rightmost(null));
  }

  public static void main(String[] args) {
    new ContigTreeEndpointTest().endpointOrderAndDirectionIncludeAncestorReversals();
  }
}
