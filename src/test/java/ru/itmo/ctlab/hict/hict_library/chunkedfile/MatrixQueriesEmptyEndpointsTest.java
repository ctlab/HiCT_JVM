package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class MatrixQueriesEmptyEndpointsTest {
  private static final ResolutionDescriptor RES = ResolutionDescriptor.fromResolutionOrder(1);

  private static ContigTree.Node node(int id, long bins, boolean shown, boolean reverse, ContigTree.Node left, ContigTree.Node right) {
    var descriptor = new ContigDescriptor(id, "c" + id, 1000, List.of(bins),
      List.of(shown ? ContigHideType.SHOWN : ContigHideType.HIDDEN), List.of(List.of()), null, 0);
    return ContigTree.Node.createNodeFromDescriptor(descriptor, ContigDirection.FORWARD).cloneBuilder()
      .left(left).right(right).needsChangingDirection(reverse).build().updateSizes();
  }

  @Test void skipsEmptyEndpointsAndHiddenContigsAfterReversal() {
    for (boolean reverse : new boolean[]{false, true}) {
      var root = node(2, 3, false, reverse,
        node(1, 0, true, false, node(0, 2, true, true, null, null), null),
        node(3, 0, true, true, null, node(4, 1, true, false, null, null)));
      for (var units : new QueryLengthUnit[]{QueryLengthUnit.BINS, QueryLengthUnit.PIXELS}) {
        var ordered = new ArrayList<ContigTree.Node>();
        ContigTree.Node.traverseNode(root, n -> {
          if (n.getContigDescriptor().getLengthInUnits(units, RES) > 0) ordered.add(n);
        });
        for (boolean last : new boolean[]{false, true}) {
          var expected = last ? ordered.getLast() : ordered.getFirst();
          var actual = MatrixQueries.nonEmptyEndpoint(root, RES, units, last);
          assertEquals(expected.getContigDescriptor().getContigId(), actual.getContigDescriptor().getContigId());
          assertEquals(expected.getTrueDirection(), actual.getTrueDirection());
        }
      }
      // A hidden, nonzero-bin root is a valid BINS endpoint but not a PIXELS endpoint.
      var hidden = node(7, 1, false, reverse, null, node(8, 1, true, false, null, null));
      assertEquals(7, MatrixQueries.nonEmptyEndpoint(hidden, RES, QueryLengthUnit.BINS, reverse).getContigDescriptor().getContigId());
      assertEquals(8, MatrixQueries.nonEmptyEndpoint(hidden, RES, QueryLengthUnit.PIXELS, reverse).getContigDescriptor().getContigId());
    }
    var empty = node(0, 0, true, false, null, null);
    assertThrows(IllegalArgumentException.class, () -> MatrixQueries.nonEmptyEndpoint(empty, RES, QueryLengthUnit.BINS, false));
  }

  public static void main(String[] args) {
    new MatrixQueriesEmptyEndpointsTest().skipsEmptyEndpointsAndHiddenContigsAfterReversal();
  }
}
