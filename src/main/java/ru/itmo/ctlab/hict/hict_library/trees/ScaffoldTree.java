/*
 * MIT License
 *
 * Copyright (c) 2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ru.itmo.ctlab.hict.hict_library.trees;

import lombok.Builder;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.LongFunction;

public class ScaffoldTree implements Iterable<ScaffoldTree.Node> {

  private static final Random rnd = new Random();
  @Getter
  private final ReadWriteLock rootLock = new ReentrantReadWriteLock();
  private Node root;
  private long rootScaffoldIdCounter;

  public ScaffoldTree(final long assemblyLengthBp) {
    try {
      this.rootLock.writeLock().lock();
      this.root = Node.builder().scaffoldDescriptor(null).nodeLengthBp(assemblyLengthBp).subtreeLengthBp(assemblyLengthBp).yPriority(rnd.nextLong()).left(null).right(null).needsChangingDirection(false).build();
      this.rootScaffoldIdCounter = 0;
    } finally {
      this.rootLock.writeLock().unlock();
    }
    //this.root = new
  }

  @Override
  public Iterator<Node> iterator() {
    final Node rootSnapshot;
    try {
      this.rootLock.readLock().lock();
      rootSnapshot = this.root;
    } finally {
      this.rootLock.readLock().unlock();
    }
    if (rootSnapshot != null) {
      return rootSnapshot.iterator();
    } else {
      return new Iterator<Node>() {
        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Node next() {
          throw new NoSuchElementException();
        }
      };
    }
  }

  public void commitExposedSegment(final @NotNull Node.ExposedSegment exposedSegment) {
    final var le = Node.mergeNodes(new Node.SplitResult(exposedSegment.less(), exposedSegment.segment()));
    final var rt = Node.mergeNodes(new Node.SplitResult(le, exposedSegment.greater()));
    try {
      this.rootLock.writeLock().lock();
      this.root = rt;
    } finally {
      this.rootLock.writeLock().unlock();
    }
  }

//  public void commitRoot(final @No Node newRoot) {
//    try {
//      this.rootLock.writeLock().lock();
//      this.root = newRoot;
//    } finally {
//      this.rootLock.writeLock().unlock();
//    }
//  }

  public @Nullable ScaffoldDescriptor getScaffoldAtBp(final long bp) {
    try {
      this.rootLock.readLock().lock();
      if (bp >= this.root.subtreeLengthBp || bp < 0) {
        return null;
      }
      final @NotNull var leGr = Node.splitNodeBp(this.root, 1 + bp, true);
      return Objects.requireNonNull(Node.rightmost(leGr.left()), "Segment was not none but its leftmost is None").scaffoldDescriptor;
    } finally {
      this.rootLock.readLock().unlock();
    }
  }

  public @NotNull ScaffoldDescriptor.ScaffoldBordersBP extendBordersToScaffolds(final long queriedStartBp, final long queriedEndBp) {
    try {
      var leftBp = queriedStartBp;
      var rightBp = queriedEndBp;
      this.rootLock.readLock().lock();
      final @Nullable var optionalLeftScaffoldDescriptor = getScaffoldAtBp(queriedStartBp);
      if (optionalLeftScaffoldDescriptor != null) {
        final var lr = Node.splitNodeBp(this.root, queriedStartBp, false);
        leftBp = (lr.left() != null) ? (lr.left().subtreeLengthBp) : (0L);
        final @NotNull var leftScaffold = Objects.requireNonNull(Node.leftmost(lr.right()));
        assert (leftScaffold.scaffoldDescriptor != null) : "Borders were extended but no scaffold present to the left?";
      }
      final @Nullable var optionalRightScaffoldDescriptor = getScaffoldAtBp(queriedEndBp);
      if (optionalRightScaffoldDescriptor != null) {
        final @NotNull var leGr = Node.splitNodeBp(this.root, queriedEndBp, true);
        rightBp = (leGr.left() != null) ? (leGr.left().subtreeLengthBp) : 0L;
        final @NotNull var rightScaffold = Objects.requireNonNull(Node.rightmost(leGr.left()));
        assert (rightScaffold.scaffoldDescriptor != null) : "Borders were extended but no scaffold present to the right?";
      }
      return new ScaffoldDescriptor.ScaffoldBordersBP(leftBp, rightBp);
    } finally {
      this.rootLock.readLock().unlock();
    }
  }

  public @NotNull ScaffoldDescriptor.ScaffoldBordersBP getScaffoldBordersAtBp(final long queriedBp) {
    try {
      var leftBp = queriedBp;
      var rightBp = queriedBp;
      this.rootLock.readLock().lock();

      final var es = Node.expose(this.root, leftBp, 1 + rightBp);
      final var sg = es.segment();
      if ((sg != null) && sg.scaffoldDescriptor != null) {
        leftBp = Optional.ofNullable(es.less()).map(n -> n.subtreeLengthBp).orElse(0L);
        rightBp = leftBp + sg.subtreeLengthBp;
      }
      return new ScaffoldDescriptor.ScaffoldBordersBP(leftBp, rightBp);
    } finally {
      this.rootLock.readLock().unlock();
    }
  }

  /**
   * @param startBp           Starting BP of new scaffold.
   * @param endBp             Last (exclusive) BP of new scaffold.
   * @param scaffoldGenerator A function that accepts one long (minimum unused scaffold ID) and returns new scaffold descriptor. In case this argument is {@code null}, scaffold is constructed automatically with spacer length of 1000 and this ID.
   * @return Descriptor of newly created scaffold after adding it to the tree.
   */
  public ScaffoldDescriptor rescaffold(final long startBp, final long endBp, final @Nullable LongFunction<@NotNull ScaffoldDescriptor> scaffoldGenerator) {
    if (startBp > endBp) {
      return rescaffold(endBp, startBp, scaffoldGenerator);
    }

    try {
      this.rootLock.writeLock().lock();
      ++this.rootScaffoldIdCounter;
      final var oldAssemblyLength = this.root.subtreeLengthBp;

      final @NotNull var extendedBorders = extendBordersToScaffolds(startBp, endBp);
      final @NotNull var es = Node.expose(this.root, extendedBorders.startBP(), extendedBorders.endBP());
      final @NotNull ScaffoldDescriptor newScaffoldDescriptor;
      if (scaffoldGenerator == null) {
        newScaffoldDescriptor = new ScaffoldDescriptor(this.rootScaffoldIdCounter, String.format("scaffold_auto_%d", this.rootScaffoldIdCounter), 1000);
      } else {
        newScaffoldDescriptor = scaffoldGenerator.apply(this.rootScaffoldIdCounter);
      }
      this.rootScaffoldIdCounter = Long.max(this.rootScaffoldIdCounter, newScaffoldDescriptor.scaffoldId());

      final @NotNull var newScaffoldNode = new ScaffoldTree.Node(
        newScaffoldDescriptor,
        es.segment().subtreeLengthBp,
        es.segment().yPriority,
        null,
        null,
        1,
        es.segment().subtreeLengthBp,
        false
      );

      commitExposedSegment(new Node.ExposedSegment(es.less(), newScaffoldNode, es.greater()));
      assert (oldAssemblyLength == this.root.subtreeLengthBp) : "Assembly length changed after rescaffolding a region?";
      return newScaffoldDescriptor;
    } finally {
      this.rootLock.writeLock().unlock();
    }
  }

  public void unscaffold(final long startBp, final long endBp) {
    if (startBp > endBp) {
      unscaffold(endBp, startBp);
      return;
    }

    try {
      this.rootLock.writeLock().lock();
      ++this.rootScaffoldIdCounter;
      final var oldAssemblyLength = this.root.subtreeLengthBp;

      final @NotNull var extendedBorders = extendBordersToScaffolds(startBp, endBp);
      final @NotNull var es = Node.expose(this.root, extendedBorders.startBP(), extendedBorders.endBP());


      final @NotNull var emptyNode = new ScaffoldTree.Node(
        null,
        es.segment().subtreeLengthBp,
        es.segment().yPriority,
        null,
        null,
        1,
        es.segment().subtreeLengthBp,
        false
      );

      commitExposedSegment(new Node.ExposedSegment(es.less(), emptyNode, es.greater()));
      assert (oldAssemblyLength == this.root.subtreeLengthBp) : "Assembly length changed after unscaffolding a region?";
    } finally {
      this.rootLock.writeLock().unlock();
    }
  }

  public void reverseSelectionRange(final long startBp, final long endBp) {
    if (startBp > endBp) {
      reverseSelectionRange(endBp, startBp);
      return;
    }

    try {
      this.rootLock.writeLock().lock();
      final var oldAssemblyLength = this.root.subtreeLengthBp;

      final @NotNull var extendedBorders = extendBordersToScaffolds(startBp, endBp);
      final @NotNull var es = Node.expose(this.root, extendedBorders.startBP(), extendedBorders.endBP());

      final var reversedSegment = es.segment().cloneBuilder().needsChangingDirection(true).build().push();

      commitExposedSegment(new Node.ExposedSegment(es.less(), reversedSegment, es.greater()));
      assert (oldAssemblyLength == this.root.subtreeLengthBp) : "Assembly length changed after moving a region?";
    } finally {
      this.rootLock.writeLock().unlock();
    }
  }

  public void moveSelectionRange(final long startBp, final long endBp, final long targetStartBp) {
    if (startBp > endBp) {
      moveSelectionRange(endBp, startBp, targetStartBp);
      return;
    }

    try {
      this.rootLock.writeLock().lock();
      final var oldAssemblyLength = this.root.subtreeLengthBp;

      final @NotNull var extendedBorders = extendBordersToScaffolds(startBp, endBp);
      final @NotNull var es = Node.expose(this.root, extendedBorders.startBP(), extendedBorders.endBP());

      final @NotNull var tmp = Node.mergeNodes(new Node.SplitResult(es.less(), es.greater()));
      final @NotNull var nlnr = Node.splitNodeBp(tmp, targetStartBp, false);

      commitExposedSegment(new Node.ExposedSegment(nlnr.left(), es.segment(), nlnr.right()));
      assert (oldAssemblyLength == this.root.subtreeLengthBp) : "Assembly length changed after moving a region?";
    } finally {
      this.rootLock.writeLock().unlock();
    }
  }

  public void traverse(final @NotNull Consumer<@NotNull Node> traverseFn) {
    try {
      this.rootLock.readLock().lock();
      Node.traverseNode(
        this.root,
        traverseFn
      );
    } finally {
      this.rootLock.readLock().unlock();
    }
  }

  public List<ScaffoldTuple> getScaffoldList() {
    final List<ScaffoldTuple> descriptors = new ArrayList<>();
    final long[] position = {0L};

    this.traverse(node -> {
      if (node.scaffoldDescriptor != null) {
        descriptors.add(new ScaffoldTuple(
          node.scaffoldDescriptor,
          new ScaffoldDescriptor.ScaffoldBordersBP(position[0], position[0] + node.nodeLengthBp)
        ));
      }
      position[0] += node.nodeLengthBp;
    });

    return descriptors;
  }

  public void removeSegmentFromAssembly(final long startBpIncl, final long endBpExcl) {
    if (startBpIncl > endBpExcl) {
      unscaffold(endBpExcl, startBpIncl);
      return;
    }

    try {
      this.rootLock.writeLock().lock();
      final var oldAssemblyLength = this.root.subtreeLengthBp;
      final @NotNull var es = Node.expose(this.root, startBpIncl, endBpExcl);
      assert (es.segment() != null) : "Requested segment is not covered by scaffold tree??";
      final @NotNull var segment = Node.optimizeEmptySpace(es.segment());

      final int[] scaffoldDescriptorCount = {0};

      Node.traverseNode(segment, node -> {
        if (node.scaffoldDescriptor != null) {
          ++scaffoldDescriptorCount[0];
        }
      });

      assert (scaffoldDescriptorCount[0] <= 1) : "At most one scaffold could cover the splicing area";
      assert (segment.left == null) : "Exposed more than one nodes and there is left??";
      assert (segment.right == null) : "Exposed more than one nodes and there is right??";

      final @NotNull var newSegment = segment.cloneBuilder().scaffoldDescriptor(null).build().push().updateSizes(); //.nodeLengthBp(segment.nodeLengthBp - (endBpExcl - startBpIncl)).subtreeLengthBp(segment.nodeLengthBp - (endBpExcl - startBpIncl)).build();

      commitExposedSegment(new Node.ExposedSegment(es.less(), newSegment, es.greater()));
//      assert (oldAssemblyLength == this.root.subtreeLengthBp) : "Assembly length changed after removing a region?";
    } finally {
      this.rootLock.writeLock().unlock();
    }
  }

  @Builder
  public static class Node implements Iterable<ScaffoldTree.Node> {
    final @Nullable ScaffoldDescriptor scaffoldDescriptor;
    final long nodeLengthBp;
    final long yPriority;
    final Node left;
    final Node right;
    final long subtreeScaffoldCount;
    final long subtreeLengthBp;

    final boolean needsChangingDirection;

    public static Node.SplitResult splitNodeBp(final Node t, final long expectedLeftSize, final boolean includeEqualToTheLeft) {
      if (t == null) {
        return new Node.SplitResult(null, null);
      }
      if (expectedLeftSize <= 0) {
        return new Node.SplitResult(null, t);
      }
      final var newTree = t.push().updateSizes();
      final long leftSubtreeLength;
      if (newTree.left != null) {
        leftSubtreeLength = newTree.left.subtreeLengthBp;
      } else {
        leftSubtreeLength = 0L;
      }

      if (expectedLeftSize <= leftSubtreeLength) {
        final var sp = splitNodeBp(newTree.left, expectedLeftSize, includeEqualToTheLeft);
        final var newRightSplit = newTree.cloneBuilder().left(sp.right).build().updateSizes();
        assert (newTree.subtreeLengthBp == (Optional.ofNullable(sp.left).map(n -> n.subtreeLengthBp).orElse(0L) + Optional.ofNullable(newRightSplit).map(n -> n.subtreeLengthBp).orElse(0L))) : "In first split case subtree length has changed??";
        return new Node.SplitResult(sp.left, newRightSplit);
      } else {
        final var nodeLength = newTree.nodeLengthBp;
        if (expectedLeftSize < leftSubtreeLength + nodeLength) {
          if (newTree.scaffoldDescriptor != null) {
            if (includeEqualToTheLeft) {
              final var new_t = newTree.cloneBuilder().right(null).build().updateSizes();
              assert (newTree.subtreeLengthBp == (new_t.subtreeLengthBp + Optional.ofNullable(newTree.right).map(n -> n.subtreeLengthBp).orElse(0L))) : "In second-include-left split case subtree length has changed??";
              return new Node.SplitResult(new_t, newTree.right);
            } else {
              final var new_t = newTree.cloneBuilder().left(null).build().updateSizes();
              assert (newTree.subtreeLengthBp == (Optional.ofNullable(newTree.left).map(n -> n.subtreeLengthBp).orElse(0L) + new_t.updateSizes().subtreeLengthBp)) : "In second-non-include-left split case subtree length has changed??";
              return new Node.SplitResult(newTree.left, new_t.updateSizes());
            }
          } else {
            final var t1 = newTree.cloneBuilder().nodeLengthBp(expectedLeftSize - leftSubtreeLength).right(null).build().updateSizes();
            final var rightPartLengthBp = newTree.subtreeLengthBp - t1.subtreeLengthBp;
            final Node t2;
            if (rightPartLengthBp > 0) {
              t2 = newTree.cloneBuilder().nodeLengthBp(rightPartLengthBp).left(null).yPriority(rnd.nextLong(Long.min(1L + newTree.yPriority, Long.MAX_VALUE - 1), Long.MAX_VALUE)).build().updateSizes();
            } else {
              t2 = newTree.right;
            }
            final var t1u = t1.updateSizes();
            final var t2u = t2.updateSizes();
            assert (newTree.subtreeLengthBp == (Optional.ofNullable(t1u).map(n -> n.subtreeLengthBp).orElse(0L) + Optional.ofNullable(t2u).map(n -> n.subtreeLengthBp).orElse(0L))) : "In second-non-scaffold split case subtree length has changed??";
            return new Node.SplitResult(t1u, t2u);
          }
        } else {
          final var sp = splitNodeBp(newTree.right, expectedLeftSize - (leftSubtreeLength + nodeLength), includeEqualToTheLeft);
          final var new_t = newTree.cloneBuilder().right(sp.left).build().updateSizes();
          assert (newTree.subtreeLengthBp == (Optional.ofNullable(new_t).map(n -> n.subtreeLengthBp).orElse(0L) + Optional.ofNullable(sp.right).map(n -> n.subtreeLengthBp).orElse(0L))) : "In second-non-scaffold split case subtree length has changed??";
          return new Node.SplitResult(new_t, sp.right);
        }
      }
    }

    public static Node optimizeEmptySpace(final Node node) {
      return optimizeEmptySpace(node, false);
    }

    public static Node optimizeEmptySpace(final Node node, final boolean recursive) {
      if (node == null || node.scaffoldDescriptor != null) {
        return node;
      }

      var t = node;

      if (t.left != null && t.left.scaffoldDescriptor == null) {
        final Node son;
        if (recursive) {
          son = optimizeEmptySpace(t.left, recursive);
        } else {
          son = t.left;
        }
        if (son.right == null) {
          t = t.cloneBuilder().nodeLengthBp(t.nodeLengthBp + son.nodeLengthBp).left(son.left).build().updateSizes();
        }
        assert (node.subtreeLengthBp == (t.subtreeLengthBp)) : "Subtree length has changed after left space optimization??";
      }

      if (t.right != null && t.right.scaffoldDescriptor == null) {
        final Node son;
        if (recursive) {
          son = optimizeEmptySpace(t.right, recursive);
        } else {
          son = t.right;
        }
        if (son.left == null) {
          t = t.cloneBuilder().nodeLengthBp(t.nodeLengthBp + son.nodeLengthBp).right(son.right).build().updateSizes();
        }
        assert (node.subtreeLengthBp == (t.subtreeLengthBp)) : "Subtree length has changed after right space optimization??";
      }

      final var result = t.updateSizes();
      assert (node.subtreeLengthBp == result.subtreeLengthBp) : "Subtree length has changed after empty space optimization??";
      return result;
    }

    public static Node mergeNodes(final Node.SplitResult sp) {
      return mergeNodes(sp, false);
    }

    public static Node mergeNodes(final Node.SplitResult sp, boolean recursiveEmptySpaceOptimization) {
      if (sp.left == null) {
        return sp.right;
      }
      if (sp.right == null) {
        return sp.left;
      }

      final var t1 = sp.left.push();
      final var t2 = sp.right.push();

      if (t1.yPriority > t2.yPriority) {
        return optimizeEmptySpace(t1.cloneBuilder().right(mergeNodes(new SplitResult(t1.right, t2))).build().updateSizes(), recursiveEmptySpaceOptimization);
      } else {
        return optimizeEmptySpace(t2.cloneBuilder().left(mergeNodes(new SplitResult(t1, t2.left))).build().updateSizes(), recursiveEmptySpaceOptimization);
      }
    }

    public static Node.ExposedSegment expose(final Node t, final long startBpIncl, final long endBpExcl) {
      if (t == null) {
        return new ExposedSegment(null, null, null);
      }

      final var toBp = Long.min(endBpExcl, t.subtreeLengthBp);
      final var fromBp = Long.max(0L, startBpIncl);

      final var leGr = splitNodeBp(t, toBp, true);
      final var lsSg = splitNodeBp(leGr.left(), fromBp, false); // TODO: Python code had true here as well, maybe that was a bug

      assert ((t.subtreeLengthBp) == (
        Optional.ofNullable(lsSg.left()).map(e -> e.subtreeLengthBp).orElse(0L)
          + Optional.ofNullable(lsSg.right()).map(e -> e.subtreeLengthBp).orElse(0L)
          + Optional.ofNullable(leGr.right()).map(e -> e.subtreeLengthBp).orElse(0L)
      )
      ) : "Total length of exposed segments is greater than source one?";

      return new ExposedSegment(lsSg.left(), lsSg.right(), leGr.right());
    }

    public static void traverseNode(final Node node, final @NotNull Consumer<@NotNull Node> f) {
      if (node != null) {
        final var newNode = node.push();
        traverseNode(newNode.left, f);
        f.accept(newNode);
        traverseNode(newNode.right, f);
      }
    }

    public static @Nullable Node leftmost(final @Nullable Node t) {
      if (t == null) {
        return null;
      }
      var node = t;
      while (true) {
        final @Nullable Node candidate;
        if (node.needsChangingDirection) {
          candidate = node.right;
        } else {
          candidate = node.left;
        }
        if (candidate == null) {
          return node;
        }
        node = candidate;
      }
    }

    public static @Nullable Node rightmost(final @Nullable Node t) {
      if (t == null) {
        return null;
      }
      var node = t;
      while (true) {
        final @Nullable Node candidate;
        if (!node.needsChangingDirection) {
          candidate = node.right;
        } else {
          candidate = node.left;
        }
        if (candidate == null) {
          return node;
        }
        node = candidate;
      }
    }

    public @NotNull Node leftmost() {
      return leftmost(this);
    }

    public @NotNull Node rightmost() {
      return rightmost(this);
    }


    @Override
    public Iterator<Node> iterator() {
      final Node node = this;
      return new Iterator<Node>() {
        final boolean rightVisited = (right == null);
        boolean leftVisited = (left == null);
        boolean nodeVisited = false;

        @Override
        public boolean hasNext() {
          if (!leftVisited) {
            return leftIterator.hasNext();
          } else if (!nodeVisited) {
            return true;
          } else if (!rightVisited) {
            return rightIterator.hasNext();
          } else {
            return false;
          }
        }

        @Override
        public Node next() {
          if (!leftVisited) {
            assert (leftIterator != null) : "Cannot have leftVisited = false and no iterator";
            final var res = leftIterator.next();
            if (!leftIterator.hasNext()) {
              leftVisited = true;
            }
          } else if (!nodeVisited) {
            nodeVisited = true;
            return node;
          } else if (!rightVisited) {
            return rightIterator.next();
          }
          throw new NoSuchElementException();
        }

        final Iterator<Node> leftIterator = (left != null) ? left.iterator() : null;


        final Iterator<Node> rightIterator = (right != null) ? right.iterator() : null;
      };
    }

    public Node push() {
      if (this.needsChangingDirection) {
        final Node newLeft;
        final Node newRight;
        if (this.left != null) {
          newRight = this.left.cloneBuilder().needsChangingDirection(!this.left.needsChangingDirection).left(this.left.right).right(this.left.left).build();
        } else {
          newRight = null;
        }

        if (this.right != null) {
          newLeft = this.right.cloneBuilder().needsChangingDirection(!this.right.needsChangingDirection).left(this.right.right).right(this.right.left).build();
        } else {
          newLeft = null;
        }

        return this.cloneBuilder().left(newLeft).right(newRight).needsChangingDirection(false).build();
      }
      return this;
    }

    public Node updateSizes() {
      final var newSubtreeScaffoldCount = 1 + ((this.left != null) ? this.left.subtreeScaffoldCount : 0L) + ((this.right != null) ? this.right.subtreeScaffoldCount : 0L);
      final var newLengthBp = this.nodeLengthBp + ((this.left != null) ? this.left.subtreeLengthBp : 0L) + ((this.right != null) ? this.right.subtreeLengthBp : 0L);
      return this.cloneBuilder().subtreeCount(newSubtreeScaffoldCount).subtreeLengthBp(newLengthBp).build();
    }

    public NodeCloneBuilder cloneBuilder() {
      return new NodeCloneBuilder(this);
    }

    public record ExposedSegment(Node less, Node segment, Node greater) {

    }

    public record SplitResult(Node left, Node right) {
    }

    private static class NodeCloneBuilder {
      private @Nullable ScaffoldDescriptor scaffoldDescriptor;
      private long yPriority;
      private long nodeLengthBp;
      private Node left;
      private Node right;
      private long subtreeScaffoldCount;
      private long subtreeLengthBp;
      private boolean needsChangingDirection;

      public NodeCloneBuilder(final Node base) {
        this.scaffoldDescriptor = base.scaffoldDescriptor;
        this.yPriority = base.yPriority;
        this.left = base.left;
        this.right = base.right;
        this.subtreeScaffoldCount = base.subtreeScaffoldCount;
        this.subtreeLengthBp = base.subtreeLengthBp;
        this.needsChangingDirection = base.needsChangingDirection;
        this.nodeLengthBp = base.nodeLengthBp;
      }

      public Node.NodeCloneBuilder scaffoldDescriptor(final @Nullable ScaffoldDescriptor scaffoldDescriptor) {
        this.scaffoldDescriptor = scaffoldDescriptor;
        return this;
      }

      public Node.NodeCloneBuilder yPriority(final long yPriority) {
        this.yPriority = yPriority;
        return this;
      }

      public Node.NodeCloneBuilder nodeLengthBp(final long nodeLengthBp) {
        this.nodeLengthBp = nodeLengthBp;
        return this;
      }

      public Node.NodeCloneBuilder left(final @Nullable Node left) {
        this.left = left;
        return this;
      }

      public Node.NodeCloneBuilder right(final @Nullable Node right) {
        this.right = right;
        return this;
      }

      public Node.NodeCloneBuilder subtreeCount(final long subtreeScaffoldCount) {
        this.subtreeScaffoldCount = subtreeScaffoldCount;
        return this;
      }

      public Node.NodeCloneBuilder subtreeLengthBp(final long subtreeLengthBp) {
        this.subtreeLengthBp = subtreeLengthBp;
        return this;
      }


      public Node.NodeCloneBuilder needsChangingDirection(final boolean needsChangingDirection) {
        this.needsChangingDirection = needsChangingDirection;
        return this;
      }


      public Node build() {
        return new Node(this.scaffoldDescriptor, this.nodeLengthBp, this.yPriority, this.left, this.right, this.subtreeScaffoldCount, this.subtreeLengthBp, this.needsChangingDirection);
      }
    }
  }

  public record ScaffoldTuple(
    @Nullable ScaffoldDescriptor scaffoldDescriptor,
    @NotNull ScaffoldDescriptor.ScaffoldBordersBP scaffoldBordersBP
  ) {
    public long getLengthBp() {
      return this.scaffoldBordersBP.getLengthBp();
    }
  }


}
