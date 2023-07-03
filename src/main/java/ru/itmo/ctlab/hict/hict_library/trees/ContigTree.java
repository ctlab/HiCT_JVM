package ru.itmo.ctlab.hict.hict_library.trees;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class ContigTree implements Iterable<ContigTree.Node> {
  private static final Random rnd = new Random();
  private final @NonNull ReadWriteLock rootLock = new ReentrantReadWriteLock();
  private Node root;

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

  public void traverse(final @NotNull @NonNull Consumer<@NotNull @NonNull Node> traverseFn) {
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

  public List<ContigTuple> getContigList() {
    final List<ContigTuple> descriptors = new ArrayList<>();

    this.traverse(node -> {
      descriptors.add(new ContigTuple(
        node.contigDescriptor,
        node.getTrueDirection()
      ));

    });

    return descriptors;
  }

  public long getLengthInUnits(final @NotNull QueryLengthUnit units, final @NotNull ResolutionDescriptor resolution) {
    try {
      this.rootLock.readLock().lock();
      return this.root.getSubtreeLengthInUnits(units, resolution);
    } finally {
      this.rootLock.readLock().unlock();
    }
  }

  public void appendContig(final @NonNull ContigDescriptor contigDescriptor, final @NonNull ContigDirection contigDirection) {
    final Node newNode = Node.createNodeFromDescriptor(contigDescriptor, contigDirection);
    try {
      this.rootLock.writeLock().lock();
      this.root = Node.mergeNodes(new Node.SplitResult(this.root, newNode));
    } finally {
      this.rootLock.writeLock().unlock();
    }
  }

  public @NonNull Node.ExposedSegment expose(final @NotNull ResolutionDescriptor resolution, final long startIncl, final long endExcl, final @NonNull QueryLengthUnit units) {
    final Node rootSnapshot;
    try {
      this.rootLock.readLock().lock();
      rootSnapshot = this.root;
    } finally {
      this.rootLock.readLock().unlock();
    }
    return Node.exposeNodeByLength(rootSnapshot, resolution, startIncl, endExcl, units);
  }

  @Builder
  @Getter
  public static class Node implements Iterable<Node> {
    final @NonNull ContigDescriptor contigDescriptor;
    final long yPriority;
    final Node left;
    final Node right;
    final long subtreeCount;
    final long[] subtreeLengthBins;
    final long[] subtreeLengthPixels;
    final boolean needsChangingDirection;
    final @NonNull ContigDirection contigDirection;


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


    public @NotNull @NonNull Node leftmost() {
      return leftmost(this);
    }

    public @NotNull @NonNull Node rightmost() {
      return rightmost(this);
    }


    public static @NonNull Node createNodeFromDescriptor(final @NonNull ContigDescriptor contigDescriptor, final @NonNull ContigDirection contigDirection) {
      final var resolutionCount = contigDescriptor.getLengthBinsAtResolution().length;
      return Node.builder().contigDescriptor(contigDescriptor).subtreeCount(1L).needsChangingDirection(false).subtreeLengthBins(contigDescriptor.getLengthBinsAtResolution()).subtreeLengthPixels(IntStream.range(0, resolutionCount).mapToLong(resolutionIdx -> {
        final long length;
        if (contigDescriptor.getPresenceAtResolution().get(resolutionIdx).equals(ContigHideType.SHOWN)) {
          length = contigDescriptor.getLengthBinsAtResolution()[resolutionIdx];
        } else {
          length = 0L;
        }
        return length;
      }).toArray()).yPriority(rnd.nextLong()).contigDirection(contigDirection).left(null).right(null).build();
    }

    public static SplitResult splitNodeByLength(final ResolutionDescriptor resolutionDescriptor, final Node t, final long k, final boolean includeEqualToTheLeft, final boolean excludeHiddenContigs) {
      if (t == null) {
        return new SplitResult(null, null);
      }
      if (k <= 0) {
        return new SplitResult(null, t);
      }
      final var newTree = t.push().updateSizes();
      final int resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
      final long leftLength;
      if (newTree.left != null) {
        if (excludeHiddenContigs) {
          leftLength = newTree.left.subtreeLengthPixels[resolutionOrder];
        } else {
          leftLength = newTree.left.subtreeLengthBins[resolutionOrder];
        }
      } else {
        leftLength = 0L;
      }

      if (k <= leftLength) {
        final @NonNull var sp = splitNodeByLength(resolutionDescriptor, newTree.left, k, includeEqualToTheLeft, excludeHiddenContigs);

        final var newRightSplit = newTree.cloneBuilder().left(sp.right).build().updateSizes();

        return new SplitResult(sp.left, newRightSplit);
      } else {
        final var nodeLength = newTree.contigDescriptor.getLengthInUnits(excludeHiddenContigs ? QueryLengthUnit.PIXELS : QueryLengthUnit.BINS, resolutionDescriptor);
        if (k < leftLength + nodeLength) {
          if (includeEqualToTheLeft) {
            final var new_t = newTree.cloneBuilder().right(null).build().updateSizes();
            return new SplitResult(new_t, newTree.right);
          } else {
            final var new_t = newTree.cloneBuilder().left(null).build().updateSizes();
            return new SplitResult(newTree.left, new_t);
          }
        } else {
          final var sp = splitNodeByLength(resolutionDescriptor, newTree.right, k - (leftLength + nodeLength), includeEqualToTheLeft, excludeHiddenContigs);
          final var new_t = newTree.cloneBuilder().right(sp.left).build().updateSizes();
          return new SplitResult(new_t, sp.right);
        }
      }
    }

    public static Node mergeNodes(final @NonNull SplitResult sp) {
      if (sp.left == null) {
        return sp.right;
      }
      if (sp.right == null) {
        return sp.left;
      }

      final var t1 = sp.left.push();
      final var t2 = sp.right.push();

      if (t1.yPriority > t2.yPriority) {
        return t1.cloneBuilder().right(mergeNodes(new SplitResult(t1.right, t2))).build().updateSizes();
      } else {
        return t2.cloneBuilder().left(mergeNodes(new SplitResult(t1, t2.left))).build().updateSizes();
      }
    }

    public static @NonNull SplitResult splitNodeByCount(final Node t, final long k) {
      if (t == null) {
        return new SplitResult(null, null);
      }
      final var newTree = t.push();
      final var leftCount = (newTree.left != null) ? newTree.left.subtreeCount : 0L;

      if (leftCount >= k) {
        final var sp = splitNodeByCount(newTree.left, k);
        return new SplitResult(sp.left, newTree.cloneBuilder().left(sp.right).build().updateSizes());
      } else {
        final var sp = splitNodeByCount(newTree.right, k - leftCount - 1);
        return new SplitResult(newTree.cloneBuilder().right(sp.left).build().updateSizes(), sp.right);
      }
    }

    public static @NonNull SplitResult splitNodeByLength(final @NotNull ResolutionDescriptor resolutionDescriptor, final Node t, final long k, final boolean includeEqualToTheLeft, final @NonNull QueryLengthUnit units) {
      return switch (units) {
        case BASE_PAIRS, BINS -> splitNodeByLength(resolutionDescriptor, t, k, includeEqualToTheLeft, false);
        case PIXELS -> splitNodeByLength(resolutionDescriptor, t, k, includeEqualToTheLeft, true);
      };
    }

    public static @NonNull ExposedSegment exposeNodeByLength(final Node node, final @NotNull ResolutionDescriptor resolutionDescriptor, final long startIncl, final long endExcl, final @NonNull QueryLengthUnit units) {
      if (node != null) {
        final var sp1 = splitNodeByLength(resolutionDescriptor, node, endExcl - 1, true, units);
        final var sp2 = splitNodeByLength(resolutionDescriptor, sp1.left, startIncl, false, units);
        return new ExposedSegment(sp2.left, sp2.right, sp1.right);
      } else {
        return new ExposedSegment(null, null, null);
      }
    }

    public static void traverseNode(final Node node, final Consumer<Node> f) {
      if (node != null) {
        final var newNode = node.push();
        traverseNode(newNode.left, f);
        f.accept(newNode);
        traverseNode(newNode.right, f);
      }
    }

    public static void traverseNodeAtResolution(final Node node, final ResolutionDescriptor resolutionDescriptor, final Consumer<Node> f) {
      if (node != null) {
        final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
        final var newNode = node.push();
        traverseNodeAtResolution(newNode.left, resolutionDescriptor, f);
        if (ContigHideType.SHOWN.equals(newNode.getContigDescriptor().getPresenceAtResolution().get(resolutionOrder))) {
          f.accept(newNode);
        }
        traverseNodeAtResolution(newNode.right, resolutionDescriptor, f);
      }
    }

    public static @Nullable Node leftmostVisibleNode(final Node node, final ResolutionDescriptor resolutionDescriptor) {
      if (node != null) {
        final @Nullable var leftSonVisibleNode = leftmostVisibleNode(node.needsChangingDirection ? node.right : node.left, resolutionDescriptor);
        if (leftSonVisibleNode == null) {
          if (ContigHideType.SHOWN.equals(node.getContigDescriptor().getPresenceAtResolution().get(resolutionDescriptor.getResolutionOrderInArray()))) {
            return node;
          } else {
            return leftmostVisibleNode(node.needsChangingDirection ? node.left : node.right, resolutionDescriptor);
          }
        } else {
          return leftSonVisibleNode;
        }
      } else {
        return null;
      }
    }

    public static @Nullable Node rightmostVisibleNode(final Node node, final ResolutionDescriptor resolutionDescriptor) {
      if (node != null) {
        final @Nullable var rightSonVisibleNode = leftmostVisibleNode(node.needsChangingDirection ? node.left : node.right, resolutionDescriptor);
        if (rightSonVisibleNode == null) {
          if (ContigHideType.SHOWN.equals(node.getContigDescriptor().getPresenceAtResolution().get(resolutionDescriptor.getResolutionOrderInArray()))) {
            return node;
          } else {
            return leftmostVisibleNode(node.needsChangingDirection ? node.right : node.left, resolutionDescriptor);
          }
        } else {
          return rightSonVisibleNode;
        }
      } else {
        return null;
      }
    }

    public @NotNull @NonNull Node leftmostVisibleNode(final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor) {
      return leftmostVisibleNode(this, resolutionDescriptor);
    }

    public @NotNull @NonNull Node rightmostVisibleNode(final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor) {
      return rightmostVisibleNode(this, resolutionDescriptor);
    }


    public long getSubtreeLengthInUnits(final @NonNull QueryLengthUnit units, final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor) {
      final int resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
      return switch (units) {
        case BASE_PAIRS -> this.subtreeLengthBins[0];
        case PIXELS -> this.subtreeLengthPixels[resolutionOrder];
        case BINS -> this.subtreeLengthBins[resolutionOrder];
      };
    }

    public @NonNull Node push() {
      if (this.needsChangingDirection) {
        final Node newLeft;
        final Node newRight;
        if (this.left != null) {
          newLeft = this.left.cloneBuilder().needsChangingDirection(!this.left.needsChangingDirection).left(this.left.right).right(this.left.left).contigDirection(this.left.contigDirection.inverse()).build();
        } else {
          newLeft = null;
        }

        if (this.right != null) {
          newRight = this.right.cloneBuilder().needsChangingDirection(!this.right.needsChangingDirection).left(this.right.right).right(this.right.left).contigDirection(this.right.contigDirection.inverse()).build();
        } else {
          newRight = null;
        }

        return this.cloneBuilder().left(newLeft).right(newRight).needsChangingDirection(false).contigDirection(this.contigDirection.inverse()).build();
      }
      return this;
    }

    public @NonNull Node updateSizes() {
      final var newSubtreeCount = 1 + ((this.left != null) ? this.left.subtreeCount : 0L) + ((this.right != null) ? this.right.subtreeCount : 0L);

      final var resolutionCount = this.subtreeLengthBins.length;

      final var newLengthBins = this.contigDescriptor.getLengthBinsAtResolution().clone();
      if (this.left != null) {
        IntStream.range(0, resolutionCount).parallel().forEach(idx -> newLengthBins[idx] += this.left.subtreeLengthBins[idx]);
      }
      if (this.right != null) {
        IntStream.range(0, resolutionCount).parallel().forEach(idx -> newLengthBins[idx] += this.right.subtreeLengthBins[idx]);
      }

      final long[] newLengthPixels = this.contigDescriptor.getLengthBinsAtResolution().clone();

      IntStream.range(0, resolutionCount).parallel().forEach(resolutionOrder -> {
        if (!this.contigDescriptor.getPresenceAtResolution().get(resolutionOrder).equals(ContigHideType.SHOWN)) {
          newLengthPixels[resolutionOrder] = 0L;
        }
      });

      if (this.left != null) {
        IntStream.range(0, resolutionCount).parallel().forEach(idx -> newLengthPixels[idx] += this.left.subtreeLengthPixels[idx]);
      }
      if (this.right != null) {
        IntStream.range(0, resolutionCount).parallel().forEach(idx -> newLengthPixels[idx] += this.right.subtreeLengthPixels[idx]);
      }

      return this.cloneBuilder().subtreeCount(newSubtreeCount).subtreeLengthBins(newLengthBins).subtreeLengthPixels(newLengthPixels).build();
    }

    public ContigDirection getTrueDirection() {
      return (this.needsChangingDirection) ? this.contigDirection.inverse() : this.contigDirection;
    }

    public @NonNull SplitResult splitByLength(final @NotNull ResolutionDescriptor resolutionDescriptor, final long k, final boolean includeEqualToTheLeft, final boolean excludeHiddenContigs) {
      return splitNodeByLength(resolutionDescriptor, this, k, includeEqualToTheLeft, excludeHiddenContigs);
    }

    public @NonNull SplitResult splitByLength(final @NotNull ResolutionDescriptor resolutionDescriptor, final long k, final boolean includeEqualToTheLeft, final @NonNull QueryLengthUnit units) {
      return splitNodeByLength(resolutionDescriptor, this, k, includeEqualToTheLeft, units);
    }

    public @NonNull SplitResult splitByCount(final long k) {
      return splitNodeByCount(this, k);
    }

    public void traverse(final Consumer<Node> f) {
      traverseNode(this, f);
    }

    public @NonNull ExposedSegment expose(final @NotNull ResolutionDescriptor resolution, final long startIncl, final long endExcl, final @NonNull QueryLengthUnit units) {
      return exposeNodeByLength(this, resolution, startIncl, endExcl, units);
    }

    public NodeCloneBuilder cloneBuilder() {
      return new NodeCloneBuilder(this);
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

    private static class NodeCloneBuilder {
      private @NonNull ContigDescriptor contigDescriptor;
      private long yPriority;
      private Node left;
      private Node right;
      private long subtreeCount;
      private long[] subtreeLengthBins;
      private long[] subtreeLengthPixels;
      private boolean needsChangingDirection;
      private @NonNull ContigDirection contigDirection;

      public NodeCloneBuilder(final @NonNull Node base) {
        this.contigDescriptor = base.contigDescriptor;
        this.yPriority = base.yPriority;
        this.left = base.left;
        this.right = base.right;
        this.subtreeCount = base.subtreeCount;
        this.subtreeLengthBins = base.subtreeLengthBins;
        this.subtreeLengthPixels = base.subtreeLengthPixels;
        this.needsChangingDirection = base.needsChangingDirection;
        this.contigDirection = base.contigDirection;
      }

      public NodeCloneBuilder contigDescriptor(final @NonNull ContigDescriptor contigDescriptor) {
        this.contigDescriptor = contigDescriptor;
        return this;
      }

      public NodeCloneBuilder yPriority(final long yPriority) {
        this.yPriority = yPriority;
        return this;
      }

      public NodeCloneBuilder left(final Node left) {
        this.left = left;
        return this;
      }

      public NodeCloneBuilder right(final Node right) {
        this.right = right;
        return this;
      }

      public NodeCloneBuilder subtreeCount(final long subtreeCount) {
        this.subtreeCount = subtreeCount;
        return this;
      }

      public NodeCloneBuilder subtreeLengthBins(final long[] subtreeLengthBins) {
        this.subtreeLengthBins = subtreeLengthBins;
        return this;
      }

      public NodeCloneBuilder subtreeLengthPixels(final long[] subtreeLengthPixels) {
        this.subtreeLengthPixels = subtreeLengthPixels;
        return this;
      }

      public NodeCloneBuilder needsChangingDirection(final boolean needsChangingDirection) {
        this.needsChangingDirection = needsChangingDirection;
        return this;
      }

      public NodeCloneBuilder contigDirection(final @NonNull ContigDirection contigDirection) {
        this.contigDirection = contigDirection;
        return this;
      }

      public @NonNull Node build() {
        return new Node(this.contigDescriptor, this.yPriority, this.left, this.right, this.subtreeCount, this.subtreeLengthBins, this.subtreeLengthPixels, this.needsChangingDirection, this.contigDirection);
      }
    }

    public record ExposedSegment(Node less, Node segment, Node greater) {

    }

    public record SplitResult(Node left, Node right) {
    }
  }

  public record ContigTuple(ContigDescriptor descriptor, ContigDirection direction) {
  }
}
