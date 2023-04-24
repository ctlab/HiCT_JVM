package ru.itmo.ctlab.hict.hict_library.trees;

import lombok.Builder;
import lombok.NonNull;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    public void appendContig(final @NonNull ContigDescriptor contigDescriptor, final @NonNull ContigDirection contigDirection) {
        final Node newNode = Node.createNodeFromDescriptor(contigDescriptor, contigDirection);
        try {
            this.rootLock.writeLock().lock();
            this.root = Node.mergeNodes(new Node.SplitResult(this.root, newNode));
        } finally {
            this.rootLock.writeLock().unlock();
        }
    }

    public @NonNull Node.ExposedSegment expose(final long resolution, final long startIncl, final long endExcl, final @NonNull QueryLengthUnit units) {
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
    public static class Node implements Iterable<Node> {
        final @NonNull ContigDescriptor contigDescriptor;
        final long yPriority;
        final Node left;
        final Node right;
        final long subtreeCount;
        final @NonNull Map<@NonNull Long, @NonNull Long> subtreeLengthBins;
        final @NonNull Map<@NonNull Long, @NonNull Long> subtreeLengthPixels;
        final boolean needsChangingDirection;
        final @NonNull ContigDirection contigDirection;


        public static @NonNull Node createNodeFromDescriptor(final @NonNull ContigDescriptor contigDescriptor, final @NonNull ContigDirection contigDirection) {
            return Node.builder().contigDescriptor(contigDescriptor).subtreeCount(1L).needsChangingDirection(false).subtreeLengthBins(contigDescriptor.getLengthBinsAtResolution()).subtreeLengthPixels(contigDescriptor.getLengthBinsAtResolution().entrySet().parallelStream().map(resolutionToLengthBins -> {
                final @NonNull var resolution = resolutionToLengthBins.getKey();
                final long length;
                if (contigDescriptor.getPresenceAtResolution().get(resolution).equals(ContigHideType.SHOWN)) {
                    length = resolutionToLengthBins.getValue();
                } else {
                    length = 0L;
                }
                return Map.entry(resolution, length);
            }).collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue))).yPriority(rnd.nextLong()).contigDirection(contigDirection).left(null).right(null).build();
        }

        public static SplitResult splitNodeByLength(final long resolution, final Node t, final long k, final boolean includeEqualToTheLeft, final boolean excludeHiddenContigs) {
            if (t == null) {
                return new SplitResult(null, null);
            }
            if (k <= 0) {
                return new SplitResult(null, t);
            }
            final var newTree = t.push().updateSizes();
            final long leftLength;
            if (newTree.left != null) {
                if (excludeHiddenContigs) {
                    leftLength = newTree.left.subtreeLengthPixels.get(resolution);
                } else {
                    leftLength = newTree.left.subtreeLengthBins.get(resolution);
                }
            } else {
                leftLength = 0L;
            }

            if (k <= leftLength) {
                final @NonNull var sp = splitNodeByLength(resolution, newTree.left, k, includeEqualToTheLeft, excludeHiddenContigs);

                final var newRightSplit = newTree.cloneBuilder().left(sp.right).build().updateSizes();

                return new SplitResult(sp.left, newRightSplit);
            } else {
                final var nodeLength = newTree.contigDescriptor.getLengthInUnits(excludeHiddenContigs ? QueryLengthUnit.PIXELS : QueryLengthUnit.BINS, resolution);
                if (k < leftLength + nodeLength) {
                    if (includeEqualToTheLeft) {
                        final var new_t = newTree.cloneBuilder().right(null).build().updateSizes();
                        return new SplitResult(new_t, newTree.right);
                    } else {
                        final var new_t = newTree.cloneBuilder().left(null).build().updateSizes();
                        return new SplitResult(newTree.left, new_t);
                    }
                } else {
                    final var sp = splitNodeByLength(resolution, newTree.right, k - (leftLength + nodeLength), includeEqualToTheLeft, excludeHiddenContigs);
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
                return new SplitResult(
                        sp.left,
                        newTree.cloneBuilder().left(sp.right).build().updateSizes()
                );
            } else {
                final var sp = splitNodeByCount(newTree.right, k - leftCount - 1);
                return new SplitResult(
                        newTree.cloneBuilder().right(sp.left).build().updateSizes(),
                        sp.right
                );
            }
        }

        public static @NonNull SplitResult splitNodeByLength(final long resolution, final Node t, final long k, final boolean includeEqualToTheLeft, final @NonNull QueryLengthUnit units) {
            return switch (units) {
                case BASE_PAIRS, BINS -> splitNodeByLength(resolution, t, k, includeEqualToTheLeft, false);
                case PIXELS -> splitNodeByLength(resolution, t, k, includeEqualToTheLeft, true);
            };
        }

        public static @NonNull ExposedSegment exposeNodeByLength(final Node node, final long resolution, final long startIncl, final long endExcl, final @NonNull QueryLengthUnit units) {
            if (node != null) {
                final var sp1 = splitNodeByLength(resolution, node, endExcl, true, units);
                final var sp2 = splitNodeByLength(resolution, sp1.left, startIncl, false, units);
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

        public long getSubtreeLengthInUnits(final @NonNull QueryLengthUnit units, final long resolution) {
            return switch (units) {
                case BASE_PAIRS -> this.subtreeLengthBins.get(0L);
                case PIXELS -> this.subtreeLengthPixels.get(resolution);
                case BINS -> this.subtreeLengthBins.get(resolution);
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
            final var newLengthBins = this.subtreeLengthBins.keySet().parallelStream().map(resolution -> {
                final long contigLength = this.contigDescriptor.getLengthBinsAtResolution().get(resolution);
                final long leftLength = (this.left != null) ? this.left.subtreeLengthBins.get(resolution) : 0L;
                final long rightLength = (this.right != null) ? this.right.subtreeLengthBins.get(resolution) : 0L;
                return Map.entry(resolution, contigLength + leftLength + rightLength);
            }).collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
            final var newLengthPixels = this.subtreeLengthBins.keySet().parallelStream().map(resolution -> {
                final long contigLength = (this.contigDescriptor.getPresenceAtResolution().get(resolution).equals(ContigHideType.SHOWN)) ? this.contigDescriptor.getLengthBinsAtResolution().get(resolution) : 0L;
                final long leftLength = (this.left != null) ? this.left.subtreeLengthPixels.get(resolution) : 0L;
                final long rightLength = (this.right != null) ? this.right.subtreeLengthPixels.get(resolution) : 0L;
                return Map.entry(resolution, contigLength + leftLength + rightLength);
            }).collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));

            return this.cloneBuilder().subtreeCount(newSubtreeCount).subtreeLengthBins(newLengthBins).subtreeLengthPixels(newLengthPixels).build();
        }

        public ContigDirection getTrueDirection() {
            return (this.needsChangingDirection) ? this.contigDirection.inverse() : this.contigDirection;
        }

        public @NonNull SplitResult splitByLength(final long resolution, final long k, final boolean includeEqualToTheLeft, final boolean excludeHiddenContigs) {
            return splitNodeByLength(resolution, this, k, includeEqualToTheLeft, excludeHiddenContigs);
        }

        public @NonNull SplitResult splitByLength(final long resolution, final long k, final boolean includeEqualToTheLeft, final @NonNull QueryLengthUnit units) {
            return splitNodeByLength(resolution, this, k, includeEqualToTheLeft, units);
        }

        public @NonNull SplitResult splitByCount(final long k) {
            return splitNodeByCount(this, k);
        }

        public void traverse(final Consumer<Node> f) {
            traverseNode(this, f);
        }

        public @NonNull ExposedSegment expose(final long resolution, final long startIncl, final long endExcl, final @NonNull QueryLengthUnit units) {
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
                }                final Iterator<Node> leftIterator = (left != null) ? left.iterator() : null;

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
                }                final Iterator<Node> rightIterator = (right != null) ? right.iterator() : null;




            };
        }

        private static class NodeCloneBuilder {
            private @NonNull ContigDescriptor contigDescriptor;
            private long yPriority;
            private Node left;
            private Node right;
            private long subtreeCount;
            private @NonNull Map<@NonNull Long, @NonNull Long> subtreeLengthBins;
            private @NonNull Map<@NonNull Long, @NonNull Long> subtreeLengthPixels;
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

            public NodeCloneBuilder subtreeLengthBins(final @NonNull Map<@NonNull Long, @NonNull Long> subtreeLengthBins) {
                this.subtreeLengthBins = subtreeLengthBins;
                return this;
            }

            public NodeCloneBuilder subtreeLengthPixels(final @NonNull Map<@NonNull Long, @NonNull Long> subtreeLengthPixels) {
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
}
