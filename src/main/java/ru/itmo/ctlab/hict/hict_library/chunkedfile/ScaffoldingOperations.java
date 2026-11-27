/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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

package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.assembly.ExactContigSlice;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor
@Slf4j
public class ScaffoldingOperations {
  private @NotNull
  final ChunkedFile chunkedFile;

  public void reverseSelectionRangeBp(final long queriedStartBpIncl, final long queriedEndBpExcl) {
    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();
    final var contigLock = contigTree.getRootLock();
    final var scaffoldLock = scaffoldTree.getRootLock();
    try {
      contigLock.writeLock().lock();
      scaffoldLock.writeLock().lock();
      final var range = resolveSelectionWithScaffolds(queriedStartBpIncl, queriedEndBpExcl, contigTree, scaffoldTree);
      if (range == null) {
        return;
      }
      final var es = contigTree.expose(ResolutionDescriptor.fromResolutionOrder(0), range.startBp(), range.endBpExcl(), QueryLengthUnit.BASE_PAIRS);
      if (es.segment() != null) {
        final var newSegmentNode = es.segment().cloneBuilder().needsChangingDirection(!es.segment().isNeedsChangingDirection()).build().push().updateSizes();
        contigTree.commitExposedSegment(new ContigTree.Node.ExposedSegment(es.less(), newSegmentNode, es.greater()));
        scaffoldTree.reverseSelectionRange(range.startBp(), range.endBpExcl());
      }
    } finally {
      scaffoldLock.writeLock().unlock();
      contigLock.writeLock().unlock();
    }
  }

  public void moveSelectionRangeBp(final long queriedStartBpIncl, final long queriedEndBpExcl, final long targetStartBp) {
    moveSelectionRangeBp(queriedStartBpIncl, queriedEndBpExcl, targetStartBp, false);
  }

  public void moveSelectionRangeBp(final long queriedStartBpIncl, final long queriedEndBpExcl,
                                   final long targetStartBp, final boolean exactContig) {
    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();
    final var contigLock = contigTree.getRootLock();
    final var scaffoldLock = scaffoldTree.getRootLock();
    try {
      contigLock.writeLock().lock();
      scaffoldLock.writeLock().lock();
      final var range = exactContig
        ? requireExactContig(queriedStartBpIncl, queriedEndBpExcl, contigTree)
        : resolveSelectionWithScaffolds(queriedStartBpIncl, queriedEndBpExcl, contigTree, scaffoldTree);
      if (range == null) {
        return;
      }
      final var es = contigTree.expose(ResolutionDescriptor.fromResolutionOrder(0), range.startBp(), range.endBpExcl(), QueryLengthUnit.BASE_PAIRS);
      if (es.segment() != null) {
        final var leftSizeBp = Optional.ofNullable(es.less()).map(l -> l.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0))).orElse(0L);
        final var segmentSizeBp = es.segment().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
        final var totalAssemblyLengthBp = Optional.ofNullable(contigTree.getRoot())
          .map(root -> root.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0)))
          .orElse(0L);
        // Contig and scaffold trees must use exactly the same insertion boundary.
        // A click inside an object means before that whole object.
        if (targetStartBp < 0 || targetStartBp > totalAssemblyLengthBp) {
          throw new IllegalArgumentException("Move target must be within the assembly");
        }
        long targetBp = targetStartBp;
        if (targetBp >= range.startBp() && targetBp <= range.endBpExcl()) return;
        if (targetBp < totalAssemblyLengthBp) {
          final var target = resolveSelectionWithScaffolds(targetBp, targetBp + 1, contigTree, scaffoldTree);
          if (target != null) targetBp = target.startBp();
        }
        if (targetBp >= range.startBp() && targetBp <= range.endBpExcl()) return;
        final var adjustedTargetStartBp = calculateAdjustedTargetStartBp(targetBp, totalAssemblyLengthBp, leftSizeBp, segmentSizeBp);
        final var tmp = ContigTree.Node.mergeNodes(new ContigTree.Node.SplitResult(es.less(), es.greater()));
        final var nlnr = (tmp != null)
          ? tmp.splitByLength(ResolutionDescriptor.fromResolutionOrder(0), adjustedTargetStartBp, false, QueryLengthUnit.BASE_PAIRS)
          : new ContigTree.Node.SplitResult(null, null);
        if (exactContig) {
          scaffoldTree.moveExactContig(range.startBp(), range.endBpExcl(), adjustedTargetStartBp);
        } else {
          scaffoldTree.moveSelectionRange(range.startBp(), range.endBpExcl(), adjustedTargetStartBp);
        }
        contigTree.commitExposedSegment(new ContigTree.Node.ExposedSegment(nlnr.left(), es.segment(), nlnr.right()));
      }
    } finally {
      scaffoldLock.writeLock().unlock();
      contigLock.writeLock().unlock();
    }
  }

  public void scaffoldRegion(final long startIncl, final long endExcl, final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull QueryLengthUnit units, final @Nullable LongFunction<ScaffoldDescriptor> scaffoldGenerator) {
    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();

    try {
      contigTree.getRootLock().readLock().lock();
      scaffoldTree.getRootLock().writeLock().lock();

      validateRange(startIncl, endExcl, resolutionDescriptor, units);
      final long startBp = this.chunkedFile.convertUnits(
        startIncl,
        resolutionDescriptor,
        units,
        ResolutionDescriptor.fromResolutionOrder(0),
        QueryLengthUnit.BASE_PAIRS
      );
      final long endBp = this.chunkedFile.convertUnits(
        endExcl,
        resolutionDescriptor,
        units,
        ResolutionDescriptor.fromResolutionOrder(0),
        QueryLengthUnit.BASE_PAIRS
      );

      final var es = contigTree.expose(ResolutionDescriptor.fromResolutionOrder(0), startBp, endBp, QueryLengthUnit.BASE_PAIRS);
      final var lessSize = (long) Optional.ofNullable(es.less()).map(n -> n.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0))).orElse(0L);
      final var segmentSize = (long) Optional.ofNullable(es.segment()).map(n -> n.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0))).orElse(0L);

      final var extended = scaffoldTree.extendBordersToScaffolds(lessSize, lessSize + segmentSize);


      scaffoldTree.rescaffold(extended.startBP(), extended.endBP(), scaffoldGenerator);

    } finally {
      scaffoldTree.getRootLock().writeLock().unlock();
      contigTree.getRootLock().readLock().unlock();
    }
  }


  public void unscaffoldRegion(final long startIncl, final long endExcl, final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull QueryLengthUnit units) {
    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();

    try {
      contigTree.getRootLock().readLock().lock();
      scaffoldTree.getRootLock().writeLock().lock();

      validateRange(startIncl, endExcl, resolutionDescriptor, units);
      final long startBp = this.chunkedFile.convertUnits(
        startIncl,
        resolutionDescriptor,
        units,
        ResolutionDescriptor.fromResolutionOrder(0),
        QueryLengthUnit.BASE_PAIRS
      );
      final long endBp = this.chunkedFile.convertUnits(
        endExcl,
        resolutionDescriptor,
        units,
        ResolutionDescriptor.fromResolutionOrder(0),
        QueryLengthUnit.BASE_PAIRS
      );

      final var es = contigTree.expose(ResolutionDescriptor.fromResolutionOrder(0), startBp, endBp, QueryLengthUnit.BASE_PAIRS);
      final var lessSize = (long) Optional.ofNullable(es.less()).map(n -> n.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0))).orElse(0L);
      final var segmentSize = (long) Optional.ofNullable(es.segment()).map(n -> n.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0))).orElse(0L);

      final var extended = scaffoldTree.extendBordersToScaffolds(lessSize, lessSize + segmentSize);

      scaffoldTree.unscaffold(extended.startBP(), extended.endBP());
      final var selected = contigTree.expose(ResolutionDescriptor.fromResolutionOrder(0),
        extended.startBP(), extended.endBP(), QueryLengthUnit.BASE_PAIRS);
      final long[] position = {extended.startBP()};
      ContigTree.Node.traverseNode(selected.segment(), node -> {
        final var descriptor = node.getContigDescriptor();
        final long end = position[0] + descriptor.getLengthBp();
        scaffoldTree.rescaffold(position[0], end,
          id -> new ScaffoldDescriptor(id, "scf_" + chunkedFile.getContigDisplayName(descriptor.getContigId()), ru.itmo.ctlab.hict.hict_library.assembly.AssemblyExportSettings.gapLengthBp()));
        position[0] = end;
      });

    } finally {
      scaffoldTree.getRootLock().writeLock().unlock();
      contigTree.getRootLock().readLock().unlock();
    }
  }

  /** Explicit user command: unlike ungroup, this leaves every contig unplaced. */
  public void unscaffoldAll() {
    final var contigs = chunkedFile.getContigTree();
    final var scaffolds = chunkedFile.getScaffoldTree();
    contigs.getRootLock().readLock().lock();
    scaffolds.getRootLock().writeLock().lock();
    try {
      final long length = contigs.getLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
      if (length > 0 && !scaffolds.getScaffoldList().isEmpty()) scaffolds.unscaffold(0, length);
    } finally { scaffolds.getRootLock().writeLock().unlock(); contigs.getRootLock().readLock().unlock(); }
  }

  public void moveRegionToDebris(final long startIncl, final long endExcl, final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull QueryLengthUnit units) {
    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();

    try {
      contigTree.getRootLock().writeLock().lock();
      scaffoldTree.getRootLock().writeLock().lock();

      validateRange(startIncl, endExcl, resolutionDescriptor, units);
      final long startBp = this.chunkedFile.convertUnits(
        startIncl,
        resolutionDescriptor,
        units,
        ResolutionDescriptor.fromResolutionOrder(0),
        QueryLengthUnit.BASE_PAIRS
      );
      final long endBp = this.chunkedFile.convertUnits(
        endExcl,
        resolutionDescriptor,
        units,
        ResolutionDescriptor.fromResolutionOrder(0),
        QueryLengthUnit.BASE_PAIRS
      );

      final var range = resolveSelectionWithScaffolds(startBp, endBp, contigTree, scaffoldTree);
      if (range != null) {
        // Resolution-level HIDDEN only affects PIXELS queries. Debris must not
        // contribute ATUs, BINS coordinates or FASTA/AGP records either. Remove
        // it from both active roots; persistent history retains the old roots
        // and original source descriptors for exact undo/reimport.
        final var exposed = contigTree.expose(ResolutionDescriptor.fromResolutionOrder(0),
          range.startBp(), range.endBpExcl(), QueryLengthUnit.BASE_PAIRS);
        scaffoldTree.removeSegmentFromAssembly(range.startBp(), range.endBpExcl());
        contigTree.commitRoot(ContigTree.Node.mergeNodes(
          new ContigTree.Node.SplitResult(exposed.less(), exposed.greater())));
      }
    } finally {
      scaffoldTree.getRootLock().writeLock().unlock();
      contigTree.getRootLock().writeLock().unlock();
    }
  }

  public void validateRange(long start, long end, ResolutionDescriptor resolution, QueryLengthUnit units) {
    final int order = resolution.getResolutionOrderInArray();
    if (order < 0 || order >= chunkedFile.getResolutions().length ||
      (units == QueryLengthUnit.BASE_PAIRS) != (order == 0)) {
      throw new IllegalArgumentException("Resolution does not match the coordinate units");
    }
    final long length = chunkedFile.getContigTree().getLengthInUnits(units, resolution);
    if (start < 0 || start >= end || end > length) {
      throw new IllegalArgumentException("Select a nonempty range within the assembly");
    }
  }

  public void validateExactContig(final long startBp, final long endBp) {
    final var tree = chunkedFile.getContigTree();
    final var lock = tree.getRootLock().readLock();
    lock.lock();
    try {
      requireExactContig(startBp, endBp, tree);
    } finally {
      lock.unlock();
    }
  }

  private SelectionRange requireExactContig(final long startBp, final long endBp, final ContigTree tree) {
    final var bp = ResolutionDescriptor.fromResolutionOrder(0);
    validateRange(startBp, endBp, bp, QueryLengthUnit.BASE_PAIRS);
    final var exposed = tree.expose(bp, startBp, endBp, QueryLengthUnit.BASE_PAIRS);
    final long actualStart = exposed.less() == null ? 0
      : exposed.less().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, bp);
    if (exposed.segment() == null || exposed.segment().getSubtreeCount() != 1
      || actualStart != startBp
      || exposed.segment().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, bp) != endBp - startBp) {
      throw new IllegalArgumentException("Exact Move requires the full range of one contig");
    }
    return new SelectionRange(startBp, endBp, endBp - startBp);
  }

  private @Nullable SelectionRange resolveSelectionWithScaffolds(
    final long queriedStartBpIncl,
    final long queriedEndBpExcl,
    final @NotNull ContigTree contigTree,
    final @NotNull ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree scaffoldTree
  ) {
    validateRange(queriedStartBpIncl, queriedEndBpExcl,
      ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
    final var startBp = queriedStartBpIncl;
    final var endBp = queriedEndBpExcl;

    final var initialExposed = contigTree.expose(
      ResolutionDescriptor.fromResolutionOrder(0),
      startBp,
      endBp,
      QueryLengthUnit.BASE_PAIRS
    );
    if (initialExposed.segment() == null) {
      return null;
    }
    final var initialStartBp = Optional.ofNullable(initialExposed.less())
      .map(node -> node.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0)))
      .orElse(0L);
    final var initialLengthBp = initialExposed.segment().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
    if (initialLengthBp <= 0) {
      return null;
    }

    final var extended = scaffoldTree.extendBordersToScaffolds(initialStartBp, initialStartBp + initialLengthBp);
    final var extendedExposed = contigTree.expose(
      ResolutionDescriptor.fromResolutionOrder(0),
      extended.startBP(),
      extended.endBP(),
      QueryLengthUnit.BASE_PAIRS
    );
    if (extendedExposed.segment() == null) {
      return null;
    }
    final var selectionStartBp = Optional.ofNullable(extendedExposed.less())
      .map(node -> node.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0)))
      .orElse(0L);
    final var selectionLengthBp = extendedExposed.segment().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
    if (selectionLengthBp <= 0) {
      return null;
    }
    return new SelectionRange(selectionStartBp, selectionStartBp + selectionLengthBp, selectionLengthBp);
  }

  private static long calculateAdjustedTargetStartBp(
    final long requestedTargetStartBp,
    final long totalAssemblyLengthBp,
    final long segmentStartBp,
    final long segmentLengthBp
  ) {
    final var clampedTarget = Math.max(0L, Math.min(requestedTargetStartBp, totalAssemblyLengthBp));
    final var adjustedTarget = clampedTarget - ((segmentStartBp > clampedTarget) ? 0L : segmentLengthBp);
    final var maxTarget = Math.max(0L, totalAssemblyLengthBp - segmentLengthBp);
    return Math.max(0L, Math.min(adjustedTarget, maxTarget));
  }

  private record SelectionRange(long startBp, long endBpExcl, long lengthBp) {
  }

  public void splitContigAtBin(final long splitPosition, final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor, final @NotNull @NonNull QueryLengthUnit units) {
    splitContigAtBin(splitPosition, resolutionDescriptor, units, null, false);
  }

  /** Cut at a boundary in the current oriented contig, retaining all sequence.
   * No contact blocks are written. The containing source bin is shared by ATUs. */
  public void splitContigAtBp(int contigId, long offsetBp) {
    splitContigAtBp(contigId, offsetBp, null);
  }

  private void splitContigAtBp(int contigId, long offsetBp, ChunkedFile.SplitEdit replay) {
    final var tree = chunkedFile.getContigTree();
    final var scaffolds = chunkedFile.getScaffoldTree();
    tree.getRootLock().writeLock().lock();
    scaffolds.getRootLock().writeLock().lock();
    ChunkedFile.AssemblySnapshot before = null;
    try {
      before = chunkedFile.snapshotAssembly();
      long startBp = 0;
      ContigTree.ContigTuple target = null;
      for (var tuple : tree.getOrderedContigList()) {
        if (tuple.descriptor().getContigId() == contigId) { target = tuple; break; }
        startBp = Math.addExact(startBp, tuple.descriptor().getLengthBp());
      }
      if (target == null) throw new IllegalArgumentException("Contig is not in the current assembly");
      final var parent = target.descriptor();
      if (offsetBp <= 0 || offsetBp >= parent.getLengthBp())
        throw new IllegalArgumentException("Cut must leave at least one base on each side");
      final long cutBp = Math.addExact(startBp, offsetBp);
      final long parentEnd = Math.addExact(startBp, parent.getLengthBp());
      final boolean reversed = target.direction() == ContigDirection.REVERSED;
      final long sourceCut = reversed ? parent.getLengthBp() - offsetBp : offsetBp;
      final int maxId = tree.getContigDescriptors().keySet().stream().max(Integer::compare).orElse(0);
      final int leftId = replay == null ? Math.addExact(maxId, 1) : replay.leftId();
      final int rightId = replay == null ? Math.addExact(maxId, 2) : replay.rightId();
      final var left = ExactContigSlice.slice(chunkedFile, parent,
        reversed ? sourceCut : 0, reversed ? parent.getLengthBp() : sourceCut, leftId);
      final var right = ExactContigSlice.slice(chunkedFile, parent,
        reversed ? 0 : sourceCut, reversed ? sourceCut : parent.getLengthBp(), rightId);
      final var bp = ResolutionDescriptor.fromResolutionOrder(0);
      final var exposed = tree.expose(bp, startBp, parentEnd, QueryLengthUnit.BASE_PAIRS);
      final var parentScaffold = scaffolds.getScaffoldAtBp(startBp);
      final var borders = scaffolds.extendBordersToScaffolds(startBp, parentEnd);
      final String name = parentScaffold == null ? "" : chunkedFile.getScaffoldDisplayName(parentScaffold.scaffoldId());
      final long spacer = parentScaffold == null
        ? ru.itmo.ctlab.hict.hict_library.assembly.AssemblyExportSettings.gapLengthBp() : parentScaffold.spacerLength();
      final boolean singleton = borders.startBP() == startBp && borders.endBP() == parentEnd;
      tree.getContigDescriptors().put(leftId, left);
      tree.getContigDescriptors().put(rightId, right);
      final var children = ContigTree.Node.mergeNodes(new ContigTree.Node.SplitResult(
        ContigTree.Node.createNodeFromDescriptor(left, target.direction()),
        ContigTree.Node.createNodeFromDescriptor(right, target.direction())));
      tree.commitExposedSegment(new ContigTree.Node.ExposedSegment(exposed.less(), children, exposed.greater()));
      // Divide the scaffold at the cut without deleting or inserting sequence.
      scaffolds.unscaffold(borders.startBP(), borders.endBP());
      scaffolds.rescaffold(borders.startBP(), cutBp, id -> new ScaffoldDescriptor(id,
        singleton ? "scf_" + left.getContigName() : name + "_left_" + id, spacer));
      scaffolds.rescaffold(cutBp, borders.endBP(), id -> new ScaffoldDescriptor(id,
        singleton ? "scf_" + right.getContigName() : name + "_right_" + id, spacer));
      chunkedFile.recordSplit(new ChunkedFile.SplitEdit(parent.getContigId(), parent.getContigNameInSourceFASTA(),
        parent.getOffsetInSourceFASTA(), parent.getLengthBp(), reversed, offsetBp, leftId, rightId, true));
    } catch (RuntimeException | Error failure) {
      if (before != null) chunkedFile.restoreAssembly(before);
      throw failure;
    } finally {
      scaffolds.getRootLock().writeLock().unlock();
      tree.getRootLock().writeLock().unlock();
    }
  }

  /** Rebuild native bins from a parent before the saved AGP applies the final layout. */
  public void replaySplit(ChunkedFile.SplitEdit edit, ContigDescriptor parent) {
    final var tree = chunkedFile.getContigTree();
    final var scaffoldTree = chunkedFile.getScaffoldTree();
    tree.getRootLock().writeLock().lock();
    scaffoldTree.getRootLock().writeLock().lock();
    try {
      if (parent.getContigId() != edit.parentId() || edit.leftId() <= edit.parentId()
        || (long) edit.leftId() + 1 != edit.rightId()
        || tree.getContigDescriptors().containsKey(edit.leftId())
        || tree.getContigDescriptors().containsKey(edit.rightId())) {
        throw new IllegalArgumentException("Invalid split component IDs");
      }
      if (!parent.getContigNameInSourceFASTA().equals(edit.sourceName())
        || parent.getOffsetInSourceFASTA() != edit.sourceOffsetBp()
        || parent.getLengthBp() != edit.parentLengthBp()
        || edit.splitOffsetBp() <= 0 || edit.splitOffsetBp() >= parent.getLengthBp()) {
        throw new IllegalArgumentException("Invalid split parent interval");
      }
      final var before = chunkedFile.snapshotAssembly();
      final ContigDescriptor left;
      final ContigDescriptor right;
      try {
        tree.getContigDescriptors().put(parent.getContigId(), parent);
        tree.commitRoot(ContigTree.Node.createNodeFromDescriptor(parent,
          edit.reversed() ? ContigDirection.REVERSED : ContigDirection.FORWARD));
        scaffoldTree.replaceWith(new ScaffoldTree(parent.getLengthBp()));
        if (edit.exact()) splitContigAtBp(parent.getContigId(), edit.splitOffsetBp(), edit);
        else splitContigAtBin(edit.splitOffsetBp(), ResolutionDescriptor.fromResolutionOrder(0),
          QueryLengthUnit.BASE_PAIRS, edit, false);
        left = tree.getContigDescriptors().get(edit.leftId());
        right = tree.getContigDescriptors().get(edit.rightId());
        if (left.getLengthBp() != edit.splitOffsetBp()) {
          throw new IllegalArgumentException("Split offset is not a native bin boundary");
        }
      } finally {
        chunkedFile.restoreAssembly(before);
      }
      tree.getContigDescriptors().put(parent.getContigId(), parent);
      tree.getContigDescriptors().put(edit.leftId(), left);
      tree.getContigDescriptors().put(edit.rightId(), right);
      chunkedFile.recordSplit(edit);
    } finally {
      scaffoldTree.getRootLock().writeLock().unlock();
      tree.getRootLock().writeLock().unlock();
    }
  }

  public void validateBinSplit(long splitPositionBp) {
    splitContigAtBin(splitPositionBp, ResolutionDescriptor.fromResolutionOrder(0),
      QueryLengthUnit.BASE_PAIRS, null, true);
  }

  private void splitContigAtBin(long splitPosition, ResolutionDescriptor resolutionDescriptor,
                                QueryLengthUnit units, ChunkedFile.SplitEdit replay, boolean validateOnly) {

    final var minResolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(1);
    final long minBpResolution = this.chunkedFile.getResolutions()[1];

    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();
    final var lock = contigTree.getRootLock();
    final var scaffoldLock = scaffoldTree.getRootLock();
    try {
      lock.writeLock().lock();
      scaffoldLock.writeLock().lock();
      if (splitPosition <= 0 || splitPosition == Long.MAX_VALUE) {
        throw new IllegalArgumentException("Split position must be inside the assembly");
      }
      validateRange(splitPosition, splitPosition + 1, resolutionDescriptor, units);
      final var oldContigTreeRoot = contigTree.getRoot();
      final var oldAssemblyLengthBp = oldContigTreeRoot.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
      final var splitPositionBins = this.chunkedFile.convertUnits(splitPosition, resolutionDescriptor, units, minResolutionDescriptor, QueryLengthUnit.BINS);
      // The sequence cut must use the same finest-resolution bin that the
      // ATU operation removes, even when the caller supplied an interior bp.
      final var splitPositionBp = this.chunkedFile.convertUnits(splitPositionBins, minResolutionDescriptor,
        QueryLengthUnit.BINS, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      final var es = contigTree.expose(minResolutionDescriptor, splitPositionBins, 1 + splitPositionBins, QueryLengthUnit.BINS);

      final var leftBins = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(QueryLengthUnit.BINS, minResolutionDescriptor);
      final var leftBps = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));

      if (es.segment() == null) throw new IllegalArgumentException("Split position is outside of any contig");
      final var oldContigNode = es.segment().push().updateSizes();

      final var oldContigDescriptor = oldContigNode.getContigDescriptor();
      final var parentScaffold = scaffoldTree.getScaffoldAtBp(leftBps);
      final var parentBorders = scaffoldTree.extendBordersToScaffolds(leftBps, leftBps + oldContigDescriptor.getLengthBp());
      final var parentName = parentScaffold == null ? "" : chunkedFile.getScaffoldDisplayName(parentScaffold.scaffoldId());

      final var deltaBpsFromContigStart = splitPositionBp - leftBps;
      if (deltaBpsFromContigStart <= 0L || deltaBpsFromContigStart + minBpResolution >= oldContigDescriptor.getLengthBp()) {
        throw new IllegalArgumentException("Split bin must be inside a contig and leave sequence on both sides of the removed bin.");
      }

      final int maxContigId = contigTree.getContigDescriptors().keySet().stream().max(Integer::compareTo).orElse(0);

      final var newContigIds = replay == null
        ? List.of(Math.addExact(maxContigId, 1), Math.addExact(maxContigId, 2))
        : List.of(replay.leftId(), replay.rightId());
      final var newContigLengthBps = List.of(
        deltaBpsFromContigStart,
        oldContigDescriptor.getLengthBp() - deltaBpsFromContigStart - minBpResolution
      );
      final long rightContigStartInSource = deltaBpsFromContigStart + minBpResolution;

      final var newContigNames = switch (oldContigNode.getContigDirection()) {
        case FORWARD -> List.of(
          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), 0, deltaBpsFromContigStart),
          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), rightContigStartInSource, oldContigDescriptor.getLengthBp())
        );
        case REVERSED -> List.of(
//          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), newContigLengthBps.get(1), oldContigDescriptor.getLengthBp()),
//          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), 0, deltaBpsFromContigStart),
          String.format("%s_reversed_%d_%d", oldContigDescriptor.getContigName(), 0, deltaBpsFromContigStart),
          String.format("%s_reversed_%d_%d", oldContigDescriptor.getContigName(), rightContigStartInSource, oldContigDescriptor.getLengthBp())
        );
      };

      final var newContigNamesInSourceFasta = List.of(oldContigDescriptor.getContigNameInSourceFASTA(), oldContigDescriptor.getContigNameInSourceFASTA());
      final var newContigOffsetsInSourceFasta = switch (oldContigNode.getContigDirection()) {
        case FORWARD -> List.of(oldContigDescriptor.getOffsetInSourceFASTA(), Math.addExact(oldContigDescriptor.getOffsetInSourceFASTA(), rightContigStartInSource));
        case REVERSED -> List.of(
          Math.addExact(oldContigDescriptor.getOffsetInSourceFASTA(), oldContigDescriptor.getLengthBp() - deltaBpsFromContigStart),
          oldContigDescriptor.getOffsetInSourceFASTA()
        );
      };


      final List<@NotNull List<@NotNull Long>> newContigLengthsBinsAtResolution = List.of(new ArrayList<>(), new ArrayList<>());
      final List<@NotNull List<@NotNull ContigHideType>> newContigPresenceAtResolution = List.of(new ArrayList<>(), new ArrayList<>());
      final List<@NotNull List<@NotNull List<@NotNull ATUDescriptor>>> newContigAtus = List.of(new ArrayList<>(), new ArrayList<>());

      for (int i = 1; i < this.chunkedFile.getResolutions().length; i++) {
        final var bpResolution = this.chunkedFile.getResolutions()[i];
        final var contigStartBinsAtResolution = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(QueryLengthUnit.BINS, ResolutionDescriptor.fromResolutionOrder(i));
        final var splitPositionBinsAtResolution = this.chunkedFile.convertUnits(
          splitPositionBp,
          ResolutionDescriptor.fromResolutionOrder(0),
          QueryLengthUnit.BASE_PAIRS,
          ResolutionDescriptor.fromResolutionOrder(i),
          QueryLengthUnit.BINS
        );
        final var deltaBinsFromContigStartAtResolution = splitPositionBinsAtResolution - contigStartBinsAtResolution;

        final List<Long> newLengthsAtResolution;

        final var oldContigLengthBinsAtResolution = oldContigDescriptor.getLengthInUnits(QueryLengthUnit.BINS, ResolutionDescriptor.fromResolutionOrder(i));

        if (i == 1) {
          newLengthsAtResolution = List.of(
            deltaBinsFromContigStartAtResolution,
            oldContigLengthBinsAtResolution - deltaBinsFromContigStartAtResolution - 1
          );
        } else {
          newLengthsAtResolution = List.of(
            deltaBinsFromContigStartAtResolution,
            oldContigLengthBinsAtResolution - deltaBinsFromContigStartAtResolution
          );
        }
        IntStream.range(0, 2).forEach(j -> newContigLengthsBinsAtResolution.get(j).add(newLengthsAtResolution.get(j)));
        IntStream.range(0, 2).forEach(j -> newContigPresenceAtResolution.get(j).add((newLengthsAtResolution.get(j) > 0 && newContigLengthBps.get(j) >= bpResolution) ? ContigHideType.SHOWN : ContigHideType.HIDDEN));

        final var newLeftATUs = this.chunkedFile.matrixQueries().getATUsForRange(ResolutionDescriptor.fromResolutionOrder(i), contigStartBinsAtResolution, splitPositionBinsAtResolution, false);
        final var newRightATUs = this.chunkedFile.matrixQueries().getATUsForRange(ResolutionDescriptor.fromResolutionOrder(i), splitPositionBinsAtResolution + ((i > 1) ? 0 : 1), contigStartBinsAtResolution + oldContigLengthBinsAtResolution, false);

        assert (
          newLeftATUs.parallelStream().mapToInt(ATUDescriptor::getLength).sum()
            +
            newRightATUs.parallelStream().mapToInt(ATUDescriptor::getLength).sum()
            ==
            oldContigLengthBinsAtResolution - ((i > 1) ? 0 : 1)
        ) : String.format(
          "ATUs total length %d + %d has changed after splitting contig with ATU length %d at resolution order %d??",
          newLeftATUs.parallelStream().mapToInt(ATUDescriptor::getLength).sum(),
          newRightATUs.parallelStream().mapToInt(ATUDescriptor::getLength).sum(),
          oldContigLengthBinsAtResolution,
          i
        );

        switch (oldContigNode.getTrueDirection()) {
          case FORWARD -> {
            newContigAtus.get(0).add(newLeftATUs);
            newContigAtus.get(1).add(newRightATUs);
          }
          case REVERSED -> {
            newContigAtus.get(0).add(newLeftATUs.parallelStream().map(ATUDescriptor::reversed).collect(Collectors.toList()));
            newContigAtus.get(1).add(newRightATUs.parallelStream().map(ATUDescriptor::reversed).collect(Collectors.toList()));
            Collections.reverse(newContigAtus.get(0).get(i - 1));
            Collections.reverse(newContigAtus.get(1).get(i - 1));
          }
        }

        assert (newContigAtus.get(0).size() > 0) : "Empty list of ATUs for first part??";
        assert (newContigAtus.get(1).size() > 0) : "Empty list of ATUs for second part??";
      }

      final var newCds = IntStream.range(0, 2).mapToObj(i -> new ContigDescriptor(
        newContigIds.get(i),
        newContigNames.get(i),
        newContigLengthBps.get(i),
        newContigLengthsBinsAtResolution.get(i),
        newContigPresenceAtResolution.get(i),
        newContigAtus.get(i),
        newContigNamesInSourceFasta.get(i),
        newContigOffsetsInSourceFasta.get(i), oldContigDescriptor.isSharedBinSlice()
      )).toList();

      if (validateOnly) return;
      newCds.forEach(cd -> contigTree.getContigDescriptors().put(cd.getContigId(), cd));

      final var newContigNodes = newCds.stream().map(cd -> ContigTree.Node.createNodeFromDescriptor(cd, oldContigNode.getTrueDirection())).toList();

      final var newSegment = ContigTree.Node.mergeNodes(new ContigTree.Node.SplitResult(newContigNodes.get(0), newContigNodes.get(1)));

      final var newExposedSegment = new ContigTree.Node.ExposedSegment(es.less(), newSegment, es.greater());

      scaffoldTree.unscaffold(splitPositionBp, splitPositionBp + minBpResolution);
      scaffoldTree.removeSegmentFromAssembly(splitPositionBp, splitPositionBp + minBpResolution);
      contigTree.commitExposedSegment(newExposedSegment);
      // Keep both sides of the parent scaffold, including their other contigs.
      // For an unplaced contig or a singleton, each new piece is a singleton.
      final long rightStart = splitPositionBp;
      final long rightEnd = parentBorders.endBP() - minBpResolution;
      final boolean singleton = parentBorders.startBP() == leftBps && parentBorders.endBP() == leftBps + oldContigDescriptor.getLengthBp();
      scaffoldTree.rescaffold(parentBorders.startBP(), rightStart, id -> new ScaffoldDescriptor(id,
        singleton ? "scf_" + newCds.get(0).getContigName() : parentName + "_left_" + id,
        parentScaffold == null ? ru.itmo.ctlab.hict.hict_library.assembly.AssemblyExportSettings.gapLengthBp() : parentScaffold.spacerLength()));
      scaffoldTree.rescaffold(rightStart, rightEnd, id -> new ScaffoldDescriptor(id,
        singleton ? "scf_" + newCds.get(1).getContigName() : parentName + "_right_" + id,
        parentScaffold == null ? ru.itmo.ctlab.hict.hict_library.assembly.AssemblyExportSettings.gapLengthBp() : parentScaffold.spacerLength()));


      final var newContigTreeRoot = contigTree.getRoot();
      final var newAssemblyLengthBp = newContigTreeRoot.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));

      assert (oldAssemblyLengthBp == (newAssemblyLengthBp + minBpResolution)) : "Assembly length has changed after splitting contig??";
      chunkedFile.recordSplit(new ChunkedFile.SplitEdit(
        oldContigDescriptor.getContigId(), oldContigDescriptor.getContigNameInSourceFASTA(),
        oldContigDescriptor.getOffsetInSourceFASTA(), oldContigDescriptor.getLengthBp(),
        oldContigNode.getTrueDirection() == ContigDirection.REVERSED, deltaBpsFromContigStart,
        newContigIds.get(0), newContigIds.get(1)));
    } finally {
      scaffoldLock.writeLock().unlock();
      lock.writeLock().unlock();
    }
  }

}
