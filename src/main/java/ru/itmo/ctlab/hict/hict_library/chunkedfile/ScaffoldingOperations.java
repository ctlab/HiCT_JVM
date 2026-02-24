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
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

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
        final var leftSizeBp = Optional.ofNullable(es.less()).map(l -> l.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0))).orElse(0L);
        final var segmentSizeBp = es.segment().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
        final var totalAssemblyLengthBp = Optional.ofNullable(contigTree.getRoot())
          .map(root -> root.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0)))
          .orElse(0L);
        final var adjustedTargetStartBp = calculateAdjustedTargetStartBp(targetStartBp, totalAssemblyLengthBp, leftSizeBp, segmentSizeBp);
        final var tmp = ContigTree.Node.mergeNodes(new ContigTree.Node.SplitResult(es.less(), es.greater()));
        final var nlnr = (tmp != null)
          ? tmp.splitByLength(ResolutionDescriptor.fromResolutionOrder(0), adjustedTargetStartBp, false, QueryLengthUnit.BASE_PAIRS)
          : new ContigTree.Node.SplitResult(null, null);
        contigTree.commitExposedSegment(new ContigTree.Node.ExposedSegment(nlnr.left(), es.segment(), nlnr.right()));
        scaffoldTree.moveSelectionRange(range.startBp(), range.endBpExcl(), adjustedTargetStartBp);
      }
    } finally {
      scaffoldLock.writeLock().unlock();
      contigLock.writeLock().unlock();
    }
  }

  public void scaffoldRegion(final long startIncl, final long endExcl, final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull QueryLengthUnit units, final @Nullable LongFunction<ScaffoldDescriptor> scaffoldGenerator) {
    assert (startIncl < endExcl) : "Rescaffolding: start >= end??";

    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();

    try {
      contigTree.getRootLock().readLock().lock();
      scaffoldTree.getRootLock().writeLock().lock();

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
    assert (startIncl < endExcl) : "Unscaffolding: start >= end??";

    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();

    try {
      contigTree.getRootLock().readLock().lock();
      scaffoldTree.getRootLock().writeLock().lock();

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

    } finally {
      scaffoldTree.getRootLock().writeLock().unlock();
      contigTree.getRootLock().readLock().unlock();
    }
  }

  public void moveRegionToDebris(final long startIncl, final long endExcl, final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull QueryLengthUnit units) {
    assert (startIncl < endExcl) : "Unscaffolding: start >= end??";

    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();

    try {
      contigTree.getRootLock().writeLock().lock();
      scaffoldTree.getRootLock().writeLock().lock();

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
        final var totalAssemblyLengthBp = Optional.ofNullable(contigTree.getRoot())
          .map(root -> root.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0)))
          .orElse(0L);

        scaffoldTree.unscaffold(range.startBp(), range.endBpExcl());

        final var exposedForMove = contigTree.expose(
          ResolutionDescriptor.fromResolutionOrder(0),
          range.startBp(),
          range.endBpExcl(),
          QueryLengthUnit.BASE_PAIRS
        );

        if (exposedForMove.segment() == null) {
          return;
        }

        final var leftSizeBp = Optional.ofNullable(exposedForMove.less())
          .map(node -> node.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0)))
          .orElse(0L);
        final var segmentSizeBp = exposedForMove.segment().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
        if (segmentSizeBp <= 0) {
          return;
        }

        final var adjustedTargetStartBp = calculateAdjustedTargetStartBp(totalAssemblyLengthBp, totalAssemblyLengthBp, leftSizeBp, segmentSizeBp);
        final var tmp = ContigTree.Node.mergeNodes(new ContigTree.Node.SplitResult(exposedForMove.less(), exposedForMove.greater()));
        final var nlnr = (tmp != null)
          ? tmp.splitByLength(ResolutionDescriptor.fromResolutionOrder(0), adjustedTargetStartBp, false, QueryLengthUnit.BASE_PAIRS)
          : new ContigTree.Node.SplitResult(null, null);
        contigTree.commitExposedSegment(new ContigTree.Node.ExposedSegment(nlnr.left(), exposedForMove.segment(), nlnr.right()));
        scaffoldTree.moveSelectionRange(range.startBp(), range.endBpExcl(), adjustedTargetStartBp);

        final var debrisStartBpIncl = totalAssemblyLengthBp - segmentSizeBp;
        final var debrisEndBpExcl = totalAssemblyLengthBp;

        final var debrisExposed = contigTree.expose(
          ResolutionDescriptor.fromResolutionOrder(0),
          debrisStartBpIncl,
          debrisEndBpExcl,
          QueryLengthUnit.BASE_PAIRS
        );
        if (debrisExposed.segment() != null) {
          final var hiddenSegment = cloneSegmentWithHiddenDescriptors(debrisExposed.segment(), contigTree);
          contigTree.commitExposedSegment(new ContigTree.Node.ExposedSegment(debrisExposed.less(), hiddenSegment, debrisExposed.greater()));
        }

        scaffoldTree.unscaffold(debrisStartBpIncl, debrisEndBpExcl);
        scaffoldTree.rescaffold(debrisStartBpIncl, debrisEndBpExcl, id -> new ScaffoldDescriptor(id, "debris", 1000));
      }
    } finally {
      scaffoldTree.getRootLock().writeLock().unlock();
      contigTree.getRootLock().writeLock().unlock();
    }
  }

  private @Nullable SelectionRange resolveSelectionWithScaffolds(
    final long queriedStartBpIncl,
    final long queriedEndBpExcl,
    final @NotNull ContigTree contigTree,
    final @NotNull ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree scaffoldTree
  ) {
    final var startBp = Math.min(queriedStartBpIncl, queriedEndBpExcl);
    final var endBp = Math.max(queriedStartBpIncl, queriedEndBpExcl);
    if (startBp >= endBp) {
      return null;
    }

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

  private @NotNull ContigTree.Node cloneSegmentWithHiddenDescriptors(
    final @NotNull ContigTree.Node node,
    final @NotNull ContigTree contigTree
  ) {
    final var pushed = node.push();
    final var hiddenLeft = (pushed.getLeft() != null) ? cloneSegmentWithHiddenDescriptors(pushed.getLeft(), contigTree) : null;
    final var hiddenRight = (pushed.getRight() != null) ? cloneSegmentWithHiddenDescriptors(pushed.getRight(), contigTree) : null;
    final var descriptor = pushed.getContigDescriptor();
    final var resolutionCount = descriptor.getLengthBinsAtResolution().length;
    final var hiddenDescriptor = new ContigDescriptor(
      descriptor.getContigId(),
      descriptor.getContigName(),
      descriptor.getLengthBp(),
      Arrays.stream(descriptor.getLengthBinsAtResolution()).skip(1L).boxed().toList(),
      IntStream.range(1, resolutionCount).mapToObj(i -> ContigHideType.HIDDEN).toList(),
      descriptor.getAtus().subList(1, resolutionCount).stream().map(ArrayList::new).map(List::copyOf).toList(),
      descriptor.getContigNameInSourceFASTA(),
      descriptor.getOffsetInSourceFASTA()
    );
    contigTree.getContigDescriptors().put(hiddenDescriptor.getContigId(), hiddenDescriptor);
    return pushed.cloneBuilder()
      .contigDescriptor(hiddenDescriptor)
      .left(hiddenLeft)
      .right(hiddenRight)
      .build()
      .updateSizes();
  }

  private record SelectionRange(long startBp, long endBpExcl, long lengthBp) {
  }

  public void splitContigAtBin(final long splitPosition, final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor, final @NotNull @NonNull QueryLengthUnit units) {
    assert !QueryLengthUnit.BASE_PAIRS.equals(units) || (resolutionDescriptor.getResolutionOrderInArray() == 0) : "In bp query resolution should be set to 0";

    final var minResolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(1);
    final long minBpResolution = this.chunkedFile.getResolutions()[1];

    final var contigTree = this.chunkedFile.getContigTree();
    final var lock = contigTree.getRootLock();
    try {
      lock.writeLock().lock();
      final var oldContigTreeRoot = contigTree.getRoot();
      final var oldAssemblyLengthBp = oldContigTreeRoot.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
      final var splitPositionBp = this.chunkedFile.convertUnits(splitPosition, resolutionDescriptor, units, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      final var splitPositionBins = this.chunkedFile.convertUnits(splitPosition, resolutionDescriptor, units, minResolutionDescriptor, QueryLengthUnit.BINS);
      final var es = contigTree.expose(minResolutionDescriptor, splitPositionBins, 1 + splitPositionBins, QueryLengthUnit.BINS);

      final var leftBins = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(QueryLengthUnit.BINS, minResolutionDescriptor);
      final var leftBps = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));

      unscaffoldRegion(leftBps, 1 + leftBps, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);

      final var oldContigNode = es.segment().push().updateSizes();

      assert (oldContigNode != null) : "Split position is outside of any contig??";

      final var oldContigDescriptor = oldContigNode.getContigDescriptor();
      final var deltaBpsFromContigStart = splitPositionBp - leftBps;

      final int maxContigId = contigTree.getContigDescriptors().keySet().stream().max(Integer::compareTo).orElse(0);

      final var newContigIds = List.of(1 + maxContigId, 2 + maxContigId);
      final var newContigLengthBps = List.of(
        deltaBpsFromContigStart,
        oldContigDescriptor.getLengthBp() - deltaBpsFromContigStart - minBpResolution
      );

      final var newContigNames = switch (oldContigNode.getContigDirection()) {
        case FORWARD -> List.of(
          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), 0, deltaBpsFromContigStart),
          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), newContigLengthBps.get(1), oldContigDescriptor.getLengthBp())
        );
        case REVERSED -> List.of(
//          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), newContigLengthBps.get(1), oldContigDescriptor.getLengthBp()),
//          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), 0, deltaBpsFromContigStart),
          String.format("%s_reversed_%d_%d", oldContigDescriptor.getContigName(), 0, deltaBpsFromContigStart),
          String.format("%s_reversed_%d_%d", oldContigDescriptor.getContigName(), newContigLengthBps.get(1), oldContigDescriptor.getLengthBp())
        );
      };

      final var newContigNamesInSourceFasta = List.of(oldContigDescriptor.getContigNameInSourceFASTA(), oldContigDescriptor.getContigNameInSourceFASTA());
      final var newContigOffsetsInSourceFasta = switch (oldContigNode.getContigDirection()) {
        case FORWARD -> List.of(oldContigDescriptor.getOffsetInSourceFASTA(), oldContigDescriptor.getOffsetInSourceFASTA() + (int) minBpResolution);
        case REVERSED -> List.of(oldContigDescriptor.getOffsetInSourceFASTA() + (int) minBpResolution, oldContigDescriptor.getOffsetInSourceFASTA());
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
        IntStream.range(0, 2).forEach(j -> newContigPresenceAtResolution.get(j).add((newContigLengthBps.get(j) >= bpResolution) ? ContigHideType.SHOWN : ContigHideType.HIDDEN));

        final var oldATUsLengthBinsPrefixSum = oldContigDescriptor.getAtuPrefixSumLengthBins().get(i);

        final var newLeftATUs = this.chunkedFile.matrixQueries().getATUsForRange(ResolutionDescriptor.fromResolutionOrder(i), contigStartBinsAtResolution, splitPositionBinsAtResolution, false);
        final var newRightATUs = this.chunkedFile.matrixQueries().getATUsForRange(ResolutionDescriptor.fromResolutionOrder(i), splitPositionBinsAtResolution + ((i > 1) ? 0 : 1), contigStartBinsAtResolution + oldContigLengthBinsAtResolution, false);

        assert (
          newLeftATUs.parallelStream().mapToInt(ATUDescriptor::getLength).sum()
            +
            newRightATUs.parallelStream().mapToInt(ATUDescriptor::getLength).sum()
            ==
            oldATUsLengthBinsPrefixSum[oldATUsLengthBinsPrefixSum.length - 1] - ((i > 1) ? 0 : 1)
        ) : String.format(
          "ATUs total length %d + %d has changed after splitting contig with ATU length %d at resolution order %d??",
          newLeftATUs.parallelStream().mapToInt(ATUDescriptor::getLength).sum(),
          newRightATUs.parallelStream().mapToInt(ATUDescriptor::getLength).sum(),
          oldATUsLengthBinsPrefixSum[oldATUsLengthBinsPrefixSum.length - 1],
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
        newContigOffsetsInSourceFasta.get(i)
      )).toList();

      newCds.forEach(cd -> contigTree.getContigDescriptors().put(cd.getContigId(), cd));

      final var newContigNodes = newCds.stream().map(cd -> ContigTree.Node.createNodeFromDescriptor(cd, oldContigNode.getTrueDirection())).toList();

      final var newSegment = ContigTree.Node.mergeNodes(new ContigTree.Node.SplitResult(newContigNodes.get(0), newContigNodes.get(1)));

      final var newExposedSegment = new ContigTree.Node.ExposedSegment(es.less(), newSegment, es.greater());

      contigTree.commitExposedSegment(newExposedSegment);

      final var newContigTreeRoot = contigTree.getRoot();
      final var newAssemblyLengthBp = newContigTreeRoot.getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));

      assert (oldAssemblyLengthBp == (newAssemblyLengthBp + minBpResolution)) : "Assembly length has changed after splitting contig??";

    } finally {
      lock.writeLock().unlock();
    }
  }

}
