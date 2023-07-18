package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor
public class ScaffoldingOperations {
  private @NotNull
  final ChunkedFile chunkedFile;

  public void reverseSelectionRangeBp(final long queriedStartBpIncl, final long queriedEndBpExcl) {
    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();
    final var lock = contigTree.getRootLock();
    try {
      lock.writeLock().lock();
      final var ext = scaffoldTree.extendBordersToScaffolds(queriedStartBpIncl, queriedEndBpExcl);
      final var es = contigTree.expose(ResolutionDescriptor.fromResolutionOrder(0), ext.startBP(), ext.endBP(), QueryLengthUnit.BASE_PAIRS);
      if (es.segment() != null) {
        final var newSegmentNode = es.segment().cloneBuilder().needsChangingDirection(!es.segment().isNeedsChangingDirection()).build().push().updateSizes();
        contigTree.commitExposedSegment(new ContigTree.Node.ExposedSegment(es.less(), newSegmentNode, es.greater()));
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void moveSelectionRangeBp(final long queriedStartBpIncl, final long queriedEndBpExcl, final long targetStartBp) {
    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();
    final var lock = contigTree.getRootLock();
    try {
      lock.writeLock().lock();
      final var ext = scaffoldTree.extendBordersToScaffolds(queriedStartBpIncl, queriedEndBpExcl);
      final var es = contigTree.expose(ResolutionDescriptor.fromResolutionOrder(0), ext.startBP(), ext.endBP(), QueryLengthUnit.BASE_PAIRS);
      if (es.segment() != null) {
        final var tmp = ContigTree.Node.mergeNodes(new ContigTree.Node.SplitResult(es.less(), es.greater()));
        final var nlnr = tmp.splitByLength(ResolutionDescriptor.fromResolutionOrder(0), targetStartBp, false, QueryLengthUnit.BASE_PAIRS);
        contigTree.commitExposedSegment(new ContigTree.Node.ExposedSegment(nlnr.left(), es.segment(), nlnr.right()));
        scaffoldTree.moveSelectionRange(ext.startBP(), ext.endBP(), targetStartBp);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void splitContigAtBin(final long splitPosition, final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor, final @NotNull @NonNull QueryLengthUnit units) {
    assert !QueryLengthUnit.BASE_PAIRS.equals(units) || (resolutionDescriptor.getResolutionOrderInArray() == 0) : "In bp query resolution should be set to 0";

    final var minResolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(1);
    final long minBpResolution = this.chunkedFile.getResolutions()[1];

    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();
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
        case FORWARD ->
          List.of(oldContigDescriptor.getOffsetInSourceFASTA(), oldContigDescriptor.getOffsetInSourceFASTA() + (int) minBpResolution);
        case REVERSED ->
          List.of(oldContigDescriptor.getOffsetInSourceFASTA() + (int) minBpResolution, oldContigDescriptor.getOffsetInSourceFASTA());
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

        /*
        final var oldContigATUs = switch (oldContigNode.getTrueDirection()) {
          case FORWARD -> oldContigDescriptor.getAtus().get(i);
          case REVERSED -> {
            final var tmp = oldContigDescriptor.getAtus().get(i).parallelStream().map(ATUDescriptor::reversed).collect(Collectors.toList());
            Collections.reverse(tmp);
            yield tmp;
          }
        };



        final var indexOfATUWhereSplitOccurs = switch (oldContigNode.getTrueDirection()) {
          case FORWARD ->
            BinarySearch.rightBinarySearch(oldATUsLengthBinsPrefixSum, deltaBinsFromContigStartAtResolution);
          case REVERSED ->
            oldATUsLengthBinsPrefixSum.length - 1 - BinarySearch.leftBinarySearch(oldATUsLengthBinsPrefixSum, oldContigLengthBinsAtResolution - deltaBinsFromContigStartAtResolution); //TODO: BinarySearch.
        };

        assert (indexOfATUWhereSplitOccurs >= 0) : "Wrong index of splitting ATU";
        assert (indexOfATUWhereSplitOccurs < oldATUsLengthBinsPrefixSum.length) : "Wrong index of splitting ATU";

        final var oldJoinATU = oldContigATUs.get(indexOfATUWhereSplitOccurs);

        final var newLeftATUs = new ArrayList<>(oldContigATUs.subList(0, indexOfATUWhereSplitOccurs));
        final List<@NotNull ATUDescriptor> newRightATUs;


        final var leftATUsLength = (indexOfATUWhereSplitOccurs == 0) ? 0L : oldATUsLengthBinsPrefixSum[indexOfATUWhereSplitOccurs - 1];
        final var rightATUsLength = oldATUsLengthBinsPrefixSum[oldATUsLengthBinsPrefixSum.length - 1] - oldATUsLengthBinsPrefixSum[indexOfATUWhereSplitOccurs];

        final var deltaL = (int) (deltaBinsFromContigStartAtResolution -
          switch (oldContigNode.getTrueDirection()) {
            case FORWARD -> leftATUsLength;
            case REVERSED -> rightATUsLength;
          }
        );

        assert (deltaL <= oldJoinATU.getLength()) : "Need to shrink ATU that is already smaller?";


        if (deltaL > 0) {
          final var newLeftJoinATU = ATUDescriptor.builder()
            .stripeDescriptor(oldJoinATU.getStripeDescriptor())
            .direction(oldJoinATU.getDirection())
            .startIndexInStripeIncl(
              switch (oldJoinATU.getDirection()) {
                case FORWARD -> oldJoinATU.getStartIndexInStripeIncl();
                case REVERSED -> oldJoinATU.getEndIndexInStripeExcl() - deltaL;
              }
            )
            .endIndexInStripeExcl(
              switch (oldJoinATU.getDirection()) {
                case FORWARD -> oldJoinATU.getStartIndexInStripeIncl() + deltaL;
                case REVERSED -> oldJoinATU.getEndIndexInStripeExcl();
              }
            )
            .build();
          newLeftATUs.add(newLeftJoinATU);
          assert (newLeftJoinATU.getLength() == deltaL) : "New left ATU has incorrect size??";
        }

        final var newRightJoinATUStart = switch (oldJoinATU.getDirection()) {
          case FORWARD -> oldJoinATU.getStartIndexInStripeIncl() + deltaL + ((i > 1) ? 0 : 1);
          case REVERSED -> oldJoinATU.getStartIndexInStripeIncl();
        };

        final var newRightJoinATUEnd = switch (oldJoinATU.getDirection()) {
          case FORWARD -> oldJoinATU.getEndIndexInStripeExcl();
          case REVERSED -> oldJoinATU.getEndIndexInStripeExcl() - deltaL - ((i > 1) ? 0 : 1);
        };

        if (newRightJoinATUEnd > newRightJoinATUStart) {
          newRightATUs = new ArrayList<>(oldContigATUs.subList(indexOfATUWhereSplitOccurs, oldContigATUs.size()));
          final var newRightJoinATU = ATUDescriptor.builder()
            .stripeDescriptor(oldJoinATU.getStripeDescriptor())
            .direction(oldJoinATU.getDirection())
            .startIndexInStripeIncl(newRightJoinATUStart)
            .endIndexInStripeExcl(newRightJoinATUEnd)
            .build();
          newRightATUs.set(0, newRightJoinATU);
          assert ((oldJoinATU.getLength() - ((i > 1) ? 0 : 1)) == (deltaL + newRightJoinATU.getLength()));
        } else {
          newRightATUs = oldContigATUs.subList(1 + indexOfATUWhereSplitOccurs, oldContigATUs.size());
          assert ((oldJoinATU.getLength() - ((i > 1) ? 0 : 1)) == deltaL);
        }
         */

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
            Collections.reverse(newLeftATUs);
            Collections.reverse(newRightATUs);
            newContigAtus.get(0).add(newLeftATUs.parallelStream().map(ATUDescriptor::reversed).toList());
            newContigAtus.get(1).add(newRightATUs.parallelStream().map(ATUDescriptor::reversed).toList());
          }
        }

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
