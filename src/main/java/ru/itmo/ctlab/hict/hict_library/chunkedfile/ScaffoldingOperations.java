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
import ru.itmo.ctlab.hict.hict_library.util.BinarySearch;

import java.util.ArrayList;
import java.util.List;
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
      final var splitPositionBp = this.chunkedFile.convertUnits(splitPosition, resolutionDescriptor, units, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      final var splitPositionBins = this.chunkedFile.convertUnits(splitPosition, resolutionDescriptor, units, minResolutionDescriptor, QueryLengthUnit.BINS);
      final var es = contigTree.expose(minResolutionDescriptor, splitPositionBins, 1 + splitPositionBins, QueryLengthUnit.BINS);

      final var leftBins = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(QueryLengthUnit.BINS, minResolutionDescriptor);
      final var leftBps = (es.less() == null) ? 0L : es.less().getSubtreeLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));

      final var node = es.segment();

      assert (node != null) : "Split position is outside of any contig??";

      final var oldContigDescriptor = node.getContigDescriptor();
      final var deltaBinsFromContigStartAtMinResolution = splitPositionBins - leftBins;
      final var deltaBpsFromContigStart = splitPositionBp - leftBps;

      final int maxContigId = contigTree.getContigDescriptors().keySet().stream().max(Integer::compareTo).orElse(0);

      final var newContigIds = List.of(1 + maxContigId, 2 + maxContigId);
      final var newContigLengthBps = List.of(
        deltaBpsFromContigStart,
        oldContigDescriptor.getLengthBp() - deltaBpsFromContigStart - minBpResolution
      );

      final var newContigNames = switch (node.getContigDirection()) {
        case FORWARD -> List.of(
          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), 0, deltaBpsFromContigStart),
          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), newContigLengthBps.get(1), oldContigDescriptor.getLengthBp())
        );
        case REVERSED -> List.of(
          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), newContigLengthBps.get(1), oldContigDescriptor.getLengthBp()),
          String.format("%s_%d_%d", oldContigDescriptor.getContigName(), 0, deltaBpsFromContigStart)
        );
      };

      final var newContigNamesInSourceFasta = List.of(oldContigDescriptor.getContigNameInSourceFASTA(), oldContigDescriptor.getContigNameInSourceFASTA());
      final var newContigOffsetsInSourceFasta = switch (node.getContigDirection()) {
        case FORWARD ->
          List.of(oldContigDescriptor.getOffsetInSourceFASTA(), oldContigDescriptor.getOffsetInSourceFASTA() + (int) minBpResolution);
        case REVERSED ->
          List.of(oldContigDescriptor.getOffsetInSourceFASTA() + (int) minBpResolution, oldContigDescriptor.getOffsetInSourceFASTA());
      };


      final List<@NotNull List<@NotNull Long>> newContigLengthsBinsAtResolution = new ArrayList<>();
      final List<@NotNull List<@NotNull ContigHideType>> newContigPresenceAtResolution = new ArrayList<>();
      final List<@NotNull List<@NotNull List<@NotNull ATUDescriptor>>> newContigAtus = new ArrayList<>();

      for (int i = 1; i < this.chunkedFile.getResolutions().length; i++) {
        final var bpResolution = this.chunkedFile.getResolutions()[i];
        final var contigStartBinsAtResolution = this.chunkedFile.convertUnits(
          leftBps,
          ResolutionDescriptor.fromResolutionOrder(0),
          QueryLengthUnit.BASE_PAIRS,
          ResolutionDescriptor.fromResolutionOrder(i),
          QueryLengthUnit.BINS
        );
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
        newContigLengthsBinsAtResolution.add(newLengthsAtResolution);

        newContigPresenceAtResolution.add(newContigLengthBps.stream().map(bp -> (bp >= bpResolution) ? ContigHideType.SHOWN : ContigHideType.HIDDEN).toList());

        final var oldContigATUs = oldContigDescriptor.getAtus().get(i);
        final var oldATUsLengthBinsPrefixSum = oldContigDescriptor.getAtuPrefixSumLengthBins().get(i);

        final var indexOfATUWhereSplitOccurs = switch (node.getTrueDirection()) {
          case FORWARD ->
            BinarySearch.rightBinarySearch(oldATUsLengthBinsPrefixSum, deltaBinsFromContigStartAtResolution);
          case REVERSED ->
            BinarySearch.leftBinarySearch(oldATUsLengthBinsPrefixSum, oldContigLengthBinsAtResolution - deltaBinsFromContigStartAtResolution); //TODO: BinarySearch.
        };

        final var oldJoinATU = oldContigATUs.get(indexOfATUWhereSplitOccurs);

        final var ATUsL = oldContigATUs.subList(0, indexOfATUWhereSplitOccurs);
        final var ATUsR = oldContigATUs.subList(indexOfATUWhereSplitOccurs, oldContigATUs.size());

        final var leftATUsLength = (indexOfATUWhereSplitOccurs == 0) ? 0L : oldATUsLengthBinsPrefixSum[indexOfATUWhereSplitOccurs - 1];

        final var deltaL = deltaBinsFromContigStartAtResolution - leftATUsLength;


        if (deltaL > 0){

        }



        switch (node.getTrueDirection()) {
          case FORWARD -> {

          }
          case REVERSED -> {

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


    } finally {
      lock.writeLock().unlock();
    }
  }

}
