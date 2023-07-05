package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

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
        final var newSegmentNode = es.segment().cloneBuilder().needsChangingDirection(!es.segment().isNeedsChangingDirection()).build().push();
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

}
