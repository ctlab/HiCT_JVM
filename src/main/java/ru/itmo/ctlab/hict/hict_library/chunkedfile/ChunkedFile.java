package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IndexMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;
import ru.itmo.ctlab.hict.hict_library.util.BinarySearch;
import ru.itmo.ctlab.hict.hict_library.util.matrix.SparseCOOMatrixLong;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.PathGenerators.*;

@Getter
@Slf4j
public class ChunkedFile {

  private final @NotNull Path hdfFilePath;
  //  private final long[] blockCount;
  private final int denseBlockSize;
  private final long @NotNull [] resolutions;
  private final Map<@NotNull Long, @NotNull Integer> resolutionToIndex;
  private final long @NotNull [] matrixSizeBins;
  private final int @NotNull [] stripeCount;
  private final @NotNull ContigTree contigTree;
  private final @NotNull ScaffoldTree scaffoldTree;


  public ChunkedFile(final @NotNull Path hdfFilePath, final int denseBlockSize) {
    this.hdfFilePath = hdfFilePath;
    // TODO: fix
//    this.blockCount = 5;
    this.denseBlockSize = denseBlockSize;

    try (final var reader = HDF5Factory.openForReading(this.hdfFilePath.toFile())) {

      this.resolutions = LongStream.concat(LongStream.of(0L), reader.object().getAllGroupMembers("/resolutions").parallelStream().filter(s -> {
        try {
          log.debug("Trying to parse " + s + " as a resolution");
          Long.parseLong(s);
          log.debug("Found new resolution: " + s);
          return true;
        } catch (final NumberFormatException nfe) {
          log.debug("Not a resolution: " + s);
          return false;
        }
      }).mapToLong(Long::parseLong)).sorted().toArray();

      log.debug("Resolutions count: " + resolutions.length);

      this.stripeCount = new int[resolutions.length];

      this.resolutionToIndex = new ConcurrentHashMap<>();
      for (int i = 0; i < this.resolutions.length; i++) {
        this.resolutionToIndex.put(this.resolutions[i], i);
      }
    }
    this.contigTree = new ContigTree();
    Initializers.initializeContigTree(this);
    this.matrixSizeBins = new long[this.resolutions.length];
    this.matrixSizeBins[0] = this.contigTree.getLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
    for (int i = 1; i < this.resolutions.length; ++i) {
      this.matrixSizeBins[i] = this.contigTree.getLengthInUnits(QueryLengthUnit.BINS, ResolutionDescriptor.fromResolutionOrder(i));
      log.debug("Matrix size at resolution order=" + i + " is " + this.matrixSizeBins[i]);
    }
    this.scaffoldTree = new ScaffoldTree(this.matrixSizeBins[0]);
    Initializers.initializeScaffoldTree(this);
  }


  // TODO: Implement
  public MatrixWithWeights getSubmatrix(final @NotNull ResolutionDescriptor resolutionDescriptor, final long startRowIncl, final long startColIncl, final long endRowExcl, final long endColExcl, final boolean excludeHiddenContigs) {
    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var units = excludeHiddenContigs ? QueryLengthUnit.PIXELS : QueryLengthUnit.BINS;
    final var totalAssemblyLength = excludeHiddenContigs ? (this.contigTree.getLengthInUnits(units, resolutionDescriptor)) : (this.matrixSizeBins[resolutionOrder]);
    final var startRow = clamp(startRowIncl, 0L, totalAssemblyLength);
    final var endRow = clamp(endRowExcl, 0L, totalAssemblyLength);
    final var startCol = clamp(startColIncl, 0L, totalAssemblyLength);
    final var endCol = clamp(endColExcl, 0L, totalAssemblyLength);
    final var symmetricQuery = (startRow == startCol) && (endRow == endCol);


    final var rowATUs = getATUsForRange(resolutionDescriptor, startRow, endRow, excludeHiddenContigs);
    final List<ATUDescriptor> colATUs;
    if (symmetricQuery) {
      colATUs = rowATUs;
    } else {
      colATUs = getATUsForRange(resolutionDescriptor, startCol, endCol, excludeHiddenContigs);
    }

    final long[][] result = new long[(int) (endRowExcl - startRowIncl)][(int) (endColExcl - startColIncl)];

    int deltaRow = (int) (startRow - startRowIncl);
    int deltaCol = (int) (startCol - startColIncl);

    final double[] rowWeights = rowATUs.parallelStream().flatMapToDouble(atu -> Arrays.stream(getWeightsByATU(atu))).toArray();
    final double[] colWeights = colATUs.parallelStream().flatMapToDouble(atu -> Arrays.stream(getWeightsByATU(atu))).toArray();

    try (final var pool = Executors.newWorkStealingPool(64)) {
      if (symmetricQuery) {
        final var atuCount = rowATUs.size();
        log.debug("Symmetric query with " + atuCount + " ATUs");
        var startDeltaCol = (int) (startCol - startColIncl);
        for (int i = 0; i < atuCount; ++i) {
          final var rowATU = rowATUs.get(i);
          deltaCol = startDeltaCol;
          final var rowCount = rowATU.getLength();
          for (int j = i; j < atuCount; ++j) {
            final var colATU = colATUs.get(j);
            final int finalDeltaCol = deltaCol;
            final int finalDeltaRow = deltaRow;
            final var colCount = colATU.getLength();
            pool.submit(() -> {
              final var dense = getATUIntersection(resolutionDescriptor, rowATU, colATU);
              for (int k = 0; k < rowCount; k++) {
                System.arraycopy(dense[k], 0, result[finalDeltaRow + k], finalDeltaCol, colCount);
              }
              for (int k = 0; k < rowCount; k++) {
                for (int l = 0; l < colCount; l++) {
                  result[finalDeltaCol + l][finalDeltaRow + k] = dense[k][l];
                }
              }
            });
            deltaCol += colCount;
          }
          startDeltaCol += colATUs.get(i).getLength();
          deltaRow += rowCount;
        }
      } else {
        for (final var rowATU : rowATUs) {
          deltaCol = (int) (startCol - startColIncl);
          final var rowCount = rowATU.getLength();
          for (final var colATU : colATUs) {
            final int finalDeltaCol = deltaCol;
            final int finalDeltaRow = deltaRow;
            final var colCount = colATU.getLength();

            if (finalDeltaRow > finalDeltaCol) {
              log.debug("AAA");
            }

            pool.submit(() -> {
              try {
                final var dense = getATUIntersection(resolutionDescriptor, rowATU, colATU);
                for (int k = 0; k < rowCount; k++) {
                  System.arraycopy(dense[k], 0, result[finalDeltaRow + k], finalDeltaCol, colCount);
                }
              } catch (final Throwable ex) {
                throw new RuntimeException("Dense matrix fetch failed");
              }
            });

            deltaCol += colCount;
          }
          deltaRow += rowCount;
        }
      }
    }


    return new MatrixWithWeights(result, rowWeights, colWeights);
  }

  private double @NotNull [] getWeightsByATU(final @NotNull ATUDescriptor atu) {
    return getWeightsByATU(atu, false);
  }

  private double @NotNull [] getWeightsByATU(final @NotNull ATUDescriptor atu, final boolean needsReversal) {
    final var length = atu.getLength();
    final var weights = new double[length];
    System.arraycopy(atu.getStripeDescriptor().bin_weights(), atu.getStartIndexInStripeIncl(), weights, 0, length);
    if ((atu.getDirection() == ATUDirection.REVERSED) ^ needsReversal) {
      ArrayUtils.reverse(weights);
    }
    return weights;
  }

  public static long clamp(final long x, final long min, final long max) {
    return Long.max(min, Long.min(x, max));
  }

  // TODO: Implement
  public List<ATUDescriptor> getATUsForRange(final @NotNull ResolutionDescriptor resolutionDescriptor, final long startPxIncl, final long endPxExcl, final boolean excludeHiddenContigs) {
    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var units = excludeHiddenContigs ? QueryLengthUnit.PIXELS : QueryLengthUnit.BINS;
    final var totalAssemblyLength = excludeHiddenContigs ? (this.contigTree.getLengthInUnits(units, resolutionDescriptor)) : (this.matrixSizeBins[resolutionOrder]);
    final var startPx = clamp(startPxIncl, 0L, totalAssemblyLength);
    final var endPx = clamp(endPxExcl, 0L, totalAssemblyLength);

    final var queryLength = endPx - startPx;
    if (queryLength <= 0) {
      return List.of();
    }

    final var es = this.contigTree.expose(resolutionDescriptor, startPx, endPx, units);

    assert (es.segment() != null) : "Non-zero query length but no segment?";

    final var segmentSize = es.segment().getSubtreeLengthInUnits(units, resolutionDescriptor);
    final long lessSize;
    if (es.less() != null) {
      lessSize = es.less().getSubtreeLengthInUnits(units, resolutionDescriptor);
    } else {
      lessSize = 0L;
    }

    final List<ContigTree.Node> debugContigNodes = new ArrayList<>();
    ContigTree.Node.traverseNodeAtResolution(es.segment(), resolutionDescriptor, node -> {
      debugContigNodes.add(node);
    });

    final List<ATUDescriptor> debugAllATUs = new ArrayList<>();

    ContigTree.Node.traverseNodeAtResolution(es.segment(), resolutionDescriptor, node -> {
      final var contigDirection = node.getTrueDirection();
      final var contigATUs = node.getContigDescriptor().getAtus().get(resolutionOrder);
      if (contigDirection == ContigDirection.FORWARD) {
        debugAllATUs.addAll(contigATUs);
      } else {
        final var reversedATUs = contigATUs.stream().map(ATUDescriptor::reversed).collect(Collectors.toList());
        Collections.reverse(reversedATUs);
        debugAllATUs.addAll(reversedATUs);
      }
    });


    final var deltaBetweenSegmentFirstContigAndQueryStart = startPx - lessSize;
    final var firstContigNodeInSegment = es.segment().leftmostVisibleNode(resolutionDescriptor);
    final var firstContigInSegment = firstContigNodeInSegment.getContigDescriptor();
    final var firstContigATUs = firstContigInSegment.getAtus().get(resolutionOrder);
    final var firstContigATUPrefixSum = firstContigInSegment.getAtuPrefixSumLengthBins().get(resolutionOrder);

    final var deltaBetweenRightPxAndExposedSegment = (lessSize + segmentSize) - endPx;
    final var lastContigNode = es.segment().rightmostVisibleNode(resolutionDescriptor);
    final var lastContigDescriptor = lastContigNode.getContigDescriptor();
    final var lastContigATUs = lastContigDescriptor.getAtus().get(resolutionOrder);
    final var lastContigATUPrefixSum = lastContigDescriptor.getAtuPrefixSumLengthBins().get(resolutionOrder);

    final var startContigId = firstContigInSegment.getContigId();
    final var endContigId = lastContigDescriptor.getContigId();
    final var onlyOneContig = (startContigId == endContigId);


    final int indexOfATUContainingStartPx;
    if (firstContigNodeInSegment.getTrueDirection() == ContigDirection.FORWARD) {
      indexOfATUContainingStartPx = BinarySearch.rightBinarySearch(
        firstContigATUPrefixSum,
        deltaBetweenSegmentFirstContigAndQueryStart
      );
    } else {
      final var topSum = firstContigATUPrefixSum[firstContigATUPrefixSum.length - 1];
      indexOfATUContainingStartPx = BinarySearch.leftBinarySearch(
        firstContigATUPrefixSum,
        topSum - deltaBetweenSegmentFirstContigAndQueryStart
      );
    }

    final var oldFirstATU = firstContigATUs.get(indexOfATUContainingStartPx);
    final var lengthOfATUsBeforeOneContainingStart = (indexOfATUContainingStartPx == 0) ? 0L : (
      switch (firstContigNodeInSegment.getTrueDirection()) {
        case FORWARD -> firstContigATUPrefixSum[indexOfATUContainingStartPx - 1];
        case REVERSED ->
          firstContigATUPrefixSum[firstContigATUPrefixSum.length - 1] - firstContigATUPrefixSum[indexOfATUContainingStartPx];
      }
    );
    final ATUDescriptor newFirstATU;

    if (oldFirstATU.getDirection() == ATUDirection.FORWARD) {
      newFirstATU = new ATUDescriptor(oldFirstATU.getStripeDescriptor(), oldFirstATU.getStartIndexInStripeIncl() + (int) (deltaBetweenSegmentFirstContigAndQueryStart -
        lengthOfATUsBeforeOneContainingStart), oldFirstATU.getEndIndexInStripeExcl(), oldFirstATU.getDirection());
    } else {
      newFirstATU = new ATUDescriptor(oldFirstATU.getStripeDescriptor(), oldFirstATU.getStartIndexInStripeIncl(), oldFirstATU.getEndIndexInStripeExcl() - (int) (deltaBetweenSegmentFirstContigAndQueryStart -
        lengthOfATUsBeforeOneContainingStart), oldFirstATU.getDirection());
    }

    assert (newFirstATU.getLength() > 0) : "Incorrect new first ATU??";

    final int indexOfATUContainingEndPx;

    if (lastContigNode.getTrueDirection() == ContigDirection.FORWARD) {
      final var lastContigLengthBins = lastContigATUPrefixSum[lastContigATUPrefixSum.length - 1];
      indexOfATUContainingEndPx = BinarySearch.leftBinarySearch(lastContigATUPrefixSum, lastContigLengthBins - deltaBetweenRightPxAndExposedSegment);
    } else {
      indexOfATUContainingEndPx = BinarySearch.rightBinarySearch(
        lastContigATUPrefixSum,
        deltaBetweenSegmentFirstContigAndQueryStart
      );
    }

    final long deletedATUsLength = switch (lastContigNode.getTrueDirection()) {
      case FORWARD ->
        lastContigATUPrefixSum[lastContigATUPrefixSum.length - 1] - lastContigATUPrefixSum[indexOfATUContainingEndPx];
      case REVERSED -> (indexOfATUContainingEndPx == 0) ? 0L : (lastContigATUPrefixSum[indexOfATUContainingEndPx - 1]);
    };

    final ATUDescriptor oldLastATU;

    if (onlyOneContig && indexOfATUContainingStartPx == indexOfATUContainingEndPx) {
      oldLastATU = newFirstATU;
    } else {
      oldLastATU = lastContigATUs.get(indexOfATUContainingEndPx);
    }
    final ATUDescriptor newLastATU;

    // TODO: Where ATU direction matters, where contigs'?

    if (oldLastATU.getDirection() == ATUDirection.FORWARD) {
      newLastATU = new ATUDescriptor(
        oldLastATU.getStripeDescriptor(),
        oldLastATU.getStartIndexInStripeIncl(),
        oldLastATU.getEndIndexInStripeExcl() + (int) (deletedATUsLength - deltaBetweenRightPxAndExposedSegment),
        oldLastATU.getDirection());
    } else {
      newLastATU = new ATUDescriptor(
        oldLastATU.getStripeDescriptor(),
        oldLastATU.getStartIndexInStripeIncl() - (int) (deletedATUsLength - deltaBetweenRightPxAndExposedSegment),
        oldLastATU.getEndIndexInStripeExcl(),
        oldLastATU.getDirection());
    }

    assert (newLastATU.getLength() > 0) : "Incorrect new last ATU??";

    final var atus = new ArrayList<ATUDescriptor>();

    atus.add(newFirstATU);

    if (onlyOneContig) {
      if (indexOfATUContainingStartPx != indexOfATUContainingEndPx) {
        final var firstContigIntermediateATUs = firstContigInSegment.getAtus().get(resolutionOrder).subList(1 + Integer.min(indexOfATUContainingStartPx, indexOfATUContainingEndPx), Integer.max(indexOfATUContainingStartPx, indexOfATUContainingEndPx));
        if (firstContigNodeInSegment.getTrueDirection() == ContigDirection.REVERSED) {
          Collections.reverse(firstContigIntermediateATUs);
        }
        atus.addAll(firstContigIntermediateATUs);
      } else {
        return atus;
      }
    } else {
      final List<@NotNull ATUDescriptor> firstContigRestATUs;
      if (firstContigNodeInSegment.getTrueDirection() == ContigDirection.FORWARD) {
        firstContigRestATUs = firstContigATUs.subList(1 + indexOfATUContainingStartPx, firstContigATUs.size());
      } else {
        firstContigRestATUs = firstContigATUs.parallelStream().limit(indexOfATUContainingStartPx).map(ATUDescriptor::reversed).collect(Collectors.toList());
        Collections.reverse(firstContigRestATUs);
      }
      atus.addAll(firstContigRestATUs);


      ContigTree.Node.traverseNodeAtResolution(es.segment(), resolutionDescriptor, node -> {
        final var nodeContigId = node.getContigDescriptor().getContigId();
        if (nodeContigId != startContigId && nodeContigId != endContigId) {
          final var contigDirection = node.getTrueDirection();
          final var contigATUs = node.getContigDescriptor().getAtus().get(resolutionOrder);
          if (contigDirection == ContigDirection.FORWARD) {
            atus.addAll(contigATUs);
          } else {
            final var reversedATUs = contigATUs.stream().map(ATUDescriptor::reversed).collect(Collectors.toList());
            Collections.reverse(reversedATUs);
            atus.addAll(reversedATUs);
          }
        }
      });

      final List<@NotNull ATUDescriptor> lastContigBeginningATUs;
      if (lastContigNode.getTrueDirection() == ContigDirection.FORWARD) {
        lastContigBeginningATUs = lastContigATUs.subList(0, indexOfATUContainingEndPx);
      } else {
        lastContigBeginningATUs = lastContigATUs.parallelStream().skip(1 + indexOfATUContainingEndPx).map(ATUDescriptor::reversed).collect(Collectors.toList());
        Collections.reverse(lastContigBeginningATUs);
      }
      atus.addAll(lastContigBeginningATUs);
    }

    atus.add(newLastATU);

    {


      final var sourceATUTotalLength = debugContigNodes.stream().flatMap(node -> node.getContigDescriptor().getAtus().get(resolutionOrder).stream()).mapToInt(ATUDescriptor::getLength).sum();

      assert (segmentSize == sourceATUTotalLength) : "Expose returned more ATUs than segment length??";

      final var collectedATUsTotalLength = atus.stream().mapToLong(ATUDescriptor::getLength).sum();

      assert (
        collectedATUsTotalLength == (endPx - startPx)
      ) : "Wrong total length of ATUs before reduction??";
    }

    final var reducedATUs = ATUDescriptor.reduce(atus);

    assert (
      reducedATUs.stream().mapToLong(atu -> atu.getEndIndexInStripeExcl() - atu.getStartIndexInStripeIncl()).sum() == (endPx - startPx)
    ) : "Wrong total length of ATUs after reduction??";

    return reducedATUs;
  }

  public long @NotNull [][] getATUIntersection(final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull ATUDescriptor rowATU, final @NotNull ATUDescriptor colATU) {
    return getATUIntersection(resolutionDescriptor, rowATU, colATU, false);
  }

  // TODO: Implement
  public long @NotNull [][] getATUIntersection(final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull ATUDescriptor rowATU, final @NotNull ATUDescriptor colATU, final boolean needsTranspose) {
    if (rowATU.getStripeDescriptor().stripeId() > colATU.getStripeDescriptor().stripeId()) {
      return getATUIntersection(resolutionDescriptor, colATU, rowATU, !needsTranspose);
    }

    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var resolution = this.resolutions[resolutionOrder];
    final var rowStripe = rowATU.getStripeDescriptor();
    final var colStripe = colATU.getStripeDescriptor();
    final var rowStripeId = rowStripe.stripeId();
    final var colStripeId = colStripe.stripeId();
    final var queryRows = rowATU.getLength();
    final var queryCols = colATU.getLength();

//    log.debug("Getting intersection of ATUs with stripes " + rowStripeId + " and " + colStripeId);

    try (final var reader = HDF5Factory.openForReading(this.hdfFilePath.toFile())) {
      final var blockIndexInDatasets = rowStripeId * this.stripeCount[resolutionOrder] + colStripeId;
      final long blockLength;
      final long blockOffset;

      try (final var blockLengthDataset = reader.object().openDataSet(getBlockLengthDatasetPath(resolution))) {
        final long[] buf = reader.int64().readArrayBlockWithOffset(blockLengthDataset, 1, blockIndexInDatasets);
        blockLength = buf[0];
      }

      final boolean isEmpty = (blockLength == 0L);
      final long[][] denseMatrix = new long[needsTranspose ? queryCols : queryRows][needsTranspose ? queryRows : queryCols];

      if (isEmpty) {
        log.debug("Zero ATU intersection");
        return denseMatrix;
      }

      final var firstRow = rowATU.getStartIndexInStripeIncl();
      final var firstCol = colATU.getStartIndexInStripeIncl();
      final var lastRow = rowATU.getEndIndexInStripeExcl();
      final var lastCol = colATU.getEndIndexInStripeExcl();

      try (final var blockOffsetDataset = reader.object().openDataSet(getBlockOffsetDatasetPath(resolution))) {
        final long[] buf = reader.int64().readArrayBlockWithOffset(blockOffsetDataset, 1, blockIndexInDatasets);
        blockOffset = buf[0];
      }

      if (blockOffset >= 0L) {
        log.debug("Fetching sparse block");
        // Fetch sparse block
        final long[] blockRows;
        final long[] blockCols;
        final long[] blockValues;

        try (final var blockRowsDataset = reader.object().openDataSet(PathGenerators.getBlockRowsDatasetPath(resolution))) {
          blockRows = reader.int64().readArrayBlockWithOffset(blockRowsDataset, (int) blockLength, blockOffset);
        }
        try (final var blockColsDataset = reader.object().openDataSet(PathGenerators.getBlockColsDatasetPath(resolution))) {
          blockCols = reader.int64().readArrayBlockWithOffset(blockColsDataset, (int) blockLength, blockOffset);
        }
        try (final var blockValuesDataset = reader.object().openDataSet(getBlockValuesDatasetPath(resolution))) {
          blockValues = reader.int64().readArrayBlockWithOffset(blockValuesDataset, (int) blockLength, blockOffset);
        }

        final int rowStartIndex = BinarySearch.leftBinarySearch(blockRows, Integer.min(rowATU.getStartIndexInStripeIncl(), colATU.getStartIndexInStripeIncl()));
        final int rowEndIndex = BinarySearch.leftBinarySearch(blockRows, Integer.max(rowATU.getEndIndexInStripeExcl(), colATU.getEndIndexInStripeExcl()));
        final var maxCol = (int) Arrays.stream(blockCols).max().orElse(0L);


        final var sparseMatrix = new SparseCOOMatrixLong(
          Arrays.stream(blockRows).skip(rowStartIndex).limit(rowEndIndex - rowStartIndex).mapToInt(l -> (int) l).toArray(),
          Arrays.stream(blockCols).skip(rowStartIndex).limit(rowEndIndex - rowStartIndex).mapToInt(l -> (int) l).toArray(),
          Arrays.stream(blockValues).skip(rowStartIndex).limit(rowEndIndex - rowStartIndex).toArray(),
          needsTranspose,
          (rowStripeId == colStripeId)
        );
        final var denseSquarePartial = sparseMatrix.toDense(this.denseBlockSize, this.denseBlockSize);

        if (!needsTranspose) {
          for (int i = firstRow; i < lastRow; ++i) {
            System.arraycopy(denseSquarePartial[i], firstCol, denseMatrix[i - firstRow], 0, queryCols);
          }
        } else {
          for (int i = firstCol; i < lastCol; ++i) {
            System.arraycopy(denseSquarePartial[i], firstRow, denseMatrix[i - firstCol], 0, queryRows);
          }
        }
      } else {
        log.debug("Fetching dense block");
        // Fetch dense block
        final var idx = new IndexMap().bind(0, -(blockOffset + 1L)).bind(1, 0L);
        final MDLongArray block;
        try (final var denseBlockDataset = reader.object().openDataSet(getDenseBlockDatasetPath(resolution))) {
          block = reader.int64().readSlicedMDArrayBlockWithOffset(denseBlockDataset, new int[]{this.denseBlockSize, this.denseBlockSize}, new long[]{0L, 0L}, idx);
        }
        final long[][] denseBlock;
        if (rowStripeId == colStripeId) {
          denseBlock = block.toMatrix();
          for (int i = 0; i < denseBlock.length; ++i) {
            for (int j = 1 + i; j < denseBlock.length; ++j) {
              denseBlock[j][i] = denseBlock[i][j];
            }
          }
        } else {
          denseBlock = block.toMatrix();
        }
        if (needsTranspose) {
          for (int i = firstRow; i < lastRow; ++i) {
            for (int j = firstCol; j < lastCol; ++j) {
              denseMatrix[j - firstCol][i - firstRow] = denseBlock[i][j];
            }
          }
        } else {
          for (int i = firstRow; i < lastRow; ++i) {
            System.arraycopy(denseBlock[i], firstCol, denseMatrix[i - firstRow], 0, queryCols);
          }
        }
      }

      return denseMatrix;
    }
  }

  public long @NotNull [] getResolutions() {
    return this.resolutions;
  }

  public @NotNull List<@NotNull Long> getResolutionsList() {
    return Arrays.stream(this.resolutions).boxed().toList();
  }

  public @NotNull AssemblyInfo getAssemblyInfo() {
    return new AssemblyInfo(this.contigTree.getContigList(), this.scaffoldTree.getScaffoldList());
  }

  public record MatrixWithWeights(long @NotNull [][] matrix, double @NotNull [] rowWeights,
                                  double @NotNull [] colWeights) {
  }
}
