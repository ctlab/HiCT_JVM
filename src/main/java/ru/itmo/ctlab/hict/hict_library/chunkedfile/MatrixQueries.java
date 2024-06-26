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

package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.IndexMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5FileDatasetsBundle;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.util.BinarySearch;
import ru.itmo.ctlab.hict.hict_library.util.CommonUtils;
import ru.itmo.ctlab.hict.hict_library.util.matrix.SparseCOOMatrixLong;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class MatrixQueries {
  private final @NotNull ChunkedFile chunkedFile;

  public MatrixQueries.MatrixWithWeights getSubmatrix(final @NotNull ResolutionDescriptor resolutionDescriptor, final long startRowIncl, final long startColIncl, final long endRowExcl, final long endColExcl, final boolean excludeHiddenContigs) {
    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var units = excludeHiddenContigs ? QueryLengthUnit.PIXELS : QueryLengthUnit.BINS;
    final var totalAssemblyLength = excludeHiddenContigs ? (this.chunkedFile.getContigTree().getLengthInUnits(units, resolutionDescriptor)) : (this.chunkedFile.getMatrixSizeBins()[resolutionOrder]);
    final var startRow = CommonUtils.clamp(startRowIncl, 0L, totalAssemblyLength);
    final var endRow = CommonUtils.clamp(endRowExcl, 0L, totalAssemblyLength);
    final var startCol = CommonUtils.clamp(startColIncl, 0L, totalAssemblyLength);
    final var endCol = CommonUtils.clamp(endColExcl, 0L, totalAssemblyLength);
    final var symmetricQuery = (startRow == startCol) && (endRow == endCol);


    final var rowATUs = getATUsForRange(resolutionDescriptor, startRow, endRow, excludeHiddenContigs);
    final List<ATUDescriptor> colATUs;
    if (symmetricQuery) {
      colATUs = rowATUs;
    } else {
      colATUs = getATUsForRange(resolutionDescriptor, startCol, endCol, excludeHiddenContigs);
    }

    final var queryRows = (int) (endRowExcl - startRowIncl);
    final var queryCols = (int) (endColExcl - startColIncl);
    final long[][] result = new long[queryRows][queryCols];

    int deltaRow = (int) (startRow - startRowIncl);
    int deltaCol = (int) (startCol - startColIncl);

    final double[] rowWeights = rowATUs.parallelStream().flatMapToDouble(atu -> Arrays.stream(getWeightsByATU(atu))).toArray();
    final double[] colWeights = colATUs.parallelStream().flatMapToDouble(atu -> Arrays.stream(getWeightsByATU(atu))).toArray();

    final double[] paddedRowWeights = new double[queryRows];
    final double[] paddedColWeights = new double[queryCols];

    if (startCol < totalAssemblyLength && startRow < totalAssemblyLength && endRow > 0 && endCol > 0) {

      System.arraycopy(rowWeights, 0, paddedRowWeights, deltaRow, rowWeights.length);
      System.arraycopy(colWeights, 0, paddedColWeights, deltaCol, colWeights.length);


      try (final var pool = Executors.newWorkStealingPool()) {
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
    }


    return new MatrixQueries.MatrixWithWeights(result, paddedRowWeights, paddedColWeights, startRow, startCol, endRow, endCol, units, resolutionDescriptor);
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

  // TODO: Implement
  public List<ATUDescriptor> getATUsForRange(final @NotNull ResolutionDescriptor resolutionDescriptor, final long startPxIncl, final long endPxExcl, final boolean excludeHiddenContigs) {
    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var units = excludeHiddenContigs ? QueryLengthUnit.PIXELS : QueryLengthUnit.BINS;
    final var totalAssemblyLength = excludeHiddenContigs ? (this.chunkedFile.getContigTree().getLengthInUnits(units, resolutionDescriptor)) : (this.chunkedFile.getMatrixSizeBins()[resolutionOrder]);
    final var startPx = CommonUtils.clamp(startPxIncl, 0L, totalAssemblyLength);
    final var endPx = CommonUtils.clamp(endPxExcl, 0L, totalAssemblyLength);

    final var queryLength = endPx - startPx;
    if (queryLength <= 0) {
      return List.of();
    }

    final var es = this.chunkedFile.getContigTree().expose(resolutionDescriptor, startPx, endPx, units);

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
    final var firstContigNode = excludeHiddenContigs ? es.segment().leftmostVisibleNode(resolutionDescriptor) : es.segment().leftmost();
    final var firstContigDescriptor = firstContigNode.getContigDescriptor();
    final var firstContigATUs = firstContigDescriptor.getAtus().get(resolutionOrder);
    final var firstContigATUPrefixSum = firstContigDescriptor.getAtuPrefixSumLengthBins().get(resolutionOrder);
    final var firstContigDirection = firstContigNode.getTrueDirection();
    final var firstContigId = firstContigDescriptor.getContigId();

    final var deltaBetweenRightPxAndExposedSegment = (lessSize + segmentSize) - endPx;
    final var lastContigNode = excludeHiddenContigs ? es.segment().rightmostVisibleNode(resolutionDescriptor) : es.segment().rightmost();
    final var lastContigDescriptor = lastContigNode.getContigDescriptor();
    final var lastContigATUs = lastContigDescriptor.getAtus().get(resolutionOrder);
    final var lastContigATUPrefixSum = lastContigDescriptor.getAtuPrefixSumLengthBins().get(resolutionOrder);
    final var lastContigDirection = lastContigNode.getTrueDirection();
    final var lastContigId = lastContigDescriptor.getContigId();

    final var onlyOneContig = (firstContigId == lastContigId);


    final int indexOfATUContainingStartPx;
    if (firstContigNode.getTrueDirection() == ContigDirection.FORWARD) {
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

    final var oldFirstATU = switch (firstContigDirection) {
      case FORWARD -> firstContigATUs.get(indexOfATUContainingStartPx);
      case REVERSED -> firstContigATUs.get(indexOfATUContainingStartPx).reversed();
    };

    final var lengthOfATUsBeforeOneContainingStart = switch (firstContigDirection) {
      case FORWARD -> (indexOfATUContainingStartPx == 0) ? 0L : firstContigATUPrefixSum[indexOfATUContainingStartPx - 1];
      case REVERSED -> firstContigATUPrefixSum[firstContigATUPrefixSum.length - 1] - firstContigATUPrefixSum[indexOfATUContainingStartPx];
    };

    final ATUDescriptor newFirstATU = switch (oldFirstATU.getDirection()) {
      case FORWARD -> new ATUDescriptor(
        oldFirstATU.getStripeDescriptor(),
        oldFirstATU.getStartIndexInStripeIncl() + (int) (deltaBetweenSegmentFirstContigAndQueryStart -
          lengthOfATUsBeforeOneContainingStart),
        oldFirstATU.getEndIndexInStripeExcl(),
        oldFirstATU.getDirection()
      );
      case REVERSED -> new ATUDescriptor(
        oldFirstATU.getStripeDescriptor(),
        oldFirstATU.getStartIndexInStripeIncl(),
        oldFirstATU.getEndIndexInStripeExcl() - (int) (deltaBetweenSegmentFirstContigAndQueryStart -
          lengthOfATUsBeforeOneContainingStart),
        oldFirstATU.getDirection()
      );
    };

    assert (newFirstATU.getLength() > 0) : "Incorrect new first ATU??";

    final int indexOfATUContainingEndPx = switch (lastContigDirection) {
      case FORWARD -> BinarySearch.leftBinarySearch(
        lastContigATUPrefixSum,
        lastContigATUPrefixSum[lastContigATUPrefixSum.length - 1] - deltaBetweenRightPxAndExposedSegment
      );
      case REVERSED -> BinarySearch.rightBinarySearch(
        lastContigATUPrefixSum,
        deltaBetweenRightPxAndExposedSegment
      );
    };

    final long deletedATUsLength = switch (lastContigDirection) {
      case FORWARD -> lastContigATUPrefixSum[lastContigATUPrefixSum.length - 1] - lastContigATUPrefixSum[indexOfATUContainingEndPx];
      case REVERSED -> (indexOfATUContainingEndPx == 0) ? 0L : (lastContigATUPrefixSum[indexOfATUContainingEndPx - 1]);
    };


    final var sameATUIsFirstAndLast = onlyOneContig && (indexOfATUContainingStartPx == indexOfATUContainingEndPx);

    final ATUDescriptor oldLastATU = sameATUIsFirstAndLast ? newFirstATU : (
      switch (lastContigNode.getTrueDirection()) {
        case FORWARD -> lastContigATUs.get(indexOfATUContainingEndPx);
        case REVERSED -> lastContigATUs.get(indexOfATUContainingEndPx).reversed();
      }
    );

    final ATUDescriptor newLastATU = switch (oldLastATU.getDirection()) {
      case FORWARD -> new ATUDescriptor(
        oldLastATU.getStripeDescriptor(),
        oldLastATU.getStartIndexInStripeIncl(),
        oldLastATU.getEndIndexInStripeExcl() - (int) (deltaBetweenRightPxAndExposedSegment - deletedATUsLength),
        oldLastATU.getDirection()
      );
      case REVERSED -> new ATUDescriptor(
        oldLastATU.getStripeDescriptor(),
        oldLastATU.getStartIndexInStripeIncl() + (int) (deltaBetweenRightPxAndExposedSegment - deletedATUsLength),
        oldLastATU.getEndIndexInStripeExcl(),
        oldLastATU.getDirection()
      );
    };

    assert (newLastATU.getLength() > 0) : "Incorrect new last ATU??";

    final var atus = new ArrayList<ATUDescriptor>();

    if (onlyOneContig) {
      if (sameATUIsFirstAndLast) {
        final var result = new CopyOnWriteArrayList<ATUDescriptor>();
        result.add(newLastATU);
        return result;
      } else {
        atus.add(newFirstATU);
        final var firstContigIntermediateATUs = firstContigATUs.subList(
          1 + Integer.min(indexOfATUContainingStartPx, indexOfATUContainingEndPx),
          Integer.max(indexOfATUContainingStartPx, indexOfATUContainingEndPx)
        );
        final List<@NotNull ATUDescriptor> firstContigRestATUs = switch (firstContigDirection) {
          case FORWARD -> firstContigIntermediateATUs;
          case REVERSED -> {
            final var firstContigIntermediateATUsReversed = firstContigIntermediateATUs.parallelStream()
              .map(ATUDescriptor::reversed)
              .collect(Collectors.toList());
            Collections.reverse(firstContigIntermediateATUsReversed);
            yield firstContigIntermediateATUsReversed;
          }
        };
        atus.addAll(firstContigRestATUs);
      }
    } else {
      atus.add(newFirstATU);
      final List<@NotNull ATUDescriptor> firstContigRestATUs = switch (firstContigDirection) {
        case FORWARD -> firstContigATUs.subList(1 + indexOfATUContainingStartPx, firstContigATUs.size());
        case REVERSED -> {
          final var firstContigRestATUsReversed = firstContigATUs.parallelStream()
            .limit(indexOfATUContainingStartPx)
            .map(ATUDescriptor::reversed)
            .collect(Collectors.toList());
          Collections.reverse(firstContigRestATUsReversed);
          yield firstContigRestATUsReversed;
        }
      };

      atus.addAll(firstContigRestATUs);


      ContigTree.Node.traverseNodeAtResolution(es.segment(), resolutionDescriptor, node -> {
        final var nodeContigId = node.getContigDescriptor().getContigId();
        if (nodeContigId != firstContigId && nodeContigId != lastContigId) {
          final var contigDirection = node.getTrueDirection();
          final var contigATUs = node.getContigDescriptor().getAtus().get(resolutionOrder);
          final var processedATUs = switch (contigDirection) {
            case FORWARD -> contigATUs;
            case REVERSED -> {
              final var reversedATUs = contigATUs.stream()
                .map(ATUDescriptor::reversed)
                .collect(Collectors.toList());
              Collections.reverse(reversedATUs);
              yield reversedATUs;
            }
          };
          atus.addAll(processedATUs);
        }
      });

      final List<@NotNull ATUDescriptor> lastContigBeginningATUs = switch (lastContigDirection) {
        case FORWARD -> lastContigATUs.subList(0, indexOfATUContainingEndPx);
        case REVERSED -> {
          final var reversedATUs = lastContigATUs.parallelStream()
            .skip(1 + indexOfATUContainingEndPx)
            .map(ATUDescriptor::reversed)
            .collect(Collectors.toList());
          Collections.reverse(reversedATUs);
          yield reversedATUs;
        }
      };
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

    return new CopyOnWriteArrayList<>(reducedATUs);
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
    final var resolution = this.chunkedFile.getResolutions()[resolutionOrder];
    final var rowStripe = rowATU.getStripeDescriptor();
    final var colStripe = colATU.getStripeDescriptor();
    final var rowStripeId = rowStripe.stripeId();
    final var colStripeId = colStripe.stripeId();
    final var queryRows = rowATU.getLength();
    final var queryCols = colATU.getLength();
    final var blockOnMainDiagonal = (rowStripeId == colStripeId);

//    log.debug("Getting intersection of ATUs with stripes " + rowStripeId + " and " + colStripeId);
    final @NotNull var pool = this.chunkedFile.getDatasetBundlePools().get(resolutionOrder);
    @Nullable HDF5FileDatasetsBundle dsBundle = null;
    try {
      dsBundle = pool.borrowObject();
      Objects.requireNonNull(dsBundle);
      final var reader = dsBundle.getReader();
      final var blockIndexInDatasets = rowStripeId * this.chunkedFile.getStripeCount()[resolutionOrder] + colStripeId;
      final long blockLength;
      final long blockOffset;

      {
        final var blockLengthDataset = dsBundle.getBlockLengthDataSet();
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

      final var flipRows = ATUDirection.REVERSED.equals(rowATU.getDirection());
      final var flipCols = ATUDirection.REVERSED.equals(colATU.getDirection());

      {
        final var blockOffsetDataset = dsBundle.getBlockOffsetDataSet();
        final long[] buf = reader.int64().readArrayBlockWithOffset(blockOffsetDataset, 1, blockIndexInDatasets);
        blockOffset = buf[0];
      }

      final var savedAsSparse = (blockOffset >= 0L);

      if (savedAsSparse) {
        log.debug("Fetching sparse block");
        final long[] blockRows;
        final long[] blockCols;
        final long[] blockValues;


        blockRows = reader.int64().readArrayBlockWithOffset(dsBundle.getBlockRowsDataSet(), (int) blockLength, blockOffset);
        blockCols = reader.int64().readArrayBlockWithOffset(dsBundle.getBlockColsDataSet(), (int) blockLength, blockOffset);
        blockValues = reader.int64().readArrayBlockWithOffset(dsBundle.getBlockValuesDataSet(), (int) blockLength, blockOffset);


        final int rowStartIndex = BinarySearch.leftBinarySearch(blockRows, Integer.min(rowATU.getStartIndexInStripeIncl(), colATU.getStartIndexInStripeIncl()));
        final int rowEndIndex = BinarySearch.leftBinarySearch(blockRows, Integer.max(rowATU.getEndIndexInStripeExcl(), colATU.getEndIndexInStripeExcl()));
        final var maxCol = (int) Arrays.stream(blockCols).max().orElse(0L);


//        final var sparseMatrix = new SparseCOOMatrixLong(
//          Arrays.stream(blockRows).skip(rowStartIndex).limit(rowEndIndex - rowStartIndex).mapToInt(l -> (int) l).toArray(),
//          Arrays.stream(blockCols).skip(rowStartIndex).limit(rowEndIndex - rowStartIndex).mapToInt(l -> (int) l).toArray(),
//          Arrays.stream(blockValues).skip(rowStartIndex).limit(rowEndIndex - rowStartIndex).toArray(),
//          needsTranspose,
//          blockOnMainDiagonal,
//          flipRows,
//          flipCols
//        );
        final var sparseMatrix = new SparseCOOMatrixLong(
          Arrays.stream(blockRows).mapToInt(i -> (int) i).toArray(),
          Arrays.stream(blockCols).mapToInt(i -> (int) i).toArray(),
          blockValues,
          blockOnMainDiagonal
        );

        final var denseSquarePartial = sparseMatrix.toDense(this.chunkedFile.getDenseBlockSize(), this.chunkedFile.getDenseBlockSize());
        final var denseBlock = new long[queryRows][queryCols];

        for (int i = firstRow; i < lastRow; ++i) {
          System.arraycopy(denseSquarePartial[i], firstCol, denseBlock[i - firstRow], 0, queryCols);
        }

        if (flipRows) {
          ArrayUtils.reverse(denseBlock);
        }

        if (flipCols) {
          for (final var row : denseBlock) {
            ArrayUtils.reverse(row);
          }
        }

        if (needsTranspose) {
          for (int i = 0; i < queryRows; ++i) {
            for (int j = 0; j < queryCols; ++j) {
              denseMatrix[j][i] = denseBlock[i][j];
            }
          }
        } else {
          System.arraycopy(denseBlock, 0, denseMatrix, 0, queryRows);
        }
      } else {
        log.debug("Fetching dense block");
        final var idx = new IndexMap().bind(0, -(blockOffset + 1L)).bind(1, 0L);
        final MDLongArray block;
        {
          block = reader.int64().readSlicedMDArrayBlockWithOffset(dsBundle.getDenseBlockDataSet(), new int[]{this.chunkedFile.getDenseBlockSize(), this.chunkedFile.getDenseBlockSize()}, new long[]{0L, 0L}, idx);
        }
        final long[][] denseBlock;
        denseBlock = block.toMatrix();
        if (blockOnMainDiagonal) {
          for (int i = 0; i < denseBlock.length; ++i) {
            for (int j = 1 + i; j < denseBlock.length; ++j) {
              denseBlock[j][i] = denseBlock[i][j];
            }
          }
        }
        final var denseSubBlock = new long[queryRows][queryCols];

        for (int i = firstRow; i < lastRow; ++i) {
          System.arraycopy(denseBlock[i], firstCol, denseSubBlock[i - firstRow], 0, queryCols);
        }

        if (flipRows) {
          ArrayUtils.reverse(denseSubBlock);
        }
        if (flipCols) {
          for (final var row : denseSubBlock) {
            ArrayUtils.reverse(row);
          }
        }
        if (needsTranspose) {
          for (int i = 0; i < queryRows; ++i) {
            for (int j = 0; j < queryCols; ++j) {
              denseMatrix[j][i] = denseSubBlock[i][j];
            }
          }
        } else {
          System.arraycopy(denseSubBlock, 0, denseMatrix, 0, queryRows);
        }
      }

      return denseMatrix;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (null != dsBundle) {
        try {
          pool.returnObject(dsBundle);
        } catch (final Exception ignored) {
          // ignored
        }
      }
    }
  }

  public record MatrixWithWeights(long @NotNull [][] matrix, double @NotNull [] rowWeights,
                                  double @NotNull [] colWeights, long startRowIncl, long startColIncl, long endRowExcl,
                                  long endColExcl, @NotNull QueryLengthUnit units,
                                  @NotNull ResolutionDescriptor resolutionDescriptor) {
  }

}
