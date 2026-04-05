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

import ch.systemsx.cisd.hdf5.HDF5DataClass;
import ch.systemsx.cisd.hdf5.IndexMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class MatrixQueries {
  private final @NotNull ChunkedFile chunkedFile;
  private static final boolean ASSERTIONS_ENABLED = MatrixQueries.class.desiredAssertionStatus();
  private final Map<Integer, BlockMetaRowCache> blockMetaRowCaches = new ConcurrentHashMap<>();
  private final BlockDataCache blockDataCache = new BlockDataCache(Long.getLong("HICT_BLOCK_DATA_CACHE_BYTES", 128L * 1024L * 1024L));

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

    int deltaRow = (int) (startRow - startRowIncl);
    int deltaCol = (int) (startCol - startColIncl);

    final double[] rowWeights = flattenWeights(rowATUs);
    final double[] colWeights = symmetricQuery ? rowWeights : flattenWeights(colATUs);

    final double[] paddedRowWeights = new double[queryRows];
    final double[] paddedColWeights = new double[queryCols];

    if (startCol < totalAssemblyLength && startRow < totalAssemblyLength && endRow > 0 && endCol > 0) {

      System.arraycopy(rowWeights, 0, paddedRowWeights, deltaRow, rowWeights.length);
      System.arraycopy(colWeights, 0, paddedColWeights, deltaCol, colWeights.length);


      final @NotNull var pool = this.chunkedFile.getDatasetBundlePools().get(resolutionOrder);
      @Nullable HDF5FileDatasetsBundle dsBundle = null;
      try {
        dsBundle = pool.borrowObject();
        Objects.requireNonNull(dsBundle);
        final var blockMetaCache = new HashMap<Long, BlockMeta>();
        prefillBlockMetaCache(dsBundle, resolutionOrder, rowATUs, colATUs, blockMetaCache);
        final var result = isFloatingPointDataset(dsBundle.getReader(), dsBundle.getBlockValuesDataSet())
          ? fillSubmatrixAsDoubles(
          dsBundle,
          resolutionDescriptor,
          rowATUs,
          colATUs,
          queryRows,
          queryCols,
          symmetricQuery,
          startRow,
          startRowIncl,
          startCol,
          startColIncl,
          blockMetaCache
        )
          : fillSubmatrixAsLongs(
          dsBundle,
          resolutionDescriptor,
          rowATUs,
          colATUs,
          queryRows,
          queryCols,
          symmetricQuery,
          startRow,
          startRowIncl,
          startCol,
          startColIncl,
          blockMetaCache
        );
        return new MatrixQueries.MatrixWithWeights(result, paddedRowWeights, paddedColWeights, startRow, startCol, endRow, endCol, units, resolutionDescriptor);
      } catch (final Exception ex) {
        throw new RuntimeException("Dense matrix fetch failed", ex);
      } finally {
        if (dsBundle != null) {
          try {
            pool.returnObject(dsBundle);
          } catch (final Exception ignored) {
            // ignored
          }
        }
      }
    }


    return new MatrixQueries.MatrixWithWeights(new LongMatrix(new long[queryRows][queryCols]), paddedRowWeights, paddedColWeights, startRow, startCol, endRow, endCol, units, resolutionDescriptor);
  }

  private @NotNull RawMatrix fillSubmatrixAsLongs(
    final @NotNull HDF5FileDatasetsBundle dsBundle,
    final @NotNull ResolutionDescriptor resolutionDescriptor,
    final @NotNull List<@NotNull ATUDescriptor> rowATUs,
    final @NotNull List<@NotNull ATUDescriptor> colATUs,
    final int queryRows,
    final int queryCols,
    final boolean symmetricQuery,
    final long startRow,
    final long startRowIncl,
    final long startCol,
    final long startColIncl,
    final @NotNull Map<Long, BlockMeta> blockMetaCache
  ) {
    final var result = new long[queryRows][queryCols];
    int deltaRow = (int) (startRow - startRowIncl);
    int deltaCol;
    if (symmetricQuery) {
      final var atuCount = rowATUs.size();
      var startDeltaCol = (int) (startCol - startColIncl);
      for (int i = 0; i < atuCount; ++i) {
        final var rowATU = rowATUs.get(i);
        deltaCol = startDeltaCol;
        final var rowCount = rowATU.getLength();
        for (int j = i; j < atuCount; ++j) {
          final var colATU = colATUs.get(j);
          final var colCount = colATU.getLength();
          fillATUIntersectionIntoWithBundle(dsBundle, resolutionDescriptor, rowATU, colATU, result, deltaRow, deltaCol, false, blockMetaCache);
          if (i != j) {
            for (int k = 0; k < rowCount; k++) {
              for (int l = 0; l < colCount; l++) {
                result[deltaCol + l][deltaRow + k] = result[deltaRow + k][deltaCol + l];
              }
            }
          }
          deltaCol += colCount;
        }
        startDeltaCol += colATUs.get(i).getLength();
        deltaRow += rowCount;
      }
    } else {
      for (final var rowATU : rowATUs) {
        deltaCol = (int) (startCol - startColIncl);
        for (final var colATU : colATUs) {
          fillATUIntersectionIntoWithBundle(dsBundle, resolutionDescriptor, rowATU, colATU, result, deltaRow, deltaCol, false, blockMetaCache);
          deltaCol += colATU.getLength();
        }
        deltaRow += rowATU.getLength();
      }
    }
    return new LongMatrix(result);
  }

  private @NotNull RawMatrix fillSubmatrixAsDoubles(
    final @NotNull HDF5FileDatasetsBundle dsBundle,
    final @NotNull ResolutionDescriptor resolutionDescriptor,
    final @NotNull List<@NotNull ATUDescriptor> rowATUs,
    final @NotNull List<@NotNull ATUDescriptor> colATUs,
    final int queryRows,
    final int queryCols,
    final boolean symmetricQuery,
    final long startRow,
    final long startRowIncl,
    final long startCol,
    final long startColIncl,
    final @NotNull Map<Long, BlockMeta> blockMetaCache
  ) {
    final var result = new double[queryRows][queryCols];
    int deltaRow = (int) (startRow - startRowIncl);
    int deltaCol;
    if (symmetricQuery) {
      final var atuCount = rowATUs.size();
      var startDeltaCol = (int) (startCol - startColIncl);
      for (int i = 0; i < atuCount; ++i) {
        final var rowATU = rowATUs.get(i);
        deltaCol = startDeltaCol;
        final var rowCount = rowATU.getLength();
        for (int j = i; j < atuCount; ++j) {
          final var colATU = colATUs.get(j);
          final var colCount = colATU.getLength();
          fillATUIntersectionIntoWithBundle(dsBundle, resolutionDescriptor, rowATU, colATU, result, deltaRow, deltaCol, false, blockMetaCache);
          if (i != j) {
            for (int k = 0; k < rowCount; k++) {
              for (int l = 0; l < colCount; l++) {
                result[deltaCol + l][deltaRow + k] = result[deltaRow + k][deltaCol + l];
              }
            }
          }
          deltaCol += colCount;
        }
        startDeltaCol += colATUs.get(i).getLength();
        deltaRow += rowCount;
      }
    } else {
      for (final var rowATU : rowATUs) {
        deltaCol = (int) (startCol - startColIncl);
        for (final var colATU : colATUs) {
          fillATUIntersectionIntoWithBundle(dsBundle, resolutionDescriptor, rowATU, colATU, result, deltaRow, deltaCol, false, blockMetaCache);
          deltaCol += colATU.getLength();
        }
        deltaRow += rowATU.getLength();
      }
    }
    return new DoubleMatrix(result);
  }

  private double @NotNull [] flattenWeights(final @NotNull List<@NotNull ATUDescriptor> atus) {
    final var totalLength = atus.stream().mapToInt(ATUDescriptor::getLength).sum();
    final var result = new double[totalLength];
    var dst = 0;
    for (final var atu : atus) {
      final var src = atu.getStripeDescriptor().bin_weights();
      final var start = atu.getStartIndexInStripeIncl();
      final var end = atu.getEndIndexInStripeExcl();
      final var len = end - start;
      if (atu.getDirection() == ATUDirection.FORWARD) {
        System.arraycopy(src, start, result, dst, len);
      } else {
        for (int i = 0; i < len; i++) {
          result[dst + i] = src[end - 1 - i];
        }
      }
      dst += len;
    }
    return result;
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

    final List<ContigTree.Node> debugContigNodes;
    if (ASSERTIONS_ENABLED) {
      debugContigNodes = new ArrayList<>();
      ContigTree.Node.traverseNodeAtResolution(es.segment(), resolutionDescriptor, debugContigNodes::add);
    } else {
      debugContigNodes = List.of();
    }


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
        return List.of(newLastATU);
      } else {
        atus.add(newFirstATU);
        final var firstContigIntermediateATUs = firstContigATUs.subList(
          1 + Integer.min(indexOfATUContainingStartPx, indexOfATUContainingEndPx),
          Integer.max(indexOfATUContainingStartPx, indexOfATUContainingEndPx)
        );
        final List<@NotNull ATUDescriptor> firstContigRestATUs = switch (firstContigDirection) {
          case FORWARD -> firstContigIntermediateATUs;
          case REVERSED -> {
            final var firstContigIntermediateATUsReversed = firstContigIntermediateATUs.stream()
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
          final var firstContigRestATUsReversed = firstContigATUs.stream()
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
          final var reversedATUs = lastContigATUs.stream()
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

    if (ASSERTIONS_ENABLED) {
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

  public double @NotNull [][] getATUIntersection(final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull ATUDescriptor rowATU, final @NotNull ATUDescriptor colATU) {
    return getATUIntersection(resolutionDescriptor, rowATU, colATU, false);
  }

  public double @NotNull [][] getATUIntersection(final @NotNull ResolutionDescriptor resolutionDescriptor, final @NotNull ATUDescriptor rowATU, final @NotNull ATUDescriptor colATU, final boolean needsTranspose) {
    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final @NotNull var pool = this.chunkedFile.getDatasetBundlePools().get(resolutionOrder);
    @Nullable HDF5FileDatasetsBundle dsBundle = null;
    try {
      dsBundle = pool.borrowObject();
      Objects.requireNonNull(dsBundle);
      return getATUIntersectionWithBundle(dsBundle, resolutionDescriptor, rowATU, colATU, needsTranspose, null);
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

  private double @NotNull [][] getATUIntersectionWithBundle(
    final @NotNull HDF5FileDatasetsBundle dsBundle,
    final @NotNull ResolutionDescriptor resolutionDescriptor,
    final @NotNull ATUDescriptor rowATU,
    final @NotNull ATUDescriptor colATU,
    final boolean needsTranspose,
    final @Nullable Map<Long, BlockMeta> blockMetaCache
  ) {
    final var denseMatrix = new double[needsTranspose ? colATU.getLength() : rowATU.getLength()][needsTranspose ? rowATU.getLength() : colATU.getLength()];
    fillATUIntersectionIntoWithBundle(dsBundle, resolutionDescriptor, rowATU, colATU, denseMatrix, 0, 0, needsTranspose, blockMetaCache);
    return denseMatrix;
  }

  private void fillATUIntersectionIntoWithBundle(
    final @NotNull HDF5FileDatasetsBundle dsBundle,
    final @NotNull ResolutionDescriptor resolutionDescriptor,
    final @NotNull ATUDescriptor rowATU,
    final @NotNull ATUDescriptor colATU,
    final double[][] target,
    final int dstRow,
    final int dstCol,
    final boolean needsTranspose,
    final @Nullable Map<Long, BlockMeta> blockMetaCache
  ) {
    if (rowATU.getStripeDescriptor().stripeId() > colATU.getStripeDescriptor().stripeId()) {
      fillATUIntersectionIntoWithBundle(dsBundle, resolutionDescriptor, colATU, rowATU, target, dstRow, dstCol, !needsTranspose, blockMetaCache);
      return;
    }

    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var rowStripe = rowATU.getStripeDescriptor();
    final var colStripe = colATU.getStripeDescriptor();
    final var rowStripeId = rowStripe.stripeId();
    final var colStripeId = colStripe.stripeId();
    final var queryRows = rowATU.getLength();
    final var queryCols = colATU.getLength();
    final var blockOnMainDiagonal = (rowStripeId == colStripeId);

    final long blockIndexInDatasets = ((long) rowStripeId) * this.chunkedFile.getStripeCount()[resolutionOrder] + colStripeId;
    final long blockLength;
    final long blockOffset;
    try {
      final var reader = dsBundle.getReader();
      final BlockMeta blockMeta;
      if (blockMetaCache != null) {
        blockMeta = blockMetaCache.computeIfAbsent(blockIndexInDatasets, key -> {
          final var blockLengthDataset = dsBundle.getBlockLengthDataSet();
          final var blockOffsetDataset = dsBundle.getBlockOffsetDataSet();
          final long[] blockLengthBuf = reader.int64().readArrayBlockWithOffset(blockLengthDataset, 1, key.longValue());
          final long[] blockOffsetBuf = reader.int64().readArrayBlockWithOffset(blockOffsetDataset, 1, key.longValue());
          return new BlockMeta(blockLengthBuf[0], blockOffsetBuf[0]);
        });
      } else {
        final var blockLengthDataset = dsBundle.getBlockLengthDataSet();
        final var blockOffsetDataset = dsBundle.getBlockOffsetDataSet();
        final long[] blockLengthBuf = reader.int64().readArrayBlockWithOffset(blockLengthDataset, 1, blockIndexInDatasets);
        final long[] blockOffsetBuf = reader.int64().readArrayBlockWithOffset(blockOffsetDataset, 1, blockIndexInDatasets);
        blockMeta = new BlockMeta(blockLengthBuf[0], blockOffsetBuf[0]);
      }
      blockLength = blockMeta.blockLength();
      blockOffset = blockMeta.blockOffset();

      if (blockLength == 0L) {
        return;
      }

      final var firstRow = rowATU.getStartIndexInStripeIncl();
      final var firstCol = colATU.getStartIndexInStripeIncl();
      final var lastRow = rowATU.getEndIndexInStripeExcl();
      final var lastCol = colATU.getEndIndexInStripeExcl();

      final var flipRows = ATUDirection.REVERSED.equals(rowATU.getDirection());
      final var flipCols = ATUDirection.REVERSED.equals(colATU.getDirection());

      final var savedAsSparse = (blockOffset >= 0L);
      final var cacheKey = new BlockCacheKey(resolutionOrder, blockIndexInDatasets);
      final var cachedPayload = this.blockDataCache.get(cacheKey);

      if (savedAsSparse) {
        final SparseDoubleBlockData sparseBlockData;
        if (cachedPayload instanceof SparseDoubleBlockData cachedSparse) {
          sparseBlockData = cachedSparse;
        } else {
          sparseBlockData = new SparseDoubleBlockData(
            reader.int64().readArrayBlockWithOffset(dsBundle.getBlockRowsDataSet(), (int) blockLength, blockOffset),
            reader.int64().readArrayBlockWithOffset(dsBundle.getBlockColsDataSet(), (int) blockLength, blockOffset),
            readNumericArrayBlockWithOffset(reader, dsBundle.getBlockValuesDataSet(), (int) blockLength, blockOffset)
          );
          this.blockDataCache.put(cacheKey, sparseBlockData);
        }

        final var queryRowCount = lastRow - firstRow;
        final var queryColCount = lastCol - firstCol;
        for (int i = 0; i < sparseBlockData.values().length; i++) {
          final var value = sparseBlockData.values()[i];
          writeSparseValueToTarget(
            target,
            dstRow,
            dstCol,
            sparseBlockData.rows()[i],
            sparseBlockData.cols()[i],
            value,
            firstRow,
            firstCol,
            queryRowCount,
            queryColCount,
            flipRows,
            flipCols,
            needsTranspose
          );
          if (blockOnMainDiagonal && sparseBlockData.rows()[i] != sparseBlockData.cols()[i]) {
            writeSparseValueToTarget(
              target,
              dstRow,
              dstCol,
              sparseBlockData.cols()[i],
              sparseBlockData.rows()[i],
              value,
              firstRow,
              firstCol,
              queryRowCount,
              queryColCount,
              flipRows,
              flipCols,
              needsTranspose
            );
          }
        }
      } else {
        final DenseDoubleBlockData denseBlockData;
        if (cachedPayload instanceof DenseDoubleBlockData cachedDense) {
          denseBlockData = cachedDense;
        } else {
          final var idx = new IndexMap().bind(0, -(blockOffset + 1L)).bind(1, 0L);
          final var block = readNumericDenseBlock(reader, dsBundle.getDenseBlockDataSet(), this.chunkedFile.getDenseBlockSize(), idx);
          final var denseBlock = block.toMatrix();
          if (blockOnMainDiagonal) {
            for (int i = 0; i < denseBlock.length; ++i) {
              for (int j = 1 + i; j < denseBlock.length; ++j) {
                denseBlock[j][i] = denseBlock[i][j];
              }
            }
          }
          denseBlockData = new DenseDoubleBlockData(denseBlock);
          this.blockDataCache.put(cacheKey, denseBlockData);
        }
        final var denseBlock = denseBlockData.matrix();
        if (blockOnMainDiagonal) {
          for (int outRow = 0; outRow < queryRows; outRow++) {
            final int srcRow = flipRows ? (lastRow - 1 - outRow) : (firstRow + outRow);
            for (int outCol = 0; outCol < queryCols; outCol++) {
              final int srcCol = flipCols ? (lastCol - 1 - outCol) : (firstCol + outCol);
              if (needsTranspose) {
                target[dstRow + outCol][dstCol + outRow] = denseBlock[srcRow][srcCol];
              } else {
                target[dstRow + outRow][dstCol + outCol] = denseBlock[srcRow][srcCol];
              }
            }
          }
        } else {
          for (int outRow = 0; outRow < queryRows; outRow++) {
            final int srcRow = flipRows ? (queryRows - 1 - outRow) : outRow;
            for (int outCol = 0; outCol < queryCols; outCol++) {
              final int srcCol = flipCols ? (queryCols - 1 - outCol) : outCol;
              if (needsTranspose) {
                target[dstRow + outCol][dstCol + outRow] = denseBlock[firstRow + srcRow][firstCol + srcCol];
              } else {
                target[dstRow + outRow][dstCol + outCol] = denseBlock[firstRow + srcRow][firstCol + srcCol];
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void fillATUIntersectionIntoWithBundle(
    final @NotNull HDF5FileDatasetsBundle dsBundle,
    final @NotNull ResolutionDescriptor resolutionDescriptor,
    final @NotNull ATUDescriptor rowATU,
    final @NotNull ATUDescriptor colATU,
    final long[][] target,
    final int dstRow,
    final int dstCol,
    final boolean needsTranspose,
    final @Nullable Map<Long, BlockMeta> blockMetaCache
  ) {
    if (rowATU.getStripeDescriptor().stripeId() > colATU.getStripeDescriptor().stripeId()) {
      fillATUIntersectionIntoWithBundle(dsBundle, resolutionDescriptor, colATU, rowATU, target, dstRow, dstCol, !needsTranspose, blockMetaCache);
      return;
    }

    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var rowStripe = rowATU.getStripeDescriptor();
    final var colStripe = colATU.getStripeDescriptor();
    final var rowStripeId = rowStripe.stripeId();
    final var colStripeId = colStripe.stripeId();
    final var blockOnMainDiagonal = (rowStripeId == colStripeId);

    final long blockIndexInDatasets = ((long) rowStripeId) * this.chunkedFile.getStripeCount()[resolutionOrder] + colStripeId;
    final long blockLength;
    final long blockOffset;
    try {
      final var reader = dsBundle.getReader();
      final BlockMeta blockMeta;
      if (blockMetaCache != null) {
        blockMeta = blockMetaCache.computeIfAbsent(blockIndexInDatasets, key -> {
          final var blockLengthDataset = dsBundle.getBlockLengthDataSet();
          final var blockOffsetDataset = dsBundle.getBlockOffsetDataSet();
          final long[] blockLengthBuf = reader.int64().readArrayBlockWithOffset(blockLengthDataset, 1, key.longValue());
          final long[] blockOffsetBuf = reader.int64().readArrayBlockWithOffset(blockOffsetDataset, 1, key.longValue());
          return new BlockMeta(blockLengthBuf[0], blockOffsetBuf[0]);
        });
      } else {
        final var blockLengthDataset = dsBundle.getBlockLengthDataSet();
        final var blockOffsetDataset = dsBundle.getBlockOffsetDataSet();
        final long[] blockLengthBuf = reader.int64().readArrayBlockWithOffset(blockLengthDataset, 1, blockIndexInDatasets);
        final long[] blockOffsetBuf = reader.int64().readArrayBlockWithOffset(blockOffsetDataset, 1, blockIndexInDatasets);
        blockMeta = new BlockMeta(blockLengthBuf[0], blockOffsetBuf[0]);
      }
      blockLength = blockMeta.blockLength();
      blockOffset = blockMeta.blockOffset();

      if (blockLength == 0L) {
        return;
      }

      final var firstRow = rowATU.getStartIndexInStripeIncl();
      final var firstCol = colATU.getStartIndexInStripeIncl();
      final var lastRow = rowATU.getEndIndexInStripeExcl();
      final var lastCol = colATU.getEndIndexInStripeExcl();
      final var queryRowCount = lastRow - firstRow;
      final var queryColCount = lastCol - firstCol;
      final var flipRows = ATUDirection.REVERSED.equals(rowATU.getDirection());
      final var flipCols = ATUDirection.REVERSED.equals(colATU.getDirection());
      final var savedAsSparse = (blockOffset >= 0L);
      final var cacheKey = new BlockCacheKey(resolutionOrder, blockIndexInDatasets);
      final var cachedPayload = this.blockDataCache.get(cacheKey);

      if (savedAsSparse) {
        final SparseLongBlockData sparseBlockData;
        if (cachedPayload instanceof SparseLongBlockData cachedSparse) {
          sparseBlockData = cachedSparse;
        } else {
          sparseBlockData = new SparseLongBlockData(
            reader.int64().readArrayBlockWithOffset(dsBundle.getBlockRowsDataSet(), (int) blockLength, blockOffset),
            reader.int64().readArrayBlockWithOffset(dsBundle.getBlockColsDataSet(), (int) blockLength, blockOffset),
            reader.int64().readArrayBlockWithOffset(dsBundle.getBlockValuesDataSet(), (int) blockLength, blockOffset)
          );
          this.blockDataCache.put(cacheKey, sparseBlockData);
        }

        for (int i = 0; i < sparseBlockData.values().length; i++) {
          final var value = sparseBlockData.values()[i];
          writeSparseValueToTarget(
            target,
            dstRow,
            dstCol,
            sparseBlockData.rows()[i],
            sparseBlockData.cols()[i],
            value,
            firstRow,
            firstCol,
            queryRowCount,
            queryColCount,
            flipRows,
            flipCols,
            needsTranspose
          );
          if (blockOnMainDiagonal && sparseBlockData.rows()[i] != sparseBlockData.cols()[i]) {
            writeSparseValueToTarget(
              target,
              dstRow,
              dstCol,
              sparseBlockData.cols()[i],
              sparseBlockData.rows()[i],
              value,
              firstRow,
              firstCol,
              queryRowCount,
              queryColCount,
              flipRows,
              flipCols,
              needsTranspose
            );
          }
        }
      } else {
        final DenseLongBlockData denseBlockData;
        if (cachedPayload instanceof DenseLongBlockData cachedDense) {
          denseBlockData = cachedDense;
        } else {
          final var idx = new IndexMap().bind(0, -(blockOffset + 1L)).bind(1, 0L);
          final var block = reader.int64().readSlicedMDArrayBlockWithOffset(
            dsBundle.getDenseBlockDataSet(),
            new int[]{this.chunkedFile.getDenseBlockSize(), this.chunkedFile.getDenseBlockSize()},
            new long[]{0L, 0L},
            idx
          );
          final var denseBlock = block.toMatrix();
          if (blockOnMainDiagonal) {
            for (int i = 0; i < denseBlock.length; ++i) {
              for (int j = 1 + i; j < denseBlock.length; ++j) {
                denseBlock[j][i] = denseBlock[i][j];
              }
            }
          }
          denseBlockData = new DenseLongBlockData(denseBlock);
          this.blockDataCache.put(cacheKey, denseBlockData);
        }
        final var denseBlock = denseBlockData.matrix();
        if (blockOnMainDiagonal) {
          for (int outRow = 0; outRow < queryRowCount; outRow++) {
            final int srcRow = flipRows ? (lastRow - 1 - outRow) : (firstRow + outRow);
            for (int outCol = 0; outCol < queryColCount; outCol++) {
              final int srcCol = flipCols ? (lastCol - 1 - outCol) : (firstCol + outCol);
              if (needsTranspose) {
                target[dstRow + outCol][dstCol + outRow] = denseBlock[srcRow][srcCol];
              } else {
                target[dstRow + outRow][dstCol + outCol] = denseBlock[srcRow][srcCol];
              }
            }
          }
        } else {
          for (int outRow = 0; outRow < queryRowCount; outRow++) {
            final int srcRow = flipRows ? (queryRowCount - 1 - outRow) : outRow;
            for (int outCol = 0; outCol < queryColCount; outCol++) {
              final int srcCol = flipCols ? (queryColCount - 1 - outCol) : outCol;
              if (needsTranspose) {
                target[dstRow + outCol][dstCol + outRow] = denseBlock[firstRow + srcRow][firstCol + srcCol];
              } else {
                target[dstRow + outRow][dstCol + outCol] = denseBlock[firstRow + srcRow][firstCol + srcCol];
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void writeSparseValueToTarget(
    final double[][] target,
    final int dstRow,
    final int dstCol,
    final long row,
    final long col,
    final double value,
    final int firstRow,
    final int firstCol,
    final int queryRowCount,
    final int queryColCount,
    final boolean flipRows,
    final boolean flipCols,
    final boolean needsTranspose
  ) {
    final int localRow = (int) row - firstRow;
    final int localCol = (int) col - firstCol;
    if (localRow < 0 || localRow >= queryRowCount || localCol < 0 || localCol >= queryColCount) {
      return;
    }
    final int transformedRow = flipRows ? (queryRowCount - 1 - localRow) : localRow;
    final int transformedCol = flipCols ? (queryColCount - 1 - localCol) : localCol;
    if (needsTranspose) {
      target[dstRow + transformedCol][dstCol + transformedRow] = value;
    } else {
      target[dstRow + transformedRow][dstCol + transformedCol] = value;
    }
  }

  private static void writeSparseValueToTarget(
    final long[][] target,
    final int dstRow,
    final int dstCol,
    final long row,
    final long col,
    final long value,
    final int firstRow,
    final int firstCol,
    final int queryRowCount,
    final int queryColCount,
    final boolean flipRows,
    final boolean flipCols,
    final boolean needsTranspose
  ) {
    final int localRow = (int) row - firstRow;
    final int localCol = (int) col - firstCol;
    if (localRow < 0 || localRow >= queryRowCount || localCol < 0 || localCol >= queryColCount) {
      return;
    }
    final int transformedRow = flipRows ? (queryRowCount - 1 - localRow) : localRow;
    final int transformedCol = flipCols ? (queryColCount - 1 - localCol) : localCol;
    if (needsTranspose) {
      target[dstRow + transformedCol][dstCol + transformedRow] = value;
    } else {
      target[dstRow + transformedRow][dstCol + transformedCol] = value;
    }
  }

  private void prefillBlockMetaCache(
    final @NotNull HDF5FileDatasetsBundle dsBundle,
    final int resolutionOrder,
    final @NotNull List<@NotNull ATUDescriptor> rowATUs,
    final @NotNull List<@NotNull ATUDescriptor> colATUs,
    final @NotNull Map<Long, BlockMeta> blockMetaCache
  ) {
    final int stripeCount = this.chunkedFile.getStripeCount()[resolutionOrder];
    if (stripeCount <= 0) {
      return;
    }
    final var rowStripeIds = rowATUs.stream().map(atu -> atu.getStripeDescriptor().stripeId()).collect(Collectors.toSet());
    final var colStripeIds = colATUs.stream().map(atu -> atu.getStripeDescriptor().stripeId()).collect(Collectors.toSet());
    final var neededMetaRows = new HashSet<Integer>();
    for (final var rowStripeId : rowStripeIds) {
      for (final var colStripeId : colStripeIds) {
        neededMetaRows.add(Math.min(rowStripeId, colStripeId));
      }
    }
    final var rowCache = this.blockMetaRowCaches.computeIfAbsent(
      resolutionOrder,
      key -> new BlockMetaRowCache(Integer.getInteger("HICT_BLOCK_META_ROW_CACHE_ROWS", 256))
    );
    final var loadedRows = new HashMap<Integer, BlockMetaRow>(neededMetaRows.size());
    for (final var rowStripeId : neededMetaRows) {
      loadedRows.put(rowStripeId, rowCache.getOrLoad(dsBundle, stripeCount, rowStripeId));
    }
    for (final var rowStripeId : rowStripeIds) {
      for (final var colStripeId : colStripeIds) {
        final var canonicalRow = Math.min(rowStripeId, colStripeId);
        final var canonicalCol = Math.max(rowStripeId, colStripeId);
        final var row = loadedRows.get(canonicalRow);
        if (row == null) {
          continue;
        }
        final long blockIndex = ((long) canonicalRow) * stripeCount + canonicalCol;
        blockMetaCache.putIfAbsent(blockIndex, new BlockMeta(row.blockLengths()[canonicalCol], row.blockOffsets()[canonicalCol]));
      }
    }
  }

  private record BlockMeta(long blockLength, long blockOffset) {
  }

  private record BlockCacheKey(int resolutionOrder, long blockIndex) {
  }

  private sealed interface BlockPayload permits SparseDoubleBlockData, SparseLongBlockData, DenseDoubleBlockData, DenseLongBlockData {
    long estimatedBytes();
  }

  private record SparseDoubleBlockData(long[] rows, long[] cols, double[] values) implements BlockPayload {
    @Override
    public long estimatedBytes() {
      return (((long) rows.length + cols.length) * Long.BYTES) + (((long) values.length) * Double.BYTES);
    }
  }

  private record SparseLongBlockData(long[] rows, long[] cols, long[] values) implements BlockPayload {
    @Override
    public long estimatedBytes() {
      return (((long) rows.length + cols.length + values.length) * Long.BYTES);
    }
  }

  private record DenseDoubleBlockData(double[][] matrix) implements BlockPayload {
    @Override
    public long estimatedBytes() {
      if (matrix.length == 0) {
        return 0L;
      }
      return ((long) matrix.length) * matrix[0].length * Double.BYTES;
    }
  }

  private record DenseLongBlockData(long[][] matrix) implements BlockPayload {
    @Override
    public long estimatedBytes() {
      if (matrix.length == 0) {
        return 0L;
      }
      return ((long) matrix.length) * matrix[0].length * Long.BYTES;
    }
  }

  private record BlockMetaRow(long[] blockLengths, long[] blockOffsets) {
  }

  private static boolean isFloatingPointDataset(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
    final @NotNull ch.systemsx.cisd.hdf5.HDF5DataSet dataSet
  ) {
    return reader.object().getDataSetInformation(dataSet.getDataSetPath()).getTypeInformation().getDataClass() == HDF5DataClass.FLOAT;
  }

  private static double @NotNull [] readNumericArrayBlockWithOffset(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
    final @NotNull ch.systemsx.cisd.hdf5.HDF5DataSet dataSet,
    final int length,
    final long offset
  ) {
    if (isFloatingPointDataset(reader, dataSet)) {
      return reader.float64().readArrayBlockWithOffset(dataSet, length, offset);
    }
    final var values = reader.int64().readArrayBlockWithOffset(dataSet, length, offset);
    final var result = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = values[i];
    }
    return result;
  }

  private static @NotNull ch.systemsx.cisd.base.mdarray.MDDoubleArray readNumericDenseBlock(
    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader,
    final @NotNull ch.systemsx.cisd.hdf5.HDF5DataSet dataSet,
    final int denseBlockSize,
    final @NotNull IndexMap idx
  ) {
    if (isFloatingPointDataset(reader, dataSet)) {
      return reader.float64().readSlicedMDArrayBlockWithOffset(
        dataSet,
        new int[]{denseBlockSize, denseBlockSize},
        new long[]{0L, 0L},
        idx
      );
    }
    final var block = reader.int64().readSlicedMDArrayBlockWithOffset(
      dataSet,
      new int[]{denseBlockSize, denseBlockSize},
      new long[]{0L, 0L},
      idx
    );
    final var longs = block.getAsFlatArray();
    final var doubles = new double[longs.length];
    for (int i = 0; i < longs.length; i++) {
      doubles[i] = longs[i];
    }
    return new ch.systemsx.cisd.base.mdarray.MDDoubleArray(doubles, new int[]{denseBlockSize, denseBlockSize});
  }

  private static final class BlockMetaRowCache {
    private final int maxRows;
    private final LinkedHashMap<Integer, BlockMetaRow> cache;

    private BlockMetaRowCache(final int maxRows) {
      this.maxRows = Math.max(16, maxRows);
      this.cache = new LinkedHashMap<>(this.maxRows, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<Integer, BlockMetaRow> eldest) {
          return size() > BlockMetaRowCache.this.maxRows;
        }
      };
    }

    private synchronized @NotNull BlockMetaRow getOrLoad(
      final @NotNull HDF5FileDatasetsBundle dsBundle,
      final int stripeCount,
      final int rowStripeId
    ) {
      final var cached = this.cache.get(rowStripeId);
      if (cached != null) {
        return cached;
      }
      final var reader = dsBundle.getReader();
      final var rowOffset = ((long) rowStripeId) * stripeCount;
      final var rowLengths = reader.int64().readArrayBlockWithOffset(dsBundle.getBlockLengthDataSet(), stripeCount, rowOffset);
      final var rowOffsets = reader.int64().readArrayBlockWithOffset(dsBundle.getBlockOffsetDataSet(), stripeCount, rowOffset);
      final var loaded = new BlockMetaRow(rowLengths, rowOffsets);
      this.cache.put(rowStripeId, loaded);
      return loaded;
    }
  }

  private static final class BlockDataCache {
    private final long maxBytes;
    private long usedBytes = 0L;
    private final LinkedHashMap<BlockCacheKey, BlockPayload> cache = new LinkedHashMap<>(256, 0.75f, true);

    private BlockDataCache(final long maxBytes) {
      this.maxBytes = Math.max(8L * 1024L * 1024L, maxBytes);
    }

    private synchronized @Nullable BlockPayload get(final @NotNull BlockCacheKey key) {
      return cache.get(key);
    }

    private synchronized void put(final @NotNull BlockCacheKey key, final @NotNull BlockPayload payload) {
      final var size = payload.estimatedBytes();
      if (size <= 0 || size > this.maxBytes) {
        return;
      }
      final var old = cache.put(key, payload);
      if (old != null) {
        usedBytes -= old.estimatedBytes();
      }
      usedBytes += size;
      while (usedBytes > maxBytes && !cache.isEmpty()) {
        final var it = cache.entrySet().iterator();
        if (!it.hasNext()) {
          break;
        }
        final var eldest = it.next();
        usedBytes -= eldest.getValue().estimatedBytes();
        it.remove();
      }
    }
  }

  public sealed interface RawMatrix permits LongMatrix, DoubleMatrix {
    int rows();

    int cols();

    boolean isFloatingPoint();

    double getAsDouble(int row, int col);
  }

  public record LongMatrix(long @NotNull [][] values) implements RawMatrix {
    @Override
    public int rows() {
      return values.length;
    }

    @Override
    public int cols() {
      return values.length > 0 ? values[0].length : 0;
    }

    @Override
    public boolean isFloatingPoint() {
      return false;
    }

    @Override
    public double getAsDouble(final int row, final int col) {
      return values[row][col];
    }
  }

  public record DoubleMatrix(double @NotNull [][] values) implements RawMatrix {
    @Override
    public int rows() {
      return values.length;
    }

    @Override
    public int cols() {
      return values.length > 0 ? values[0].length : 0;
    }

    @Override
    public boolean isFloatingPoint() {
      return true;
    }

    @Override
    public double getAsDouble(final int row, final int col) {
      return values[row][col];
    }
  }

  public record MatrixWithWeights(@NotNull RawMatrix matrix, double @NotNull [] rowWeights,
                                  double @NotNull [] colWeights, long startRowIncl, long startColIncl, long endRowExcl,
                                  long endColExcl, @NotNull QueryLengthUnit units,
                                  @NotNull ResolutionDescriptor resolutionDescriptor) {
  }

}
