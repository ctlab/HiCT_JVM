package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IndexMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;
import ru.itmo.ctlab.hict.hict_library.util.matrix.SparseCOOMatrixLong;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static ru.itmo.ctlab.hict.hict_library.chunkedfile.PathGenerators.*;

@Getter
@Slf4j
public class ChunkedFile {

  private final @NotNull @NonNull Path hdfFilePath;
  //  private final long blockCount;
  private final int denseBlockSize;
  private final long @NotNull @NonNull [] resolutions;
  private final Map<@NotNull @NonNull Long, @NotNull @NonNull Integer> resolutionToIndex;
  private final long @NotNull @NonNull [] matrixSizeBins;
  private final int @NotNull @NonNull [] stripeCount;
  private final @NotNull @NonNull ContigTree contigTree;
  private final @NotNull @NonNull ScaffoldTree scaffoldTree;


  public ChunkedFile(final @NotNull @NonNull Path hdfFilePath, final int denseBlockSize) {
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
    this.matrixSizeBins = new long[1 + this.resolutions.length];
    this.matrixSizeBins[0] = this.contigTree.getLengthInUnits(QueryLengthUnit.BASE_PAIRS, ResolutionDescriptor.fromResolutionOrder(0));
    for (int i = 0; i < this.resolutions.length; ++i) {
      this.matrixSizeBins[1 + i] = this.contigTree.getLengthInUnits(QueryLengthUnit.BINS, ResolutionDescriptor.fromResolutionOrder(1 + i));
    }
    this.scaffoldTree = new ScaffoldTree(this.matrixSizeBins[0]);
    Initializers.initializeScaffoldTree(this);
  }


  // TODO: Implement
  public long[][] getSubmatrix(final @NotNull @NonNull ResolutionDescriptor resolution, final long startRowIncl, final long startColIncl, final long endRowExcl, final long endColExcl, final boolean excludeHiddenContigs) {

  }

  public static long clamp(final long x, final long min, final long max) {
    return Long.max(min, Long.min(x, max));
  }

  // TODO: Implement
  public List<ATUDescriptor> getATUsForRange(final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor, final long startPxIncl, final long endPxExcl, final boolean excludeHiddenContigs) {
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

    final var deltaBetweenSegmentFirstContigAndQueryStart = startPx - lessSize;

    final var atus = new ArrayList<ATUDescriptor>();

    ContigTree.Node.traverseNode(es.segment(), node -> {
      final var contigATUs = node.getContigDescriptor().getAtus().get(resolutionOrder);
      final var contigDirection = node.getTrueDirection();
      if (contigDirection == ContigDirection.FORWARD) {
        atus.addAll(contigATUs);
      } else {
        final var reversedATUs = contigATUs.stream().map(ATUDescriptor::reversed).collect(Collectors.toList());
        Collections.reverse(reversedATUs);
        atus.addAll(reversedATUs);
      }
    });

    final var firstContigNodeInSegment = es.segment().leftmost();
    final var firstContigInSegment = firstContigNodeInSegment.getContigDescriptor();

    final long[] firstContigATUPrefixSum = firstContigInSegment.getAtuPrefixSumLengthBins().get(resolutionOrder);
    final int indexOfATUContainingStartContig;


    if (firstContigNodeInSegment.getTrueDirection() == ContigDirection.FORWARD) {
      final var insertionPoint = Arrays.binarySearch(
        firstContigATUPrefixSum,
        1 + deltaBetweenSegmentFirstContigAndQueryStart
      );
      if (insertionPoint >= 0) {
        indexOfATUContainingStartContig = insertionPoint - 1;
      } else {
        indexOfATUContainingStartContig = (-insertionPoint - 1) - 1;
      }
    } else {
      final var topSum = firstContigATUPrefixSum[firstContigATUPrefixSum.length - 1];
      final var insertionPoint = Arrays.binarySearch(
        firstContigATUPrefixSum,
        topSum - deltaBetweenSegmentFirstContigAndQueryStart
      );
      if (insertionPoint >= 0) {
        indexOfATUContainingStartContig = insertionPoint;
      } else {
        indexOfATUContainingStartContig = (-insertionPoint - 1);
      }
    }

    final var lengthOfATUsBeforeOneContainingStart = (indexOfATUContainingStartContig > 0) ? (firstContigATUPrefixSum[indexOfATUContainingStartContig - 1]) : 0L;
    final var oldFirstATU = atus.get(indexOfATUContainingStartContig);
    final ATUDescriptor newFirstATU;

    if (oldFirstATU.getDirection() == ATUDirection.FORWARD) {
      newFirstATU = new ATUDescriptor(oldFirstATU.getStripeDescriptor(), oldFirstATU.getStartIndexInStripeIncl() + deltaBetweenSegmentFirstContigAndQueryStart -
        lengthOfATUsBeforeOneContainingStart, oldFirstATU.getEndIndexInStripeExcl(), oldFirstATU.getDirection());
    } else {
      newFirstATU = new ATUDescriptor(oldFirstATU.getStripeDescriptor(), oldFirstATU.getStartIndexInStripeIncl(), oldFirstATU.getEndIndexInStripeExcl() - (deltaBetweenSegmentFirstContigAndQueryStart -
        lengthOfATUsBeforeOneContainingStart), oldFirstATU.getDirection());
    }

  }

  public @NotNull @NonNull MatrixWithWeights getATUIntersection(final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor, final @NotNull @NonNull ATUDescriptor rowATU, final @NotNull @NonNull ATUDescriptor colATU) {
    return getATUIntersection(resolutionDescriptor, rowATU, colATU, false);
  }

  // TODO: Implement
  public @NotNull @NonNull MatrixWithWeights getATUIntersection(final @NotNull @NonNull ResolutionDescriptor resolutionDescriptor, final @NotNull @NonNull ATUDescriptor rowATU, final @NotNull @NonNull ATUDescriptor colATU, final boolean needsTranspose) {
    if (rowATU.getStripeDescriptor().stripeId() > colATU.getStripeDescriptor().stripeId()) {
      return getATUIntersection(resolutionDescriptor, rowATU, colATU, !needsTranspose);
    }

    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var rowStripe = rowATU.getStripeDescriptor();
    final var colStripe = colATU.getStripeDescriptor();
    final var rowStripeId = rowStripe.stripeId();
    final var colStripeId = colStripe.stripeId();
    final var queryRows = (int) (rowATU.getEndIndexInStripeExcl() - rowATU.getStartIndexInStripeIncl());
    final var queryCols = (int) (colATU.getEndIndexInStripeExcl() - colATU.getStartIndexInStripeIncl());
    final var rowWeights = rowStripe.bin_weights();
    final var colWeights = colStripe.bin_weights();

    if (rowATU.getDirection() == ATUDirection.REVERSED) {
      ArrayUtils.reverse(rowWeights);
    }

    if (colATU.getDirection() == ATUDirection.REVERSED) {
      ArrayUtils.reverse(colWeights);
    }


    try (final var reader = HDF5Factory.openForReading(this.hdfFilePath.toFile())) {
      final var blockIndexInDatasets = rowStripeId * this.stripeCount[resolutionOrder] + colStripeId;
      final long blockLength;
      final long blockOffset;

      try (final var blockLengthDataset = reader.object().openDataSet(getBlockLengthDatasetPath(this.resolutions[resolutionOrder]))) {
        final long[] buf = reader.int64().readArrayBlockWithOffset(blockLengthDataset, 1, blockIndexInDatasets);
        blockLength = buf[0];
      }

      final boolean isEmpty = (blockLength == 0L);

      if (isEmpty) {
        return new MatrixWithWeights(
          new long[needsTranspose ? queryCols : queryRows][needsTranspose ? queryRows : queryCols],
          needsTranspose ? colWeights : rowWeights,
          needsTranspose ? rowWeights : colWeights
        );
      }

      try (final var blockOffsetDataset = reader.object().openDataSet(getBlockOffsetDatasetPath(resolutionOrder))) {
        final long[] buf = reader.int64().readArrayBlockWithOffset(blockOffsetDataset, 1, blockIndexInDatasets);
        blockOffset = buf[0];
      }

      final long[][] denseMatrix;

      if (blockOffset >= 0L) {
        // Fetch sparse block
        final long[] blockRows;
        final long[] blockCols;
        final long[] blockValues;

        try (final var blockRowsDataset = reader.object().openDataSet(PathGenerators.getBlockRowsDatasetPath(resolutionOrder))) {
          blockRows = reader.int64().readArrayBlockWithOffset(blockRowsDataset, (int) blockLength, blockOffset);
        }
        try (final var blockColsDataset = reader.object().openDataSet(PathGenerators.getBlockColsDatasetPath(resolutionOrder))) {
          blockCols = reader.int64().readArrayBlockWithOffset(blockColsDataset, (int) blockLength, blockOffset);
        }
        try (final var blockValuesDataset = reader.object().openDataSet(getBlockValuesDatasetPath(resolutionOrder))) {
          blockValues = reader.int64().readArrayBlockWithOffset(blockValuesDataset, (int) blockLength, blockOffset);
        }

        final var sparseMatrix = new SparseCOOMatrixLong(
          Arrays.stream(blockRows).mapToInt(l -> (int) l).toArray(),
          Arrays.stream(blockCols).mapToInt(l -> (int) l).toArray(),
          blockValues,
          needsTranspose,
          (rowStripeId == colStripeId)
        );
        denseMatrix = sparseMatrix.toDense(needsTranspose ? queryCols : queryRows, needsTranspose ? queryRows : queryCols);
      } else {
        // Fetch dense block
        final var idx = new IndexMap().bind(0, -(blockOffset + 1L)).bind(1, 0L);
        final MDLongArray block;
        try (final var denseBlockDataset = reader.object().openDataSet(getDenseBlockDatasetPath(resolutionOrder))) {
          block = reader.int64().readSlicedMDArrayBlockWithOffset(denseBlockDataset, new int[]{this.denseBlockSize, this.denseBlockSize}, new long[]{0L, 0L}, idx);
        }
        if (rowStripeId == colStripeId) {
          denseMatrix = block.toMatrix();
          for (int i = 0; i < denseMatrix.length; ++i) {
            for (int j = 1 + i; j < denseMatrix.length; ++j) {
              denseMatrix[j][i] = denseMatrix[i][j];
            }
          }
        } else {
          if (needsTranspose) {
            final var dT = block.toMatrix();
            denseMatrix = new long[dT[0].length][dT.length];
            for (int i = 0; i < dT.length; ++i) {
              for (int j = 0; j < dT[0].length; ++j) {
                denseMatrix[j][i] = dT[i][j];
              }
            }
          } else {
            denseMatrix = block.toMatrix();
          }
        }
      }

      return new MatrixWithWeights(
        denseMatrix,
        needsTranspose ? colWeights : rowWeights,
        needsTranspose ? rowWeights : colWeights
      );
    }
  }

  public record MatrixWithWeights(long @NotNull @NonNull [][] matrix, double @NotNull @NonNull [] rowWeights,
                                  double @NotNull @NonNull [] colWeights) {
  }

  public @NotNull List<@NotNull Long> getResolutionsList() {
    return Arrays.stream(this.resolutions).boxed().toList();
  }

  public long @NotNull @NonNull [] getResolutions() {
    return this.resolutions;
  }

  public @NotNull @NonNull AssemblyInfo getAssemblyInfo() {
    return new AssemblyInfo(this.contigTree.getContigList(), this.scaffoldTree.getScaffoldList());
  }
}
