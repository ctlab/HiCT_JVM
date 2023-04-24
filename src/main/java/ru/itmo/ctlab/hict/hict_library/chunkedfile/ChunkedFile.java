package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IndexMap;
import io.vertx.core.shareddata.Shareable;
import ru.itmo.ctlab.hict.hict_library.util.matrix.SparseCOOMatrixLong;

import java.nio.file.Path;
import java.util.Arrays;

public class ChunkedFile {

    private final Path hdfFilePath;
    private final long blockCount;
    private final int denseBlockSize;


    public ChunkedFile(final Path hdfFilePath) {
        this.hdfFilePath = hdfFilePath;
        // TODO: fix
        this.blockCount = 5;
        this.denseBlockSize = 256;
    }


    public long[][] getStripeIntersectionAsDenseMatrix(final long row, final long col, final long resolution) {
        try (final var reader = HDF5Factory.openForReading(this.hdfFilePath.toFile())) {
            final var blockIndexInDatasets = row * this.blockCount + col;
            final long blockLength;
            final long blockOffset;

//            try (final var blockLengthDataset = reader.object().openDataSet(getBlockLengthDatasetPath(resolution))) {
//                final long[] buf = reader.int64().readArrayBlockWithOffset(blockLengthDataset, 1, blockIndexInDatasets);
//                blockLength = buf[0];
//            }


            blockLength = reader.int64().readArrayBlockWithOffset(getBlockLengthDatasetPath(resolution), 1, blockIndexInDatasets)[0];


            if (blockLength == 0L) {
                return new long[this.denseBlockSize][this.denseBlockSize];
            }

            blockOffset = reader.int64().readArrayBlockWithOffset(getBlockOffsetDatasetPath(resolution), 1, blockIndexInDatasets)[0];

            final long[][] denseMatrix;

            if (blockOffset >= 0L) {
                // Fetch sparse block
                final long[] blockRows;
                final long[] blockCols;
                final long[] blockValues;

//                try (final var blockRowsDataset = reader.object().openDataSet(getBlockRowsDatasetPath(resolution))) {
                blockRows = reader.int64().readArrayBlockWithOffset(getBlockRowsDatasetPath(resolution), (int) blockLength, blockOffset);
//                }
//                try (final var blockColsDataset = reader.object().openDataSet(getBlockColsDatasetPath(resolution))) {
                blockCols = reader.int64().readArrayBlockWithOffset(getBlockColsDatasetPath(resolution), (int) blockLength, blockOffset);
//                }
//                try (final var blockValuesDataset = reader.object().openDataSet(getBlockValuesDatasetPath(resolution))) {
                blockValues = reader.int64().readArrayBlockWithOffset(getBlockValuesDatasetPath(resolution), (int) blockLength, blockOffset);
//                }

                final var sparseMatrix = new SparseCOOMatrixLong(
                        Arrays.stream(blockRows).mapToInt(l -> (int) l).toArray(),
                        Arrays.stream(blockCols).mapToInt(l -> (int) l).toArray(),
                        blockValues,
                        false,
                        (row == col)
                );
                denseMatrix = sparseMatrix.toDense(this.denseBlockSize, this.denseBlockSize);

            } else {
                // Fetch dense block
                final var idx = new IndexMap().bind(0, -(blockOffset + 1L)).bind(1, 0L);
//                try (final var denseBlockDataset = reader.object().openDataSet(getDenseBlockDatasetPath(resolution))) {
                final var block = reader.int64().readSlicedMDArrayBlockWithOffset(getDenseBlockDatasetPath(resolution), new int[]{this.denseBlockSize, this.denseBlockSize}, new long[]{0L, 0L}, idx);
                denseMatrix = block.toMatrix();
//                }
            }

            return denseMatrix;
        }
    }


    private String getBlockLengthDatasetPath(final long resolution) {
        return String.format("/resolutions/%d/treap_coo/block_length", resolution);
    }

    private String getBlockOffsetDatasetPath(final long resolution) {
        return String.format("/resolutions/%d/treap_coo/block_offset", resolution);
    }

    private String getBlockColsDatasetPath(final long resolution) {
        return String.format("/resolutions/%d/treap_coo/block_cols", resolution);
    }

    private String getBlockRowsDatasetPath(final long resolution) {
        return String.format("/resolutions/%d/treap_coo/block_rows", resolution);
    }

    private String getBlockValuesDatasetPath(final long resolution) {
        return String.format("/resolutions/%d/treap_coo/block_vals", resolution);
    }

    private String getDenseBlockDatasetPath(final long resolution) {
        return String.format("/resolutions/%d/treap_coo/dense_blocks", resolution);
    }

}
