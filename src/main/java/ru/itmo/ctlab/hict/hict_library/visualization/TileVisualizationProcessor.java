/*
 * MIT License
 *
 * Copyright (c) 2021-2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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

package ru.itmo.ctlab.hict.hict_library.visualization;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries;

import java.awt.*;
import java.awt.image.*;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Getter
public class TileVisualizationProcessor {
  private final @NotNull ChunkedFile chunkedFile;

  protected DoubleStream applyCoolerWeightsToRow(final DoubleStream rowStream, final double rowWeight, final double @Nullable [] columnWeights) {
    var doubleStream = rowStream;
    if (rowWeight != 1.0) {
      doubleStream = doubleStream.map(signal -> signal * rowWeight);
    }

    if (columnWeights != null) {
      final var atomicColumnIndex = new AtomicInteger();
      doubleStream = doubleStream.sequential().map(signal -> signal * columnWeights[atomicColumnIndex.getAndIncrement()]);
    }
    return doubleStream;
  }

  protected DoubleStream applyCoolerWeightsToRow(final LongStream rowStream, final double rowWeight, final double @Nullable [] columnWeights) {
    DoubleStream doubleStream;
    if (rowWeight != 1.0) {
      doubleStream = rowStream.mapToDouble(signal -> signal * rowWeight);
    } else {
      doubleStream = rowStream.mapToDouble(signal -> (double) signal);
    }

    if (columnWeights != null) {
      final var atomicColumnIndex = new AtomicInteger();
      doubleStream = doubleStream.sequential().map(signal -> signal * columnWeights[atomicColumnIndex.getAndIncrement()]);
    }
    return doubleStream;
  }

  public @NotNull TileWithWeights applyWeightsToTile(final @NotNull RawTileWithWeights rawTile) {
    final var rowCount = rawTile.values().length;
    final var columnCount = (rawTile.values().length > 0) ? (rawTile.values()[0].length) : 0;
    final var rawValues = rawTile.values();
    final double[] rowWeights;
    final double[] colWeights;
    if (rawTile.rowWeights() != null) {
      rowWeights = rawTile.rowWeights();
    } else {
      rowWeights = new double[rowCount];
      Arrays.fill(rowWeights, 1.0d);
    }
    if (rawTile.columnWeights() != null) {
      colWeights = rawTile.columnWeights();
    } else {
      colWeights = new double[columnCount];
      Arrays.fill(colWeights, 1.0d);
    }
    final var weightedValues = new double[rowCount][];
    try (final var pool = Executors.newFixedThreadPool(this.chunkedFile.getParallelThreadCount().get())) {
      for (int i = 0; i < rowCount; ++i) {
        final var rowIndex = i;
        pool.submit(() -> {
          final var rawRow = rawValues[rowIndex];
          final var wRow = new double[columnCount];
          for (int j = 0; j < columnCount; j++) {
            wRow[j] = ((double) rawRow[j]) * rowWeights[rowIndex] * colWeights[j];
          }
          weightedValues[rowIndex] = wRow;
        });
      }
    }
    return new TileWithWeights(
      weightedValues,
      rowWeights,
      colWeights
    );
  }

  public TileWithWeights processTile(final @NotNull MatrixQueries.MatrixWithWeights rawTile, final @NotNull SimpleVisualizationOptions visualizationOptions) {
    final var input = rawTile.matrix();
    final var rowWeights = rawTile.rowWeights();
    final var columnWeights = rawTile.colWeights();
    final var rowCount = input.length;
    final var columnCount = (rowCount > 0) ? input[0].length : 0;
    final var result = new double[rowCount][columnCount];
    final var resolutionScalingCoeffs = this.chunkedFile.getResolutionScalingCoefficient();
    final var resolutionLinearScalingCoeffs = this.chunkedFile.getResolutionLinearScalingCoefficient();
    final var resolutionScalingCoeff = resolutionScalingCoeffs[rawTile.resolutionDescriptor().getResolutionOrderInArray()];
    final var resolutionLinearScalingCoeff = resolutionLinearScalingCoeffs[rawTile.resolutionDescriptor().getResolutionOrderInArray()];

    for (int rowIndex = 0; rowIndex < input.length; ++rowIndex) {
      final var startStream = Arrays.stream(input[rowIndex]).parallel();
      final var rowWeight = (rowWeights != null) ? rowWeights[rowIndex] : 1.0;

      final var pre = visualizationOptions.getLnPreLogBase();
      final var post = visualizationOptions.getLnPostLogBase();

      DoubleStream doubleStream = startStream.mapToDouble(signal -> (double) signal);

      if (pre > 0) {
        doubleStream = doubleStream.map(signal -> Math.log1p(signal) / pre);
      }

      if (visualizationOptions.isResolutionScaling()) {
        doubleStream = doubleStream.map(signal -> signal * resolutionScalingCoeff);
      }

      if (visualizationOptions.isResolutionLinearScaling()) {
        doubleStream = doubleStream.map(signal -> signal * resolutionLinearScalingCoeff);
      }

      if (visualizationOptions.isApplyCoolerWeights()) {
        doubleStream = applyCoolerWeightsToRow(doubleStream, rowWeight, columnWeights);
      }

      if (post > 0) {
        doubleStream = doubleStream.map(signal -> Math.log1p(signal) / post);
      }

      result[rowIndex] = doubleStream.toArray();
    }
    return new TileWithWeights(result, rowWeights, columnWeights);
  }

  public @NotNull BufferedImage visualizeTile(final @NotNull MatrixQueries.MatrixWithWeights rawTile, final @NotNull SimpleVisualizationOptions options) {
    final var input = rawTile.matrix();
    final var rowCount = input.length;
    final var columnCount = (rowCount > 0) ? input[0].length : 0;
    final var normalized = processTile(rawTile, options);
    final var colormap = options.getColormap();
    final var boxedARGBValues = Arrays.stream(normalized.values())
      .flatMap(arrayRow ->
        Arrays.stream(arrayRow)
          .mapToObj(colormap::mapSignal)
          .flatMap(color -> Stream.of(
              (byte) (color.getRed()), // Red
              (byte) (color.getGreen()), // Green
              (byte) (color.getBlue()), // Blue,
              (byte) (color.getAlpha()) // Alpha,
            )
          )
      )
      .toArray(Byte[]::new);

    final byte[] rgbValues = new byte[boxedARGBValues.length];
    for (var i = 0; i < boxedARGBValues.length; ++i) {
      rgbValues[i] = boxedARGBValues[i];
    }

    final DataBuffer buffer = new DataBufferByte(rgbValues, rgbValues.length);

    final WritableRaster raster = Raster.createInterleavedRaster(buffer, columnCount, rowCount, 4 * columnCount, 4, new int[]{0, 1, 2, 3}, null);
    final ColorModel cm = new ComponentColorModel(ColorModel.getRGBdefault().getColorSpace(), true, false, Transparency.TRANSLUCENT, DataBuffer.TYPE_BYTE);
    //final ColorModel cm = new ComponentColorModel(ColorModel.getRGBdefault().getColorSpace(), false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
    final BufferedImage image = new BufferedImage(cm, raster, true, null);

    return image;
  }
}
