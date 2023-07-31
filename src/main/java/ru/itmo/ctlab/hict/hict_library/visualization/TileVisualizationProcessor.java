package ru.itmo.ctlab.hict.hict_library.visualization;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

@RequiredArgsConstructor
@Getter
public class TileVisualizationProcessor {
  @Setter
  private @NotNull SimpleVisualizationOptions visualizationOptions;
  private final @NotNull ReadWriteLock visualizationOptionsLock = new ReentrantReadWriteLock();
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

  public long @NotNull [][] processTile(final long @NotNull [][] input, final double @Nullable [] rowWeights, final double @Nullable [] columnWeights) {
    assert (input.length > 0) : "Zero-height tile??";
    final var result = new long[input.length][input[0].length];
    final SimpleVisualizationOptions options;
    try {
      this.visualizationOptionsLock.readLock().lock();
      options = this.visualizationOptions;
    } finally {
      this.visualizationOptionsLock.readLock().unlock();
    }
    for (int rowIndex = 0; rowIndex < input.length; ++rowIndex) {
      final var startStream = Arrays.stream(input[rowIndex]).parallel();
      final var rowWeight = (rowWeights != null) ? rowWeights[rowIndex] : 1.0;

      final var pre = options.getLnPreLogBase();
      final var post = options.getLnPostLogBase();

      if (pre > 0) {
        var doubleStream = startStream.mapToDouble(
          signal -> Math.log1p(signal) / pre
        );

        if (options.isApplyCoolerWeights()) {
          doubleStream = applyCoolerWeightsToRow(doubleStream, rowWeight, columnWeights);
        }

        if (post > 0) {
          doubleStream = doubleStream.map(signal -> Math.log1p(signal) / post);
        }
        result[rowIndex] = doubleStream.mapToLong(Math::round).toArray();
      } else {
        if (options.isApplyCoolerWeights()) {
          var doubleStream = applyCoolerWeightsToRow(startStream, rowWeight, columnWeights);
          if (post > 0) {
            doubleStream = doubleStream.map(signal -> Math.log1p(signal) / post);
          }
          result[rowIndex] = doubleStream.mapToLong(Math::round).toArray();
        } else {
          if (post > 0) {
            var doubleStream = startStream.mapToDouble(signal -> Math.log1p(signal) / post);
            result[rowIndex] = doubleStream.mapToLong(Math::round).toArray();
          } else {
            System.arraycopy(input[rowIndex], 0, result[rowIndex], 0, input[rowIndex].length);
          }
        }
      }
    }
    return result;
  }
}
