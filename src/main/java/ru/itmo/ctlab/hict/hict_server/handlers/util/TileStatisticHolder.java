package ru.itmo.ctlab.hict.hict_server.handlers.util;

import io.vertx.core.shareddata.Shareable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.LongStream;

public record TileStatisticHolder(@NotNull AtomicLong versionCounter,
                                  @NotNull AtomicLongArray minimumsAtResolutionDoubleBits,
                                  @NotNull AtomicLongArray maximumsAtResolutionDoubleBits) implements Shareable {

  public static TileStatisticHolder newDefaultStatisticHolder(final int resolutionCount) {
    return new TileStatisticHolder(
      new AtomicLong(0),
      new AtomicLongArray(LongStream.generate(() -> Double.doubleToLongBits(0.0)).limit(resolutionCount).toArray()),
      new AtomicLongArray(LongStream.generate(() -> Double.doubleToLongBits(1.0)).limit(resolutionCount).toArray())
    );
  }

}
