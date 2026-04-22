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

package ru.itmo.ctlab.hict.hict_server.util.shareable;

import io.vertx.core.shareddata.Shareable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.visualization.DistanceExpectedNormalizer;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;

import java.nio.file.Path;

public class ShareableWrappers {
  @Getter
  @RequiredArgsConstructor
  public static class ChunkedFileWrapper implements Shareable {
    private final @NotNull ChunkedFile chunkedFile;
  }

  @Getter
  @RequiredArgsConstructor
  public static class PathWrapper implements Shareable {
    private final @NotNull Path path;
  }

  @Getter
  @RequiredArgsConstructor
  public static class SimpleVisualizationOptionsWrapper implements Shareable {
    private final @NotNull SimpleVisualizationOptions simpleVisualizationOptions;
  }

  @Getter
  @RequiredArgsConstructor
  public static class Track1DManagerWrapper implements Shareable {
    private final @NotNull Track1DManager track1DManager;
  }

  @Getter
  @RequiredArgsConstructor
  public static class RequestTaskSchedulerWrapper implements Shareable {
    private final @NotNull RequestTaskScheduler requestTaskScheduler;
  }

  @Getter
  @RequiredArgsConstructor
  public static class RenderPipelineConfigWrapper implements Shareable {
    private final @NotNull RenderPipelineConfig renderPipelineConfig;
  }

  @Getter
  @RequiredArgsConstructor
  public static class DiagonalExpectedProfileWrapper implements Shareable {
    private final @NotNull DistanceExpectedNormalizer.DiagonalProfile diagonalProfile;
  }
}
