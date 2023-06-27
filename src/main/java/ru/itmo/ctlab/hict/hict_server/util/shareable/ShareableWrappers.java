package ru.itmo.ctlab.hict.hict_server.util.shareable;

import io.vertx.core.shareddata.Shareable;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

import java.nio.file.Path;

public class ShareableWrappers {
  @Getter
  @RequiredArgsConstructor
  public static class ChunkedFileWrapper implements Shareable {
    private final @NonNull @NotNull ChunkedFile chunkedFile;
  }

  @Getter
  @RequiredArgsConstructor
  public static class PathWrapper implements Shareable {
    private final @NonNull @NotNull Path path;
  }
}
