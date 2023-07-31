package ru.itmo.ctlab.hict.hict_library.assembly;

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

@RequiredArgsConstructor
public class FASTAProcessor {
  private final @NotNull ChunkedFile chunkedFile;
}
