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

package ru.itmo.ctlab.hict.hict_server.util.cache;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.ConcurrentHashMap;

public final class FileFingerprintService {
  private static final int BUFFER_SIZE = 1 << 20;

  private final @NotNull ConcurrentHashMap<Path, CachedFingerprint> cache = new ConcurrentHashMap<>();

  public @NotNull FileFingerprint fingerprint(final @NotNull Path inputPath) {
    final var path = inputPath.normalize().toAbsolutePath();
    final BasicFileAttributes attrs;
    try {
      attrs = Files.readAttributes(path, BasicFileAttributes.class);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to stat file " + path, e);
    }
    final var sizeBytes = attrs.size();
    final var modifiedAtMs = attrs.lastModifiedTime().toMillis();
    final var cached = this.cache.get(path);
    if (cached != null && cached.sizeBytes == sizeBytes && cached.modifiedAtMs == modifiedAtMs) {
      return cached.fingerprint;
    }
    final var computed = computeFingerprint(path, sizeBytes, modifiedAtMs);
    this.cache.put(path, new CachedFingerprint(sizeBytes, modifiedAtMs, computed));
    return computed;
  }

  private static @NotNull FileFingerprint computeFingerprint(final @NotNull Path path,
                                                             final long sizeBytes,
                                                             final long modifiedAtMs) {
    final MessageDigest sha256;
    final MessageDigest sha512;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
      sha512 = MessageDigest.getInstance("SHA-512");
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("Required hashing algorithm is not available", e);
    }

    final var buffer = new byte[BUFFER_SIZE];
    try (final InputStream inputStream = Files.newInputStream(path)) {
      int read;
      while ((read = inputStream.read(buffer)) >= 0) {
        if (read == 0) {
          continue;
        }
        sha256.update(buffer, 0, read);
        sha512.update(buffer, 0, read);
      }
    } catch (final IOException e) {
      throw new RuntimeException("Failed to hash file " + path, e);
    }

    return new FileFingerprint(
      sizeBytes,
      modifiedAtMs,
      HexFormat.of().formatHex(sha256.digest()),
      HexFormat.of().formatHex(sha512.digest())
    );
  }

  private record CachedFingerprint(long sizeBytes,
                                   long modifiedAtMs,
                                   @NotNull FileFingerprint fingerprint) {
  }
}
