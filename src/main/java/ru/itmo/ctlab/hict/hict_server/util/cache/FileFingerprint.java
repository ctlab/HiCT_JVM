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

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public record FileFingerprint(long sizeBytes,
                              long modifiedAtMs,
                              @NotNull String sha256,
                              @NotNull String sha512) {
  public @NotNull JsonObject toJson() {
    return new JsonObject()
      .put("sizeBytes", this.sizeBytes)
      .put("modifiedAtMs", this.modifiedAtMs)
      .put("sha256", this.sha256)
      .put("sha512", this.sha512);
  }

  public static @NotNull FileFingerprint fromJson(final @NotNull JsonObject json) {
    return new FileFingerprint(
      json.getLong("sizeBytes", -1L),
      json.getLong("modifiedAtMs", 0L),
      Objects.requireNonNullElse(json.getString("sha256"), ""),
      Objects.requireNonNullElse(json.getString("sha512"), "")
    );
  }

  public boolean matches(final FileFingerprint other) {
    return other != null
      && this.sizeBytes == other.sizeBytes
      && this.modifiedAtMs == other.modifiedAtMs
      && this.sha256.equalsIgnoreCase(other.sha256)
      && this.sha512.equalsIgnoreCase(other.sha512);
  }
}
