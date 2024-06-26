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

package ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization;

import io.vertx.core.json.JsonObject;
import lombok.*;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.Colormap;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;

@RequiredArgsConstructor
@Getter(AccessLevel.PUBLIC)
@EqualsAndHashCode
@ToString
public abstract class ColormapDTO {
  public final @NotNull String colormapType;

  public static @NotNull ColormapDTO fromEntity(final @NotNull Colormap cmap, final @NotNull ChunkedFile chunkedFile) {
    if (cmap instanceof SimpleLinearGradient) {
      return SimpleLinearGradientDTO.fromEntity((SimpleLinearGradient) cmap, chunkedFile);
    } else {
      throw new IllegalArgumentException("Unknown serialization for this colormap");
    }
  }

  public static @NotNull ColormapDTO fromJSONObject(final @NotNull JsonObject json) {
    final String jsonColormapType = json.getString("colormapType");
    if ("SimpleLinearGradient".equals(jsonColormapType)) {
      return SimpleLinearGradientDTO.fromJSONObject(json);
    } else {
      throw new IllegalArgumentException("Unknown type of colormap dto in JSON: " + jsonColormapType);
    }
  }

  public @NotNull Colormap toEntity() {
    if ("SimpleLinearGradient".equals(this.colormapType)) {
      assert (this instanceof SimpleLinearGradientDTO) : "Colormap type was set to SimpleLinearGradient but DTO is not instance of SimpleLinearGradientDTO??";
      return ((SimpleLinearGradientDTO) this).toEntity();
    } else {
      throw new IllegalArgumentException("Unknown type of colormap dto: " + this.colormapType);
    }
  }


}
