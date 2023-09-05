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
