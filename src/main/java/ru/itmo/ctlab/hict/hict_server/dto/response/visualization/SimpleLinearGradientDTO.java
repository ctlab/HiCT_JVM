package ru.itmo.ctlab.hict.hict_server.dto.response.visualization;

import io.vertx.core.json.JsonObject;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;

import java.awt.*;

@Getter(AccessLevel.PUBLIC)
@ToString
public class SimpleLinearGradientDTO extends ColormapDTO {
  private final String startColorRGBAHEX;
  private final String endColorRGBAHEX;
  private final double minSignal;
  private final double maxSignal;

  public SimpleLinearGradientDTO(String startColorHEX, String endColorHEX, double minSignal, double maxSignal) {
    super("SimpleLinearGradient");
    this.startColorRGBAHEX = startColorHEX;
    this.endColorRGBAHEX = endColorHEX;
    this.minSignal = minSignal;
    this.maxSignal = maxSignal;
  }

  public static @NotNull SimpleLinearGradientDTO fromEntity(final @NotNull SimpleLinearGradient cmap, final @NotNull ChunkedFile chunkedFile) {
    final var resolutions = chunkedFile.getResolutions();
    final String startHEX = String.format("#%02x%02x%02x%02x", cmap.getStartColor().getRed(), cmap.getStartColor().getGreen(), cmap.getStartColor().getBlue(), cmap.getStartColor().getAlpha());
    final String endHEX = String.format("#%02x%02x%02x%02x", cmap.getEndColor().getRed(), cmap.getEndColor().getGreen(), cmap.getEndColor().getBlue(), cmap.getEndColor().getAlpha());

    return new SimpleLinearGradientDTO(
      startHEX,
      endHEX,
      cmap.getMinSignal(),
      cmap.getMaxSignal()
    );
  }

  public static @NotNull SimpleLinearGradientDTO fromJSONObject(final @NotNull JsonObject json) {
    return new SimpleLinearGradientDTO(
      json.getString("startColorRGBAHEX"),
      json.getString("endColorRGBAHEX"),
      json.getDouble("minSignal"),
      json.getDouble("maxSignal")
    );
  }

  public @NotNull SimpleLinearGradient toEntity() {
    return new SimpleLinearGradient(
      32,
      new Color(Integer.parseUnsignedInt(this.startColorRGBAHEX, 1, 9, 16), true),
      new Color(Integer.parseUnsignedInt(this.endColorRGBAHEX, 1, 9, 16), true),
      this.minSignal,
      this.maxSignal
    );
  }
}
