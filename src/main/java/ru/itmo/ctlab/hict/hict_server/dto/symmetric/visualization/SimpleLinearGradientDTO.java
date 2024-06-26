/*
 * MIT License
 *
 * Copyright (c) 2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;

import java.awt.*;
import java.util.Locale;
import java.util.regex.Pattern;

@Getter(AccessLevel.PUBLIC)
@ToString
public class SimpleLinearGradientDTO extends ColormapDTO {
  private static final Pattern RGBA_EXTRACT_PATTERN = Pattern.compile("rgba\\p{Zs}*\\(\\p{Zs}*(?<red>\\d+)\\p{Zs}*,\\p{Zs}*(?<green>\\d+)\\p{Zs}*,\\p{Zs}*(?<blue>\\d+)\\p{Zs}*,\\p{Zs}*(?<alpha>[+-]?\\d+[.,]?\\d*)\\p{Zs}*\\)");
  private final String startColorRGBAString;
  private final double minSignal;
  private final double maxSignal;
  private final String endColorRGBAString;

  public SimpleLinearGradientDTO(String startColorHEX, String endColorHEX, double minSignal, double maxSignal) {
    super("SimpleLinearGradient");
    this.startColorRGBAString = startColorHEX;
    this.endColorRGBAString = endColorHEX;
    this.minSignal = minSignal;
    this.maxSignal = maxSignal;
  }

  public static @NotNull SimpleLinearGradientDTO fromEntity(final @NotNull SimpleLinearGradient cmap, final @NotNull ChunkedFile chunkedFile) {
//    final String startHEX = String.format("#%02x%02x%02x%02x", cmap.getStartColor().getRed(), cmap.getStartColor().getGreen(), cmap.getStartColor().getBlue(), cmap.getStartColor().getAlpha());
//    final String endHEX = String.format("#%02x%02x%02x%02x", cmap.getEndColor().getRed(), cmap.getEndColor().getGreen(), cmap.getEndColor().getBlue(), cmap.getEndColor().getAlpha());

    return new SimpleLinearGradientDTO(
      String.format(Locale.US, "rgba(%d,%d,%d,%f)", cmap.getStartColor().getRed(), cmap.getStartColor().getGreen(), cmap.getStartColor().getBlue(), ((double) cmap.getStartColor().getAlpha() / 255.0d)),
      String.format(Locale.US, "rgba(%d,%d,%d,%f)", cmap.getEndColor().getRed(), cmap.getEndColor().getGreen(), cmap.getEndColor().getBlue(), ((double) cmap.getEndColor().getAlpha() / 255.0d)),
      cmap.getMinSignal(),
      cmap.getMaxSignal()
    );
  }

  public static @NotNull SimpleLinearGradientDTO fromJSONObject(final @NotNull JsonObject json) {
    return new SimpleLinearGradientDTO(
      json.getString("startColorRGBAString"),
      json.getString("endColorRGBAString"),
      json.getDouble("minSignal"),
      json.getDouble("maxSignal")
    );
  }

  public @NotNull SimpleLinearGradient toEntity() {
    final var startColorMatcher = RGBA_EXTRACT_PATTERN.matcher(this.startColorRGBAString);
    final var endColorMatcher = RGBA_EXTRACT_PATTERN.matcher(this.endColorRGBAString);
    final var startMatches = startColorMatcher.matches();
    final var endMatches = endColorMatcher.matches();
    assert startMatches : "Wrong start RGBA color?";
    assert endMatches : "Wrong end RGBA color?";
    return new SimpleLinearGradient(
      32,
      new Color(
        Integer.parseInt(startColorMatcher.group("red")),
        Integer.parseInt(startColorMatcher.group("green")),
        Integer.parseInt(startColorMatcher.group("blue")),
        (int) (255.0d * Double.parseDouble(startColorMatcher.group("alpha").replaceAll(",", ".")))
      ),
      new Color(
        Integer.parseInt(endColorMatcher.group("red")),
        Integer.parseInt(endColorMatcher.group("green")),
        Integer.parseInt(endColorMatcher.group("blue")),
        (int) (255.0d * Double.parseDouble(endColorMatcher.group("alpha").replaceAll(",", ".")))
      ),
      this.minSignal,
      this.maxSignal
    );
  }
}
