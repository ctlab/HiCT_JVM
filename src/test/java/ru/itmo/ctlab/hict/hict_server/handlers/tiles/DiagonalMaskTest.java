package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.DiagonalMask;
import ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization.VisualizationOptionsDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.SessionVisualizationCodec;
import java.awt.image.BufferedImage;

import static org.junit.jupiter.api.Assertions.*;

class DiagonalMaskTest {
  @Test void distanceUsesStrictAssemblyBinOffsets() {
    assertEquals(-1, DiagonalMask.DEFAULT.maskedOffsetBins(1_000));
    final var mask = new DiagonalMask(true, 100_000);
    assertEquals(99, mask.maskedOffsetBins(1_000));
    assertEquals(0, mask.maskedOffsetBins(100_000));
    assertEquals(0, mask.maskedOffsetBins(1_000_000));
    assertEquals(1, new DiagonalMask(true, 150_000).maskedOffsetBins(100_000));
    assertThrows(IllegalArgumentException.class, () -> new DiagonalMask(true, 0));
  }

  @Test void maskedPixelsAndStatisticsShareAbsoluteAssemblyCoordinates() {
    final var mask = new TileHandlersHolder.DiagonalGrid(new long[]{10, 11}, new long[]{9, 10, 11, 12}, 0);
    final double[][] values = {{2, 99, 3, 4}, {5, 6, 99, 7}};
    final var image = new BufferedImage(4, 2, BufferedImage.TYPE_INT_ARGB);
    for (int row = 0; row < 2; row++) {
      for (int col = 0; col < 4; col++) image.setRGB(col, row, 0xff440022);
    }
    mask.clear(image);
    assertEquals(0, image.getRGB(1, 0));
    assertEquals(0, image.getRGB(2, 1));
    assertEquals(0xff440022, image.getRGB(0, 0));
    assertEquals(0xff440022, image.getRGB(3, 1));
    final var stats = new TileSignalStatistics();
    stats.traditional(values, mask::contains);
    final var range = stats.snapshot("PRIMARY", 1000, "test").processed().getFirst().range();
    assertEquals(6, range.count());
    assertEquals(2, range.min());
    assertEquals(7, range.max());
    assertEquals(99, values[0][1]);
  }

  @Test void maskSettingsPersistInOptionsAndSessions() {
    final var raw = new JsonObject()
      .put("diagonalMask", new JsonObject().put("enabled", true).put("distanceBp", 150_000))
      .put("colormap", new JsonObject().put("colormapType", "SimpleLinearGradient")
        .put("startColorRGBAString", "rgba(255,255,255,1)")
        .put("endColorRGBAString", "rgba(0,0,0,1)").put("minSignal", 0).put("maxSignal", 1));
    final var options = VisualizationOptionsDTO.fromJSONObject(raw).toEntity();
    final var session = SessionVisualizationCodec.encode(options);
    assertEquals(new DiagonalMask(true, 150_000), SessionVisualizationCodec.decode(session).getDiagonalMask());
    session.remove("diagonalMask");
    assertEquals(DiagonalMask.DEFAULT, SessionVisualizationCodec.decode(session).getDiagonalMask());
    assertThrows(IllegalArgumentException.class, () -> VisualizationOptionsDTO.readDiagonalMask(raw.copy()
      .put("diagonalMask", new JsonObject().put("enabled", true).put("distanceBp", 2.5))));
  }
}
