package ru.itmo.ctlab.hict.hict_library.visualization;

import org.junit.jupiter.api.Test;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import static org.junit.jupiter.api.Assertions.*;

class TileVisualizationProcessorRgbaTest {
  // Exercise the shared native/Java RGBA image boundary without HDF5 or JNI.
  private BufferedImage image(byte[] rgba, int width, int height) throws Exception {
    var factory = TileVisualizationProcessor.class.getDeclaredMethod(
      "createRgbaImage", byte[].class, int.class, int.class);
    factory.setAccessible(true);
    return (BufferedImage) factory.invoke(null, rgba, width, height);
  }

  @Test void straightAlphaDoesNotChangeCallerPixels() throws Exception {
    byte[] rgba = {(byte) 80, 40, 20, (byte) 128, 60, 30, 15, (byte) 255};
    byte[] original = rgba.clone();
    var tile = image(rgba, 2, 1);
    assertArrayEquals(original, rgba, "Image wrapping must not rewrite straight RGBA");
    assertFalse(tile.isAlphaPremultiplied());
    assertEquals(0x80502814, tile.getRGB(0, 0));
    assertEquals(0xff3c1e0f, tile.getRGB(1, 0));
  }

  @Test void transparentAndTranslucentPixelsSurvivePngRoundTrip() throws Exception {
    byte[] rgba = {80, 40, 20, (byte) 128, 0, 0, 0, 0,
                   10, 20, 30, (byte) 255, 50, 60, 70, 1};
    var tile = image(rgba, 2, 2);
    var encoded = new ByteArrayOutputStream();
    assertTrue(ImageIO.write(tile, "png", encoded));
    var decoded = ImageIO.read(new ByteArrayInputStream(encoded.toByteArray()));
    assertArrayEquals(new int[]{0x80502814, 0, 0xff0a141e, 0x01323c46},
      decoded.getRGB(0, 0, 2, 2, null, 0, 2));
  }

  @Test void opaqueRectangularTilesRetainRowAndChannelOrder() throws Exception {
    byte[] rgba = new byte[3 * 2 * 4];
    int[] expected = new int[6];
    for (int i=0; i<6; i++) {
      rgba[4*i]=(byte)(20*i); rgba[4*i+1]=(byte)(10*i);
      rgba[4*i+2]=(byte)(5*i); rgba[4*i+3]=(byte)255;
      expected[i]=0xff000000 | (20*i << 16) | (10*i << 8) | 5*i;
    }
    assertArrayEquals(expected, image(rgba, 3, 2).getRGB(0, 0, 3, 2, null, 0, 3));
  }
}
