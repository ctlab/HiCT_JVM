package ru.itmo.ctlab.hict.hict_library.visualization;

import org.junit.jupiter.api.Test;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import static org.junit.jupiter.api.Assertions.*;

class TileVisualizationProcessorRgbaTest {
  @Test void undefinedRatiosAreTransparentWhileMeasuredZeroIsBlue() throws Exception {
    var bounds = new ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient(
      32, java.awt.Color.WHITE, java.awt.Color.BLACK, 0.5, 2.0);
    double[][] ratios = {{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -1, 0, 1}};
    var tile = image(TileVisualizationProcessor.mapObservedOverExpectedRgba(ratios, bounds), 6, 1);
    for (int i = 0; i < 4; i++) assertEquals(0, tile.getRGB(i, 0));
    assertEquals(0xff2166ac, tile.getRGB(4, 0));
    assertEquals(0xffffffff, tile.getRGB(5, 0));
  }

  @Test void observedOverExpectedUsesReciprocalBoundsAndANeutralMidpoint() throws Exception {
    var bounds = new ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient(
      32, java.awt.Color.WHITE, java.awt.Color.BLACK, 0.5, 2.0);
    double[][] ratios = {{0, 0.5, Math.sqrt(0.5), 1, Math.sqrt(2), 2, 4}};
    var tile = image(TileVisualizationProcessor.mapObservedOverExpectedRgba(ratios, bounds), 7, 1);
    assertEquals(0xff2166ac, tile.getRGB(0, 0));
    assertEquals(0xff2166ac, tile.getRGB(1, 0));
    assertEquals(0xffffffff, tile.getRGB(3, 0));
    assertEquals(0xffb2182b, tile.getRGB(5, 0));
    assertEquals(0xffb2182b, tile.getRGB(6, 0));
    assertEquals(144, (tile.getRGB(2, 0) >> 16) & 0xff, 1);
    assertEquals(140, (tile.getRGB(4, 0) >> 8) & 0xff, 1);
  }

  @Test void javaFallbackPreservesEveryColormapPixelIncludingNonfiniteAndAlpha() throws Exception {
    var map = new ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient(
      32, new java.awt.Color(16,32,64,0), new java.awt.Color(240,128,192,175), -2, 10);
    double[][] values = {{-100,-2,0,3,10,100},
      {Double.NaN,Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY,7.3,-0.1,9.9}};
    var image = image(TileVisualizationProcessor.mapJavaRgba(values,map),6,2);
    for (int row=0;row<2;row++) for(int col=0;col<6;col++)
      assertEquals(map.mapSignal(values[row][col]).getRGB(), image.getRGB(col,row), "pixel "+row+","+col);
    var bytes = new ByteArrayOutputStream();
    ImageIO.write(image,"png",bytes);
    var roundtrip = ImageIO.read(new ByteArrayInputStream(bytes.toByteArray()));
    assertArrayEquals(image.getRGB(0,0,6,2,null,0,6),roundtrip.getRGB(0,0,6,2,null,0,6));
  }
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
