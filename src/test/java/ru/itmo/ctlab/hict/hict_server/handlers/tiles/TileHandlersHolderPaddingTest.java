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

package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import org.junit.jupiter.api.Test;

import java.awt.image.BufferedImage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TileHandlersHolderPaddingTest {
  private static final int TRANSPARENT = 0x00000000;
  private static final int RED = 0xffff0000;
  private static final int GREEN = 0xff00ff00;
  private static final int BLUE = 0xff0000ff;
  private static final int WHITE = 0xffffffff;

  @Test
  void padTileImageAtOffset_keepsFullTileImagesUnchanged() {
    final var image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_ARGB);

    assertSame(image, TileHandlersHolder.padTileImageAtOffset(image, 0, 0, 2, 2));
  }

  @Test
  void padTileImageAtOffset_padsRightAndBottomEdgesWithTransparency() {
    final var image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_ARGB);
    image.setRGB(0, 0, RED);
    image.setRGB(1, 0, GREEN);
    image.setRGB(0, 1, BLUE);
    image.setRGB(1, 1, WHITE);

    final var padded = TileHandlersHolder.padTileImageAtOffset(image, 0, 0, 4, 4);

    assertEquals(4, padded.getWidth());
    assertEquals(4, padded.getHeight());
    assertEquals(RED, padded.getRGB(0, 0));
    assertEquals(GREEN, padded.getRGB(1, 0));
    assertEquals(BLUE, padded.getRGB(0, 1));
    assertEquals(WHITE, padded.getRGB(1, 1));
    assertEquals(TRANSPARENT, padded.getRGB(2, 0));
    assertEquals(TRANSPARENT, padded.getRGB(0, 2));
    assertEquals(TRANSPARENT, padded.getRGB(3, 3));
  }

  @Test
  void padTileImageAtOffset_respectsClampedMatrixOffsetInsideRequestedTile() {
    final var image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_ARGB);
    image.setRGB(0, 0, RED);
    image.setRGB(1, 0, GREEN);
    image.setRGB(0, 1, BLUE);
    image.setRGB(1, 1, WHITE);

    final var padded = TileHandlersHolder.padTileImageAtOffset(image, 1, 2, 5, 5);

    assertEquals(5, padded.getWidth());
    assertEquals(5, padded.getHeight());
    assertEquals(TRANSPARENT, padded.getRGB(0, 0));
    assertEquals(TRANSPARENT, padded.getRGB(1, 1));
    assertEquals(RED, padded.getRGB(1, 2));
    assertEquals(GREEN, padded.getRGB(2, 2));
    assertEquals(BLUE, padded.getRGB(1, 3));
    assertEquals(WHITE, padded.getRGB(2, 3));
    assertEquals(TRANSPARENT, padded.getRGB(3, 3));
  }
}
