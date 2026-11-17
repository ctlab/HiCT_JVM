package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.ext.web.handler.HttpException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FigureFitBudgetTest {
  @Test void boundsBothCellWorkAndThinRectangleArraysWithoutOverflow() {
    assertDoesNotThrow(() -> TileHandlersHolder.validateCaptureFitWindow(4096, 4096));
    assertDoesNotThrow(() -> TileHandlersHolder.validateCaptureFitWindow(1, 65536));
    for (var axes : new long[][]{{4097, 4097}, {1, 65537}, {65537, 1}, {Long.MAX_VALUE, Long.MAX_VALUE}})
      assertEquals(413, assertThrows(HttpException.class,
        () -> TileHandlersHolder.validateCaptureFitWindow(axes[0], axes[1])).getStatusCode());
  }
}
