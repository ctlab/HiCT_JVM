package ru.itmo.ctlab.hict.hict_server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.files.FSHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MainVerticle extends AbstractVerticle {


  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    // TODO: This should be in the queries
    final var dataDirectory = Path.of("/home/tux/HiCT/HiCT_Server/data/").normalize();
    final var chunkedFile = new ChunkedFile(Path.of(dataDirectory.toString(), "zanu_male_4DN.mcool.hict.hdf5"), 256);
    final var chunkedFileWrapper = new ShareableWrappers.ChunkedFileWrapper(chunkedFile);


    // set vertx logger delegate factory to slf4j
    String logFactory = System.getProperty("org.vertx.logger-delegate-factory-class-name");
    if (logFactory == null) {
      System.setProperty("org.vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    }

    final var server = vertx.createHttpServer();
    final var router = Router.router(vertx);

    try {
      log.info("Trying local map");
      final var map = vertx.sharedData().getLocalMap("hict_server");
      map.put("chunkedFile", chunkedFileWrapper);
      map.put("dataDirectory", new ShareableWrappers.PathWrapper(dataDirectory));
      map.put("tileSize", 256);
      log.info("Added to local map");
    } finally {
      log.info("Finished maps");
    }


    log.info("Initializing handlers");
    final List<HandlersHolder> handlersHolders = new ArrayList<>();
    handlersHolders.add(new FSHandlersHolder(vertx));
    handlersHolders.add(new TileHandlersHolder(vertx));
    handlersHolders.add(new FileOpHandlersHolder(vertx));


    log.info("Configuring router");
    handlersHolders.forEach(handlersHolder -> handlersHolder.addHandlersToRouter(router));

    log.info("Starting server");
    server.requestHandler(router).listen(5000);
    log.info("Server started");
  }
}
