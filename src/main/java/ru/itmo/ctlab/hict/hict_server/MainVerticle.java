package ru.itmo.ctlab.hict.hict_server;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import hdf.hdf5lib.H5;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.bcel.Repository;
import org.apache.bcel.classfile.JavaClass;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.LoggerFactory;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.files.FSHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.operations.ScaffoldingOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.awt.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

@Slf4j
public class MainVerticle extends AbstractVerticle {


  static {
    final var libraryNames = new LinkedHashMap<String, String>();
    libraryNames.put("hdf5", "HDF5");
    libraryNames.put("hdf5_tools", "HDF5_tools");
    libraryNames.put("hdf5_java", "HDF5_java");
    libraryNames.put("libh5blosc", "HDF5 BLOSC filter plugin (Linux-style naming)");
//    libraryNames.put("h5blosc", "HDF5 BLOSC filter plugin (Windows-style naming)");
    libraryNames.put("libh5bshuf", "HDF5 Shuffle filter plugin (Linux-style naming)");
//    libraryNames.put("h5bshuf", "HDF5 Shuffle filter plugin (Windows-style naming)");
    libraryNames.put("libh5bz2", "HDF5 BZ2 filter plugin (Linux-style naming)");
//    libraryNames.put("h5bz2", "HDF5 BZ2 filter plugin (Windows-style naming)");
    libraryNames.put("libh5lz4", "HDF5 LZ4 filter plugin (Linux-style naming)");
//    libraryNames.put("h5lz4", "HDF5 LZ4 filter plugin (Windows-style naming)");
    libraryNames.put("libh5lzf", "HDF5 LZF filter plugin (Linux-style naming)");
//    libraryNames.put("h5lzf", "HDF5 LZF filter plugin (Windows-style naming)");
    libraryNames.put("libh5zfp", "HDF5 ZFP filter plugin (Linux-style naming)");
//    libraryNames.put("h5zfp", "HDF5 ZFP filter plugin (Windows-style naming)");
    libraryNames.put("libh5zstd", "HDF5 zSTD filter plugin (Linux-style naming)");
//    libraryNames.put("h5zstd", "HDF5 zSTD filter plugin (Windows-style naming)");

    for (final var e : libraryNames.entrySet()) {
      final var lib = e.getKey();
      final var name = e.getValue();
      log.info("Loading " + name + " library");
      try {
        NativeLoader.loadLibrary(lib);
      } catch (final IOException err) {
        log.error("Failed to load native library " + name + " by NativeLoader");
        log.error("Failed to load native library due to IOException");
//        throw new RuntimeException("Failed to load native library " + name + " by NativeLoader", err);
      } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
        log.error("Failed to load native library " + name + " by NativeLoader due to unsatisfied link error");
        log.error("Failed to load native library due to UnsatisfiedLinkError");
//        throw new RuntimeException("Failed to load native library " + name + " by NativeLoader due to unsatisfied link error", unsatisfiedLinkError);
      }
    }

    try {
      H5.loadH5Lib();
    } catch (final Throwable uoe) {
      log.info("Caught an Unsupported Operation Exception while initializing HDF5 Library, if it complains about library version, you can simply ignore that", uoe);
    }

    try {
      log.info("Trying to patch library version checker in H5");
      final var loadH5LibMethod = H5.class.getDeclaredMethod("loadH5Lib");
      loadH5LibMethod.setAccessible(true);

      final JavaClass H5Class = Repository.lookupClass(H5.class); //("hdf.hdf5lib.H5");

      final var h5m = H5Class.getMethod(loadH5LibMethod);

      log.debug("H5 Class found");


      // hdf.hdf5lib.H5

    } catch (NoSuchMethodException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      log.debug("H5 patcher: finally");
    }
  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    // set vertx logger delegate factory to slf4j
    String logFactory = System.getProperty("org.vertx.logger-delegate-factory-class-name");
    if (logFactory == null) {
      System.setProperty("org.vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    }

    final Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);


    log.info("Logging initialized");

    final ConfigStoreOptions jsonEnvConfig = new ConfigStoreOptions().setType("env")
      .setConfig(new JsonObject().put("keys", new JsonArray().add("DATA_DIR").add("TILE_SIZE").add("VXPORT").add("MIN_DS_POOL").add("MAX_DS_POOL")));
    final ConfigRetrieverOptions myOptions = new ConfigRetrieverOptions().addStore(jsonEnvConfig);
    final ConfigRetriever myConfigRetriver = ConfigRetriever.create(vertx, myOptions);
    myConfigRetriver.getConfig(asyncResults -> System.out.println(asyncResults.result().encodePrettily()));
    final CyclicBarrier barrier = new CyclicBarrier(1);

    myConfigRetriver.getConfig(event -> {
      final var dataDirectoryString = event.result().getString("DATA_DIR", ".");
      final var dataDirectory = Path.of(dataDirectoryString).normalize().toAbsolutePath().normalize();
      final var tileSize = event.result().getInteger("TILE_SIZE", 256);
      final var minDSPool = event.result().getInteger("MIN_DS_POOL", 4);
      final var maxDSPool = event.result().getInteger("MAX_DS_POOL", 16);
      final var port = event.result().getInteger("VXPORT", 5000);

      try {
        log.info("Trying to write configuration to local map");
        final var map = vertx.sharedData().getLocalMap("hict_server");
        map.put("dataDirectory", new ShareableWrappers.PathWrapper(dataDirectory));
        map.put("tileSize", tileSize);
        map.put("VXPORT", port);
        map.put("MIN_DS_POOL", minDSPool);
        map.put("MAX_DS_POOL", maxDSPool);

        final var defaultVisualizationOptions = new SimpleVisualizationOptions(10.0, 0.0, false, false, false,
          new SimpleLinearGradient(
            32,
            new Color(255, 255, 255, 0),
            new Color(0, 96, 0, 255),
            0.0d,
            1.0d
          ));

        map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(defaultVisualizationOptions));

        log.info("Added to local map");
      } finally {
        log.info("Finished configuration write to maps");
      }

      log.info("Using " + dataDirectory + " as data directory");
      log.info("Using tile size " + tileSize);
      log.info("Server will start on port " + port);
      try {
        barrier.await();
      } catch (final InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    });

    final HttpServerOptions serverOptions = new HttpServerOptions();
    serverOptions.setCompressionSupported(true);
    final var server = vertx.createHttpServer(serverOptions);
    final var router = Router.router(vertx);

    router.route().handler(CorsHandler.create()
      .allowedMethod(io.vertx.core.http.HttpMethod.GET)
      .allowedMethod(io.vertx.core.http.HttpMethod.POST)
      .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
      .allowedHeader("Access-Control-Request-Method")
      .allowedHeader("Access-Control-Allow-Credentials")
      .allowedHeader("Access-Control-Allow-Origin")
      .allowedHeader("Access-Control-Allow-Headers")
      .allowedHeader("Content-Type"));
    router.route().handler(BodyHandler.create());
//    router.route().handler(ErrorHandler.create(Vertx.vertx()));
    vertx.exceptionHandler(event -> {
      log.error("An exception was caught at the top level", event);
      log.debug(event.getMessage());
    });
    log.info("Awaiting configuration to be written into the local map");
    barrier.await();

    final int port;
    try {
      final var map = vertx.sharedData().getLocalMap("hict_server");
      port = (int) map.get("VXPORT");
    } finally {
      log.info("Finished maps");
    }


    log.info("Initializing handlers");
    final List<HandlersHolder> handlersHolders = new ArrayList<>();
    handlersHolders.add(new FSHandlersHolder(vertx));
    handlersHolders.add(new TileHandlersHolder(vertx));
    handlersHolders.add(new FileOpHandlersHolder(vertx));
    handlersHolders.add(new ScaffoldingOpHandlersHolder(vertx));

    router.route().failureHandler(ctx -> ctx.response().end(
      ctx.failure().getMessage()
    ));


    log.info("Configuring router");
    handlersHolders.forEach(handlersHolder -> handlersHolder.addHandlersToRouter(router));

    log.info("Starting server on port " + port);
    server.requestHandler(router).listen(port);
    log.info("Server started");

    log.info("Deploying WebUI Verticle");
    vertx.deployVerticle(new WebUIVerticle());
  }
}
