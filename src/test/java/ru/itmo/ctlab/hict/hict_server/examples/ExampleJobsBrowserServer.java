package ru.itmo.ctlab.hict.hict_server.examples;

import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import ru.itmo.ctlab.hict.hict_server.handlers.examples.ExamplesHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.jobs.BackgroundJobsHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test-only slow TLS origin plus real example/job handlers. No production URL-policy switches. */
public final class ExampleJobsBrowserServer {
  public static void main(String[] args) throws Exception {
    var data = Path.of(args[0]).toRealPath();
    var output = Path.of(args[1]).toRealPath();
    if (!data.startsWith("/tmp") || !output.startsWith("/tmp")) throw new IllegalArgumentException("Disposable /tmp paths required");
    var keyFile = output.resolve("fixture.p12");
    var keys = KeyStore.getInstance("PKCS12");
    try (var input = Files.newInputStream(keyFile)) { keys.load(input, "fixture-only".toCharArray()); }
    var managers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    managers.init(keys, "fixture-only".toCharArray());
    var ssl = SSLContext.getInstance("TLS"); ssl.init(managers.getKeyManagers(), null, null);
    var origin = HttpsServer.create(new InetSocketAddress("127.0.0.1",0),0);
    origin.setHttpsConfigurator(new HttpsConfigurator(ssl));
    origin.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
    var bytes = Files.readAllBytes(data.resolve("tiny-primary.hict.hdf5"));
    var payload = java.util.Arrays.copyOf(bytes, bytes.length + 4 * 1024 * 1024);
    var hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
    var cooler = Files.readAllBytes(data.resolve("valid-example.mcool"));
    var coolerPayload = java.util.Arrays.copyOf(cooler, cooler.length + 4 * 1024 * 1024);
    var reference = ">tiny\nACGTACGTACGTACGTA\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    java.util.function.Function<String,byte[]> contentFor = name -> name.startsWith("reference-") ? reference
      : name.equals("convert-fail") ? "Not a Cooler file".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
      : name.startsWith("convert-") ? coolerPayload : payload;
    var paused = new AtomicBoolean(true);
    var fail = new AtomicBoolean(true);
    var ranges = new java.util.concurrent.CopyOnWriteArrayList<String>();
    origin.createContext("/payload/", exchange -> {
      try {
        var content = contentFor.apply(exchange.getRequestURI().getPath().substring("/payload/".length()));
        if (exchange.getRequestURI().getPath().endsWith("optional") || exchange.getRequestURI().getPath().endsWith("failure") && fail.get()) {
          exchange.sendResponseHeaders(503, -1); return;
        }
        int offset = 0;
        var range = exchange.getRequestHeaders().getFirst("Range");
        if (range != null) { synchronized (ranges) { ranges.add(exchange.getRequestURI().getPath() + " " + range); Files.writeString(output.resolve("ranges.json"), new JsonArray(ranges).encode()); } }
        if (range != null) offset = Integer.parseInt(range.replace("bytes=", "").replace("-", ""));
        exchange.getResponseHeaders().set("ETag", "\"fixture-v1\"");
        if (offset > 0) exchange.getResponseHeaders().set("Content-Range", "bytes " + offset + "-" + (content.length-1) + "/" + content.length);
        exchange.sendResponseHeaders(offset > 0 ? 206 : 200, exchange.getRequestURI().getPath().endsWith("unknown") ? 0 : content.length-offset);
        int written = 0;
        for (int position = offset; position < content.length; position += 16384) {
          int length = Math.min(16384, content.length-position);
          exchange.getResponseBody().write(content, position, length); exchange.getResponseBody().flush();
          written += length;
          if (written < 131072) Thread.sleep(50);
          while (written >= 131072 && paused.get()) Thread.sleep(25);
        }
      } catch (Exception expectedOnCancellation) {
        // The downloader owns cancellation; the origin may observe a reset while paused.
      } finally { exchange.close(); }
    });
    origin.start();
    String base = "https://127.0.0.1:" + origin.getAddress().getPort();
    var catalog = new JsonObject().put("schemaVersion",1).put("catalogId","jobs-fixture")
      .put("sections", new JsonArray().add(new JsonObject().put("id","jobs-fixture").put("title","Download workflow fixtures").put("order",0)));
    var examples = new JsonArray();
    for (var id : List.of("automatic","manual","cancel","failure","unknown","changed","starting","convert-auto","convert-close","convert-change","convert-late","convert-fail","convert-cancel")) {
      boolean converting = id.startsWith("convert-");
      var filePayload = contentFor.apply(id);
      var fileHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(filePayload));
      var file = new JsonObject().put("id","matrix").put("role","matrix").put("format",converting ? "mcool" : "hict")
        .put("filename",converting ? "example.mcool" : "example.hict.hdf5").put("url",base + "/payload/" + id).put("required",true).put("sha256",fileHash);
      if (!id.equals("unknown")) file.put("bytes",filePayload.length);
      var files = new JsonArray().add(file);
      var recipe = new JsonObject().put("matrix","matrix").put("quickStart",true);
      if (converting) {
        files.add(new JsonObject().put("id","reference").put("role","fasta").put("format","fasta").put("filename","example.fa")
          .put("url",base + "/payload/reference-" + id).put("required",true).put("bytes",reference.length)
          .put("sha256",HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(reference))));
        recipe.put("fasta","reference");
      }
      if (id.equals("manual")) files.add(new JsonObject().put("id","optional").put("role","track").put("format","bed")
        .put("url",base+"/payload/optional").put("filename","optional.bed").put("required",false));
      examples.add(new JsonObject().put("id","jobs-"+id).put("sectionId","jobs-fixture").put("title","Workflow " + id)
        .put("description","Controlled download for browser acceptance.")
        .put("publications",new JsonArray().add(new JsonObject().put("title","Synthetic fixture").put("citation","HiCT automated fixture").put("url","https://example.org/fixture")))
        .put("files",files).put("openingRecipe",recipe)
        .put("attribution",new JsonObject().put("provider","HiCT tests").put("url","https://example.org/").put("termsUrl","https://example.org/terms").put("acknowledgement","Synthetic data, no biological interpretation.")));
    }
    catalog.put("examples",examples);
    var catalogs = Files.createDirectories(data.resolve("examples/catalogs"));
    Files.writeString(catalogs.resolve("jobs-fixture.json"),catalog.encodePrettily());
    var downloader = new ExampleDownloader(data, uri -> {
      if (!uri.getScheme().equals("https") || !uri.getHost().equals("127.0.0.1") || uri.getPort()!=origin.getAddress().getPort())
        throw new java.io.IOException("Only the owned test TLS origin is allowed");
    }, 0);
    var vertx = Vertx.vertx();
    vertx.sharedData().getLocalMap("hict_server").put("dataDirectory",new ShareableWrappers.PathWrapper(data));
    var handlers = new ExamplesHandlersHolder(vertx);
    // Inject the package-private test-policy downloader without adding a production configuration hook.
    var field = ExamplesHandlersHolder.class.getDeclaredField("downloader"); field.setAccessible(true); field.set(handlers,downloader);
    field = ExamplesHandlersHolder.class.getDeclaredField("downloaderRoot"); field.setAccessible(true); field.set(handlers,data);
    var router = Router.router(vertx);
    router.route().handler(ctx -> {
      ctx.response().putHeader("Access-Control-Allow-Origin", args[2])
        .putHeader("Access-Control-Allow-Headers", "*").putHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
      if (ctx.request().method() == io.vertx.core.http.HttpMethod.OPTIONS) ctx.response().end(); else ctx.next();
    });
    router.route().handler(BodyHandler.create());
    router.route().failureHandler(ctx -> ctx.response().setStatusCode(ctx.statusCode()>0?ctx.statusCode():500).end(new JsonObject().put("error",ctx.failure()==null?"Failed":ctx.failure().getMessage()).encode()));
    handlers.addHandlersToRouter(router);
    new BackgroundJobsHandlersHolder(List.of(new BackgroundJobsHandlersHolder.Service("download",handlers::backgroundJobs,handlers::cancelBackgroundJob,handlers::close))).addHandlersToRouter(router);
    router.post("/fixture/control").handler(ctx -> {
      var body=ctx.body().asJsonObject();if(body.containsKey("paused"))paused.set(body.getBoolean("paused"));if(body.containsKey("fail"))fail.set(body.getBoolean("fail"));ctx.response().end("{}");
    });
    var server=vertx.createHttpServer().requestHandler(router).listen(0,"127.0.0.1").toCompletionStage().toCompletableFuture().get();
    Files.writeString(output.resolve("fixture-info.json"),new JsonObject().put("api","http://127.0.0.1:"+server.actualPort()).put("sha256",hash).put("bytes",payload.length).encode());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {paused.set(false);downloader.close();origin.stop(0);vertx.close();}));
  }
}
