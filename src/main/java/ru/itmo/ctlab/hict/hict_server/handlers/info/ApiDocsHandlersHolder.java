package ru.itmo.ctlab.hict.hict_server.handlers.info;

import io.vertx.ext.web.Router;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Serves interactive API documentation and the OpenAPI v1 specification.
 */
public class ApiDocsHandlersHolder extends HandlersHolder {
  private static final String OPENAPI_SPEC_PATH = "/openapi/hict-api-v1.yaml";
  private static final String SWAGGER_ASSET_ROOT = "/META-INF/resources/webjars/swagger-ui/5.30.2/";
  private static final String OPENAPI_YAML = readResourceUtf8(OPENAPI_SPEC_PATH);
  private static final String OPENAPI_ETAG = computeWeakEtag(OPENAPI_YAML);
  private static final String DOCS_HTML = """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>HiCT API v1</title>
      <link rel="stylesheet" href="/api/v1/assets/swagger-ui.css" />
      <style>
        html, body {
          margin: 0;
          padding: 0;
          height: 100%;
          background: #f5f6f8;
        }
        #swagger-ui {
          height: 100%;
        }
      </style>
    </head>
    <body>
      <div id="swagger-ui"></div>
      <script src="/api/v1/assets/swagger-ui-bundle.js"></script>
      <script>
        window.ui = SwaggerUIBundle({
          url: "/api/v1/openapi.yaml",
          dom_id: "#swagger-ui",
          deepLinking: true,
          displayRequestDuration: true,
          filter: true,
          persistAuthorization: true,
          validatorUrl: null,
          tryItOutEnabled: true
        });
      </script>
    </body>
    </html>
    """;
  private static final String DOCS_ETAG = computeWeakEtag(DOCS_HTML);

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.getWithRegex("^/api/v1$").handler(ctx -> ctx.response()
      .setStatusCode(307)
      .putHeader("location", "/api/v1/")
      .end());

    router.get("/api/v1/").handler(ctx -> {
      if (etagMatches(ctx.request().getHeader("if-none-match"), DOCS_ETAG)) {
        ctx.response().setStatusCode(304).end();
        return;
      }
      ctx.response()
        .putHeader("content-type", "text/html; charset=utf-8")
        .putHeader("cache-control", "public, max-age=300")
        .putHeader("etag", DOCS_ETAG)
        .end(DOCS_HTML);
    });

    router.getWithRegex("^/api/v1/openapi$").handler(ctx -> ctx.response()
      .setStatusCode(307)
      .putHeader("location", "/api/v1/openapi.yaml")
      .end());

    router.get("/api/v1/openapi.yaml").handler(ctx -> {
      if (etagMatches(ctx.request().getHeader("if-none-match"), OPENAPI_ETAG)) {
        ctx.response().setStatusCode(304).end();
        return;
      }
      ctx.response()
        .putHeader("content-type", "application/yaml; charset=utf-8")
        .putHeader("cache-control", "public, max-age=300")
        .putHeader("etag", OPENAPI_ETAG)
        .end(OPENAPI_YAML);
    });

    router.get("/api/v1/assets/swagger-ui.css").handler(ctx ->
      serveAsset(ctx, SwaggerAssets.CSS, "text/css; charset=utf-8"));
    router.get("/api/v1/assets/swagger-ui-bundle.js").handler(ctx ->
      serveAsset(ctx, SwaggerAssets.JAVASCRIPT, "text/javascript; charset=utf-8"));
  }

  private record Asset(String body, String etag) {
    private static Asset read(String filename) {
      final var body = readResourceUtf8(SWAGGER_ASSET_ROOT + filename);
      return new Asset(body, computeWeakEtag(body));
    }
  }

  private static final class SwaggerAssets {
    private static final Asset CSS = Asset.read("swagger-ui.css");
    private static final Asset JAVASCRIPT = Asset.read("swagger-ui-bundle.js");
  }

  private static void serveAsset(io.vertx.ext.web.RoutingContext ctx, Asset asset, String contentType) {
    final var response = ctx.response()
      .putHeader("cache-control", "public, max-age=300")
      .putHeader("etag", asset.etag());
    if (etagMatches(ctx.request().getHeader("if-none-match"), asset.etag())) {
      response.setStatusCode(304).end();
      return;
    }
    response.putHeader("content-type", contentType).end(asset.body());
  }

  private static boolean etagMatches(final String ifNoneMatchHeader, final String expectedEtag) {
    return ifNoneMatchHeader != null && ifNoneMatchHeader.trim().equals(expectedEtag);
  }

  private static @NotNull String readResourceUtf8(final @NotNull String path) {
    try (final InputStream stream = ApiDocsHandlersHolder.class.getResourceAsStream(path)) {
      if (stream == null) {
        throw new IllegalStateException("Resource not found: " + path);
      }
      try (final var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        final var sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append('\n');
        }
        return sb.toString();
      }
    } catch (final Exception ex) {
      throw new IllegalStateException("Failed to load resource " + path, ex);
    }
  }

  private static @NotNull String computeWeakEtag(final @NotNull String content) {
    try {
      final var digest = MessageDigest.getInstance("SHA-256")
        .digest(content.getBytes(StandardCharsets.UTF_8));
      return "W/\"" + HexFormat.of().formatHex(digest) + "\"";
    } catch (final NoSuchAlgorithmException ex) {
      throw new IllegalStateException("SHA-256 is unavailable", ex);
    }
  }
}
