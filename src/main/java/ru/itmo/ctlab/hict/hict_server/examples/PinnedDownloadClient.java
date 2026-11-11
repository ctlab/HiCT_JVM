package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.net.SocketAddress;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Connects to the validated address while retaining the origin Host, TLS SNI and certificate checks. */
final class PinnedDownloadClient implements AutoCloseable {
  record Response(int statusCode, HttpHeaders headers, InputStream body) { }
  private volatile DownloadStream active;
  private volatile HttpClientRequest activeRequest;
  private final Vertx vertx;
  private final boolean ownsVertx;
  private final HttpClient client;

  PinnedDownloadClient() {
    final var context = Vertx.currentContext();
    ownsVertx = context == null;
    vertx = ownsVertx ? Vertx.vertx() : context.owner();
    client = vertx.createHttpClient(new HttpClientOptions()
      .setConnectTimeout(15_000).setIdleTimeout(60).setVerifyHost(true)
      .setKeepAlive(false).setTryUseCompression(false));
  }

  Response open(URI uri, InetAddress address, long offset, String validator, java.util.concurrent.atomic.AtomicBoolean cancelled) throws IOException {
    final boolean ssl = "https".equalsIgnoreCase(uri.getScheme());
    final int port = uri.getPort() > 0 ? uri.getPort() : ssl ? 443 : 80;
    final String path = (uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath())
      + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
    try {
      if (cancelled.get()) throw new java.util.concurrent.CancellationException("Cancelled");
      final var request = client.request(new RequestOptions().setHost(uri.getHost()).setPort(port)
        .setSsl(ssl).setURI(path).setServer(SocketAddress.inetSocketAddress(port, address.getHostAddress()))
        .setTimeout(60_000)).map(value -> {
          activeRequest = value;
          if (cancelled.get()) { value.reset(); throw new java.util.concurrent.CancellationException("Cancelled"); }
          return value;
        }).toCompletionStage().toCompletableFuture().get(65, TimeUnit.SECONDS);
      if (offset > 0) {
        request.putHeader("Range", "bytes=" + offset + "-");
        if (validator != null) request.putHeader("If-Range", validator);
      }
      // Install backpressure and terminal handlers on the event loop before handing off the body.
      return request.send().map(response -> {
        if (cancelled.get()) { request.reset(); throw new java.util.concurrent.CancellationException("Cancelled"); }
        response.pause();
        final var headers = new HashMap<String, List<String>>();
        for (var name : response.headers().names()) headers.put(name, response.headers().getAll(name));
        final var stream = new DownloadStream(request, response);
        active = stream;
        return new Response(response.statusCode(), HttpHeaders.of(headers, (name, value) -> true), stream);
      }).toCompletionStage().toCompletableFuture().get(65, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new java.util.concurrent.CancellationException("Download interrupted");
    } catch (Exception failure) {
      cancelActive();
      throw new IOException("Could not download from " + uri.getHost(), failure);
    }
  }

  private final class DownloadStream extends InputStream {
    private static final Object END = new Object();
    private final LinkedBlockingQueue<Object> events = new LinkedBlockingQueue<>(3);
    private final HttpClientRequest request;
    private final HttpClientResponse response;
    private byte[] current;
    private int position;
    private volatile boolean ended;

    DownloadStream(HttpClientRequest request, HttpClientResponse response) {
      this.request = request;
      this.response = response;
      response.exceptionHandler(error -> events.offer(error));
      response.endHandler(ignored -> events.offer(END));
      response.handler(buffer -> events.offer(buffer));
    }

    @Override public int read() throws IOException {
      final byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
    }

    @Override public int read(byte[] destination, int offset, int length) throws IOException {
      java.util.Objects.checkFromIndexSize(offset, length, destination.length);
      if (length == 0) return 0;
      while (current == null || position == current.length) {
        if (ended) return -1;
        response.fetch(1);
        final Object event;
        try { event = events.poll(65, TimeUnit.SECONDS); }
        catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          close();
          throw new java.util.concurrent.CancellationException("Download interrupted");
        }
        if (event == END) { ended = true; return -1; }
        if (event instanceof Throwable error) throw new IOException("Download connection failed", error);
        if (!(event instanceof Buffer buffer)) throw new IOException("Download timed out");
        current = buffer.getBytes();
        position = 0;
      }
      final int count = Math.min(length, current.length - position);
      System.arraycopy(current, position, destination, offset, count);
      position += count;
      return count;
    }

    @Override public void close() {
      ended = true;
      if (active == this) active = null;
      if (activeRequest == request) activeRequest = null;
      request.reset();
      events.clear();
      events.offer(END);
    }
  }

  void cancelActive() {
    final var request = activeRequest;
    if (request != null) request.reset();
    final var stream = active;
    if (stream != null) stream.close();
  }

  @Override public void close() {
    cancelActive();
    client.close();
    if (ownsVertx) vertx.close();
  }
}
