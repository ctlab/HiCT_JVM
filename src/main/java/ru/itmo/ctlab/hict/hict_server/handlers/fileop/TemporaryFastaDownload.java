package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.ext.web.RoutingContext;
import java.io.*;
import java.nio.file.*;

/** Prepare on an export worker, then transfer asynchronously without a genome-sized String/Buffer. */
final class TemporaryFastaDownload {
  @FunctionalInterface interface Writer { void write(OutputStream output) throws IOException; }
  private final RoutingContext context;
  private Path path;
  private boolean writing, closed;
  TemporaryFastaDownload(RoutingContext context) {
    this.context=context;
    context.addEndHandler(ignored -> close());
  }
  Path write(Writer writer) throws IOException {
    synchronized(this) {
      if(closed)throw new InterruptedIOException("FASTA download closed");
      path=Files.createTempFile("hict-fasta-export-", ".fa");writing=true;
    }
    boolean complete=false;
    try(var output=new BufferedOutputStream(Files.newOutputStream(path),1<<20)) {
      writer.write(output);output.flush();complete=true;return path;
    } finally {
      synchronized(this) {writing=false;if(!complete||Thread.currentThread().isInterrupted())closed=true;cleanIfClosed();}
    }
  }
  void send(Path file) {
    if(context.response().ended()||context.response().closed()) {close();return;}
    context.response().putHeader("Content-Type","text/plain; charset=utf-8")
      .sendFile(file.toString()).onComplete(result -> {close();if(result.failed()&&!context.response().ended())context.fail(result.cause());});
  }
  synchronized void close() {closed=true;cleanIfClosed();}
  private void cleanIfClosed() {
    if(closed&&!writing&&path!=null) {
      Path file=path;path=null;
      context.vertx().fileSystem().delete(file.toString()).onFailure(error ->
        org.slf4j.LoggerFactory.getLogger(TemporaryFastaDownload.class).warn("Cannot remove temporary FASTA download {}",file,error));
    }
  }
}
