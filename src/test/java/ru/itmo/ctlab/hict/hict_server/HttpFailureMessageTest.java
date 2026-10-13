package ru.itmo.ctlab.hict.hict_server;

import io.vertx.ext.web.handler.HttpException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HttpFailureMessageTest {
  @Test void conflictRetainsActionablePayloadInsteadOfOnlyStatusText() {
    var error = new HttpException(409, "Link the FASTA file before generating a dotplot.");
    assertEquals("Conflict", error.getMessage());
    assertEquals("Link the FASTA file before generating a dotplot.", MainVerticle.failureMessage(error));
    assertEquals("Bad Request", MainVerticle.failureMessage(new HttpException(400)));
    assertEquals("Request failed", MainVerticle.failureMessage(null));
  }
}
