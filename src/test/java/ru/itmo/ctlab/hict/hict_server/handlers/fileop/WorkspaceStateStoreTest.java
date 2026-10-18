package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;
import org.junit.jupiter.api.Test;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;
import static org.junit.jupiter.api.Assertions.*;

class WorkspaceStateStoreTest {
  @Test void snapshotsAreCopiedAndStaleClientsCannotOverwriteThem() {
    var store=new WorkspaceStateStore();var initial=store.version();
    var state=new JsonObject().put("version",1).put("view",new JsonObject().put("center",10));
    var saved=store.save(initial,state);
    assertEquals(1L,saved.getJsonObject("workspaceVersion").getLong("revision"));
    state.getJsonObject("view").put("center",20);
    assertEquals(10,store.snapshot().getJsonObject("workspaceState").getJsonObject("view").getInteger("center"));
    var failure=assertThrows(HttpException.class,()->store.save(initial,state));
    assertEquals(409,failure.getStatusCode());
    store.snapshot().getJsonObject("workspaceState").clear();
    assertEquals(1,store.snapshot().getJsonObject("workspaceState").getInteger("version"));
  }

  @Test void reopeningSameFileStillNeedsNewSessionToken() {
    var old=new WorkspaceStateStore();var reopened=new WorkspaceStateStore();
    assertNotEquals(old.version().getString("session"),reopened.version().getString("session"));
    assertEquals(409,assertThrows(HttpException.class,()->reopened.save(old.version(),new JsonObject().put("version",1))).getStatusCode());
    assertEquals(0L,reopened.version().getLong("revision"));
  }

  @Test void invalidInputDoesNotAdvanceRevision() {
    var store=new WorkspaceStateStore();var version=store.version();
    for(var state:new JsonObject[]{new JsonObject(),new JsonObject().put("version","1"),
        new JsonObject().put("version",2),new JsonObject().put("version",1).put("data","x".repeat(1_000_000))}) {
      assertEquals(400,assertThrows(HttpException.class,()->store.save(version,state)).getStatusCode());
      assertEquals(version,store.version());
    }
    for(var token:new JsonObject[]{new JsonObject(),version.copy().put("revision",0.5),version.copy().put("revision","0")})
      assertEquals(409,assertThrows(HttpException.class,()->store.save(token,new JsonObject().put("version",1))).getStatusCode());
  }

  @Test void onlyOneConcurrentWriterCanCommitTheSameRevision() throws Exception {
    var store=new WorkspaceStateStore();var version=store.version();
    try(var pool=Executors.newFixedThreadPool(8)) {
      var calls=IntStream.range(0,24).mapToObj(i->(Callable<Boolean>)()->{
        try {store.save(version,new JsonObject().put("version",1).put("writer",i));return true;}
        catch(HttpException conflict) {assertEquals(409,conflict.getStatusCode());return false;}
      }).toList();
      int succeeded=0;for(var result:pool.invokeAll(calls))if(result.get())succeeded++;
      assertEquals(1,succeeded);assertEquals(1L,store.version().getLong("revision"));
    }
  }
}
