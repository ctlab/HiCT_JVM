package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;
import io.vertx.ext.web.handler.HttpException;
import java.util.UUID;

/** One in-memory workspace per open map; stale clients cannot replace newer views. */
final class WorkspaceStateStore implements Shareable {
  private final String session = UUID.randomUUID().toString();
  private long revision;
  private JsonObject state;

  /** A new store for a restored session. Its identity differs, so windows of the replaced session must attach again. */
  static WorkspaceStateStore restored(JsonObject savedState) {
    final var store = new WorkspaceStateStore();
    if (savedState != null && savedState.getValue("version") instanceof Number v && v.doubleValue() == 1
        && savedState.encode().length() <= 1_000_000) {
      store.state = savedState.copy();
    }
    return store;
  }

  synchronized JsonObject version() {
    return new JsonObject().put("session", session).put("revision", revision);
  }

  synchronized JsonObject snapshot() {
    return new JsonObject().put("workspaceVersion", version())
      .put("workspaceState", state == null ? null : state.copy());
  }

  synchronized JsonObject save(JsonObject expected, JsonObject next) {
    if (expected == null || !session.equals(expected.getValue("session")) ||
        !(expected.getValue("revision") instanceof Number expectedRevision) ||
        expectedRevision.doubleValue() != revision) {
      throw new HttpException(409, "Workspace settings changed in another window. Attach again to restore the latest saved view.");
    }
    if (next == null || !(next.getValue("version") instanceof Number stateVersion) ||
        stateVersion.doubleValue() != 1 || next.encode().length() > 1_000_000) {
      throw new HttpException(400, "Invalid workspace state");
    }
    state = next.copy();
    revision++;
    return new JsonObject().put("saved", true).put("workspaceVersion", version());
  }
}
