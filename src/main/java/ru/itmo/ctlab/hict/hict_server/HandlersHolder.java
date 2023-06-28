package ru.itmo.ctlab.hict.hict_server;

import io.vertx.ext.web.Router;
import org.jetbrains.annotations.NotNull;

public abstract class HandlersHolder {
  public abstract void addHandlersToRouter(final @NotNull Router router);
}
