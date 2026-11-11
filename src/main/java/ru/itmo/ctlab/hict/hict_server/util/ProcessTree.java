package ru.itmo.ctlab.hict.hict_server.util;

/** Stop tracked subprocesses before their parent can exit and orphan them. */
public final class ProcessTree {
  private ProcessTree() { }

  public static void stop(Process process) {
    if (process == null) return;
    final var descendants = process.descendants().toList();
    for (var child : descendants.reversed()) child.destroyForcibly();
    process.destroyForcibly();
  }
}
