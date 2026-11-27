package ru.itmo.ctlab.hict.hict_server;

/** A map-dependent request arrived before a map was opened. */
public final class NoOpenFileException extends RuntimeException {
  public NoOpenFileException(String message) {
    super(message);
  }
}
