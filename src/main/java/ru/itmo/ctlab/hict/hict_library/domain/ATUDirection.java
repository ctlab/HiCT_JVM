package ru.itmo.ctlab.hict.hict_library.domain;

public enum ATUDirection {
  REVERSED, FORWARD;


  public static ATUDirection inverse(final ATUDirection direction) {
    return switch (direction) {
      case FORWARD -> REVERSED;
      case REVERSED -> FORWARD;
    };
  }

  public ATUDirection inverse() {
    return switch (this) {
      case FORWARD -> REVERSED;
      case REVERSED -> FORWARD;
    };
  }
}
