package ru.itmo.ctlab.hict.hict_library.domain;

public enum ContigDirection {
  REVERSED,
  FORWARD;

  public static ContigDirection inverse(final ContigDirection direction) {
    return switch (direction) {
      case FORWARD -> REVERSED;
      case REVERSED -> FORWARD;
    };
  }

  public ContigDirection inverse() {
    return ContigDirection.inverse(this);
  }
}
