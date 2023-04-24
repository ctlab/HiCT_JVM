package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.NonNull;

public enum ContigDirection {
    FORWARD,
    REVERSED;

    public static ContigDirection inverse(final @NonNull ContigDirection direction) {
        return switch (direction) {
            case FORWARD -> REVERSED;
            case REVERSED -> FORWARD;
        };
    }

    public ContigDirection inverse() {
        return switch (this) {
            case FORWARD -> REVERSED;
            case REVERSED -> FORWARD;
        };
    }
}
