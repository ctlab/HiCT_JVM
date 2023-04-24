package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.NonNull;

public record ScaffoldDescriptor(
        long scaffoldId,
        @NonNull String scaffoldName,
        long spacerLength
) {
}
