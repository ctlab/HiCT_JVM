package ru.itmo.ctlab.hict.hict_library.visualization;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record TileWithWeights(double @NotNull [][] values, double @Nullable [] rowWeights,
                              double @Nullable [] columnWeights) {
}
