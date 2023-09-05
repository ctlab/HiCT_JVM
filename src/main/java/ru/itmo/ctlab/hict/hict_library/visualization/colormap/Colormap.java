package ru.itmo.ctlab.hict.hict_library.visualization.colormap;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.awt.*;

@RequiredArgsConstructor
@Getter(AccessLevel.PUBLIC)
public abstract class Colormap {
  protected final int bitDepth;

  public abstract @NotNull Color mapSignal(double value);

  public abstract @NotNull Color mapSignal(long signalValue);
}
