package ru.itmo.ctlab.hict.hict_library.visualization.colormap;

import org.jetbrains.annotations.NotNull;

import java.awt.*;

public abstract class DoubleColormap extends Colormap {


  public DoubleColormap(int bitDepth) {
    super(bitDepth);
  }

  @Override
  public @NotNull Color mapSignal(long signalValue) {
    return this.mapSignal((double) signalValue);
  }
}
