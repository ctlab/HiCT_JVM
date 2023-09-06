package ru.itmo.ctlab.hict.hict_library.visualization;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.Colormap;

@Getter(AccessLevel.PUBLIC)
@EqualsAndHashCode
@ToString
public final class SimpleVisualizationOptions {
  private final double preLogBase;
  private final double postLogBase;
  private final boolean applyCoolerWeights;
  private final double lnPreLogBase;
  private final double lnPostLogBase;
  private final boolean resolutionScaling;
  private final boolean resolutionLinearScaling;
  private final @NotNull Colormap colormap;


  public SimpleVisualizationOptions(
    double preLogBase,
    double postLogBase,
    boolean applyCoolerWeights,
    final boolean resolutionScaling,
    final boolean resolutionLinearScaling,
    @NotNull Colormap colormap) {
    this.preLogBase = preLogBase;
    this.postLogBase = postLogBase;
    this.applyCoolerWeights = applyCoolerWeights;
    this.colormap = colormap;
    this.lnPreLogBase = Math.log(Math.max(Double.MIN_NORMAL, this.preLogBase));
    this.lnPostLogBase = Math.log(Math.max(Double.MIN_NORMAL, this.postLogBase));
    this.resolutionScaling = resolutionScaling;
    this.resolutionLinearScaling = resolutionLinearScaling;
  }
}
