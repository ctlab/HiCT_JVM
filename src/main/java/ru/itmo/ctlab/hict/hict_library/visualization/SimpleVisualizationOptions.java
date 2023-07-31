package ru.itmo.ctlab.hict.hict_library.visualization;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter(AccessLevel.PUBLIC)
@EqualsAndHashCode
@ToString
public final class SimpleVisualizationOptions {
  private final double preLogBase;
  private final double postLogBase;
  private final boolean applyCoolerWeights;
  private final double lowerThreshold;
  private final double upperThreshold;
  private final double lnPreLogBase;
  private final double lnPostLogBase;

  public SimpleVisualizationOptions(
    double preLogBase,
    double postLogBase,
    boolean applyCoolerWeights,
    double lowerThreshold,
    double upperThreshold
  ) {
    this.preLogBase = preLogBase;
    this.postLogBase = postLogBase;
    this.applyCoolerWeights = applyCoolerWeights;
    this.lowerThreshold = lowerThreshold;
    this.upperThreshold = upperThreshold;
    this.lnPreLogBase = Math.log(Math.max(Double.MIN_NORMAL, this.preLogBase));
    this.lnPostLogBase = Math.log(Math.max(Double.MIN_NORMAL, this.postLogBase));
  }
}
