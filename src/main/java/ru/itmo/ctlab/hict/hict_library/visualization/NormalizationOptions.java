package ru.itmo.ctlab.hict.hict_library.visualization;

public record NormalizationOptions(
  double preLogBase,
  double postLogBase,
  boolean applyCoolerWeights,
  double lowerThreshold,
  double upperThreshold
) {

}
