package ru.itmo.ctlab.hict.hict_server.dto.response.conversion;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record ConversionJobDTO(
  @NotNull String jobId,
  @NotNull String status,
  @NotNull String sourceFilename,
  @NotNull String outputFilename,
  @NotNull String direction,
  @NotNull String currentStage,
  @NotNull String currentStageLabel,
  @NotNull String stageDetail,
  double stageProgress,
  double overallProgress,
  double resolutionProgress,
  long currentResolution,
  long elapsedMillis,
  long etaMillis,
  long resolutionElapsedMillis,
  long resolutionEtaMillis,
  long inputSizeBytes,
  long outputSizeBytes,
  @NotNull String toolchainSource,
  @NotNull String toolchainSummary,
  @NotNull List<@NotNull String> toolchainNotices,
  @NotNull List<@NotNull String> toolchainCitations,
  @NotNull List<@NotNull String> logs,
  @NotNull String error
) {
}
