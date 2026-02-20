package ru.itmo.ctlab.hict.hict_server.dto.response.conversion;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record ConversionJobDTO(
  @NotNull String jobId,
  @NotNull String status,
  @NotNull String sourceFilename,
  @NotNull String outputFilename,
  @NotNull List<@NotNull String> logs,
  @NotNull String error
) {
}
