package ru.itmo.ctlab.hict.hict_server.dto.response.conversion;

import org.jetbrains.annotations.NotNull;

public record ConversionSubmitResponseDTO(
  @NotNull String status,
  @NotNull String jobId
) {
}
