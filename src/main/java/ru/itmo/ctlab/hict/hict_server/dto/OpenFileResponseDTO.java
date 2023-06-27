package ru.itmo.ctlab.hict.hict_server.dto;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public record OpenFileResponseDTO(
  @NotNull @NonNull String status,
  @NotNull @NonNull String dtype,
  @NotNull @NonNull List<@NotNull @NonNull Long> resolutions,
  @NotNull @NonNull List<@NotNull @NonNull Double> pixelResolutions,
  int tileSize,
  @NotNull @NonNull AssemblyInfoDTO assemblyInfo,
  @NotNull @NonNull List<@NotNull @NonNull Integer> matrixSizesBins) {
}
