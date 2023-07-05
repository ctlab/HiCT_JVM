package ru.itmo.ctlab.hict.hict_server.dto.response.fileop;

import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO;

import java.util.List;

public record OpenFileResponseDTO(
  @NotNull String status,
  @NotNull String dtype,
  @NotNull List<@NotNull Long> resolutions,
  @NotNull List<@NotNull Double> pixelResolutions,
  int tileSize,
  @NotNull AssemblyInfoDTO assemblyInfo,
  @NotNull List<@NotNull Integer> matrixSizesBins) {
}
