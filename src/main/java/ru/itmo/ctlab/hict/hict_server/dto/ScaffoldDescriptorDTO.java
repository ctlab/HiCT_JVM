package ru.itmo.ctlab.hict.hict_server.dto;

import lombok.NonNull;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;

public record ScaffoldDescriptorDTO(
  long scaffoldId,
  @NonNull String scaffoldName,
  long spacerLength,
  ScaffoldDescriptor.ScaffoldBordersBP scaffoldBordersBP
) {
}
