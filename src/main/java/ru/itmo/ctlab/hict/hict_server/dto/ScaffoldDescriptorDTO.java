package ru.itmo.ctlab.hict.hict_server.dto;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;

public record ScaffoldDescriptorDTO(
  long scaffoldId,
  String scaffoldName,
  long spacerLength,
  @Nullable ScaffoldDescriptor.ScaffoldBordersBP scaffoldBordersBP
) {

  public static @NotNull ScaffoldDescriptorDTO fromEntity(final @NotNull ScaffoldTree.ScaffoldTuple scaffoldTuple) {
    return new ScaffoldDescriptorDTO(
      scaffoldTuple.scaffoldDescriptor().scaffoldId(),
      scaffoldTuple.scaffoldDescriptor().scaffoldName(),
      scaffoldTuple.scaffoldDescriptor().spacerLength(),
      scaffoldTuple.scaffoldBordersBP()
    );
  }

}
