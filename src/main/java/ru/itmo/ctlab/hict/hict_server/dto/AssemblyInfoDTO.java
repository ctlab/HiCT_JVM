package ru.itmo.ctlab.hict.hict_server.dto;


import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

import java.util.List;

public record AssemblyInfoDTO(@NotNull @NonNull List<@NotNull @NonNull ContigDescriptorDTO> contigDescriptors,
                              @NotNull @NonNull List<@NotNull @NonNull ScaffoldDescriptorDTO> scaffoldDescriptors) {

  public static @NotNull @NonNull AssemblyInfoDTO generateFromChunkedFile(final @NotNull @NonNull ChunkedFile chunkedFile) {
    final @NotNull @NonNull var assemblyInfo = chunkedFile.getAssemblyInfo();
    return new AssemblyInfoDTO(
      assemblyInfo.contigs().stream().map(ctg -> ContigDescriptorDTO.fromEntity(ctg, chunkedFile)).toList(),
      assemblyInfo.scaffolds().stream().map(ScaffoldDescriptorDTO::fromEntity).toList()
    );
  }
}
