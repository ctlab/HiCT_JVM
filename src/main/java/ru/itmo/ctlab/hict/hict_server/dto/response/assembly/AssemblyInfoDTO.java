package ru.itmo.ctlab.hict.hict_server.dto.response.assembly;


import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

import java.util.List;

public record AssemblyInfoDTO(@NotNull List<@NotNull ContigDescriptorDTO> contigDescriptors,
                              @NotNull List<@NotNull ScaffoldDescriptorDTO> scaffoldDescriptors) {

  public static @NotNull AssemblyInfoDTO generateFromChunkedFile(final @NotNull ChunkedFile chunkedFile) {
    final @NotNull var assemblyInfo = chunkedFile.getAssemblyInfo();
    return new AssemblyInfoDTO(
      assemblyInfo.contigs().stream().map(ctg -> ContigDescriptorDTO.fromEntity(ctg, chunkedFile)).toList(),
      assemblyInfo.scaffolds().stream().map(ScaffoldDescriptorDTO::fromEntity).toList()
    );
  }
}
