package ru.itmo.ctlab.hict.hict_server.dto;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public record ContigDescriptorDTO(
  int contigId,
  String contigName,
  int contigDirection,
  long contigLengthBp,
  Map<Long, Long> contigLengthBins,
  Map<Long, Integer> contigPresenceAtResolution
) {

  public static @NotNull @NonNull ContigDescriptorDTO fromEntity(final @NotNull @NonNull ContigTree.ContigTuple ctg, final @NotNull @NonNull ChunkedFile chunkedFile) {
    final var resolutions = chunkedFile.getResolutions();
    return new ContigDescriptorDTO(
      ctg.descriptor().getContigId(),
      ctg.descriptor().getContigName(),
      ctg.direction().ordinal(),
      ctg.descriptor().getLengthBp(),
      IntStream.range(0, resolutions.length).boxed().collect(Collectors.toMap(resIdx -> resolutions[resIdx], resIdx -> ctg.descriptor().getLengthBinsAtResolution()[resIdx])),
      IntStream.range(0, resolutions.length).boxed().collect(Collectors.toMap(resIdx -> resolutions[resIdx], resIdx -> ctg.descriptor().getPresenceAtResolution().get(resIdx).ordinal()))
    );
  }

}
