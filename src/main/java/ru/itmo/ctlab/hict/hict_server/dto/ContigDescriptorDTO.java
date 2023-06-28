package ru.itmo.ctlab.hict.hict_server.dto;

import java.util.Map;

public record ContigDescriptorDTO(
  int contigId,
  String contigName,
  int contigDirection,
  long contigLengthBp,
  Map<Long, Long> contigLengthBins,
  Map<Long, Integer> contigPresenceAtResolution
) {
}
