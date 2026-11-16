package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;

import java.util.*;

/** Immutable physical-bin identities for a displayed axis, without expanding every bin. */
public record SourceBinProvenance(long axisLength, long referencedBins, long uniqueSourceBins,
                                  List<Run> runs) {
  /** Query-relative half-open range; bin index advances by step (+1 or -1). */
  public record Run(long offset, int length, int stripeId, int firstBinInStripe, int step) {}

  public SourceBinProvenance {
    runs = List.copyOf(runs);
  }

  public static SourceBinProvenance fromATUs(List<ATUDescriptor> atus, long axisLength, long firstOffset) {
    if (axisLength < 0 || firstOffset < 0 || firstOffset > axisLength)
      throw new IllegalArgumentException("Invalid provenance axis extent");
    final var runs = new ArrayList<Run>(atus.size());
    final var intervals = new HashMap<Integer, List<int[]>>();
    long offset = firstOffset;
    for (var atu : atus) {
      final int start = atu.getStartIndexInStripeIncl(), end = atu.getEndIndexInStripeExcl();
      final int length = atu.getLength(), stripe = atu.getStripeDescriptor().stripeId();
      if (length <= 0 || start < 0 || end > atu.getStripeDescriptor().stripeLengthBins())
        throw new IllegalArgumentException("Invalid physical bin range");
      final boolean reversed = atu.getDirection() == ATUDirection.REVERSED;
      runs.add(new Run(offset, length, stripe, reversed ? end - 1 : start, reversed ? -1 : 1));
      offset = Math.addExact(offset, length);
      intervals.computeIfAbsent(stripe, ignored -> new ArrayList<>()).add(new int[]{start, end});
    }
    if (offset > axisLength) throw new IllegalArgumentException("Physical bins exceed the provenance axis");
    long unique = 0;
    for (var ranges : intervals.values()) {
      ranges.sort(Comparator.comparingInt(range -> range[0]));
      int end = -1;
      for (var range : ranges) {
        unique += Math.max(0, range[1] - Math.max(end, range[0]));
        end = Math.max(end, range[1]);
      }
    }
    return new SourceBinProvenance(axisLength, offset - firstOffset, unique, runs);
  }
}
