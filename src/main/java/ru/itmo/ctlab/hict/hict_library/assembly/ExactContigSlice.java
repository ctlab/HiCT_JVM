package ru.itmo.ctlab.hict.hict_library.assembly;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.*;

import java.util.ArrayList;
import java.util.List;

/** A source-forward sequence slice retaining every intersecting original bin.
 * Adjacent slices intentionally share the ATU bin crossed by their boundary.
 * Contact values are referenced, never redistributed or rewritten here. */
public final class ExactContigSlice {
  private ExactContigSlice() { }

  public static ContigDescriptor slice(ChunkedFile file, ContigDescriptor parent,
                                       long from, long to, int id) {
    if (from < 0 || to <= from || to > parent.getLengthBp())
      throw new IllegalArgumentException("Exact slice must be a nonempty interval inside its parent");
    final long sourceStart = Math.addExact(parent.getOffsetInSourceFASTA(), from);
    final long sourceEnd = Math.addExact(parent.getOffsetInSourceFASTA(), to);
    // The current descriptor storage uses int offsets. Fail before any mutation
    // rather than wrap a source coordinate; long-offset migration is separate.
    final int offset = Math.toIntExact(sourceStart);
    final var lengths = new ArrayList<Long>();
    final var visibility = new ArrayList<ContigHideType>();
    final var atus = new ArrayList<List<ATUDescriptor>>();
    for (int order = 1; order < file.getResolutions().length; order++) {
      final long resolution = file.getResolutions()[order];
      final long count = parent.getLengthBinsAtResolution()[order];
      final long origin = file.firstOwnedBinOffset(parent, order);
      final long first = Math.max(0, Math.min(count, Math.floorDiv(Math.subtractExact(from, origin), resolution)));
      final long last = Math.max(first, Math.min(count,
        Math.addExact(Math.floorDiv(Math.subtractExact(to - 1, origin), resolution), 1)));
      final var pieces = new ArrayList<ATUDescriptor>();
      long cursor = 0;
      for (var atu : parent.getAtus().get(order)) {
        final long next = Math.addExact(cursor, atu.getLength());
        if (next > first && cursor < last) {
          final int start = Math.toIntExact(Math.max(first, cursor) - cursor);
          final int end = Math.toIntExact(Math.min(last, next) - cursor);
          final boolean forward = atu.getDirection() == ATUDirection.FORWARD;
          pieces.add(new ATUDescriptor(atu.getStripeDescriptor(),
            forward ? atu.getStartIndexInStripeIncl() + start : atu.getEndIndexInStripeExcl() - end,
            forward ? atu.getStartIndexInStripeIncl() + end : atu.getEndIndexInStripeExcl() - start,
            atu.getDirection()));
        }
        cursor = next;
        if (cursor >= last) break;
      }
      if (pieces.stream().mapToLong(ATUDescriptor::getLength).sum() != last - first)
        throw new IllegalArgumentException("Parent ATUs do not cover exact slice at resolution " + resolution);
      lengths.add(last - first);
      // Even a one-base fragment has a real contact-bin footprint. Preserve
      // explicit hiding, but do not hide it merely because it is shorter than a bin.
      visibility.add(last > first ? parent.getPresenceAtResolution().get(order) : ContigHideType.HIDDEN);
      atus.add(List.copyOf(pieces));
    }
    return new ContigDescriptor(id, parent.getContigNameInSourceFASTA() + "_" + sourceStart + "_" + sourceEnd,
      to - from, lengths, visibility, atus, parent.getContigNameInSourceFASTA(), offset, true);
  }
}
