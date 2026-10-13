package ru.itmo.ctlab.hict.hict_library.assembly;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import java.util.*;

/** Frozen sequence windows attached to actual source matrix bins, even after AGP splits. */
public record AssemblyAnalysisPlan(AssemblySequenceSnapshot sequence, List<Level> levels) {
  public record Region(long start, long end) {
    public Region { if (start < 0 || end <= start) throw new IllegalArgumentException("Choose a nonempty sequence interval"); }
  }
  public record Window(int stripe, int bin, long start, long end, long ownedEnd) {
    public Window { if (stripe < 0 || bin < 0 || start < 0 || end <= start || ownedEnd <= start || ownedEnd > end) throw new IllegalArgumentException("Invalid analysis window"); }
  }
  public record Level(long resolution, List<Window> rows, List<Window> columns) {
    public Level { rows = List.copyOf(rows); columns = List.copyOf(columns); }
  }
  public AssemblyAnalysisPlan { levels = List.copyOf(levels); }

  /** Caller holds the assembly scheduler/read lock; jobs retain no mutable map. */
  public static AssemblyAnalysisPlan capture(ChunkedFile map, List<Long> resolutions, Region rows, Region columns) {
    var lock = map.getContigTree().getRootLock().readLock();
    lock.lock();
    try {
      var tuples = map.getAssemblyInfo().contigs();
      var sequence = new AssemblySequenceSnapshot(tuples.stream().map(t -> {
        var d = t.descriptor();
        return new AssemblySequenceSnapshot.Fragment(d.getContigNameInSourceFASTA(), d.getOffsetInSourceFASTA(),
          d.getOffsetInSourceFASTA() + d.getLengthBp(), t.direction() == ContigDirection.REVERSED);
      }).toList());
      if (rows.end > sequence.length() || columns.end > sequence.length()) throw new IllegalArgumentException("Selection outside active assembly");
      if (resolutions.isEmpty()) throw new IllegalArgumentException("Choose at least one resolution");
      var levels = new ArrayList<Level>();
      for (long resolution : new TreeSet<>(resolutions)) {
        Integer order = map.getResolutionToIndex().get(resolution);
        if (resolution <= 0 || order == null) throw new IllegalArgumentException("Resolution is absent from the map: " + resolution);
        var owners = owners(map.getOriginalDescriptors().values(), order, resolution);
        var r = axis(tuples, owners, order, resolution, sequence.length(), rows);
        var c = rows.equals(columns) ? r : axis(tuples, owners, order, resolution, sequence.length(), columns);
        if (r.isEmpty() || c.isEmpty()) throw new IllegalArgumentException("Selection contains no owned contact bins at " + resolution + " bp; choose a larger region or finer resolution");
        levels.add(new Level(resolution, r, c));
      }
      return new AssemblyAnalysisPlan(sequence, levels);
    } finally { lock.unlock(); }
  }

  private record Owner(int first, int end, ContigDescriptor source, long ordinal, boolean reversed) {
    long ordinalOf(int bin) { return ordinal + (reversed ? end - 1 - bin : bin - first); }
  }
  private static Map<Integer, List<Owner>> owners(Collection<ContigDescriptor> originals, int order, long resolution) {
    var result = new HashMap<Integer, List<Owner>>();
    for (var d : originals) {
      long ordinal = 0;
      for (var atu : d.getAtus().get(order)) {
        result.computeIfAbsent(atu.getStripeDescriptor().stripeId(), ignored -> new ArrayList<>())
          .add(new Owner(atu.getStartIndexInStripeIncl(), atu.getEndIndexInStripeExcl(), d, ordinal, atu.getDirection() == ATUDirection.REVERSED));
        ordinal += atu.getLength();
      }
    }
    for (var list : result.values()) {
      list.sort(Comparator.comparingInt(Owner::first));
      int end = -1;
      for (var o : list) {
        if (o.first < end) throw new IllegalArgumentException("Source metadata assigns a contact bin more than once");
        end = o.end;
      }
    }
    return result;
  }
  private static Owner ownerAt(List<Owner> owners, int bin) {
    if (owners == null) throw new IllegalArgumentException("Missing original stripe metadata");
    int lo = 0, hi = owners.size();
    while (lo < hi) { int mid = (lo + hi) >>> 1; if (owners.get(mid).end <= bin) lo = mid + 1; else hi = mid; }
    if (lo == owners.size() || owners.get(lo).first > bin) throw new IllegalArgumentException("Missing original contact-bin ownership");
    return owners.get(lo);
  }
  private static List<Window> axis(List<ContigTree.ContigTuple> tuples, Map<Integer, List<Owner>> owners,
                                    int order, long resolution, long sequenceLength, Region region) {
    var result = new ArrayList<Window>();
    long position = 0;
    for (var tuple : tuples) {
      var d = tuple.descriptor(); long next = Math.addExact(position, d.getLengthBp());
      if (position >= region.end) break;
      if (next > region.start) {
        boolean reversed = tuple.direction() == ContigDirection.REVERSED;
        var atus = d.getAtus().get(order);
        var traversal = reversed ? atus.reversed() : atus;
        for (var atu : traversal) {
          boolean reverseBins = reversed != (atu.getDirection() == ATUDirection.REVERSED);
          int stripe = atu.getStripeDescriptor().stripeId();
          for (int i = 0; i < atu.getLength(); i++) {
            int bin = reverseBins ? atu.getEndIndexInStripeExcl() - i - 1 : atu.getStartIndexInStripeIncl() + i;
            var owner = ownerAt(owners.get(stripe), bin); var source = owner.source;
            if (!source.getContigNameInSourceFASTA().equals(d.getContigNameInSourceFASTA()))
              throw new IllegalArgumentException("Contact bin and FASTA source disagree");
            long sourceStart = source.getOffsetInSourceFASTA() + owner.ordinalOf(bin) * resolution;
            long sourceEnd = Math.min(sourceStart + resolution, source.getOffsetInSourceFASTA() + source.getLengthBp());
            long a = Math.max(sourceStart, d.getOffsetInSourceFASTA());
            long b = Math.min(sourceEnd, d.getOffsetInSourceFASTA() + d.getLengthBp());
            if (b <= a) throw new IllegalArgumentException("Contact bin has no sequence in its active component");
            long start = position + (reversed ? d.getOffsetInSourceFASTA() + d.getLengthBp() - b : a - d.getOffsetInSourceFASTA());
            long end = start + b - a;
            if (start < region.end && end > region.start)
              result.add(new Window(stripe, bin, start, Math.min(sequenceLength, start + resolution), end));
          }
        }
      }
      position = next;
    }
    return List.copyOf(result);
  }
}
