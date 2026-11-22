package ru.itmo.ctlab.hict.hict_library.assembly;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import java.util.*;

/** Frozen sequence windows attached to actual source matrix bins, even after AGP splits. */
public record AssemblyAnalysisPlan(AssemblySequenceSnapshot sequence, List<Level> levels) {
  // Keep this layout in sync with overlay_store.py. Stored map levels use their own ATUs.
  private static final int SEQUENCE_STRIPE_WIDTH = 256;
  // Per-stripe Java descriptors, ATUs and contig mappings are resident; unit weight rows are shared.
  private static final long ESTIMATED_BYTES_PER_STRIPE = 1024;

  public record Region(long start, long end) {
    public Region { if (start < 0 || end <= start) throw new IllegalArgumentException("Choose a nonempty sequence interval"); }
  }
  public record Window(int stripe, int bin, long start, long end, long ownedEnd) {
    public Window { if (stripe < 0 || bin < 0 || start < 0 || end <= start || ownedEnd <= start || ownedEnd > end) throw new IllegalArgumentException("Invalid analysis window"); }
  }
  public record Level(long resolution, List<Window> rows, List<Window> columns) {
    public Level { rows = List.copyOf(rows); columns = List.copyOf(columns); }
  }
  public record ResourceEstimate(long windows, long cells, long outputBytes) { }
  public AssemblyAnalysisPlan { levels = List.copyOf(levels); }

  public static List<Long> pyramidLevels(List<Long> requested, long[] primaryLevels) {
    var anchors = new TreeSet<>(requested);
    long finest = anchors.first();
    if (finest <= 0) throw new IllegalArgumentException("Resolution must be positive");
    for (long bp : primaryLevels) if (bp > finest) anchors.add(bp);
    var levels = new ArrayList<Long>();
    long current = finest;
    for (long anchor : anchors) {
      while (current <= (anchor - 1) / 4) {
        current *= 4;
        levels.add(current);
      }
      levels.add(anchor);
      current = anchor;
    }
    return List.copyOf(levels);
  }

  /**
   * Upper bound before materializing windows. Each nonzero stores three int64s.
   * Power-of-two capacities can retain less than four times the final entries
   * across successive rewrites, with a 64-entry minimum per physical block.
   */
  public static ResourceEstimate estimate(ChunkedFile map, List<Long> resolutions, Region rows, Region columns) {
    return estimate(map, resolutions, rows, columns, (bp, estimate) -> {});
  }

  public static ResourceEstimate estimate(ChunkedFile map, List<Long> resolutions, Region rows, Region columns,
      java.util.function.BiConsumer<Long, ResourceEstimate> levelEstimate) {
    final var tuples = map.getAssemblyInfo().contigs();
    final long length = tuples.stream().mapToLong(t -> t.descriptor().getLengthBp()).reduce(0, Math::addExact);
    if (rows.end > length || columns.end > length) throw new IllegalArgumentException("Selection outside active assembly");
    long windows = 0;
    long cells = 0;
    long outputBytes = 0;
    long syntheticStripes = 0;
    for (long bp : resolutions) {
      final var order = map.getResolutionToIndex().get(bp);
      if (bp <= 0) throw new IllegalArgumentException("Resolution must be positive: " + bp);
      final long stripes;
      if (order == null) {
        final long bins = map.getOriginalDescriptors().values().stream()
          .mapToLong(d -> Math.ceilDiv(d.getLengthBp(), bp)).reduce(0, Math::addExact);
        stripes = Math.ceilDiv(bins, SEQUENCE_STRIPE_WIDTH);
        syntheticStripes = Math.addExact(syntheticStripes,stripes);
        if (Math.multiplyExact(syntheticStripes,ESTIMATED_BYTES_PER_STRIPE) > Runtime.getRuntime().maxMemory()/4)
          throw new IllegalArgumentException("The fine-resolution overlay metadata exceeds the available heap. Select fewer fine levels.");
        outputBytes = Math.addExact(outputBytes, Math.multiplyExact(stripes, 4096L));
      } else {
        stripes = map.getStripeCount()[order];
      }
      final long rowBins = estimateAxis(tuples, rows, order, bp);
      final long colBins = rows.equals(columns) ? rowBins : estimateAxis(tuples, columns, order, bp);
      final long levelCells = Math.multiplyExact(rowBins, colBins);
      final long rowStripes = Math.min(stripes, estimateStripes(tuples, rows, order, bp));
      final long colStripes = rows.equals(columns) ? rowStripes : Math.min(stripes, estimateStripes(tuples, columns, order, bp));
      final long maxBlocks = Math.multiplyExact(rowStripes, colStripes);
      final long bytes = Math.addExact(Math.multiplyExact(levelCells, 4L * 3 * Long.BYTES),
        Math.multiplyExact(maxBlocks, (64L * 3 + 2) * Long.BYTES));
      windows = Math.addExact(windows, Math.addExact(rowBins, colBins));
      cells = Math.addExact(cells, levelCells);
      outputBytes = Math.addExact(outputBytes, bytes);
      levelEstimate.accept(bp, new ResourceEstimate(windows, cells, outputBytes));
    }
    return new ResourceEstimate(windows, cells, outputBytes);
  }

  private static long estimateStripes(List<ContigTree.ContigTuple> tuples, Region region, Integer order, long bp) {
    long result = 0, start = 0;
    for (var tuple : tuples) {
      var descriptor = tuple.descriptor();
      long end = Math.addExact(start, descriptor.getLengthBp());
      long span = Math.min(end,region.end) - Math.max(start,region.start);
      if (span > 0) {
        long bins = Math.ceilDiv(span,bp) + 1;
        // One extra stripe accounts for an unaligned first bin, including reversed fragments.
        long stripes = order == null ? Math.ceilDiv(bins, SEQUENCE_STRIPE_WIDTH) + 1 : descriptor.getAtus().get(order).size();
        result = Math.addExact(result, Math.min(bins,stripes));
      }
      if (end >= region.end) break;
      start = end;
    }
    return result;
  }

  private static long estimateAxis(List<ContigTree.ContigTuple> tuples, Region region, Integer order, long bp) {
    long result = 0;
    long start = 0;
    for (var tuple : tuples) {
      final var descriptor = tuple.descriptor();
      final long end = Math.addExact(start, descriptor.getLengthBp());
      final long span = Math.min(end, region.end) - Math.max(start, region.start);
      if (span > 0) {
        final long bound = Math.addExact((span - 1) / bp, 2);
        result = Math.addExact(result, order == null ? bound : Math.min(bound, descriptor.getLengthBinsAtResolution()[order]));
      }
      if (end >= region.end) break;
      start = end;
    }
    return result;
  }

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
        if (resolution <= 0) throw new IllegalArgumentException("Resolution must be positive: " + resolution);
        final List<Window> r, c;
        if (order == null) {
          var sourceBins = sequenceBins(map.getOriginalDescriptors().values(), resolution);
          r = sequenceAxis(tuples, sourceBins, resolution, sequence.length(), rows);
          c = rows.equals(columns) ? r : sequenceAxis(tuples, sourceBins, resolution, sequence.length(), columns);
        } else {
          var owners = owners(map.getOriginalDescriptors().values(), order, resolution);
          r = axis(tuples, owners, order, resolution, sequence.length(), rows);
          c = rows.equals(columns) ? r : axis(tuples, owners, order, resolution, sequence.length(), columns);
        }
        if (r.isEmpty() || c.isEmpty()) throw new IllegalArgumentException("Selection contains no owned contact bins at " + resolution + " bp; choose a larger region or finer resolution");
        levels.add(new Level(resolution, r, c));
      }
      return new AssemblyAnalysisPlan(sequence, levels);
    } finally { lock.unlock(); }
  }

  private record SourceBins(ContigDescriptor contig, long firstBin) { }

  private static Map<String, List<SourceBins>> sequenceBins(Collection<ContigDescriptor> originals, long resolution) {
    var bySourceName = new HashMap<String, List<SourceBins>>();
    long firstBin = 0;
    for (var contig : originals.stream().sorted(Comparator.comparingInt(ContigDescriptor::getContigId)).toList()) {
      bySourceName.computeIfAbsent(contig.getContigNameInSourceFASTA(), ignored -> new ArrayList<>())
        .add(new SourceBins(contig, firstBin));
      firstBin = Math.addExact(firstBin, Math.ceilDiv(contig.getLengthBp(), resolution));
    }
    return bySourceName;
  }

  private static SourceBins findSourceBins(Map<String, List<SourceBins>> sources, ContigDescriptor active) {
    List<SourceBins> candidates = sources.getOrDefault(active.getContigNameInSourceFASTA(), List.of());
    SourceBins owner = null;
    final long start = active.getOffsetInSourceFASTA();
    final long end = Math.addExact(start, active.getLengthBp());
    for (var candidate : candidates) {
      var contig = candidate.contig();
      long sourceStart = contig.getOffsetInSourceFASTA();
      if (start < sourceStart || end - sourceStart > contig.getLengthBp()) continue;
      if (contig.getContigId() == active.getContigId()) return candidate;
      if (owner != null) throw new IllegalArgumentException("Ambiguous original contig for " + active.getContigNameInSourceFASTA());
      owner = candidate;
    }
    if (owner == null) throw new IllegalArgumentException("Missing original contig for " + active.getContigNameInSourceFASTA());
    return owner;
  }

  private static List<Window> sequenceAxis(List<ContigTree.ContigTuple> tuples, Map<String, List<SourceBins>> sources,
                                           long resolution, long sequenceLength, Region region) {
    var windows = new LinkedHashMap<Long, Window>();
    long position = 0;
    for (var tuple : tuples) {
      if (position >= region.end()) break;
      var active = tuple.descriptor();
      long next = Math.addExact(position, active.getLengthBp());
      if (next > region.start()) {
        var source = findSourceBins(sources, active);
        long offset = active.getOffsetInSourceFASTA() - source.contig().getOffsetInSourceFASTA();
        long left = Math.max(0, region.start() - position);
        long right = Math.min(active.getLengthBp(), region.end() - position);
        boolean reversed = tuple.direction() == ContigDirection.REVERSED;
        long first = (offset + (reversed ? active.getLengthBp() - right : left)) / resolution;
        long last = (offset + (reversed ? active.getLengthBp() - left : right) - 1) / resolution;
        for (long index = first; index <= last; index++) {
          long ordinal = reversed ? last - (index - first) : index;
          long nominalStart = ordinal * resolution;
          long startInSource = Math.max(offset, nominalStart);
          long endInSource = nominalStart + Math.min(resolution, offset + active.getLengthBp() - nominalStart);
          long start = position + (reversed ? offset + active.getLengthBp() - endInSource : startInSource - offset);
          long sourceBin = source.firstBin() + ordinal;
          var window = new Window(Math.toIntExact(sourceBin / SEQUENCE_STRIPE_WIDTH), (int) (sourceBin % SEQUENCE_STRIPE_WIDTH),
            start, start + Math.min(resolution, sequenceLength - start), start + endInSource - startInSource);
          // An unaligned cut can expose the same physical bin twice. Keep the
          // longest surviving piece once, breaking equal lengths by assembly order.
          windows.merge(sourceBin, window, (firstWindow, secondWindow) ->
            firstWindow.ownedEnd() - firstWindow.start() >= secondWindow.ownedEnd() - secondWindow.start()
              ? firstWindow : secondWindow);
        }
      }
      position = next;
    }
    return windows.values().stream().sorted(Comparator.comparingLong(Window::start)).toList();
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
