package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDirection;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;

import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;

/**
 * Explicitly approximate redistribution of original contacts onto an AGP fixed-bin grid.
 * Endpoints are modeled as independent and uniform within each original bin's actual
 * sequence interval. Shared display references never enter the calculation.
 */
public final class ApproximateContactProjection {
  public static final String METHOD = "uniform-independent-endpoints-v1";
  public record Chromosome(String name, long lengthBp, long firstBin, long binCount) {}
  public record Contribution(long targetBin, double fraction) {}
  public record BinProjection(long sourceLengthBp, long retainedLengthBp, List<Contribution> contributions) {
    public BinProjection { contributions = List.copyOf(contributions); }
    public double retainedFraction() { return retainedLengthBp / (double) sourceLengthBp; }
  }
  @FunctionalInterface public interface ContactConsumer { void accept(long row, long col, double value); }

  private record SourceSpan(long first, long end, ContigDescriptor source, long ordinal, boolean reversed) {}
  private record Component(long start, long end, long targetStart, long firstTargetBin, boolean reversed) {}
  private final long resolution;
  private final List<Chromosome> chromosomes;
  private final List<SourceSpan> sources;
  private final Map<String, List<Component>> components;
  private final Map<Long, BinProjection> cache = Collections.synchronizedMap(new LinkedHashMap<>(128, 0.75f, true) {
    @Override protected boolean removeEldestEntry(Map.Entry<Long, BinProjection> entry) { return size() > 4096; }
  });

  private ApproximateContactProjection(long resolution, List<Chromosome> chromosomes,
      List<SourceSpan> sources, Map<String, List<Component>> components) {
    this.resolution = resolution;
    this.chromosomes = List.copyOf(chromosomes);
    this.sources = List.copyOf(sources);
    this.components = Map.copyOf(components);
  }

  public long resolution() { return resolution; }
  public List<Chromosome> chromosomes() { return chromosomes; }
  public long targetBinCount() {
    final var last = chromosomes.getLast();
    return Math.addExact(last.firstBin, last.binCount);
  }

  /** Preparation reads immutable source metadata and never changes the active assembly. */
  public static ApproximateContactProjection prepare(ChunkedFile file,
      List<AGPProcessor.AGPFileRecord> records, long resolution) {
    AGPProcessor.validateLayout(records);
    final var order = file.getResolutionToIndex().get(resolution);
    if (resolution <= 0 || order == null || order == 0)
      throw new IllegalArgumentException("Approximate export requires a stored contact resolution");
    final var chroms = new LinkedHashMap<String, Chromosome>();
    final var sizes = new LinkedHashMap<String, Long>();
    for (var record : records) sizes.merge(record.getScaffoldName(), record.getInterScaffoldEndIncl(), Math::max);
    long cursor = 0;
    for (var entry : sizes.entrySet()) {
      final long count = Math.ceilDiv(entry.getValue(), resolution);
      chroms.put(entry.getKey(), new Chromosome(entry.getKey(), entry.getValue(), cursor, count));
      cursor = Math.addExact(cursor, count);
    }

    final var bySource = new HashMap<String, List<Component>>();
    for (var record : records) {
      if (!(record instanceof AGPProcessor.ContigAGPRecord component)) continue;
      if (component.getContigOrientation() != AGPProcessor.AGPContigOrientation.PLUS
        && component.getContigOrientation() != AGPProcessor.AGPContigOrientation.MINUS)
        throw new IllegalArgumentException("Approximate export requires an explicit + or - component orientation");
      final var named = file.resolveContigDescriptorByName(component.getContigName());
      final long start = component.getIntraContigStartBpIncl() - 1, end = component.getIntraContigEndBpIncl();
      final var original = file.getOriginalDescriptors().values().stream().filter(d ->
        d.getContigNameInSourceFASTA().equals(named.getContigNameInSourceFASTA())
          && start >= d.getOffsetInSourceFASTA() && end <= Math.addExact(d.getOffsetInSourceFASTA(), d.getLengthBp()))
        .findFirst().orElseThrow(() -> new IllegalArgumentException("AGP component is outside its original source sequence"));
      bySource.computeIfAbsent(original.getContigNameInSourceFASTA(), ignored -> new ArrayList<>()).add(
        new Component(start, end, component.getInterScaffoldStartIncl() - 1,
          chroms.get(component.getScaffoldName()).firstBin,
          component.getContigOrientation() == AGPProcessor.AGPContigOrientation.MINUS));
    }
    if (bySource.isEmpty()) throw new IllegalArgumentException("Approximate export requires sequence components");
    bySource.replaceAll((name, list) -> {
      list.sort(Comparator.comparingLong(Component::start));
      long previousEnd = -1;
      for (var component : list) {
        if (component.start < previousEnd)
          throw new IllegalArgumentException("AGP repeats a source interval: " + name);
        previousEnd = component.end;
      }
      return List.copyOf(list);
    });

    final long[] stripeOffsets;
    try (var reader = HDF5Factory.openForReading(file.getHdfFilePath().toFile())) {
      stripeOffsets = AgpCoolerLayout.readStripeOffsets(resolution, reader);
    }
    final var spans = new ArrayList<SourceSpan>();
    for (var source : file.getOriginalDescriptors().values()) {
      long ordinal = 0;
      for (var atu : source.getAtus().get(order)) {
        final long first = stripeOffsets[atu.getStripeDescriptor().stripeId()] + atu.getStartIndexInStripeIncl();
        spans.add(new SourceSpan(first, first + atu.getLength(), source, ordinal,
          atu.getDirection() == ATUDirection.REVERSED));
        ordinal = Math.addExact(ordinal, atu.getLength());
      }
    }
    spans.sort(Comparator.comparingLong(SourceSpan::first));
    long previousEnd = -1;
    for (var span : spans) {
      if (span.first < previousEnd) throw new IllegalArgumentException("Original source bins have multiple owners");
      previousEnd = span.end;
    }
    return new ApproximateContactProjection(resolution, new ArrayList<>(chroms.values()), spans, bySource);
  }

  /** Map a physical source-axis bin, using its actual length (including a short terminal bin). */
  public BinProjection projectBin(long sourceBin) {
    final var cached = cache.get(sourceBin);
    if (cached != null) return cached;
    final var projection = computeBin(sourceBin);
    cache.put(sourceBin, projection);
    return projection;
  }

  private BinProjection computeBin(long sourceBin) {
    int lo = 0, hi = sources.size();
    while (lo < hi) { int mid = (lo + hi) >>> 1; if (sources.get(mid).end <= sourceBin) lo = mid + 1; else hi = mid; }
    if (lo == sources.size() || sourceBin < sources.get(lo).first)
      throw new IllegalArgumentException("Contact bin has no original sequence owner: " + sourceBin);
    final var owner = sources.get(lo);
    final long ordinal = owner.ordinal + (owner.reversed ? owner.end - 1 - sourceBin : sourceBin - owner.first);
    final long sourceStart = Math.addExact(owner.source.getOffsetInSourceFASTA(), Math.multiplyExact(ordinal, resolution));
    final long sourceEnd = Math.min(Math.addExact(sourceStart, resolution),
      Math.addExact(owner.source.getOffsetInSourceFASTA(), owner.source.getLengthBp()));
    if (sourceEnd <= sourceStart) throw new IllegalArgumentException("Source bin has no sequence interval");
    final var candidates = components.getOrDefault(owner.source.getContigNameInSourceFASTA(), List.of());
    lo = 0; hi = candidates.size();
    while (lo < hi) { int mid = (lo + hi) >>> 1; if (candidates.get(mid).end <= sourceStart) lo = mid + 1; else hi = mid; }
    final var covered = new TreeMap<Long, Long>();
    long retained = 0;
    for (int i = lo; i < candidates.size() && candidates.get(i).start < sourceEnd; i++) {
      if (Thread.currentThread().isInterrupted()) throw new CancellationException();
      final var component = candidates.get(i);
      final long a = Math.max(sourceStart, component.start), b = Math.min(sourceEnd, component.end);
      if (b <= a) continue;
      retained = Math.addExact(retained, b - a);
      final long targetStart = Math.addExact(component.targetStart,
        component.reversed ? component.end - b : a - component.start);
      final long targetEnd = Math.addExact(targetStart, b - a);
      for (long bin = targetStart / resolution; bin <= (targetEnd - 1) / resolution; bin++) {
        final long start = Math.max(targetStart, Math.multiplyExact(bin, resolution));
        final long end = Math.min(targetEnd, Math.multiplyExact(bin + 1, resolution));
        covered.merge(Math.addExact(component.firstTargetBin, bin), end - start, Math::addExact);
      }
    }
    final long width = sourceEnd - sourceStart;
    if (retained > width) throw new IllegalStateException("Projection repeats source sequence");
    return new BinProjection(width, retained, covered.entrySet().stream()
      .map(entry -> new Contribution(entry.getKey(), entry.getValue() / (double) width)).toList());
  }

  /**
   * Emit an unordered original contact exactly once. A source-diagonal contact has
   * two exchangeable endpoints: distinct output bins receive 2*w_i*w_j, not w_i*w_j.
   * Returns the expected retained value; omitted source intervals are not renormalized.
   */
  public double projectContact(long sourceRow, long sourceCol, double value,
      ContactConsumer consumer, BooleanSupplier cancelled) {
    if (!Double.isFinite(value)) throw new IllegalArgumentException("Approximate export requires finite source contacts");
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new CancellationException();
    final var row = projectBin(sourceRow);
    final var col = sourceRow == sourceCol ? row : projectBin(sourceCol);
    final boolean diagonal = sourceRow == sourceCol;
    final var rows = row.contributions; final var cols = col.contributions;
    for (int r = 0; r < rows.size(); r++) for (int c = diagonal ? r : 0; c < cols.size(); c++) {
      if ((c & 1023) == 0 && (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()))
        throw new CancellationException();
      final var a = rows.get(r); final var b = cols.get(c);
      final double fraction = a.fraction * b.fraction * (diagonal && r != c ? 2 : 1);
      final double projected = value * fraction;
      if (projected != 0) consumer.accept(Math.min(a.targetBin, b.targetBin), Math.max(a.targetBin, b.targetBin), projected);
    }
    return value * row.retainedFraction() * col.retainedFraction();
  }
}
