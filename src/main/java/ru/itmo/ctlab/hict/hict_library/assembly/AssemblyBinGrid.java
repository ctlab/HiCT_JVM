package ru.itmo.ctlab.hict.hict_library.assembly;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** Projects visible assembly pixels through their actual source intervals. */
public final class AssemblyBinGrid {
  public record Bin(String source, long start, long end, long pixel, boolean reversed) {
    public long length() {
      return end - start;
    }
  }

  public record Overlap(long pixel, long bp, long binLength) { }

  private record Component(String source, long start, long end, long firstBin,
                           long pixel, long bins, boolean reversed) {
    long endPixel() {
      return pixel + bins;
    }

    long pixelOf(long ordinal) {
      return pixel + (reversed ? bins - 1 - ordinal : ordinal);
    }
  }

  private final long resolution;
  private final List<Component> components;
  private final Map<String, List<Component>> bySource;

  public AssemblyBinGrid(ChunkedFile file, ResolutionDescriptor descriptor) {
    final int order = descriptor.getResolutionOrderInArray();
    resolution = file.getResolutions()[order];
    if (resolution <= 0) throw new IllegalArgumentException("A matrix resolution is required");

    final var components = new ArrayList<Component>();
    final var bySource = new HashMap<String, List<Component>>();
    long pixel = 0;
    for (var tuple : file.getAssemblyInfo().contigs()) {
      final var contig = tuple.descriptor();
      final long bins = contig.getLengthInUnits(QueryLengthUnit.PIXELS, descriptor);
      if (bins == 0) continue;
      final long start = contig.getOffsetInSourceFASTA();
      final var component = new Component(contig.getContigNameInSourceFASTA(), start,
        Math.addExact(start, contig.getLengthBp()), file.firstOwnedBinOffset(contig, order),
        pixel, bins, tuple.direction() == ContigDirection.REVERSED);
      pixel = Math.addExact(pixel, bins);
      components.add(component);
      bySource.computeIfAbsent(component.source, ignored -> new ArrayList<>()).add(component);
    }
    this.components = List.copyOf(components);
    bySource.replaceAll((name, pieces) -> {
      pieces.sort(Comparator.comparingLong(Component::start));
      return List.copyOf(pieces);
    });
    this.bySource = Map.copyOf(bySource);
  }

  public Bin binAt(long pixel) {
    int lo = 0;
    int hi = components.size();
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (components.get(mid).endPixel() <= pixel) lo = mid + 1;
      else hi = mid;
    }
    if (lo == components.size()) return null;
    final var component = components.get(lo);
    if (pixel < component.pixel) return null;
    final long delta = pixel - component.pixel;
    final long ordinal = component.reversed ? component.bins - 1 - delta : delta;
    final long start = binStart(component, ordinal);
    final long end = Math.min(component.end, Math.addExact(start, resolution));
    final long clippedStart = Math.max(component.start, start);
    return clippedStart < end ? new Bin(component.source, clippedStart, end, pixel, component.reversed) : null;
  }

  /** Subdivide the screen footprint of a primary bin without changing its assembly boundaries. */
  Bin sampleAt(long pixel, int factor) {
    final var bin = binAt(Math.floorDiv(pixel, factor));
    if (bin == null || factor == 1) return bin;
    final int displayIndex = Math.floorMod(pixel, factor);
    final int ordinal = bin.reversed ? factor - 1 - displayIndex : displayIndex;
    final long length = bin.length();
    final long offset = subdivision(length, ordinal, factor);
    final long next = subdivision(length, ordinal + 1, factor);
    final long start = Math.addExact(bin.start, Math.min(length - 1, offset));
    final long end = Math.addExact(bin.start, Math.max(Math.min(length, next), Math.min(length, offset + 1)));
    return new Bin(bin.source, start, end, pixel, bin.reversed);
  }

  private static long subdivision(long length, int index, int factor) {
    return (length / factor) * index + (length % factor) * index / factor;
  }

  public void overlaps(Bin interval, Consumer<Overlap> consumer) {
    final var candidates = bySource.get(interval.source);
    if (candidates == null) return;
    for (var component : candidates) {
      if (component.start >= interval.end) break;
      final long start = Math.max(interval.start, component.start);
      final long end = Math.min(interval.end, component.end);
      if (end <= start) continue;
      final long first = Math.max(0L, Math.floorDiv(start - component.start - component.firstBin, resolution));
      final long last = Math.min(component.bins - 1, Math.floorDiv(end - 1 - component.start - component.firstBin, resolution));
      for (long ordinal = first; ordinal <= last; ordinal++) {
        final long binStart = binStart(component, ordinal);
        final long binEnd = Math.min(component.end, Math.addExact(binStart, resolution));
        final long overlap = Math.min(end, binEnd) - Math.max(start, binStart);
        if (overlap > 0) {
          consumer.accept(new Overlap(component.pixelOf(ordinal), overlap,
            binEnd - Math.max(component.start, binStart)));
        }
      }
    }
  }

  private long binStart(Component component, long ordinal) {
    return Math.addExact(component.start, Math.addExact(component.firstBin, Math.multiplyExact(ordinal, resolution)));
  }
}
