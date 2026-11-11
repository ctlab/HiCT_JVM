package ru.itmo.ctlab.hict.hict_library.assembly;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Immutable, gap-free sequence view of the active assembly. All coordinates are
 * zero-based, half-open base-pair intervals; matrix bins never enter this API.
 * Retains no open map, mutable tree, nucleotide strings, or HDF5 resources.
 */
public final class AssemblySequenceSnapshot {
  public record Fragment(String source, long start, long end, boolean reversed) {
    public Fragment {
      Objects.requireNonNull(source, "source");
      if (source.isBlank() || start < 0 || end <= start)
        throw new IllegalArgumentException("Invalid source fragment: " + source + ":" + start + ".." + end);
    }
    public long length() { return end - start; }
    public Fragment slice(long from, long to) {
      if (from < 0 || to <= from || to > length()) throw new IllegalArgumentException("Invalid fragment slice");
      return new Fragment(source, reversed ? end - to : start + from,
        reversed ? end - from : start + to, reversed);
    }
  }

  private record PositionedFragment(Fragment fragment, long position) { }
  private final List<Fragment> fragments;
  private final long[] offsets;
  private final Map<String, List<PositionedFragment>> sourceIndex;
  private final String fingerprint;

  public AssemblySequenceSnapshot(List<Fragment> fragments) {
    this.fragments = List.copyOf(fragments);
    offsets = new long[fragments.size() + 1];
    var index = new HashMap<String, List<PositionedFragment>>();
    for (int i = 0; i < this.fragments.size(); i++) {
      var fragment = this.fragments.get(i);
      offsets[i + 1] = Math.addExact(offsets[i], fragment.length());
      index.computeIfAbsent(fragment.source(), ignored -> new ArrayList<>())
        .add(new PositionedFragment(fragment, offsets[i]));
    }
    index.values().forEach(items -> {
      items.sort(Comparator.comparingLong(item -> item.fragment.start()));
      long previousEnd = -1;
      for (var item : items) {
        if (item.fragment.start() < previousEnd)
          throw new IllegalArgumentException("Overlapping source intervals: " + item.fragment.source());
        previousEnd = item.fragment.end();
      }
    });
    var frozenIndex = new HashMap<String, List<PositionedFragment>>();
    index.forEach((key, value) -> frozenIndex.put(key, List.copyOf(value)));
    sourceIndex = Map.copyOf(frozenIndex);
    fingerprint = digest();
  }

  /** Call while holding the application's assembly scheduler/read lock. */
  public static AssemblySequenceSnapshot capture(ChunkedFile map) {
    return new AssemblySequenceSnapshot(map.getAssemblyInfo().contigs().stream().map(tuple -> {
      var descriptor = tuple.descriptor();
      return new Fragment(descriptor.getContigNameInSourceFASTA(), descriptor.getOffsetInSourceFASTA(),
        Math.addExact(descriptor.getOffsetInSourceFASTA(), descriptor.getLengthBp()),
        tuple.direction() == ContigDirection.REVERSED);
    }).toList());
  }

  public List<Fragment> fragments() { return fragments; }
  public long length() { return offsets[offsets.length - 1]; }
  public String fingerprint() { return fingerprint; }

  /** Selections are strict: never silently expand, swap, or clamp user coordinates. */
  public AssemblySequenceSnapshot slice(long start, long end) {
    if (start < 0 || end < start || end > length()) throw new IllegalArgumentException("Selection outside active assembly");
    if (start == end) return new AssemblySequenceSnapshot(List.of());
    var pieces = new ArrayList<Fragment>();
    int i = Arrays.binarySearch(offsets, start);
    if (i < 0) i = -i - 2;
    while (i < fragments.size() && offsets[i] < end) {
      pieces.add(fragments.get(i).slice(Math.max(start, offsets[i]) - offsets[i],
        Math.min(end, offsets[i + 1]) - offsets[i]));
      i++;
    }
    return new AssemblySequenceSnapshot(pieces);
  }

  public record ProjectedPiece(long assemblyStart, long assemblyEnd,
                               long featureStart, long featureEnd, boolean reversed) { }
  public enum FeatureStatus { INTACT, FRAGMENTED, REARRANGED, PARTIAL, ABSENT }
  public record Projection(List<ProjectedPiece> pieces, FeatureStatus status, boolean sequencePreserved) {
    public Projection { pieces = List.copyOf(pieces); }
  }

  /**
   * Project an originally matched path into this assembly. A whole inversion
   * preserves a match (on the opposite strand); changed joins do not. The index
   * avoids a full assembly traversal for every match.
   */
  public Projection project(AssemblySequenceSnapshot feature) {
    if (feature.length() == 0) throw new IllegalArgumentException("Empty feature");
    var pieces = new ArrayList<ProjectedPiece>();
    long featureOffset = 0, covered = 0;
    for (var anchor : feature.fragments) {
      var candidates = sourceIndex.getOrDefault(anchor.source(), List.of());
      int low = 0, high = candidates.size();
      while (low < high) {
        int mid = (low + high) >>> 1;
        if (candidates.get(mid).fragment.end() <= anchor.start()) low = mid + 1;
        else high = mid;
      }
      for (int i = low; i < candidates.size(); i++) {
        var item = candidates.get(i);
        var current = item.fragment;
        if (current.start() >= anchor.end()) break;
        long a = Math.max(anchor.start(), current.start()), b = Math.min(anchor.end(), current.end());
        if (a >= b) continue;
        pieces.add(new ProjectedPiece(
          item.position + (current.reversed() ? current.end() - b : a - current.start()),
          item.position + (current.reversed() ? current.end() - a : b - current.start()),
          featureOffset + (anchor.reversed() ? anchor.end() - b : a - anchor.start()),
          featureOffset + (anchor.reversed() ? anchor.end() - a : b - anchor.start()),
          anchor.reversed() != current.reversed()));
        covered = Math.addExact(covered, b - a);
      }
      featureOffset += anchor.length();
    }
    pieces.sort(Comparator.comparingLong(ProjectedPiece::featureStart));
    if (pieces.isEmpty()) return new Projection(pieces, FeatureStatus.ABSENT, false);
    if (covered != feature.length()) return new Projection(pieces, FeatureStatus.PARTIAL, false);
    boolean preserved = true;
    for (int i = 1; i < pieces.size(); i++) {
      var left = pieces.get(i - 1); var right = pieces.get(i);
      preserved &= left.reversed() == right.reversed() && left.featureEnd() == right.featureStart()
        && (left.reversed() ? left.assemblyStart() == right.assemblyEnd() : left.assemblyEnd() == right.assemblyStart());
    }
    return new Projection(pieces, !preserved ? FeatureStatus.REARRANGED
      : pieces.size() > feature.fragments.size() ? FeatureStatus.FRAGMENTED : FeatureStatus.INTACT, preserved);
  }

  private String digest() {
    try {
      var digest = MessageDigest.getInstance("SHA-256");
      digest.update("HiCT-sequence-snapshot-v1\0".getBytes(StandardCharsets.UTF_8));
      for (var fragment : fragments) {
        var name = fragment.source().getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(4).putInt(name.length).array());
        digest.update(name);
        digest.update(ByteBuffer.allocate(17).putLong(fragment.start()).putLong(fragment.end())
          .put((byte) (fragment.reversed() ? 1 : 0)).array());
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
  }
}
