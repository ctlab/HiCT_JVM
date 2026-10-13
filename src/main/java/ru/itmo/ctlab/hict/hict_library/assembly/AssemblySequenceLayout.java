package ru.itmo.ctlab.hict.hict_library.assembly;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import java.util.*;

/** Sequence-only padding. Matrix/selection coordinates and physical contact bins never change. */
public final class AssemblySequenceLayout {
  public record Gap(long position, long length) {
    public Gap { if (position < 0 || length <= 0) throw new IllegalArgumentException("Invalid sequence gap"); }
  }
  private final AssemblySequenceSnapshot sequence;
  private final List<Gap> gaps;
  private final long[] positions, prefix;
  public AssemblySequenceLayout(AssemblySequenceSnapshot sequence, List<Gap> gaps) {
    this.sequence = sequence;
    var merged = new TreeMap<Long, Long>();
    for (var gap : gaps) {
      if (gap.position() > sequence.length()) throw new IllegalArgumentException("Gap outside sequence");
      merged.merge(gap.position(), gap.length(), Math::addExact);
    }
    this.gaps = merged.entrySet().stream().map(e -> new Gap(e.getKey(), e.getValue())).toList();
    positions = this.gaps.stream().mapToLong(Gap::position).toArray(); prefix = new long[positions.length + 1];
    for (int i = 0; i < positions.length; i++) prefix[i+1] = Math.addExact(prefix[i], this.gaps.get(i).length());
    Math.addExact(sequence.length(), prefix[positions.length]);
  }
  public static AssemblySequenceLayout capture(ChunkedFile map, long separator) {
    map.getContigTree().getRootLock().readLock().lock();
    map.getScaffoldTree().getRootLock().readLock().lock();
    try { return captureLocked(map, separator); }
    finally { map.getScaffoldTree().getRootLock().readLock().unlock(); map.getContigTree().getRootLock().readLock().unlock(); }
  }
  private static AssemblySequenceLayout captureLocked(ChunkedFile map, long separator) {
    AssemblyExportSettings.requireGapLength(separator);
    var gaps = new ArrayList<Gap>(); String object = null; long position = 0;
    for (var record : map.getAgpProcessor().getAssemblyAGPRecords()) {
      if (object != null && !object.equals(record.getScaffoldName()) && separator > 0) gaps.add(new Gap(position, separator));
      object = record.getScaffoldName();
      if (record instanceof AGPProcessor.GapAGPRecord gap) gaps.add(new Gap(position, gap.getGapLength()));
      else if (record instanceof AGPProcessor.ContigAGPRecord c) position = Math.addExact(position, c.getIntraContigEndBpIncl() - c.getIntraContigStartBpIncl() + 1);
    }
    var sequence = AssemblySequenceSnapshot.capture(map);
    if (sequence.length() != position) throw new IllegalStateException("AGP and active sequence lengths differ");
    return new AssemblySequenceLayout(sequence, gaps);
  }
  public AssemblySequenceSnapshot sequence() { return sequence; }
  public List<Gap> gaps() { return gaps; }
  /** Versioned signature includes sequence joins and padding, not names or contact bins. */
  public String fingerprint(long separator) {
    try {
      var hash = java.security.MessageDigest.getInstance("SHA-256");
      hash.update(sequence.fingerprint().getBytes(java.nio.charset.StandardCharsets.US_ASCII));
      for (var gap : gaps) hash.update(java.nio.ByteBuffer.allocate(16).putLong(gap.position()).putLong(gap.length()).array());
      return "layout-v1:" + separator + ":" + java.util.HexFormat.of().formatHex(hash.digest());
    } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
  }
  public long length() { return sequence.length() + prefix[positions.length]; }
  public long padded(long bp, boolean afterGap) {
    if (bp < 0 || bp > sequence.length()) throw new IllegalArgumentException("Coordinate outside assembly");
    int i = Arrays.binarySearch(positions, bp);
    i = i < 0 ? -i-1 : afterGap ? i+1 : i;
    return Math.addExact(bp, prefix[i]);
  }
  /** Bounds of the contiguous, gap-free sequence segment owning this base. */
  public boolean hasGapBetween(long start, long end) {
    int i = Arrays.binarySearch(positions,start); i = i < 0 ? -i-1 : i+1;
    return i < positions.length && positions[i] < end;
  }
  public long segmentStart(long bp) {
    int i = Arrays.binarySearch(positions, bp); if (i < 0) i = -i-2;
    return i < 0 ? 0 : positions[i];
  }
  public long segmentEnd(long bp) {
    int i = Arrays.binarySearch(positions, bp); i = i < 0 ? -i-1 : i+1;
    return i >= positions.length ? sequence.length() : positions[i];
  }
  public AssemblySequenceLayout slice(long start, long end) {
    return new AssemblySequenceLayout(sequence.slice(start,end), gaps.stream()
      .filter(g -> g.position() > start && g.position() < end).map(g -> new Gap(g.position()-start,g.length())).toList());
  }
}
