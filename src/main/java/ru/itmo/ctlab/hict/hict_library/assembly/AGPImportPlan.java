package ru.itmo.ctlab.hict.hict_library.assembly;

import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import java.util.*;
import java.util.function.Function;
import static ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor.*;

/** Prepared without touching live trees or reading/writing contact blocks. */
public record AGPImportPlan(List<ContigTree.ContigTuple> selected,
                            List<ContigTree.ContigTuple> omitted,
                            ImportReport report) {
  public enum MissingComponentsPolicy { REQUIRE_CHOICE, APPEND_UNPLACED, EXCLUDE }
  public record ImportReport(int splitSourceContigs, int partialComponents, int omittedPieces,
                             long omittedBp, int unknownOrientations, String missingComponentsPolicy,
                             String boundaryPolicy, List<String> warnings) {
    public boolean requiresChoice() {
      return omittedPieces > 0 && MissingComponentsPolicy.REQUIRE_CHOICE.name().equals(missingComponentsPolicy);
    }
  }
  private record Interval(long start, long end) {}

  public static AGPImportPlan prepare(List<AGPFileRecord> records,
      Collection<ContigDescriptor> originals, Function<String, ContigDescriptor> resolver,
      long[] resolutions, int firstNewId, MissingComponentsPolicy policy) {
    validateLayout(records);
    var selected = new ArrayList<ContigTree.ContigTuple>();
    var omitted = new ArrayList<ContigTree.ContigTuple>();
    var covered = new HashMap<String, List<Interval>>();
    var splitSources = new HashSet<String>();
    int nextId = firstNewId, partials = 0, unknown = 0;
    for (var record : records) {
      if (!(record instanceof ContigAGPRecord c)) continue;
      var source = resolver.apply(c.getContigName());
      long start = c.getIntraContigStartBpIncl() - 1, end = c.getIntraContigEndBpIncl();
      long offset = source.getOffsetInSourceFASTA();
      if (start < offset || end > offset + source.getLengthBp()) {
        throw new IllegalArgumentException("AGP component '" + c.getContigName() + "' interval "
          + (start + 1) + "-" + end + " is outside the map's source interval. The assembly was not changed.");
      }
      covered.computeIfAbsent(source.getContigNameInSourceFASTA(), k -> new ArrayList<>()).add(new Interval(start, end));
      boolean partial = start != offset || end != offset + source.getLengthBp();
      var descriptor = partial ? slice(source, start - offset, end - offset, nextId++, resolutions) : source;
      if (partial) { partials++; splitSources.add(source.getContigNameInSourceFASTA()); }
      if (c.getContigOrientation() == AGPContigOrientation.UNKNOWN || c.getContigOrientation() == AGPContigOrientation.IRRELEVANT) unknown++;
      selected.add(new ContigTree.ContigTuple(descriptor,
        c.getContigOrientation() == AGPContigOrientation.MINUS ? ContigDirection.REVERSED : ContigDirection.FORWARD));
    }
    if (selected.isEmpty()) throw new IllegalArgumentException("AGP contains no sequence components.");
    for (var entry : covered.entrySet()) {
      entry.getValue().sort(Comparator.comparingLong(Interval::start));
      long previousEnd = -1;
      for (var interval : entry.getValue()) {
        if (interval.start < previousEnd) throw new IllegalArgumentException("Overlapping or duplicated AGP intervals for '"
          + entry.getKey() + "'. The assembly was not changed.");
        previousEnd = interval.end;
      }
    }
    long omittedBp = 0;
    for (var source : originals.stream().sorted(Comparator.comparingInt(ContigDescriptor::getContigId)).toList()) {
      long start = source.getOffsetInSourceFASTA(), limit = start + source.getLengthBp(), cursor = start;
      for (var interval : covered.getOrDefault(source.getContigNameInSourceFASTA(), List.of())) {
        if (interval.end <= cursor || interval.start >= limit) continue;
        long end = Math.min(interval.start, limit);
        if (end > cursor) {
          omitted.add(new ContigTree.ContigTuple(slice(source, cursor - start, end - start, nextId++, resolutions), ContigDirection.FORWARD));
          omittedBp += end - cursor;
        }
        cursor = Math.max(cursor, Math.min(interval.end, limit));
      }
      if (cursor < limit) {
        omitted.add(new ContigTree.ContigTuple(cursor == start ? source : slice(source, cursor - start, limit - start, nextId++, resolutions), ContigDirection.FORWARD));
        omittedBp += limit - cursor;
      }
    }
    var warnings = new ArrayList<String>();
    if (partials > 0) warnings.add("AGP import split " + splitSources.size() + " source contigs into " + partials
      + " partial components. Sequence coordinates are exact. Each crossing contact bin stays with the piece containing its start in the original contig; matrix boundaries may differ by up to one bin at each resolution. No contact bin was removed by splitting.");
    if (unknown > 0) warnings.add(unknown + " components have unknown orientation; displayed and exported to FASTA in forward orientation.");
    if (omittedBp > 0) warnings.add(omitted.size() + " omitted sequence pieces (" + omittedBp + " bp): " + switch (policy) {
      case APPEND_UNPLACED -> "appended as unplaced; included as unscaffolded FASTA records.";
      case EXCLUDE -> "excluded from the assembly and FASTA export.";
      case REQUIRE_CHOICE -> "choose whether to append them as unplaced or exclude them. The assembly has not changed.";
    });
    return new AGPImportPlan(List.copyOf(selected), List.copyOf(omitted),
      new ImportReport(splitSources.size(), partials, omitted.size(), omittedBp, unknown, policy.name(),
        "SOURCE_BIN_START_OWNS_BIN", List.copyOf(warnings)));
  }

  /** Exact sequence slice; a bin belongs to the interval containing its original start. */
  static ContigDescriptor slice(ContigDescriptor source, long from, long to, int id, long[] resolutions) {
    if (from < 0 || to <= from || to > source.getLengthBp()) throw new IllegalArgumentException("Invalid component slice");
    if (from == 0 && to == source.getLengthBp()) return source;
    var lengths = new ArrayList<Long>();
    var atus = new ArrayList<List<ATUDescriptor>>();
    for (int order = 1; order < resolutions.length; order++) {
      long resolution = resolutions[order], count = source.getLengthBinsAtResolution()[order];
      long origin = source.getOffsetInSourceFASTA();
      long sourceFirstBin = ceilDiv(origin, resolution);
      long first = Math.min(count, ceilDiv(origin + from, resolution) - sourceFirstBin);
      long last = to == source.getLengthBp() ? count : Math.min(count, ceilDiv(origin + to, resolution) - sourceFirstBin);
      lengths.add(last - first);
      var pieces = new ArrayList<ATUDescriptor>();
      long position = 0;
      for (var atu : source.getAtus().get(order)) {
        long next = position + atu.getLength();
        int left = Math.toIntExact(Math.max(first, position) - position);
        int right = Math.toIntExact(Math.min(last, next) - position);
        if (right > left) {
          boolean forward = atu.getDirection() == ATUDirection.FORWARD;
          pieces.add(new ATUDescriptor(atu.getStripeDescriptor(),
            forward ? atu.getStartIndexInStripeIncl() + left : atu.getEndIndexInStripeExcl() - right,
            forward ? atu.getStartIndexInStripeIncl() + right : atu.getEndIndexInStripeExcl() - left,
            atu.getDirection()));
        }
        position = next;
        if (position >= last) break;
      }
      if (pieces.stream().mapToLong(ATUDescriptor::getLength).sum() != last - first)
        throw new IllegalArgumentException("Source ATUs do not cover component bins at resolution " + resolution);
      atus.add(pieces);
    }
    long absoluteStart = source.getOffsetInSourceFASTA() + from;
    return new ContigDescriptor(id, source.getContigNameInSourceFASTA() + ":" + (absoluteStart + 1) + "-" + (source.getOffsetInSourceFASTA() + to),
      to - from, lengths, source.getPresenceAtResolution().subList(1, resolutions.length), atus,
      source.getContigNameInSourceFASTA(), Math.toIntExact(absoluteStart));
  }
  private static long ceilDiv(long value, long divisor) { return value / divisor + (value % divisor == 0 ? 0 : 1); }
}
