package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.Initializers;
import ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** Shared AGP coordinate mapping for internal and hictk-assisted Cooler writers. */
public final class AgpCoolerLayout {
  private AgpCoolerLayout() {}

  public static @NotNull HictToMcoolConverter.CoolerAssemblyLayout buildAgpCoolerAssemblyLayout(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull List<AGPProcessor.AGPFileRecord> agpRecords,
    final @NotNull List<Long> selectedResolutions,
    final @NotNull Consumer<String> logger
  ) {
    final var scaffoldSizes = new LinkedHashMap<String, Long>();
    for (final var record : agpRecords) {
      scaffoldSizes.merge(record.getScaffoldName(), record.getInterScaffoldEndIncl(), Math::max);
    }
    if (scaffoldSizes.isEmpty()) {
      throw new IllegalStateException("AGP export requested, but AGP did not define any scaffold/object records");
    }

    final var chroms = new ArrayList<HictToMcoolConverter.CoolerChrom>(scaffoldSizes.size());
    final var chromIndexByName = new LinkedHashMap<String, Integer>();
    int chromIndex = 0;
    for (final var entry : scaffoldSizes.entrySet()) {
      final long lengthBp = entry.getValue();
      if (lengthBp <= 0L) {
        throw new IllegalStateException("AGP scaffold " + entry.getKey() + " has non-positive length " + lengthBp);
      }
      chromIndexByName.put(entry.getKey(), chromIndex);
      chroms.add(new HictToMcoolConverter.CoolerChrom(entry.getKey(), lengthBp, ContigDirection.FORWARD.ordinal(), chromIndex));
      chromIndex++;
    }

    final var resolutionLayouts = new ArrayList<HictToMcoolConverter.ResolutionLayout>(selectedResolutions.size());
    for (final var resolution : selectedResolutions) {
      final Integer resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
      if (resolutionOrder == null) {
        throw new IllegalStateException("Resolution " + resolution + " is not present in the input HiCT file");
      }
      final var chromOffsets = new long[chroms.size() + 1];
      long binCursor = 0L;
      for (int i = 0; i < chroms.size(); i++) {
        chromOffsets[i] = binCursor;
        binCursor += ceilDiv(chroms.get(i).lengthBp(), resolution);
      }
      chromOffsets[chroms.size()] = binCursor;

      final var spans = new ArrayList<HictToMcoolConverter.ContigBinSpan>();
      long skippedRecords = 0L;
      for (final var record : agpRecords) {
        if (!(record instanceof AGPProcessor.ContigAGPRecord contigRecord)) {
          continue;
        }
        final Integer scaffoldIndex = chromIndexByName.get(record.getScaffoldName());
        if (scaffoldIndex == null) {
          throw new IllegalStateException("Internal error: AGP scaffold " + record.getScaffoldName() + " has no Cooler chrom index");
        }
        final var descriptor = chunkedFile.resolveContigDescriptorByName(contigRecord.getContigName());
        validateFullLengthAgpComponent(contigRecord, descriptor.getLengthBp());
        final var atus = orientedAtus(
          descriptor.getAtus().get(resolutionOrder),
          directionFromAgp(contigRecord.getContigOrientation())
        );
        final long binCount = atus.stream().mapToLong(ATUDescriptor::getLength).sum();
        if (binCount <= 0L) {
          skippedRecords++;
          continue;
        }
        final long targetStart = chromOffsets[scaffoldIndex] + Math.floorDiv(record.getInterScaffoldStartIncl() - 1L, resolution);
        final long targetEnd = targetStart + binCount;
        if (targetStart < chromOffsets[scaffoldIndex] || targetEnd > chromOffsets[scaffoldIndex + 1]) {
          throw new IllegalStateException(
            "AGP component " + contigRecord.getContigName() + " in scaffold " + record.getScaffoldName() +
              " maps outside Cooler bins at resolution " + resolution +
              ": target=[" + targetStart + "," + targetEnd + "), chromBins=[" +
              chromOffsets[scaffoldIndex] + "," + chromOffsets[scaffoldIndex + 1] + ")"
          );
        }
        spans.add(new HictToMcoolConverter.ContigBinSpan(
          scaffoldIndex,
          chroms.get(scaffoldIndex),
          targetStart,
          targetEnd,
          atus
        ));
      }
      if (spans.isEmpty()) {
        throw new IllegalStateException("AGP export requested, but no AGP components have visible bins at resolution " + resolution);
      }
      if (skippedRecords > 0L) {
        logger.accept("AGP Cooler layout skipped " + skippedRecords + " component(s) hidden at resolution " + resolution);
      }
      spans.sort(Comparator
        .comparingInt(HictToMcoolConverter.ContigBinSpan::chromIndex)
        .thenComparingLong(HictToMcoolConverter.ContigBinSpan::globalBinStart));
      resolutionLayouts.add(new HictToMcoolConverter.ResolutionLayout(
        resolution,
        List.copyOf(chroms),
        List.copyOf(spans),
        chromOffsets,
        binCursor
      ));
    }
    logger.accept(
      "Built AGP Cooler layout: scaffolds=" + chroms.size() +
        ", components=" + agpRecords.stream().filter(record -> record instanceof AGPProcessor.ContigAGPRecord).count() +
        ", resolutions=" + selectedResolutions
    );
    return new HictToMcoolConverter.CoolerAssemblyLayout(List.copyOf(chroms), List.copyOf(resolutionLayouts));
  }

  public static @NotNull Map<Long, List<HictToMcoolConverter.MappingSegment>> buildAgpMappingSegmentsByResolution(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull Path inputPath,
    final @NotNull List<AGPProcessor.AGPFileRecord> agpRecords,
    final @NotNull List<Long> selectedResolutions
  ) throws IOException {
    final var mappings = new LinkedHashMap<Long, List<HictToMcoolConverter.MappingSegment>>();
    try (final var reader = HDF5Factory.openForReading(inputPath.toFile())) {
      for (final var resolution : selectedResolutions) {
        final Integer resolutionOrder = chunkedFile.getResolutionToIndex().get(resolution);
        if (resolutionOrder == null) {
          throw new IllegalStateException("Resolution " + resolution + " is not present in the input HiCT file");
        }
        final var stripeOffsets = readStripeOffsets(resolution, reader);
        final var chromOffsets = buildAgpChromOffsets(agpRecords, resolution);

        final var segments = new ArrayList<HictToMcoolConverter.MappingSegment>();
        for (final var record : agpRecords) {
          if (!(record instanceof AGPProcessor.ContigAGPRecord contigRecord)) {
            continue;
          }
          final var descriptor = chunkedFile.resolveContigDescriptorByName(contigRecord.getContigName());
          validateFullLengthAgpComponent(contigRecord, descriptor.getLengthBp());
          final var atus = orientedAtus(
            descriptor.getAtus().get(resolutionOrder),
            directionFromAgp(contigRecord.getContigOrientation())
          );
          long targetCursor = chromOffsets.get(record.getScaffoldName())
            + Math.floorDiv(record.getInterScaffoldStartIncl() - 1L, resolution);
          for (final ATUDescriptor atu : atus) {
            final var stripe = atu.getStripeDescriptor();
            final long sourceStart = stripeOffsets[stripe.stripeId()] + atu.getStartIndexInStripeIncl();
            final long sourceEnd = stripeOffsets[stripe.stripeId()] + atu.getEndIndexInStripeExcl();
            final long targetStart = targetCursor;
            final long targetEnd = targetStart + atu.getLength();
            segments.add(new HictToMcoolConverter.MappingSegment(
              sourceStart,
              sourceEnd,
              targetStart,
              targetEnd,
              atu.getDirection()
            ));
            targetCursor = targetEnd;
          }
        }
        if (segments.isEmpty()) {
          throw new IllegalStateException("AGP export requested, but no AGP components have visible bins at resolution " + resolution);
        }
        segments.sort(Comparator.comparingLong(HictToMcoolConverter.MappingSegment::sourceStart));
        mappings.put(resolution, List.copyOf(segments));
      }
    }
    return mappings;
  }

  private static @NotNull LinkedHashMap<String, Long> buildAgpChromOffsets(
    final @NotNull List<AGPProcessor.AGPFileRecord> agpRecords,
    final long resolution
  ) {
    final var sizes = new LinkedHashMap<String, Long>();
    for (final var record : agpRecords) {
      sizes.merge(record.getScaffoldName(), record.getInterScaffoldEndIncl(), Math::max);
    }
    final var offsets = new LinkedHashMap<String, Long>();
    long cursor = 0L;
    for (final var entry : sizes.entrySet()) {
      offsets.put(entry.getKey(), cursor);
      cursor += ceilDiv(entry.getValue(), resolution);
    }
    return offsets;
  }

  public static long @NotNull [] readStripeOffsets(final long resolution,
                                                    final @NotNull ch.systemsx.cisd.hdf5.IHDF5Reader reader) {
    final var stripes = Initializers.readStripeDescriptors(resolution, reader);
    final long[] stripeOffsets = new long[stripes.size()];
    long stripeCursor = 0L;
    for (int i = 0; i < stripes.size(); i++) {
      stripeOffsets[i] = stripeCursor;
      stripeCursor += stripes.get(i).stripeLengthBins();
    }
    return stripeOffsets;
  }

  private static void validateFullLengthAgpComponent(final @NotNull AGPProcessor.ContigAGPRecord record,
                                                     final long contigLengthBp) {
    final long componentLength = record.getIntraContigEndBpIncl() - record.getIntraContigStartBpIncl() + 1L;
    final long scaffoldComponentLength = record.getInterScaffoldEndIncl() - record.getInterScaffoldStartIncl() + 1L;
    if (componentLength != scaffoldComponentLength) {
      throw new IllegalStateException(
        "AGP component " + record.getContigName() + " has inconsistent source/scaffold lengths: source=" +
          componentLength + ", scaffold=" + scaffoldComponentLength
      );
    }
    if (record.getIntraContigStartBpIncl() != 1L || componentLength != contigLengthBp) {
      throw new IllegalStateException(
        "AGP component splitting is not supported for Cooler export yet: " + record.getContigName() +
          " uses " + record.getIntraContigStartBpIncl() + "-" + record.getIntraContigEndBpIncl() +
          " of source length " + contigLengthBp
      );
    }
  }

  private static @NotNull ContigDirection directionFromAgp(final @NotNull AGPProcessor.AGPContigOrientation orientation) {
    return switch (orientation) {
      case PLUS, UNKNOWN, IRRELEVANT -> ContigDirection.FORWARD;
      case MINUS -> ContigDirection.REVERSED;
    };
  }

  public static @NotNull List<ATUDescriptor> orientedAtus(final @NotNull List<ATUDescriptor> sourceAtus,
                                                           final @NotNull ContigDirection direction) {
    if (direction == ContigDirection.FORWARD) {
      return List.copyOf(sourceAtus);
    }
    final var atus = new ArrayList<ATUDescriptor>(sourceAtus.size());
    for (int i = sourceAtus.size() - 1; i >= 0; i--) {
      atus.add(sourceAtus.get(i).reversed());
    }
    return List.copyOf(atus);
  }

  private static long ceilDiv(final long value, final long divisor) {
    if (divisor <= 0L) {
      throw new IllegalArgumentException("Divisor must be positive");
    }
    return value <= 0L ? 0L : 1L + Math.floorDiv(value - 1L, divisor);
  }
}
