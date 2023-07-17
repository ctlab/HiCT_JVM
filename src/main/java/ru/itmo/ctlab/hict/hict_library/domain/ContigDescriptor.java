package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

@Getter
@Slf4j
@EqualsAndHashCode
public class ContigDescriptor {
  private final int contigId;
  private final @NotNull
  String contigName;

  private final long lengthBp;
  private final long[] lengthBinsAtResolution;
  private final @NotNull
  List<@NotNull ContigHideType> presenceAtResolution;
  private final @NotNull List<@NotNull List<@NotNull ATUDescriptor>> atus;
  private final @NotNull List<long @NotNull []> atuPrefixSumLengthBins;
  private final @NotNull String contigNameInSourceFASTA;
  private final int offsetInSourceFASTA;

  public ContigDescriptor(
    final int contigId,
    final @NotNull String contigName,
    final long lengthBp,
    final @NotNull List<@NotNull Long> lengthBinsAtResolution,
    final @NotNull List<@NotNull ContigHideType> presenceAtResolution,
    final @NotNull List<@NotNull List<@NotNull ATUDescriptor>> atus,
    final @Nullable String contigNameInSourceFASTA,
    final int offsetInSourceFASTA
  ) {
    this.contigId = contigId;
    this.contigName = contigName;
    this.lengthBp = lengthBp;

    assert (lengthBinsAtResolution.get(0) != lengthBp) : "Length bp should not be added at zero position of lengthBinsAtResolutions for constructor";
    this.lengthBinsAtResolution = Stream.concat(Stream.of(this.lengthBp), lengthBinsAtResolution.stream()).mapToLong(lng -> lng).toArray();
    if (contigNameInSourceFASTA != null) {
      this.contigNameInSourceFASTA = contigNameInSourceFASTA;
      this.offsetInSourceFASTA = offsetInSourceFASTA;
    } else {
      this.contigNameInSourceFASTA = contigName;
      this.offsetInSourceFASTA = 0;
    }
    final var resolutionCount = this.lengthBinsAtResolution.length;
    this.presenceAtResolution = new CopyOnWriteArrayList<>();
    this.presenceAtResolution.add(ContigHideType.SHOWN);
    this.presenceAtResolution.addAll(presenceAtResolution);
    assert (atus.size() == resolutionCount - 1) : "Only ATUs for non-zero resolutions must be supplied";
    this.atus = new CopyOnWriteArrayList<>();
    this.atus.add(new CopyOnWriteArrayList<>());
    this.atus.addAll(atus);

    this.atuPrefixSumLengthBins = new CopyOnWriteArrayList<>(this.atus.parallelStream().map(atusAtResolution -> {
      final var atusLengthArray = atusAtResolution.parallelStream().mapToLong(atu -> atu.endIndexInStripeExcl - atu.startIndexInStripeIncl).toArray();
      Arrays.parallelPrefix(atusLengthArray, Long::sum);
      return atusLengthArray;
    }).toList());
  }

  public long getLengthInUnits(final @NotNull QueryLengthUnit units, final ResolutionDescriptor resolution) {
    final int resolutionOrder = resolution.getResolutionOrderInArray();
    return switch (units) {
      case PIXELS -> {
        final var presence = this.presenceAtResolution.get(resolutionOrder);
        if (presence == ContigHideType.SHOWN) {
          yield this.lengthBinsAtResolution[resolutionOrder];
        } else {
          yield 0L;
        }
      }
      case BASE_PAIRS -> this.lengthBp;
      case BINS -> this.lengthBinsAtResolution[resolutionOrder];
    };
  }
}
