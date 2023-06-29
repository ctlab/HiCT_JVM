package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.Getter;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

@Getter
public class ContigDescriptor {
  private final int contigId;
  private final @NotNull
  @NonNull String contigName;

  private final long lengthBp;
  private final long[] lengthBinsAtResolution;
  private final @NotNull
  @NonNull List<@NotNull @NonNull ContigHideType> presenceAtResolution;
  private final @NotNull @NonNull List<@NotNull @NonNull List<@NotNull @NonNull ATUDescriptor>> atus;
  private final @NotNull @NonNull List<long @NotNull @NonNull []> atuPrefixSumLengthBins;

  public ContigDescriptor(
    final int contigId,
    final @NotNull @NonNull String contigName,
    final long lengthBp,
    final @NotNull @NonNull List<@NotNull @NonNull Long> lengthBinsAtResolution,
    final @NotNull @NonNull List<@NotNull @NonNull ContigHideType> presenceAtResolution,
    final @NotNull @NonNull List<@NotNull @NonNull List<@NotNull @NonNull ATUDescriptor>> atus
  ) {
    this.contigId = contigId;
    this.contigName = contigName;
    this.lengthBp = lengthBp;


    this.lengthBinsAtResolution = Stream.concat(Stream.of(this.lengthBp), lengthBinsAtResolution.stream()).mapToLong(lng -> lng).toArray();
    final var resolutionCount = this.lengthBinsAtResolution.length;
    this.presenceAtResolution = new CopyOnWriteArrayList<>();
    this.presenceAtResolution.add(ContigHideType.SHOWN);
    this.presenceAtResolution.addAll(presenceAtResolution);
    assert (atus.size() == resolutionCount - 1) : "Only ATUs for non-zero resolutions must be supplied";
    this.atus = new CopyOnWriteArrayList<>();
    this.atus.add(new CopyOnWriteArrayList<>());
    this.atus.addAll(atus);


    this.atuPrefixSumLengthBins = new CopyOnWriteArrayList<>(atus.parallelStream().map(atusAtResolution -> {
      final var atusLengthArray = atusAtResolution.parallelStream().mapToLong(atu -> atu.end_index_in_stripe_excl - atu.start_index_in_stripe_incl).toArray();
      Arrays.parallelPrefix(atusLengthArray, Long::sum);
      return atusLengthArray;
    }).toList());
  }

  public long getLengthInUnits(final @NotNull @NonNull QueryLengthUnit units, final int resolutionOrder) {
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
