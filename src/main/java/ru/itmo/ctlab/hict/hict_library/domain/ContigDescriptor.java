package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.Getter;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Getter
public class ContigDescriptor {
  private final long contigId;
  private final @NotNull @NonNull String contigName;

  private final long lengthBp;
  private final @NotNull @NonNull ConcurrentMap<@NotNull @NonNull Long, @NotNull @NonNull Long> lengthBinsAtResolution;
  private final @NotNull @NonNull ConcurrentMap<@NotNull @NonNull Long, @NotNull @NonNull ContigHideType> presenceAtResolution;


  private final @NotNull @NonNull ConcurrentMap<@NotNull @NonNull Long, @NotNull @NonNull List<@NotNull @NonNull ATUDescriptor>> atus;
  private final @NotNull @NonNull ConcurrentMap<@NotNull @NonNull Long, long[]> atuPrefixSumLengthBins;

  public ContigDescriptor(
    final long contigId,
    final @NotNull @NonNull String contigName,
    final long lengthBp,
    final @NotNull @NonNull Map<@NotNull @NonNull Long, @NotNull @NonNull Long> lengthBinsAtResolution,
    final @NotNull @NonNull Map<@NotNull @NonNull Long, @NotNull @NonNull ContigHideType> presenceAtResolution,
    final @NotNull @NonNull Map<@NotNull @NonNull Long, @NotNull @NonNull List<@NotNull @NonNull ATUDescriptor>> atus
  ) {
    this.contigId = contigId;
        this.contigName = contigName;
        this.lengthBp = lengthBp;
    final @NotNull @NonNull ConcurrentMap<@NotNull @NonNull Long, @NotNull @NonNull Long> newLengthBinsAtResolution = new ConcurrentHashMap<>(lengthBinsAtResolution);
        newLengthBinsAtResolution.put(0L, this.lengthBp);
        this.lengthBinsAtResolution = newLengthBinsAtResolution;
        this.presenceAtResolution = new ConcurrentHashMap<>(presenceAtResolution);
        this.presenceAtResolution.put(0L, ContigHideType.SHOWN);
        this.atus = new ConcurrentHashMap<>(atus);
        this.atuPrefixSumLengthBins = atus.entrySet().parallelStream().map(longListEntry -> {
            final var resolution = longListEntry.getKey();
            final var atusLengthArray = longListEntry.getValue().parallelStream().map(atu -> atu.end_index_in_stripe_excl - atu.start_index_in_stripe_incl).toArray(Long[]::new);
            Arrays.parallelPrefix(atusLengthArray, Long::sum);
            return Map.entry(resolution, Arrays.stream(atusLengthArray).mapToLong(Long::longValue).toArray());
        }).collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
    }

  public long getLengthInUnits(final @NotNull @NonNull QueryLengthUnit units, final long resolution) {
    return switch (units) {
      case PIXELS -> {
        final var presence = this.presenceAtResolution.get(resolution);
        if (presence == ContigHideType.SHOWN) {
          yield this.lengthBinsAtResolution.get(resolution);
        } else {
          yield 0L;
        }
      }
      case BASE_PAIRS -> this.lengthBp;
            case BINS -> this.lengthBinsAtResolution.get(resolution);
        };
    }
}
