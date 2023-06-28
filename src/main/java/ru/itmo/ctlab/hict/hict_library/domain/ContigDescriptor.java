package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.Getter;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Getter
public class ContigDescriptor {
  private final long contigId;
  private final @NotNull @NonNull String contigName;

  private final long lengthBp;
  private final CopyOnWriteArrayList<@NotNull @NonNull Long> lengthBinsAtResolution;
  private final @NotNull @NonNull CopyOnWriteArrayList<@NotNull @NonNull ContigHideType> presenceAtResolution;
  private final @NotNull @NonNull List<@NotNull @NonNull ATUDescriptor> @NotNull @NonNull[] atus;
  private final CopyOnWriteArrayList<@NotNull @NonNull Long>[] atuPrefixSumLengthBins;

  public ContigDescriptor(
    final long contigId,
    final @NotNull @NonNull String contigName,
    final long lengthBp,
    final @NotNull @NonNull List<@NotNull @NonNull Long> lengthBinsAtResolution,
    final @NotNull @NonNull List<@NotNull @NonNull ContigHideType> presenceAtResolution,
    final @NotNull @NonNull List<@NotNull @NonNull ATUDescriptor> @NotNull @NonNull[] atus
  ) {
    this.contigId = contigId;
        this.contigName = contigName;
        this.lengthBp = lengthBp;
    final @NotNull @NonNull CopyOnWriteArrayList<@NotNull @NonNull Long> newLengthBinsAtResolution = ;

        this.lengthBinsAtResolution = new CopyOnWriteArrayList<>();
        this.lengthBinsAtResolution.add(this.lengthBp);
        this.lengthBinsAtResolution.addAll(lengthBinsAtResolution);
        this.presenceAtResolution = new CopyOnWriteArrayList<>(presenceAtResolution);
        this.presenceAtResolution.put(0L, ContigHideType.SHOWN);
        this.atus = new ConcurrentHashMap<>(atus);
//        atuPrefixSumLengthBins

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
