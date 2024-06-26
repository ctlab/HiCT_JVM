/*
 * MIT License
 *
 * Copyright (c) 2021-2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
