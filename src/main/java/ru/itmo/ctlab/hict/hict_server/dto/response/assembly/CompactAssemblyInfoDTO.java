/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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

package ru.itmo.ctlab.hict.hict_server.dto.response.assembly;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.AssemblyInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** A complete assembly snapshot with shared resolution keys; never a delta. */
public record CompactAssemblyInfoDTO(String format, long[] resolutions,
                                    List<Object[]> contigs, List<Object[]> scaffolds) {
  public static final String FORMAT = "compact-v1";

  public record Response(CompactAssemblyInfoDTO assemblyInfo, long version) {}

  /** Opt-in negotiation keeps existing clients and unknown formats on the legacy contract. */
  public static Object response(ChunkedFile file, long version, Object requestedFormat) {
    if (!FORMAT.equals(requestedFormat)) {
      return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(file), version);
    }
    return new Response(fromSnapshot(file.getAssemblyInfo(), file.getResolutions(),
      file.getContigNameOverrides(), file.getScaffoldNameOverrides()), version);
  }

  static CompactAssemblyInfoDTO fromSnapshot(AssemblyInfo snapshot, long[] resolutions,
                                             Map<Integer, String> contigAliases,
                                             Map<Long, String> scaffoldAliases) {
    // Index zero is base pairs, which is already a separate contig field.
    final var keys = Arrays.copyOfRange(resolutions, 1, resolutions.length);
    final var contigs = snapshot.contigs().stream().map(tuple -> {
      final var d = tuple.descriptor();
      final var bins = Arrays.copyOfRange(d.getLengthBinsAtResolution(), 1, resolutions.length);
      final var presence = new int[keys.length];
      for (int i = 0; i < presence.length; ++i) presence[i] = d.getPresenceAtResolution().get(i + 1).ordinal();
      return new Object[]{d.getContigId(), contigAliases.getOrDefault(d.getContigId(), d.getContigName()),
        d.getContigName(), d.getContigNameInSourceFASTA(), d.getOffsetInSourceFASTA(),
        tuple.direction().ordinal(), d.getLengthBp(), bins, presence};
    }).toList();
    final var scaffolds = snapshot.scaffolds().stream().map(tuple -> {
      final var d = tuple.scaffoldDescriptor();
      final var borders = tuple.scaffoldBordersBP();
      return new Object[]{d.scaffoldId(), scaffoldAliases.getOrDefault(d.scaffoldId(), d.scaffoldName()),
        d.scaffoldName(), d.spacerLength(), borders == null ? null : new long[]{borders.startBP(), borders.endBP()}};
    }).toList();
    return new CompactAssemblyInfoDTO(FORMAT, keys, contigs, scaffolds);
  }
}
