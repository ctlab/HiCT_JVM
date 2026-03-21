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

package ru.itmo.ctlab.hict.hict_server.dto.response.fasta;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor;

import java.util.List;

public record FastaLinkResponseDTO(
  @NotNull String fastaFilename,
  boolean linked,
  boolean requiresConfirmation,
  @NotNull List<@NotNull String> warnings,
  @NotNull CompatibilityDTO compatibility
) {
  public static @NotNull FastaLinkResponseDTO fromReport(final @NotNull FASTAProcessor.FASTALinkCompatibilityReport report,
                                                         final boolean linked,
                                                         final boolean requiresConfirmation) {
    return new FastaLinkResponseDTO(
      report.fastaFilename(),
      linked,
      requiresConfirmation,
      report.warnings(),
      new CompatibilityDTO(
        report.fastaRecordCount(),
        report.assemblyContigCount(),
        report.sameRecordCount(),
        report.sameOrderAndLength(),
        report.sameOrderLengthAndCurrentNames(),
        report.sameOrderLengthAndOriginalNames(),
        report.sameOrderLengthAndSourceNames(),
        report.sameLengthMultiset(),
        report.mismatches().stream().map(MismatchDTO::fromReport).toList()
      )
    );
  }

  public record CompatibilityDTO(
    int fastaRecordCount,
    int assemblyContigCount,
    boolean sameRecordCount,
    boolean sameOrderAndLength,
    boolean sameOrderLengthAndCurrentNames,
    boolean sameOrderLengthAndOriginalNames,
    boolean sameOrderLengthAndSourceNames,
    boolean sameLengthMultiset,
    @NotNull List<@NotNull MismatchDTO> mismatches
  ) {
  }

  public record MismatchDTO(
    int index,
    @Nullable String fastaName,
    long fastaLengthBp,
    @Nullable String assemblyCurrentName,
    @Nullable String assemblyOriginalName,
    @Nullable String assemblySourceName,
    long assemblyLengthBp
  ) {
    public static @NotNull MismatchDTO fromReport(final @NotNull FASTAProcessor.FASTALinkCompatibilityReport.MismatchAtIndex mismatch) {
      return new MismatchDTO(
        mismatch.index(),
        mismatch.fastaName(),
        mismatch.fastaLengthBp(),
        mismatch.assemblyCurrentName(),
        mismatch.assemblyOriginalName(),
        mismatch.assemblySourceName(),
        mismatch.assemblyLengthBp()
      );
    }
  }
}
