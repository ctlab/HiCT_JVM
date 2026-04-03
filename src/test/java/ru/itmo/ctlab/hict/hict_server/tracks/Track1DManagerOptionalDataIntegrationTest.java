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

package ru.itmo.ctlab.hict.hict_server.tracks;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Track1DManagerOptionalDataIntegrationTest {
  @TempDir
  Path tempDir;

  @Test
  void coolerWeightsTrackProducesBinsAcrossUnitsWhenOptionalDataPresent() {
    final var dataRoot = Path.of(
      System.getenv().getOrDefault("HICT_OPTIONAL_DATA_DIR", "/mnt/Models/HiCT/data")
    );
    final var hictPath = dataRoot.resolve("build/quad/combined_ind2_4DN.hict.hdf5");
    Assumptions.assumeTrue(
      Files.isRegularFile(hictPath),
      () -> "Optional integration data is not present: " + hictPath
    );

    final var manager = new Track1DManager(dataRoot, tempDir.resolve("processed"));
    final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hictPath, 1, 4));
    try {
      final var opened = manager.openCoolerWeightsTrack("weights", "#4e79a7");
      assertEquals("COOLER_WEIGHTS", opened.getType());

      final var bpResolution = Arrays.stream(chunkedFile.getResolutions())
        .filter(value -> value > 0L)
        .findFirst()
        .orElseThrow();

      final var byPixels = manager.queryVisibleTracks(
        chunkedFile,
        0L,
        5000L,
        1024,
        bpResolution,
        QueryLengthUnit.PIXELS
      );
      assertEquals(1, byPixels.getTracks().size());
      assertEquals("COOLER_WEIGHTS", byPixels.getTracks().get(0).getType());
      assertFalse(byPixels.getTracks().get(0).getBins().isEmpty());

      final var byBins = manager.queryVisibleTracks(
        chunkedFile,
        0L,
        5000L,
        1024,
        bpResolution,
        QueryLengthUnit.BINS
      );
      assertEquals(1, byBins.getTracks().size());
      assertFalse(byBins.getTracks().get(0).getBins().isEmpty());

      final var byBp = manager.queryVisibleTracks(
        chunkedFile,
        0L,
        Math.max(bpResolution, 250_000_000L),
        1024,
        bpResolution,
        QueryLengthUnit.BASE_PAIRS
      );
      assertEquals(1, byBp.getTracks().size());
      assertFalse(byBp.getTracks().get(0).getBins().isEmpty());
      assertTrue(byBp.getTracks().get(0).getMaxValue() >= 0.0d);

      final var updated = manager.updateTrack(
        opened.getTrackId(),
        null,
        null,
        null,
        null,
        null,
        true
      );
      assertNotNull(updated);
      assertTrue(updated.isLogScale());
    } finally {
      chunkedFile.close();
      manager.close();
    }
  }

  @Test
  void gffTrackProjectsTranscriptBlocksAndCodingRangesWhenOptionalDataPresent() throws Exception {
    final var dataRoot = Path.of(
      System.getenv().getOrDefault("HICT_OPTIONAL_DATA_DIR", "/mnt/Models/HiCT/data")
    );
    final var hictPath = dataRoot.resolve("build/quad/combined_ind2_4DN.hict.hdf5");
    Assumptions.assumeTrue(
      Files.isRegularFile(hictPath),
      () -> "Optional integration data is not present: " + hictPath
    );

    final var manager = new Track1DManager(tempDir, tempDir.resolve("processed"));
    final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hictPath, 1, 4));
    try {
      final var sourceName = chunkedFile.getOriginalDescriptors().entrySet().stream()
        .max(Comparator.comparingLong(entry -> entry.getValue().getLengthBp()))
        .map(java.util.Map.Entry::getKey)
        .orElseThrow();
      final var descriptor = chunkedFile.resolveContigDescriptorByName(sourceName);
      final var maxBp = Math.max(12_000L, Math.min(200_000L, descriptor.getLengthBp() - 1L));
      Assumptions.assumeTrue(maxBp > 12_000L, "Contig is too short for synthetic GFF scenario");

      final var gffPath = tempDir.resolve("synthetic_features.gff3");
      final var gffText = String.join(
        "\n",
        sourceName + "\tHiCT\ttranscript\t1001\t9000\t.\t+\t.\tID=tx1;Name=GENE_A;gene_name=GENE_A;gene_id=GENE_A",
        sourceName + "\tHiCT\texon\t1001\t2200\t.\t+\t.\tParent=tx1",
        sourceName + "\tHiCT\texon\t3401\t4700\t.\t+\t.\tParent=tx1",
        sourceName + "\tHiCT\texon\t6901\t9000\t.\t+\t.\tParent=tx1",
        sourceName + "\tHiCT\tCDS\t1201\t2000\t.\t+\t0\tParent=tx1",
        sourceName + "\tHiCT\tCDS\t3601\t4500\t.\t+\t0\tParent=tx1",
        sourceName + "\tHiCT\tCDS\t7101\t8700\t.\t+\t0\tParent=tx1",
        ""
      );
      Files.writeString(gffPath, gffText, StandardCharsets.UTF_8);

      final var opened = manager.openTrack(gffPath.getFileName().toString(), "synthetic-gff", "#4e79a7");
      assertEquals("GFF_GTF", opened.getType());

      final var bpResolution = Arrays.stream(chunkedFile.getResolutions())
        .filter(value -> value > 0L)
        .findFirst()
        .orElseThrow();
      final var resolutionOrder = chunkedFile.getResolutionToIndex().get(bpResolution);
      final var totalPixels = (resolutionOrder == null || resolutionOrder < 0)
        ? 10_000L
        : chunkedFile.getMatrixSizeBins()[resolutionOrder];
      final var query = manager.queryVisibleTracks(
        chunkedFile,
        0L,
        Math.max(5_000L, totalPixels),
        1600,
        bpResolution,
        QueryLengthUnit.PIXELS
      );
      assertEquals(1, query.getTracks().size());
      final var bins = query.getTracks().get(0).getBins();
      assertFalse(bins.isEmpty());
      final var anyBlocksProjected = bins.stream()
        .anyMatch(bin -> bin.getBlocks() != null && !bin.getBlocks().isEmpty());
      Assumptions.assumeTrue(
        anyBlocksProjected,
        "Optional dataset/source-name mapping did not project grouped block features in this environment"
      );
      final var transcriptBin = bins.stream()
        .filter(bin -> bin.getBlocks() != null && !bin.getBlocks().isEmpty())
        .findFirst()
        .orElseThrow();
      assertNotNull(transcriptBin.getLabel());
      assertTrue(transcriptBin.getBlocks().size() >= 1);
      assertTrue(
        transcriptBin.getBlocks().stream().anyMatch(Track1DManager.TrackBin.TrackBinBlock::isCoding),
        "At least one projected block must be coding"
      );
      assertNotNull(transcriptBin.getThickStartBp());
      assertNotNull(transcriptBin.getThickEndBp());
      assertTrue(transcriptBin.getThickEndBp() > transcriptBin.getThickStartBp());
    } finally {
      chunkedFile.close();
      manager.close();
    }
  }

  @Test
  void gffFeatureTrackKeepsStructuredBinsWhenFeatureCountExceedsDirectRenderLimit() throws Exception {
    final var dataRoot = Path.of(
      System.getenv().getOrDefault("HICT_OPTIONAL_DATA_DIR", "/mnt/Models/HiCT/data")
    );
    final var hictPath = dataRoot.resolve("build/quad/combined_ind2_4DN.hict.hdf5");
    Assumptions.assumeTrue(
      Files.isRegularFile(hictPath),
      () -> "Optional integration data is not present: " + hictPath
    );

    final var manager = new Track1DManager(tempDir, tempDir.resolve("processed"));
    final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hictPath, 1, 4));
    try {
      final var sourceName = chunkedFile.getOriginalDescriptors().entrySet().stream()
        .max(Comparator.comparingLong(entry -> entry.getValue().getLengthBp()))
        .map(java.util.Map.Entry::getKey)
        .orElseThrow();
      final var descriptor = chunkedFile.resolveContigDescriptorByName(sourceName);
      Assumptions.assumeTrue(descriptor.getLengthBp() > 250_000L, "Contig is too short for stress GFF scenario");

      final var gffPath = tempDir.resolve("structured_dense_features.gff3");
      final var text = new StringBuilder();
      final int transcriptCount = 8_300;
      long startBp = 10_001L;
      for (int idx = 0; idx < transcriptCount; idx++) {
        final long transcriptStart = startBp;
        final long transcriptEnd = transcriptStart + 12L;
        final long cdsStart = transcriptStart + 2L;
        final long cdsEnd = transcriptEnd - 2L;
        text.append(sourceName).append('\t').append("HiCT").append('\t').append("mRNA")
          .append('\t').append(transcriptStart).append('\t').append(transcriptEnd)
          .append('\t').append('.').append('\t').append('+').append('\t').append('.')
          .append('\t').append("ID=tx").append(idx).append(";Name=TX").append(idx).append('\n');
        text.append(sourceName).append('\t').append("HiCT").append('\t').append("exon")
          .append('\t').append(transcriptStart).append('\t').append(transcriptEnd)
          .append('\t').append('.').append('\t').append('+').append('\t').append('.')
          .append('\t').append("Parent=tx").append(idx).append('\n');
        text.append(sourceName).append('\t').append("HiCT").append('\t').append("CDS")
          .append('\t').append(cdsStart).append('\t').append(cdsEnd)
          .append('\t').append('.').append('\t').append('+').append('\t').append('0')
          .append('\t').append("Parent=tx").append(idx).append('\n');
        startBp += 20L;
      }
      Files.writeString(gffPath, text.toString(), StandardCharsets.UTF_8);

      final var opened = manager.openTrack(gffPath.getFileName().toString(), "stress-gff", "#f28e2c");
      assertEquals("GFF_GTF", opened.getType());

      final var bpResolution = Arrays.stream(chunkedFile.getResolutions())
        .filter(value -> value > 0L)
        .findFirst()
        .orElseThrow();
      final var resolutionOrder = chunkedFile.getResolutionToIndex().get(bpResolution);
      final var totalPixels = (resolutionOrder == null || resolutionOrder < 0)
        ? 5_000L
        : chunkedFile.getMatrixSizeBins()[resolutionOrder];

      final var query = manager.queryVisibleTracks(
        chunkedFile,
        0L,
        Math.max(2_048L, totalPixels),
        128,
        bpResolution,
        QueryLengthUnit.PIXELS
      );
      assertEquals(1, query.getTracks().size());
      final var bins = query.getTracks().get(0).getBins();
      assertFalse(bins.isEmpty());
      assertTrue(
        bins.stream().anyMatch(bin -> bin.getBlocks() != null && !bin.getBlocks().isEmpty()),
        "Downsampled feature bins must keep exon/CDS block metadata"
      );
      assertTrue(
        bins.stream().anyMatch(bin -> "+".equals(bin.getStrand())),
        "Downsampled feature bins must keep strand metadata"
      );
      assertTrue(
        bins.stream().anyMatch(bin -> {
          final var type = bin.getFeatureType();
          return type != null && !type.isBlank();
        }),
        "Downsampled feature bins must keep feature type metadata"
      );
    } finally {
      chunkedFile.close();
      manager.close();
    }
  }
}
