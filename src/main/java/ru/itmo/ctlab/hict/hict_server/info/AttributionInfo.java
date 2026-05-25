/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis, Zakhar Lobanov, Nikita Zheleznov, Pavel Avdeyev, Nikolay Cherkasov and Computer Technologies Laboratory ITMO University team.
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

package ru.itmo.ctlab.hict.hict_server.info;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public final class AttributionInfo {
  private static final @NotNull String AUTHORS_LINE = "Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis, Zakhar Lobanov, Nikita Zheleznov, Pavel Avdeyev, Nikolay Cherkasov and Computer Technologies Laboratory ITMO University team.";
  private static final @NotNull String LICENSE_LINE = "MIT License";
  private static final @NotNull List<String> USED_SOFTWARE_LINES = List.of(
    "Eclipse Vert.x - HTTP API, routing and WebUI serving - https://vertx.io/",
    "HDF5 and JHDF5 - matrix storage and native HDF5 access - https://www.hdfgroup.org/solutions/hdf5/ and https://unlimited.ethz.ch/spaces/JHDF/overview",
    "hictk - optional .hic conversion path - https://github.com/paulsengroup/hictk",
    "minimap2 and mm2-plus - optional FASTA self-alignment for dotplot generation - https://github.com/lh3/minimap2 and https://github.com/at-cg/mm2-plus",
    "HTSJDK and IGV BigWig - FASTA/sequence and genomic track support - https://github.com/samtools/htsjdk and https://github.com/igvteam/igv",
    "Apache Commons - JVM utilities - https://commons.apache.org/",
    "SLF4J, Logback and picocli - logging and command-line interface - https://www.slf4j.org/, https://logback.qos.ch/ and https://picocli.info/",
    "Vue, Pinia, Vite, TypeScript, Bootstrap, PrimeVue, OpenLayers, igv.js, jsPDF and LiteGraph - WebUI framework, visualization and exports.",
    "Tauri and Electron - optional bundled WebUI browser runtimes - https://tauri.app/ and https://www.electronjs.org/",
    "Eclipse Temurin / OpenJDK - optional Java runtime in portable packages - https://adoptium.net/temurin/"
  );

  private AttributionInfo() {
  }

  public static @NotNull String authorsLine() {
    return AUTHORS_LINE;
  }

  public static @NotNull String licenseLine() {
    return LICENSE_LINE;
  }

  public static @NotNull List<String> usedSoftwareLines() {
    return USED_SOFTWARE_LINES;
  }

  public static @NotNull List<String> startupBannerLines() {
    return List.of(
      "HiCT - Hi-C scaffolding and visualization workstation.",
      "Team: " + AUTHORS_LINE,
      "License: HiCT is distributed under the " + LICENSE_LINE + ".",
      "Core libraries: Eclipse Vert.x (Eclipse Foundation), HDF5/JHDF5 (The HDF Group and ETH Zurich/CISD), HTSJDK/IGV BigWig (Broad Institute/IGV team), Apache Commons, SLF4J/Logback and picocli.",
      "WebUI libraries: Vue, Vite, Pinia, Bootstrap, OpenLayers, PrimeVue, igv.js, jsPDF, LiteGraph and contributors.",
      "Optional .hic conversion: bundled hictk builds are redistributed under the hictk MIT License; cite Rossini R, Paulsen J. Bioinformatics 2024;40(7):btae408.",
      "Optional dotplot alignment: bundled minimap2/mm2-plus builds are redistributed with upstream license files and citations.",
      "Full third-party notices are kept in package metadata, portable licenses/, runtime/legal, and toolchains/<platform>/share when bundled."
    );
  }
}
