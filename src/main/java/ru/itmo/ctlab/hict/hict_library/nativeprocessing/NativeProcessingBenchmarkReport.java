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

package ru.itmo.ctlab.hict.hict_library.nativeprocessing;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class NativeProcessingBenchmarkReport {
  private static final @NotNull List<String> OPERATIONS = List.of(
    "base-signal",
    "observed-expected",
    "post-log",
    "precomputed-1d-max"
  );
  private static final @NotNull List<String> DEFAULT_VARIANTS = List.of("java", "avx2", "avx512");

  private NativeProcessingBenchmarkReport() {
  }

  public static void main(final String @NotNull [] args) throws IOException, InterruptedException {
    final var reportDir = Path.of(System.getProperty("hict.native.benchmark.reportDir", "build/reports/hict-native-benchmark"));
    Files.createDirectories(reportDir);

    final var rows = new ArrayList<BenchmarkRow>();
    final var warnings = new ArrayList<String>();
    final var variants = parseVariants(System.getProperty("hict.native.benchmark.variants", "avx2,avx512"));

    rows.addAll(runChildBenchmark(reportDir, "java", true, warnings));
    for (final var variant : variants) {
      final var variantRows = runChildBenchmark(reportDir, variant, false, warnings);
      variantRows.stream()
        .filter(row -> variant.equals(row.variant()))
        .forEach(rows::add);
    }

    final var combinedCsv = reportDir.resolve("benchmark.csv");
    writeCombinedCsv(combinedCsv, rows);
    final var svg = reportDir.resolve("requests_per_second.svg");
    writeRequestsPerSecondSvg(svg, rows);
    final var html = reportDir.resolve("index.html");
    writeHtml(html, rows, warnings);

    System.out.println("HiCT native processing benchmark report:");
    System.out.println("  " + html.toAbsolutePath());
    System.out.println("  " + combinedCsv.toAbsolutePath());
    System.out.println("  " + svg.toAbsolutePath());
    if (!warnings.isEmpty()) {
      System.out.println("Warnings:");
      warnings.forEach(warning -> System.out.println("  " + warning));
    }
  }

  private static @NotNull List<String> parseVariants(final @NotNull String variantsProperty) {
    final var variants = new ArrayList<String>();
    for (final var rawVariant : variantsProperty.split(",")) {
      final var variant = rawVariant.trim().toLowerCase(Locale.ROOT);
      if (!variant.isBlank() && !"java".equals(variant)) {
        variants.add("baseline".equals(variant) ? "avx2" : variant);
      }
    }
    return variants.isEmpty() ? List.of("avx2", "avx512") : variants;
  }

  private static @NotNull List<BenchmarkRow> runChildBenchmark(final @NotNull Path reportDir,
                                                               final @NotNull String variant,
                                                               final boolean javaOnly,
                                                               final @NotNull List<String> warnings)
    throws IOException, InterruptedException {
    final var outputCsv = reportDir.resolve(variant + ".csv");
    final var command = new ArrayList<String>();
    command.add(javaExecutable().toString());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    passProperty(command, "hict.native.benchmark.rows");
    passProperty(command, "hict.native.benchmark.columns");
    passProperty(command, "hict.native.benchmark.warmup");
    passProperty(command, "hict.native.benchmark.iterations");
    passProperty(command, "hict.native.library.dir");
    command.add("-Dhict.native.benchmark.outputCsv=" + outputCsv.toAbsolutePath());
    if (javaOnly) {
      command.add("-Dhict.native.benchmark.javaOnly=true");
    } else {
      command.add("-Dhict.native.variant=" + variant);
    }
    command.add(NativeProcessingBenchmark.class.getName());

    final var process = new ProcessBuilder(command)
      .redirectErrorStream(true)
      .start();
    final var output = new StringBuilder();
    try (BufferedReader reader = process.inputReader(StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append(System.lineSeparator());
        System.out.println("[" + variant + "] " + line);
      }
    }
    final int exitCode = process.waitFor();
    if (exitCode != 0) {
      warnings.add("Benchmark child for " + variant + " exited with code " + exitCode);
      Files.writeString(reportDir.resolve(variant + ".log"), output.toString(), StandardCharsets.UTF_8);
      return unavailableRows(variant, "child process failed with exit code " + exitCode);
    }
    if (!Files.isRegularFile(outputCsv)) {
      warnings.add("Benchmark child for " + variant + " did not produce " + outputCsv.getFileName());
      Files.writeString(reportDir.resolve(variant + ".log"), output.toString(), StandardCharsets.UTF_8);
      return unavailableRows(variant, "child process did not produce CSV output");
    }
    return readRows(outputCsv);
  }

  private static @NotNull Path javaExecutable() {
    final boolean windows = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    return Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java");
  }

  private static void passProperty(final @NotNull List<String> command,
                                   final @NotNull String propertyName) {
    final var value = System.getProperty(propertyName);
    if (value != null && !value.isBlank()) {
      command.add("-D" + propertyName + "=" + value);
    }
  }

  private static @NotNull List<BenchmarkRow> unavailableRows(final @NotNull String variant,
                                                             final @NotNull String reason) {
    return OPERATIONS.stream()
      .map(operation -> new BenchmarkRow(operation, variant, false, Double.NaN, Double.NaN, Double.NaN, reason))
      .toList();
  }

  private static @NotNull List<BenchmarkRow> readRows(final @NotNull Path csv) throws IOException {
    final var rows = new ArrayList<BenchmarkRow>();
    final var lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
    for (int i = 1; i < lines.size(); i++) {
      if (lines.get(i).isBlank()) {
        continue;
      }
      final var columns = parseCsvLine(lines.get(i));
      rows.add(new BenchmarkRow(
        columns.get(0),
        columns.get(1),
        Boolean.parseBoolean(columns.get(2)),
        parseDouble(columns.get(3)),
        parseDouble(columns.get(4)),
        parseDouble(columns.get(5)),
        columns.get(6)
      ));
    }
    return rows;
  }

  private static @NotNull List<String> parseCsvLine(final @NotNull String line) {
    final var columns = new ArrayList<String>();
    final var current = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      final char ch = line.charAt(i);
      if (quoted) {
        if (ch == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            current.append('"');
            i++;
          } else {
            quoted = false;
          }
        } else {
          current.append(ch);
        }
      } else if (ch == '"') {
        quoted = true;
      } else if (ch == ',') {
        columns.add(current.toString());
        current.setLength(0);
      } else {
        current.append(ch);
      }
    }
    columns.add(current.toString());
    return columns;
  }

  private static double parseDouble(final @NotNull String value) {
    return value.isBlank() ? Double.NaN : Double.parseDouble(value);
  }

  private static void writeCombinedCsv(final @NotNull Path output,
                                       final @NotNull List<BenchmarkRow> rows) throws IOException {
    final var builder = new StringBuilder("operation,variant,available,meanMillis,requestsPerSecond,maxAbsDiff,notes\n");
    for (final var row : rows) {
      builder.append(row.toCsv()).append('\n');
    }
    Files.writeString(output, builder.toString(), StandardCharsets.UTF_8);
  }

  private static void writeRequestsPerSecondSvg(final @NotNull Path output,
                                                final @NotNull List<BenchmarkRow> rows) throws IOException {
    final int width = 1120;
    final int height = 430;
    final int left = 72;
    final int top = 40;
    final int plotWidth = 980;
    final int plotHeight = 280;
    final var rowsByKey = new HashMap<String, BenchmarkRow>();
    double max = 1.0d;
    for (final var row : rows) {
      rowsByKey.put(row.operation() + "/" + row.variant(), row);
      if (row.available() && Double.isFinite(row.requestsPerSecond())) {
        max = Math.max(max, row.requestsPerSecond());
      }
    }
    final var variants = variantsInDisplayOrder(rows);
    final int groupWidth = plotWidth / OPERATIONS.size();
    final int barWidth = Math.max(12, Math.min(42, (groupWidth - 48) / Math.max(1, variants.size())));
    final var colors = Map.of(
      "java", "#5b677a",
      "avx2", "#0f7a3b",
      "avx512", "#c2272d"
    );
    final var svg = new StringBuilder();
    svg.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(width).append("\" height=\"").append(height).append("\" viewBox=\"0 0 ").append(width).append(' ').append(height).append("\">\n");
    svg.append("<rect width=\"100%\" height=\"100%\" fill=\"#ffffff\"/>\n");
    svg.append("<text x=\"").append(left).append("\" y=\"24\" font-family=\"sans-serif\" font-size=\"18\" font-weight=\"700\">HiCT native processing benchmark: requests/sec</text>\n");
    svg.append("<line x1=\"").append(left).append("\" y1=\"").append(top + plotHeight).append("\" x2=\"").append(left + plotWidth).append("\" y2=\"").append(top + plotHeight).append("\" stroke=\"#1f2937\"/>\n");
    svg.append("<line x1=\"").append(left).append("\" y1=\"").append(top).append("\" x2=\"").append(left).append("\" y2=\"").append(top + plotHeight).append("\" stroke=\"#1f2937\"/>\n");
    for (int tick = 0; tick <= 4; tick++) {
      final double value = max * tick / 4.0d;
      final int y = top + plotHeight - (int) Math.round(plotHeight * tick / 4.0d);
      svg.append("<line x1=\"").append(left).append("\" y1=\"").append(y).append("\" x2=\"").append(left + plotWidth).append("\" y2=\"").append(y).append("\" stroke=\"#e5e7eb\"/>\n");
      svg.append("<text x=\"").append(left - 8).append("\" y=\"").append(y + 4).append("\" text-anchor=\"end\" font-family=\"sans-serif\" font-size=\"12\">")
        .append(formatCompact(value)).append("</text>\n");
    }
    for (int opIdx = 0; opIdx < OPERATIONS.size(); opIdx++) {
      final var operation = OPERATIONS.get(opIdx);
      final int groupX = left + (opIdx * groupWidth) + 30;
      for (int variantIdx = 0; variantIdx < variants.size(); variantIdx++) {
        final var variant = variants.get(variantIdx);
        final var row = rowsByKey.get(operation + "/" + variant);
        if (row == null || !row.available() || !Double.isFinite(row.requestsPerSecond())) {
          continue;
        }
        final int barHeight = (int) Math.round((row.requestsPerSecond() / max) * plotHeight);
        final int x = groupX + variantIdx * (barWidth + 6);
        final int y = top + plotHeight - barHeight;
        svg.append("<rect x=\"").append(x).append("\" y=\"").append(y).append("\" width=\"").append(barWidth).append("\" height=\"").append(barHeight).append("\" fill=\"")
          .append(colors.getOrDefault(variant, "#2563eb")).append("\" rx=\"3\"/>\n");
      }
      svg.append("<text x=\"").append(groupX + ((barWidth + 6) * variants.size() / 2)).append("\" y=\"").append(top + plotHeight + 26).append("\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"13\">")
        .append(escapeXml(operation)).append("</text>\n");
    }
    int legendX = left;
    final int legendY = height - 44;
    for (final var variant : variants) {
      svg.append("<rect x=\"").append(legendX).append("\" y=\"").append(legendY - 12).append("\" width=\"16\" height=\"16\" fill=\"")
        .append(colors.getOrDefault(variant, "#2563eb")).append("\" rx=\"2\"/>\n");
      svg.append("<text x=\"").append(legendX + 22).append("\" y=\"").append(legendY + 1).append("\" font-family=\"sans-serif\" font-size=\"13\">")
        .append(escapeXml(variant)).append("</text>\n");
      legendX += 110;
    }
    svg.append("</svg>\n");
    Files.writeString(output, svg.toString(), StandardCharsets.UTF_8);
  }

  private static @NotNull List<String> variantsInDisplayOrder(final @NotNull List<BenchmarkRow> rows) {
    final var seen = new LinkedHashMap<String, Boolean>();
    for (final var variant : DEFAULT_VARIANTS) {
      seen.put(variant, false);
    }
    for (final var row : rows) {
      seen.put(row.variant(), false);
    }
    return new ArrayList<>(seen.keySet());
  }

  private static void writeHtml(final @NotNull Path output,
                                final @NotNull List<BenchmarkRow> rows,
                                final @NotNull List<String> warnings) throws IOException {
    final var html = new StringBuilder();
    html.append("<!doctype html><html><head><meta charset=\"utf-8\"><title>HiCT Native Benchmark</title>");
    html.append("<style>body{font-family:system-ui,sans-serif;margin:2rem;color:#111827}table{border-collapse:collapse;margin-top:1rem}td,th{border:1px solid #d1d5db;padding:.4rem .6rem;text-align:right}td:first-child,th:first-child,td:nth-child(2),th:nth-child(2),td:last-child,th:last-child{text-align:left}.warn{background:#fff7ed;border-left:4px solid #f59e0b;padding:.7rem 1rem}.ok{color:#047857}.bad{color:#b91c1c}</style>");
    html.append("</head><body><h1>HiCT Native Processing Benchmark</h1>");
    html.append("<p>Generated ").append(escapeHtml(Instant.now().toString())).append(".</p>");
    if (!warnings.isEmpty()) {
      html.append("<div class=\"warn\"><strong>Warnings</strong><ul>");
      for (final var warning : warnings) {
        html.append("<li>").append(escapeHtml(warning)).append("</li>");
      }
      html.append("</ul></div>");
    }
    html.append("<p><img src=\"requests_per_second.svg\" alt=\"Requests per second chart\"></p>");
    html.append("<table><thead><tr><th>Operation</th><th>Variant</th><th>Available</th><th>Mean ms</th><th>Requests/sec</th><th>Max abs diff</th><th>Notes</th></tr></thead><tbody>");
    for (final var row : rows) {
      html.append("<tr><td>").append(escapeHtml(row.operation())).append("</td><td>").append(escapeHtml(row.variant())).append("</td><td class=\"")
        .append(row.available() ? "ok" : "bad").append("\">").append(row.available()).append("</td><td>").append(formatNumber(row.meanMillis())).append("</td><td>")
        .append(formatNumber(row.requestsPerSecond())).append("</td><td>").append(formatNumber(row.maxAbsDiff())).append("</td><td>")
        .append(escapeHtml(row.notes())).append("</td></tr>");
    }
    html.append("</tbody></table><p>Raw data: <a href=\"benchmark.csv\">benchmark.csv</a>.</p></body></html>");
    Files.writeString(output, html.toString(), StandardCharsets.UTF_8);
  }

  private static @NotNull String formatCompact(final double value) {
    if (value >= 1_000_000.0d) {
      return String.format(Locale.ROOT, "%.1fM", value / 1_000_000.0d);
    }
    if (value >= 1_000.0d) {
      return String.format(Locale.ROOT, "%.1fK", value / 1_000.0d);
    }
    return String.format(Locale.ROOT, "%.0f", value);
  }

  private static @NotNull String formatNumber(final double value) {
    return Double.isFinite(value) ? String.format(Locale.ROOT, "%.6g", value) : "n/a";
  }

  private static @NotNull String escapeHtml(final @NotNull String value) {
    return value
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;");
  }

  private static @NotNull String escapeXml(final @NotNull String value) {
    return escapeHtml(value).replace("'", "&apos;");
  }

  private record BenchmarkRow(@NotNull String operation,
                              @NotNull String variant,
                              boolean available,
                              double meanMillis,
                              double requestsPerSecond,
                              double maxAbsDiff,
                              @NotNull String notes) {
    @NotNull String toCsv() {
      return String.join(
        ",",
        csv(operation),
        csv(variant),
        Boolean.toString(available),
        number(meanMillis),
        number(requestsPerSecond),
        number(maxAbsDiff),
        csv(notes)
      );
    }

    private static @NotNull String number(final double value) {
      return Double.isFinite(value) ? String.format(Locale.ROOT, "%.9g", value) : "";
    }

    private static @NotNull String csv(final @NotNull String value) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
  }
}
