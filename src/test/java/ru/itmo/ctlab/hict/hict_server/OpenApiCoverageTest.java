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

package ru.itmo.ctlab.hict.hict_server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenApiCoverageTest {
  private static final Pattern ROUTE_PATTERN = Pattern.compile("router\\.(?:get|post)\\(\"([^\"]+)\"\\)");
  private static final Pattern DOCS_PATH_PATTERN = Pattern.compile("^  (/[^:]+):\\s*$", Pattern.MULTILINE);
  private static final Pattern COLON_PARAM_PATTERN = Pattern.compile(":([A-Za-z_][A-Za-z0-9_]*)");

  @Test
  void allHandlerRoutesAreDocumentedInOpenApiSpec() throws IOException {
    final var handlerPaths = extractHandlerPaths();
    final var documentedPaths = extractDocumentedPaths();
    final var missing = handlerPaths.stream()
      .filter(path -> !documentedPaths.contains(path))
      .collect(Collectors.toCollection(LinkedHashSet::new));

    assertTrue(
      missing.isEmpty(),
      () -> "OpenAPI spec is missing handler paths: " + missing
    );
  }

  private static Set<String> extractHandlerPaths() throws IOException {
    final var result = new LinkedHashSet<String>();
    final var handlersRoot = Path.of("src/main/java/ru/itmo/ctlab/hict/hict_server/handlers");
    try (final var stream = Files.walk(handlersRoot)) {
      final var handlerFiles = stream
        .filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith("HandlersHolder.java"))
        .toList();
      for (final var path : handlerFiles) {
        final var text = Files.readString(path, StandardCharsets.UTF_8);
        final var matcher = ROUTE_PATTERN.matcher(text);
        while (matcher.find()) {
          final var rawPath = matcher.group(1);
          final var normalized = COLON_PARAM_PATTERN.matcher(rawPath).replaceAll("{$1}");
          result.add(normalized);
        }
      }
    }
    return result;
  }

  private static Set<String> extractDocumentedPaths() throws IOException {
    final var openApiPath = Path.of("src/main/resources/openapi/hict-api-v1.yaml");
    final var openApiText = Files.readString(openApiPath, StandardCharsets.UTF_8);
    final var result = new LinkedHashSet<String>();
    final var matcher = DOCS_PATH_PATTERN.matcher(openApiText);
    while (matcher.find()) {
      result.add(matcher.group(1));
    }
    return result;
  }
}
