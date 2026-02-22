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

package ru.itmo.ctlab.hict.hict_server.dto.request.names;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public record ImportNameMappingRequestDTO(
  @NotNull List<@NotNull ContigNameMappingRequestDTO> contigs,
  @NotNull List<@NotNull ScaffoldNameMappingRequestDTO> scaffolds
) {
  public static @NotNull ImportNameMappingRequestDTO fromJSONObject(final @NotNull JsonObject jsonObject) {
    return new ImportNameMappingRequestDTO(
      parseContigs(jsonObject.getJsonArray("contigs", new JsonArray())),
      parseScaffolds(jsonObject.getJsonArray("scaffolds", new JsonArray()))
    );
  }

  private static @NotNull List<@NotNull ContigNameMappingRequestDTO> parseContigs(final @NotNull JsonArray array) {
    final var result = new ArrayList<ContigNameMappingRequestDTO>(array.size());
    for (int i = 0; i < array.size(); i++) {
      final var obj = array.getJsonObject(i);
      result.add(new ContigNameMappingRequestDTO(obj.getInteger("contigId"), obj.getString("name")));
    }
    return result;
  }

  private static @NotNull List<@NotNull ScaffoldNameMappingRequestDTO> parseScaffolds(final @NotNull JsonArray array) {
    final var result = new ArrayList<ScaffoldNameMappingRequestDTO>(array.size());
    for (int i = 0; i < array.size(); i++) {
      final var obj = array.getJsonObject(i);
      result.add(new ScaffoldNameMappingRequestDTO(obj.getLong("scaffoldId"), obj.getString("name")));
    }
    return result;
  }

  public record ContigNameMappingRequestDTO(int contigId, String name) {}

  public record ScaffoldNameMappingRequestDTO(long scaffoldId, String name) {}
}
