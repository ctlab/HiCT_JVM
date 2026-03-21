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

package ru.itmo.ctlab.hict.hict_server.handlers.names;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.request.names.ImportNameMappingRequestDTO;
import ru.itmo.ctlab.hict.hict_server.dto.request.names.RenameContigRequestDTO;
import ru.itmo.ctlab.hict.hict_server.dto.request.names.RenameScaffoldRequestDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoWithVersionDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.names.NameMappingDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RequiredArgsConstructor
@Slf4j
public class NameMappingHandlersHolder extends HandlersHolder {
  private final Vertx vertx;

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/names/contig").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var request = RenameContigRequestDTO.fromJSONObject(ctx.body().asJsonObject());
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final var chunkedFile = extractChunkedFile();
          final var newName = normalizeName(request.newName());
          validateContigRename(chunkedFile, request.contigId(), newName);
          chunkedFile.setContigNameOverride(request.contigId(), newName);
          final var newVersion = incrementVersionAndResetTileStats(chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });

    router.post("/names/scaffold").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var request = RenameScaffoldRequestDTO.fromJSONObject(ctx.body().asJsonObject());
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final var chunkedFile = extractChunkedFile();
          final var newName = normalizeName(request.newName());
          validateScaffoldRename(chunkedFile, request.scaffoldId(), newName);
          chunkedFile.setScaffoldNameOverride(request.scaffoldId(), newName);
          final var newVersion = incrementVersionAndResetTileStats(chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });

    router.get("/names/export").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.EXPORT,
        RequestTaskScheduler.CancellationDomain.EXPORT,
        () -> {
          final var chunkedFile = extractChunkedFile();
          final var contigs = chunkedFile.getContigTree().getOrderedContigList().stream().map(tuple ->
            new NameMappingDTO.ContigNameMappingDTO(
              tuple.descriptor().getContigId(),
              chunkedFile.getContigOriginalName(tuple.descriptor().getContigId()),
              chunkedFile.getContigDisplayName(tuple.descriptor().getContigId())
            )
          ).toList();
          final var scaffolds = chunkedFile.getScaffoldTree().getScaffoldList().stream().map(tuple ->
            new NameMappingDTO.ScaffoldNameMappingDTO(
              tuple.scaffoldDescriptor().scaffoldId(),
              chunkedFile.getScaffoldOriginalName(tuple.scaffoldDescriptor().scaffoldId()),
              chunkedFile.getScaffoldDisplayName(tuple.scaffoldDescriptor().scaffoldId())
            )
          ).toList();
          return new NameMappingDTO(contigs, scaffolds);
        },
        dto -> ctx.response().end(Json.encode(dto)),
        () -> ctx.response().end(Json.encode(new NameMappingDTO(java.util.List.of(), java.util.List.of())))
      );
    });

    router.post("/names/import").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var request = ImportNameMappingRequestDTO.fromJSONObject(ctx.body().asJsonObject());
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final var chunkedFile = extractChunkedFile();

          final Map<Integer, String> contigUpdates = new HashMap<>();
          request.contigs().forEach(entry -> contigUpdates.put(entry.contigId(), normalizeName(entry.name())));
          final Map<Long, String> scaffoldUpdates = new HashMap<>();
          request.scaffolds().forEach(entry -> scaffoldUpdates.put(entry.scaffoldId(), normalizeName(entry.name())));

          validateContigMappingImport(chunkedFile, contigUpdates);
          validateScaffoldMappingImport(chunkedFile, scaffoldUpdates);

          contigUpdates.forEach(chunkedFile::setContigNameOverride);
          scaffoldUpdates.forEach(chunkedFile::setScaffoldNameOverride);

          final var newVersion = incrementVersionAndResetTileStats(chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });
  }

  private ChunkedFile extractChunkedFile() {
    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
    if (chunkedFileWrapper == null) {
      throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
    }
    return chunkedFileWrapper.getChunkedFile();
  }

  private long incrementVersionAndResetTileStats(final @NotNull ChunkedFile chunkedFile,
                                                 final @NotNull RequestTaskScheduler scheduler) {
    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
    if (stats == null) {
      throw new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?");
    }
    final var newStats = TileStatisticHolder.resetRangesWithIncrementedVersion(stats, chunkedFile.getResolutions().length);
    map.put("TileStatisticHolder", newStats);
    scheduler.bumpAssemblyGeneration();
    return newStats.versionCounter().get();
  }

  private RequestTaskScheduler getScheduler(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
    if (wrapper == null) {
      ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
      return null;
    }
    return wrapper.getRequestTaskScheduler();
  }

  private static @NotNull String normalizeName(final String name) {
    if (name == null) {
      return "";
    }
    return name.trim();
  }

  private void validateContigRename(final @NotNull ChunkedFile chunkedFile, final int contigId, final @NotNull String newName) {
    if (newName.isBlank()) {
      return;
    }
    final var existingNames = new HashSet<>(chunkedFile.getAllContigDisplayNames());
    existingNames.remove(chunkedFile.getContigDisplayName(contigId));
    if (existingNames.contains(newName)) {
      throw new IllegalArgumentException("Contig name must be unique. Name '" + newName + "' already exists.");
    }
  }

  private void validateScaffoldRename(final @NotNull ChunkedFile chunkedFile, final long scaffoldId, final @NotNull String newName) {
    if (newName.isBlank()) {
      return;
    }
    final var existingNames = new HashSet<>(chunkedFile.getAllScaffoldDisplayNames());
    existingNames.remove(chunkedFile.getScaffoldDisplayName(scaffoldId));
    if (existingNames.contains(newName)) {
      throw new IllegalArgumentException("Scaffold name must be unique. Name '" + newName + "' already exists.");
    }
  }

  private void validateContigMappingImport(final @NotNull ChunkedFile chunkedFile, final @NotNull Map<Integer, String> updates) {
    final var finalNames = new HashMap<Integer, String>();
    chunkedFile.getContigTree().getContigDescriptors().keySet().forEach(id -> finalNames.put(id, chunkedFile.getContigDisplayName(id)));
    updates.forEach((id, name) -> finalNames.put(id, name.isBlank() ? chunkedFile.getContigOriginalName(id) : name));
    final var seen = new HashSet<String>();
    for (final var name : finalNames.values()) {
      if (!seen.add(name)) {
        throw new IllegalArgumentException("Contig name must be unique. Duplicate name '" + name + "' in import.");
      }
    }
  }

  private void validateScaffoldMappingImport(final @NotNull ChunkedFile chunkedFile, final @NotNull Map<Long, String> updates) {
    final var finalNames = new HashMap<Long, String>();
    chunkedFile.getScaffoldTree().getScaffoldList().forEach(tuple -> finalNames.put(tuple.scaffoldDescriptor().scaffoldId(), chunkedFile.getScaffoldDisplayName(tuple.scaffoldDescriptor().scaffoldId())));
    updates.forEach((id, name) -> finalNames.put(id, name.isBlank() ? chunkedFile.getScaffoldOriginalName(id) : name));
    final var seen = new HashSet<String>();
    for (final var name : finalNames.values()) {
      if (!seen.add(name)) {
        throw new IllegalArgumentException("Scaffold name must be unique. Duplicate name '" + name + "' in import.");
      }
    }
  }
}
