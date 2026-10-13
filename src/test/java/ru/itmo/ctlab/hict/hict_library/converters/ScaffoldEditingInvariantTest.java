package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ScaffoldEditingInvariantTest {
  @TempDir Path dir;
  @Test void nativeSearchJobRunsThroughHttpAndSavesVerifiableProvenance() throws Exception {
    var executable = System.getenv("HICT_SEQKIT_BIN");
    org.junit.jupiter.api.Assumptions.assumeTrue(executable != null && java.nio.file.Files.isExecutable(Path.of(executable)), "Set HICT_SEQKIT_BIN for native HTTP integration");
    var vertx = io.vertx.core.Vertx.vertx();
    var sizes = new java.util.EnumMap<ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.RequestPriority,
      ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.PoolSizing>(ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.RequestPriority.class);
    for (var priority : ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.RequestPriority.values())
      sizes.put(priority, new ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.PoolSizing(1, 1));
    var scheduler = new ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler(vertx,
      new ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.SchedulerConfig(5, 16, 10, sizes));
    try (var f = fixture(); var handler = new ru.itmo.ctlab.hict.hict_server.handlers.conversion.SequenceAnalysisHandlersHolder(vertx)) {
      var fasta = dir.resolve("http.fa");
      java.nio.file.Files.writeString(fasta, ">A\n" + "A".repeat(6000) + "\n>B\n" + "C".repeat(6000) + "\n>C\n" + "G".repeat(6000) + "\n");
      var shared = vertx.sharedData().getLocalMap("hict_server");
      shared.put("dataDirectory", new ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers.PathWrapper(dir));
      shared.put("chunkedFile", new ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers.ChunkedFileWrapper(f));
      shared.put("linkedFastaPrimaryPath", new ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers.PathWrapper(fasta));
      shared.put("requestTaskScheduler", new ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers.RequestTaskSchedulerWrapper(scheduler));
      var router = io.vertx.ext.web.Router.router(vertx); router.route().handler(io.vertx.ext.web.handler.BodyHandler.create());
      handler.addHandlersToRouter(router);
      router.route().failureHandler(ctx -> ctx.response().setStatusCode(ctx.failure() instanceof io.vertx.ext.web.handler.HttpException e ? e.getStatusCode() : 500)
        .end(new io.vertx.core.json.JsonObject().put("error", ctx.failure().getMessage()).encode()));
      var server = vertx.createHttpServer().requestHandler(router).listen(0, "127.0.0.1").toCompletionStage().toCompletableFuture().get();
      var client = java.net.http.HttpClient.newHttpClient();
      String root = "http://127.0.0.1:" + server.actualPort();
      var invalid = java.net.http.HttpRequest.newBuilder(java.net.URI.create(root + "/sequence/search/jobs"))
        .header("content-type", "application/json").POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}" )).build();
      assertEquals(400, client.send(invalid, java.net.http.HttpResponse.BodyHandlers.ofString()).statusCode());
      String body = new io.vertx.core.json.JsonObject().put("queries", ">across join\nAACCC").put("bothStrands", true)
        .put("scope", "selection").put("startBp", 5995).put("endBp", 6005).put("mode", "exact").put("threads", 1).encode();
      var request = java.net.http.HttpRequest.newBuilder(java.net.URI.create(root + "/sequence/search/jobs"))
        .header("content-type", "application/json").POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build();
      var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
      assertEquals(202, response.statusCode(), response.body());
      var submitted = new io.vertx.core.json.JsonObject(response.body());
      var list = java.net.http.HttpRequest.newBuilder(java.net.URI.create(root + "/sequence/jobs/list"))
        .POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}")).build();
      io.vertx.core.json.JsonObject result = null;
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
        var rows = new io.vertx.core.json.JsonArray(client.send(list, java.net.http.HttpResponse.BodyHandlers.ofString()).body());
        result = rows.getJsonObject(0);
        if (List.of("finished", "failed", "cancelled").contains(result.getString("status"))) break;
        Thread.sleep(50);
      }
      assertNotNull(result); assertEquals("finished", result.getString("status"), result.encode());
      assertEquals(1L, result.getLong("hits")); assertEquals(2L, result.getLong("fragments"));
      var output = dir.resolve(result.getString("outputFile")); assertTrue(java.nio.file.Files.isRegularFile(output));
      // Terminal status is published only after the provenance file is saved.
      var provenancePath = output.resolveSibling("job.json");
      var provenance = new io.vertx.core.json.JsonObject(java.nio.file.Files.readString(provenancePath));
      assertEquals("finished", provenance.getString("status"));
      assertEquals(submitted.getString("snapshot"), provenance.getString("snapshot"));
      var actual = java.security.MessageDigest.getInstance("SHA-256").digest(java.nio.file.Files.readAllBytes(output.resolveSibling("assembly-chunks.fasta")));
      assertEquals(java.util.HexFormat.of().formatHex(actual), provenance.getString("preparedInputSha256"));
      assertEquals(64, provenance.getString("toolSha256").length());
      server.close().toCompletionStage().toCompletableFuture().get();
    } finally { scheduler.close(); vertx.close().toCompletionStage().toCompletableFuture().get(); }
  }
  @Test void frozenSequenceAndOverlappingChunksSurviveLaterDebrisEdits() throws Exception {
    try (var f = fixture()) {
      var fasta = dir.resolve("frozen.fa");
      java.nio.file.Files.writeString(fasta, ">A\n" + "A".repeat(6000) + "\n>B\n" + "C".repeat(6000) + "\n>C\n" + "G".repeat(6000) + "\n");
      f.scaffoldingOperations().reverseSelectionRangeBp(0, 6000);
      var captured = ru.itmo.ctlab.hict.hict_library.assembly.AssemblySequenceSnapshot.capture(f).slice(5995, 6005);
      f.scaffoldingOperations().moveRegionToDebris(0, 18000, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      var out = new java.io.ByteArrayOutputStream();
      f.getFastaProcessor().writeSnapshot(fasta, out, captured, "captured");
      assertEquals(">captured\nTTTTTCCCCC\n", out.toString());
      out.reset(); f.getFastaProcessor().writeSearchChunks(fasta, out, captured, 4, 3);
      assertEquals(">hict_chunk_0_4\nTTTTTCC\n>hict_chunk_4_8\nTCCCCC\n>hict_chunk_8_10\nCC\n", out.toString());
    }
  }

  @Test void sourceAnchoredSearchTracksExplainEditedMatchesAndRestoreOnUndo() throws Exception {
    var tracks = new ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager(dir, dir.resolve("processed"));
    try (var f = fixture()) {
      var snapshot = ru.itmo.ctlab.hict.hict_library.assembly.AssemblySequenceSnapshot.capture(f);
      var path = snapshot.slice(5000, 7000);
      var metadata = new ru.itmo.ctlab.hict.hict_server.tracks.SequenceSearchMetadata("hit1", "query1", "cross-join motif", "+", path, snapshot.fingerprint());
      var text = new StringBuilder();
      for (var p : path.fragments()) text.append(p.source()).append('\t').append(p.start()).append('\t').append(p.end())
        .append("\tquery1\t0\t+\t").append(p.start()).append('\t').append(p.end()).append("\t30,110,190\t1\t")
        .append(p.length()).append("\t0\t").append(metadata.encode()).append('\n');
      java.nio.file.Files.writeString(dir.resolve("hits.bed"), text);
      tracks.openTrack("hits.bed", "Search", "#1e6ebe");
      var initial = f.snapshotAssembly();
      var result = tracks.queryVisibleTracks(f, 0, 18000, 18, 1000, QueryLengthUnit.BASE_PAIRS);
      assertEquals(2, result.getTracks().getFirst().getBins().size());
      for (var hit : result.getTracks().getFirst().getBins()) {
        assertEquals("intact", hit.getAttributes().get("matchStatus"));
        assertEquals("cross-join motif", hit.getLabel());
        assertFalse(hit.getAttributes().containsKey("field13"));
      }
      f.scaffoldingOperations().scaffoldRegion(0, 12000, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS, null);
      f.scaffoldingOperations().reverseSelectionRangeBp(0, 12000);
      result = tracks.queryVisibleTracks(f, 0, 18000, 18, 1000, QueryLengthUnit.BASE_PAIRS);
      for (var hit : result.getTracks().getFirst().getBins()) {
        assertEquals("-", hit.getStrand());
        assertEquals("intact", hit.getAttributes().get("matchStatus"));
      }
      f.restoreAssembly(initial);
      f.scaffoldingOperations().moveSelectionRangeBp(6000, 12000, 18000);
      result = tracks.queryVisibleTracks(f, 0, 18000, 18, 1000, QueryLengthUnit.BASE_PAIRS);
      for (var hit : result.getTracks().getFirst().getBins()) {
        assertEquals("rearranged", hit.getAttributes().get("matchStatus"));
        assertTrue(hit.getLabel().contains("part of match"));
        assertEquals("210,105,20", hit.getAttributes().get("itemRgb"));
      }
      f.restoreAssembly(initial);
      f.scaffoldingOperations().moveRegionToDebris(6000, 12000, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      result = tracks.queryVisibleTracks(f, 0, 12000, 12, 1000, QueryLengthUnit.BASE_PAIRS);
      assertEquals(1, result.getTracks().getFirst().getBins().size());
      assertEquals("partial", result.getTracks().getFirst().getBins().getFirst().getAttributes().get("matchStatus"));
      f.restoreAssembly(initial);
      result = tracks.queryVisibleTracks(f, 5000, 5500, 1, 1000, QueryLengthUnit.BASE_PAIRS);
      assertEquals("intact", result.getTracks().getFirst().getBins().getFirst().getAttributes().get("matchStatus"), "Viewport clipping must not mark the sequence broken");
    } finally { tracks.close(); }
  }

  @Test void nestedBedFeaturesRemainVisibleWhenTheirStartIsOutsideViewport() throws Exception {
    var tracks = new ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager(dir, dir.resolve("processed"));
    try (var f = fixture()) {
      java.nio.file.Files.writeString(dir.resolve("nested.bed"), "A\t0\t5900\tlong\t0\t+\nA\t1000\t1100\tshort1\t0\t+\nA\t2000\t2100\tshort2\t0\t+\n");
      tracks.openTrack("nested.bed", "Nested", "#123456");
      var result = tracks.queryVisibleTracks(f, 5000, 5500, 1, 1000, QueryLengthUnit.BASE_PAIRS);
      assertEquals(List.of("long"), result.getTracks().getFirst().getBins().stream().map(b -> b.getLabel()).toList());
    } finally { tracks.close(); }
  }
  @Test void streamingFastaPreservesMaskingGapsAndCompressedSourceAcrossEdits() throws Exception {
    try(var f=fixture()) {
      var path=dir.resolve("masked.fa");String a="aCGTrymkSWBDHVN".repeat(400);
      java.nio.file.Files.writeString(path,">A\n"+a+"\n>B\n"+"C".repeat(6000)+"\n>C\n"+"G".repeat(6000)+"\n");
      assertEquals(6000,a.length());
      var gzip=dir.resolve("masked.fa.gz");
      try(var output=new java.util.zip.GZIPOutputStream(java.nio.file.Files.newOutputStream(gzip))){output.write(java.nio.file.Files.readAllBytes(path));}
      var processor=f.getFastaProcessor();
      for(var source:List.of(path,gzip)) {
        assertEquals(">selection\naCGTrymkSWBDHVN\n",processor.exportInterval(source,0,15,"selection"));
        var out=new java.io.ByteArrayOutputStream();processor.writeAssembly(source,out,true,0);
        assertEquals(processor.exportAssemblyPreservingGaps(path),out.toString(java.nio.charset.StandardCharsets.UTF_8));
      }
      var bp=ResolutionDescriptor.fromBpResolution(0,f);
      f.getScaffoldingOperations().reverseSelectionRangeBp(0,6000);
      assertEquals(">selection\nNBDHVWSmkryACGt\n",processor.exportInterval(path,0,15,"selection"));
      assertEquals(processor.exportInterval(path,0,15,"selection"),processor.exportInterval(gzip,0,15,"selection"));
      f.getScaffoldingOperations().moveRegionToDebris(0,18000,bp,QueryLengthUnit.BASE_PAIRS);
      assertEquals("",processor.exportAssembly(path,500));
      assertThrows(IllegalArgumentException.class,()->processor.exportInterval(path,0,1,"selection"));
    }
  }
  private ChunkedFile fixture() {
    HDF5LibraryInitializer.initializeHDF5Library();
    var prefix=UUID.randomUUID().toString(); var input=dir.resolve(prefix+".mcool"); var output=dir.resolve(prefix+".hict.hdf5");
    try(var w=HDF5Factory.open(input.toFile())) {
      w.object().createGroup("/resolutions");
      for(long res: new long[]{1000,4000,10000}) {
        String p="/resolutions/"+res;
        for(String g: new String[]{"", "/chroms", "/bins", "/pixels", "/indexes"}) w.object().createGroup(p+g);
        w.string().writeArray(p+"/chroms/name",new String[]{"A","B","C"});
        w.int64().writeArray(p+"/chroms/length",new long[]{6000,6000,6000});
        int n=(int)((6000+res-1)/res),size=n*3;
        long[] chr=new long[size],start=new long[size],end=new long[size],offset=new long[size+1];
        double[] weights=new double[size];Arrays.fill(weights,1);
        var rows=new ArrayList<Long>();var cols=new ArrayList<Long>();var counts=new ArrayList<Long>();
        for(int i=0;i<size;i++) {
          chr[i]=i/n;start[i]=(i%n)*res;end[i]=Math.min(6000,start[i]+res);offset[i]=rows.size();
          for(int j=i;j<size;j++){rows.add((long)i);cols.add((long)j);counts.add(1L+i*100+j);}
        }
        offset[size]=rows.size();
        w.int64().writeArray(p+"/bins/chrom",chr);w.int64().writeArray(p+"/bins/start",start);w.int64().writeArray(p+"/bins/end",end);
        w.float64().writeArray(p+"/bins/weight",weights);
        w.int64().writeArray(p+"/indexes/chrom_offset",new long[]{0,n,2*n,3*n});
        w.int64().writeArray(p+"/indexes/bin1_offset",offset);
        w.int64().writeArray(p+"/pixels/bin1_id",rows.stream().mapToLong(Long::longValue).toArray());
        w.int64().writeArray(p+"/pixels/bin2_id",cols.stream().mapToLong(Long::longValue).toArray());
        w.int64().writeArray(p+"/pixels/count",counts.stream().mapToLong(Long::longValue).toArray());
      }
    }
    new McoolToHictConverter().convert(new ConversionOptions(input,output,List.of(1000L,4000L,10000L),64,0,
      ConversionOptions.CompressionAlgorithm.DEFLATE,ConversionOptions.NO_AGP,false,2,false,ConversionOptions.ExportMode.INTERNAL), ignored->{});
    return new ChunkedFile(new ChunkedFile.ChunkedFileOptions(output,1,2));
  }
  @Test void singletonInitializationWholeScaffoldEditsUngroupAndExplicitUnscaffold() {
    try(var f=fixture()) {
      var bp=ResolutionDescriptor.fromResolutionOrder(0);var ops=f.scaffoldingOperations();
      assertEquals(List.of("scf_A","scf_B","scf_C"),f.getScaffoldTree().getScaffoldList().stream().map(s->s.scaffoldDescriptor().scaffoldName()).toList());
      var initial=f.snapshotAssembly();
      f.setContigNameOverride(0,"renamed");
      assertEquals("scf_A",f.getScaffoldTree().getScaffoldList().getFirst().scaffoldDescriptor().scaffoldName());
      ops.scaffoldRegion(0,12000,bp,QueryLengthUnit.BASE_PAIRS,null);
      ops.reverseSelectionRangeBp(6500,7000);
      assertEquals(List.of("B","A","C"),f.getContigTree().getOrderedContigList().stream().map(c->c.descriptor().getContigName()).toList());
      ops.moveSelectionRangeBp(0,1,18000);
      assertEquals(List.of("C","B","A"),f.getContigTree().getOrderedContigList().stream().map(c->c.descriptor().getContigName()).toList());
      ops.unscaffoldRegion(7000,8000,bp,QueryLengthUnit.BASE_PAIRS);
      assertEquals(3,f.getScaffoldTree().getScaffoldList().size());
      ops.unscaffoldAll();assertTrue(f.getScaffoldTree().getScaffoldList().isEmpty());
      f.restoreAssembly(initial);assertEquals(initial,f.snapshotAssembly());
      ops.moveRegionToDebris(10,20,bp,QueryLengthUnit.BASE_PAIRS);
      assertEquals(List.of("B","C"),f.getContigTree().getOrderedContigList().stream().map(c -> c.descriptor().getContigName()).toList());
      f.restoreAssembly(initial);assertEquals(initial,f.snapshotAssembly());
    }
  }
  @Test void debrisExcludesAtusMatrixAndFastaAndCanRestoreEmptyAssembly() throws Exception {
    try (var f = fixture()) {
      var bp = ResolutionDescriptor.fromResolutionOrder(0);
      var ops = f.scaffoldingOperations();
      var fasta = dir.resolve("source.fasta");
      java.nio.file.Files.writeString(fasta, ">A\n" + "A".repeat(6000) + "\n>B\n" + "C".repeat(6000) + "\n>C\n" + "G".repeat(6000) + "\n");
      var initial = f.snapshotAssembly();
      ops.scaffoldRegion(0, 12000, bp, QueryLengthUnit.BASE_PAIRS, null);
      ops.moveRegionToDebris(6010, 6020, bp, QueryLengthUnit.BASE_PAIRS);
      assertEquals(List.of("C"), f.getAssemblyInfo().contigs().stream().map(c -> c.descriptor().getContigName()).toList());
      assertEquals(6000, f.getContigTree().getLengthInUnits(QueryLengthUnit.BASE_PAIRS, bp));
      assertEquals(6000, f.getScaffoldTree().getScaffoldList().getFirst().scaffoldBordersBP().endBP());
      var processor = new ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor(f);
      var exported = dir.resolve("debris.fasta");
      java.nio.file.Files.writeString(exported, processor.exportAssemblyPreservingGaps(fasta));
      assertEquals(Map.of("scf_C", "G".repeat(6000)), ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor.readSequenceContents(exported));
      for (long res : new long[]{1000, 4000, 10000}) {
        var r = ResolutionDescriptor.fromBpResolution(res, f);
        int n = (int)((6000 + res - 1) / res);
        for (boolean visible : new boolean[]{false, true}) {
          if (visible && res == 10000) continue;
          assertEquals(n, f.getMatrixQueries().getATUsForRange(r, 0, n + 3, visible).stream().mapToInt(ATUDescriptor::getLength).sum());
          var matrix = f.getMatrixQueries().getSubmatrix(r, 0, 0, n + 2, n + 2, visible).matrix();
          for (int i = 0; i < n + 2; i++) for (int j = 0; j < n + 2; j++)
            assertEquals(i < n && j < n ? 1L + (2*n+Math.min(i,j))*100 + 2*n+Math.max(i,j) : 0,
              matrix.getAsDouble(i,j));
        }
      }
      var debris = f.snapshotAssembly();
      ops.moveRegionToDebris(0, 6000, bp, QueryLengthUnit.BASE_PAIRS);
      assertTrue(f.getAssemblyInfo().contigs().isEmpty());
      assertTrue(f.getScaffoldTree().getScaffoldList().isEmpty());
      assertEquals("", processor.exportAssemblyPreservingGaps(fasta));
      var r = ResolutionDescriptor.fromBpResolution(1000, f);
      assertTrue(f.getMatrixQueries().getATUsForRange(r,0,100,false).isEmpty());
      assertEquals(0, f.getMatrixQueries().getSubmatrix(r,0,0,3,5,false).matrix().getAsDouble(1,2));
      f.restoreAssembly(initial); assertEquals(initial, f.snapshotAssembly());
      f.restoreAssembly(debris); assertEquals(debris, f.snapshotAssembly());
    }
  }

  @Test void partialOverlaySynchronizesSourceIntervalsInBothDirections() throws Exception {
    var vertx = io.vertx.core.Vertx.vertx();
    try (var primary = fixture(); var secondary = fixture()) {
      io.vertx.core.shareddata.LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
      map.put("chunkedFile", new ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers.ChunkedFileWrapper(primary));
      map.put("chunkedFileSecondary", new ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers.ChunkedFileWrapper(secondary));
      for (String side : List.of("PRIMARY", "SECONDARY")) {
        var source = side.equals("PRIMARY") ? primary : secondary;
        var target = side.equals("PRIMARY") ? secondary : primary;
        map.put("assemblyInfoSource", side);
        source.importAGP(new java.io.StringReader("s1 1 2500 1 W A 1 2500 +\ns1 2501 5500 2 W B 3001 6000 -\ns2 1 3500 1 W A 2501 6000 -\ns2 3501 6500 2 W B 1 3000 +\nunscaffolded_C 1 6000 1 W C 1 6000 +\n"));
        ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder.synchronizeOverlayAssemblyForSharedState(map);
        assertOverlayMatches(source, target);
        // Excluding the remaining unplaced sequence must shorten both sources.
        source.importAGP(new java.io.StringReader("s1 1 2500 1 W A 1 2500 +\ns2 1 3000 1 W B 3001 6000 -\n"));
        ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder.synchronizeOverlayAssemblyForSharedState(map);
        assertOverlayMatches(source, target);
        source.importAGP(new java.io.StringReader("unscaffolded_A 1 6000 1 W A 1 6000 +\n"),
          ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.SCAFFOLDED_ONLY);
        ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder.synchronizeOverlayAssemblyForSharedState(map);
        assertTrue(source.getAssemblyInfo().contigs().isEmpty());
        assertTrue(target.getAssemblyInfo().contigs().isEmpty());
        assertOverlayMatches(source, target);
      }
    } finally { vertx.close().toCompletionStage().toCompletableFuture().get(); }
  }

  private void assertOverlayMatches(ChunkedFile source, ChunkedFile target) {
    assertEquals(source.getAssemblyInfo().contigs().stream().map(c -> List.of(c.descriptor().getContigNameInSourceFASTA(), c.descriptor().getOffsetInSourceFASTA(), c.descriptor().getLengthBp(), c.direction())).toList(),
      target.getAssemblyInfo().contigs().stream().map(c -> List.of(c.descriptor().getContigNameInSourceFASTA(), c.descriptor().getOffsetInSourceFASTA(), c.descriptor().getLengthBp(), c.direction())).toList());
    for (long res : new long[]{1000,4000,10000}) for (boolean visible : new boolean[]{false,true}) {
      var r = ResolutionDescriptor.fromBpResolution(res, source);
      var units = visible ? QueryLengthUnit.PIXELS : QueryLengthUnit.BINS;
      long length = source.getContigTree().getLengthInUnits(units,r);
      assertEquals(length, target.getContigTree().getLengthInUnits(units,r));
      var expected = source.matrixQueries().getSubmatrix(r,0,0,length+2,length+2,visible).matrix();
      var actual = target.matrixQueries().getSubmatrix(r,0,0,length+2,length+2,visible).matrix();
      for (int i=0;i<length+2;i++) for(int j=0;j<length+2;j++) assertEquals(expected.getAsDouble(i,j),actual.getAsDouble(i,j));
    }
  }

  @Test void agpScopeChoiceExcludesExplicitUnscaffoldedAndOmittedRegions() throws Exception {
    try (var f = fixture()) {
      String agp = "s 1 2500 1 W A 1 2500 +\nunscaffolded_B 1 6000 1 W B 1 6000 -\n";
      var before = f.snapshotAssembly();
      var preview = f.importAGP(new java.io.StringReader(agp), ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.REQUIRE_CHOICE);
      assertTrue(preview.requiresChoice()); assertEquals(1, preview.unscaffoldedPieces()); assertEquals(6000,preview.unscaffoldedBp());assertEquals(9500,preview.omittedBp());
      assertEquals(before,f.snapshotAssembly());
      f.importAGP(new java.io.StringReader(agp), ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.SCAFFOLDED_ONLY);
      assertEquals(1,f.getAssemblyInfo().contigs().size());assertEquals(2500,f.getAssemblyInfo().contigs().getFirst().descriptor().getLengthBp());
      assertEquals(2500,f.getScaffoldTree().getScaffoldList().getFirst().scaffoldBordersBP().endBP());
      var fasta = dir.resolve("scope.fasta");java.nio.file.Files.writeString(fasta, ">A\n"+"A".repeat(6000)+"\n");
      // No B/C FASTA source even exists: exporting selected A must not access them.
      var processor = new ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor(f);
      var exported = dir.resolve("scope-result.fasta");java.nio.file.Files.writeString(exported,processor.exportAssemblyPreservingGaps(fasta));
      assertEquals(Map.of("s","A".repeat(2500)),ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor.readSequenceContents(exported));
      var selected=f.snapshotAssembly();f.restoreAssembly(before);f.restoreAssembly(selected);assertEquals(selected,f.snapshotAssembly());
      f.importAGP(new java.io.StringReader(agp), ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.APPEND_UNPLACED);
      assertEquals(18000,f.getContigTree().getLengthInUnits(QueryLengthUnit.BASE_PAIRS,ResolutionDescriptor.fromResolutionOrder(0)));
      assertEquals(ContigDirection.REVERSED,f.getAssemblyInfo().contigs().get(1).direction());
    }
  }

  @Test void invalidExportNamesCannotCorruptTheAssembly() {
    try (var f = fixture()) {
      var before = f.snapshotAssembly();
      var id = f.getScaffoldTree().getScaffoldList().getFirst().scaffoldDescriptor().scaffoldId();
      for (String invalid : new String[]{"two words", "tab\tname", "new\nline", "a\u00a0b", "a\u0000b"}) {
        assertThrows(IllegalArgumentException.class, () -> f.setContigNameOverride(0, invalid));
        assertThrows(IllegalArgumentException.class, () -> f.setScaffoldNameOverride(id, invalid));
        assertEquals(before, f.snapshotAssembly());
      }
      f.setContigNameOverride(0, "контig_α");
      assertEquals("контig_α", f.getContigDisplayName(0));
    }
  }

  @Test void fastaCompatibilityUsesSourceIntervalsAfterReversalAndSplit() throws Exception {
    try (var f = fixture()) {
      var path = dir.resolve("source.fasta");
      java.nio.file.Files.writeString(path, ">C\n" + "G".repeat(6000) + "\n>A\n" + "A".repeat(6000) + "\n>B\n" + "C".repeat(6000) + "\n");
      var processor = new ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor(f);
      assertFalse(processor.analyzeLinkCandidate(path).hasWarnings());
      assertTrue(processor.buildSourceNameAliases(path).isEmpty(), "Exact names must win over same-length positional matches");
      var exported = dir.resolve("exported.fasta");
      java.nio.file.Files.writeString(exported, processor.exportAssemblyPreservingGaps(path));
      var sequences = ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor.readSequenceContents(exported);
      assertEquals("A".repeat(6000), sequences.get("scf_A"));
      assertEquals("C".repeat(6000), sequences.get("scf_B"));
      assertEquals("G".repeat(6000), sequences.get("scf_C"));
      f.scaffoldingOperations().reverseSelectionRangeBp(0, 18000);
      f.scaffoldingOperations().splitContigAtBin(1000, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      var report = processor.analyzeLinkCandidate(path);
      assertTrue(report.sourceIntervalsCovered());
      assertFalse(report.hasWarnings());
      java.nio.file.Files.writeString(path, ">A\nA\n>B\nC\n>C\nG\n");
      assertTrue(processor.analyzeLinkCandidate(path).hasWarnings());
      java.nio.file.Files.writeString(path, ">A\nA\n>A\nC\n");
      assertThrows(IllegalArgumentException.class, () -> processor.analyzeLinkCandidate(path));
      assertThrows(IllegalArgumentException.class, () -> ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor.readSequenceContents(path));
    }
  }

  @Test void movementInsideTargetScaffoldSnapsBothTreesToItsStart() {
    try (var f = fixture()) {
      var ops = f.scaffoldingOperations();
      var bp = ResolutionDescriptor.fromResolutionOrder(0);
      ops.scaffoldRegion(0, 12000, bp, QueryLengthUnit.BASE_PAIRS, null);
      ops.moveSelectionRangeBp(12000, 18000, 7000);
      assertEquals(List.of("C", "A", "B"), f.getContigTree().getOrderedContigList().stream()
        .map(c -> c.descriptor().getContigName()).toList());
      var scaffolds = f.getScaffoldTree().getScaffoldList();
      assertEquals(2, scaffolds.size());
      assertEquals(6000, scaffolds.getFirst().scaffoldBordersBP().endBP());
      assertEquals(6000, scaffolds.getLast().scaffoldBordersBP().startBP());
      assertEquals(18000, scaffolds.getLast().scaffoldBordersBP().endBP());
      var before = f.snapshotAssembly();
      ops.moveSelectionRangeBp(6001, 6002, 10000);
      assertEquals(before, f.snapshotAssembly(), "Destination within source scaffold is a no-op");
    }
  }

  @Test void splitPreservesScaffoldSidesAndZeroBinPiecesAtCoarserResolutions() {
    try(var f=fixture()) {
      var bp=ResolutionDescriptor.fromResolutionOrder(0);var ops=f.scaffoldingOperations();var initial=f.snapshotAssembly();
      for(boolean reversed:new boolean[]{false,true}) for(long cut:new long[]{1000,4000}) {
        f.restoreAssembly(initial);ops.scaffoldRegion(0,18000,bp,QueryLengthUnit.BASE_PAIRS,null);
        if(reversed) ops.reverseSelectionRangeBp(0,18000);
        var before=f.snapshotAssembly();
        var original=new HashMap<Long,double[][]>();
        for(long res:new long[]{1000,4000,10000}) {
          int n=(int)f.getContigTree().getLengthInUnits(QueryLengthUnit.BINS,ResolutionDescriptor.fromBpResolution(res,f));
          var m=f.getMatrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(res,f),0,0,n,n,false).matrix();
          double[][] values=new double[n][n];for(int i=0;i<n;i++)for(int j=0;j<n;j++)values[i][j]=m.getAsDouble(i,j);original.put(res,values);
        }
        ops.splitContigAtBin(6000+cut,bp,QueryLengthUnit.BASE_PAIRS);
        var scaffolds=f.getScaffoldTree().getScaffoldList();assertEquals(2,scaffolds.size());
        assertEquals(0,scaffolds.get(0).scaffoldBordersBP().startBP());
        assertEquals(6000+cut,scaffolds.get(0).scaffoldBordersBP().endBP());
        assertEquals(6000+cut,scaffolds.get(1).scaffoldBordersBP().startBP());assertEquals(17000,scaffolds.get(1).scaffoldBordersBP().endBP());
        assertEquals(4,f.getContigTree().getOrderedContigList().size());
        for(long res:new long[]{1000,4000,10000}) {
          int n=original.get(res).length-(res==1000?1:0),removed=6+(int)(cut/1000);
          var matrix=f.getMatrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(res,f),0,0,n,n,false).matrix();
          for(int i=0;i<n;i++)for(int j=0;j<n;j++)assertEquals(original.get(res)[i+(res==1000&&i>=removed?1:0)][j+(res==1000&&j>=removed?1:0)],matrix.getAsDouble(i,j),"resolution="+res+" reversed="+reversed+" cut="+cut);
        }
        var after=f.snapshotAssembly();f.restoreAssembly(before);assertEquals(before,f.snapshotAssembly());f.restoreAssembly(after);assertEquals(after,f.snapshotAssembly());
      }
    }
  }
}
