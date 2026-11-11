package ru.itmo.ctlab.hict.hict_server.dto.response.assembly;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CompactAssemblyInfoDTOTest {
  private ContigDescriptor descriptor() {
    return new ContigDescriptor(7, "original α\"", 1701, List.of(0L, 4L),
      List.of(ContigHideType.HIDDEN, ContigHideType.SHOWN), List.of(List.of(), List.of()), "source😀", 123);
  }

  private AssemblyInfo snapshot(ContigDescriptor descriptor) {
    return new AssemblyInfo(List.of(new ContigTree.ContigTuple(descriptor, ContigDirection.REVERSED)),
      List.of(new ScaffoldTree.ScaffoldTuple(new ScaffoldDescriptor(2147483657L, "scaffold", 1000),
        new ScaffoldDescriptor.ScaffoldBordersBP(123, 987)),
        new ScaffoldTree.ScaffoldTuple(new ScaffoldDescriptor(2147483658L, "detached", 0), null)));
  }

  @Test void serializesEveryFieldInTheVersionedColumnOrder() {
    var dto = CompactAssemblyInfoDTO.fromSnapshot(snapshot(descriptor()), new long[]{0, 250, 500},
      Map.of(7, "alias"), Map.of(2147483657L, "scaffold alias"));
    var json = new JsonObject(Json.encodeToBuffer(new CompactAssemblyInfoDTO.Response(dto, 29)));
    var expected = new JsonObject("""
      {"version":29,"assemblyInfo":{"format":"compact-v1","resolutions":[250,500],
        "contigs":[[7,"alias","original α\\\"","source😀",123,0,1701,[0,4],[0,1]]],
        "scaffolds":[[2147483657,"scaffold alias","scaffold",1000,[123,987]],
                     [2147483658,"detached","detached",0,null]]}}
      """);
    assertEquals(expected, json);
    assertEquals(new JsonObject(Json.encode(new CompactAssemblyInfoDTO.Response(dto, 29))), json);
  }

  @Test void ownsResolutionValuesAndObservesSubsequentChangesWithoutStateCache() {
    var descriptor = descriptor();
    var snapshot = snapshot(descriptor);
    var resolutions = new long[]{0, 250, 500};
    var aliases = new HashMap<Integer, String>();
    aliases.put(7, "first alias");
    var first = CompactAssemblyInfoDTO.fromSnapshot(snapshot, resolutions, aliases, Map.of());
    var saved = Json.encode(first);
    aliases.clear();
    descriptor.getLengthBinsAtResolution()[1] = 8;
    descriptor.getPresenceAtResolution().set(1, ContigHideType.SHOWN);
    resolutions[1] = 125;
    assertEquals(saved, Json.encode(first));
    var next = CompactAssemblyInfoDTO.fromSnapshot(snapshot, resolutions, aliases, Map.of());
    var row = new JsonObject(Json.encode(next)).getJsonArray("contigs").getJsonArray(0);
    assertEquals("original α\"", row.getString(1));
    assertEquals(8L, row.getJsonArray(7).getLong(0));
    assertEquals(1, row.getJsonArray(8).getInteger(0));
  }

  @Test void supportsEmptyAssembliesAndNoBinnedResolutions() {
    var dto = CompactAssemblyInfoDTO.fromSnapshot(new AssemblyInfo(List.of(), List.of()), new long[]{0}, Map.of(), Map.of());
    assertEquals(new JsonObject("{\"format\":\"compact-v1\",\"resolutions\":[],\"contigs\":[],\"scaffolds\":[]}"), new JsonObject(Json.encode(dto)));
  }
}
