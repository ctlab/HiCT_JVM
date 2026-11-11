package ru.itmo.ctlab.hict.hict_server.dto.response.assembly;

import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ScaffoldDescriptorDTOTest {
  private ScaffoldTree.ScaffoldTuple tuple(long id, String name) {
    return new ScaffoldTree.ScaffoldTuple(new ScaffoldDescriptor(id, name, 1000),
      new ScaffoldDescriptor.ScaffoldBordersBP(123, 987));
  }

  @Test void preservesOriginalNameBordersAndSpacerWithOrWithoutAlias() {
    var tuple = tuple(7, "original");
    var original = ScaffoldDescriptorDTO.fromSnapshot(tuple, Map.of());
    assertEquals(new ScaffoldDescriptorDTO(7, "original", "original", 1000,
      new ScaffoldDescriptor.ScaffoldBordersBP(123, 987)), original);
    var alias = ScaffoldDescriptorDTO.fromSnapshot(tuple, Map.of(7L, "renamed α"));
    assertEquals("renamed α", alias.scaffoldName());
    assertEquals("original", alias.scaffoldOriginalName());
    assertEquals(original.scaffoldBordersBP(), alias.scaffoldBordersBP());
    assertEquals(original.spacerLength(), alias.spacerLength());
  }

  @Test void resolvesAliasesByLongIdAndObservesRenameRemovalWithoutCaching() {
    var overrides = new HashMap<Long, String>();
    long id = (long) Integer.MAX_VALUE + 10;
    var first = tuple(id, "same_name");
    var second = tuple(id + 1, "same_name");
    overrides.put(id, "alias");
    assertEquals("alias", ScaffoldDescriptorDTO.fromSnapshot(first, overrides).scaffoldName());
    assertEquals("same_name", ScaffoldDescriptorDTO.fromSnapshot(second, overrides).scaffoldName());
    overrides.remove(id);
    assertEquals("same_name", ScaffoldDescriptorDTO.fromSnapshot(first, overrides).scaffoldName());
    var detached = new ScaffoldTree.ScaffoldTuple(first.scaffoldDescriptor(), null);
    assertNull(ScaffoldDescriptorDTO.fromSnapshot(detached, overrides).scaffoldBordersBP());
  }
}
