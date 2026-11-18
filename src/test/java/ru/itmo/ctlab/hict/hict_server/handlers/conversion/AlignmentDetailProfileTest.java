package ru.itmo.ctlab.hict.hict_server.handlers.conversion;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
class AlignmentDetailProfileTest {
  @Test void tinyRepeatWindowsUseShortSeedsWhileLargerWindowsKeepTheBoundedSeededProfile() {
    for (int length : new int[]{1,10,12,64}) {
      var profile = AlignmentDetailProfile.forWindows(length,12);
      assertEquals("short-window-v1", profile.name());
      var command = profile.command(Path.of("aligner"),Path.of("row.fa"),Path.of("col.fa"));
      assertEquals("2",command.get(command.indexOf("-k")+1));
      for (String flag : List.of("-P","-c","--eqx")) assertTrue(command.contains(flag));
    }
    for (int length : new int[]{65,512,4096}) {
      var profile=AlignmentDetailProfile.forWindows(length,12);
      assertEquals("seeded-window-v1",profile.name());
      assertEquals(7,profile.kmer());assertEquals(12,profile.alignmentScore());
    }
    assertThrows(IllegalArgumentException.class,()->AlignmentDetailProfile.forWindows(0,12));
    assertThrows(IllegalArgumentException.class,()->AlignmentDetailProfile.forWindows(12,4097));
  }
}
