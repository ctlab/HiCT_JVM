package ru.itmo.ctlab.hict.hict_server.jobs;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_server.util.ProcessTree;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BackgroundJobsTest {
  @TempDir Path directory;

  private BackgroundJob job(String id, String state, long created) {
    return new BackgroundJob(id, "conversion", state, Double.NaN, created, 0L, 0L,
      null, null, List.of());
  }

  @Test void normalizesStatesAndOrdersServicePrefixedIds() {
    var holder = new BackgroundJobsHandlersHolder(List.of(
      new BackgroundJobsHandlersHolder.Service("convert", () -> List.of(job("first", "queued", 1)), id -> true, () -> {}),
      new BackgroundJobsHandlersHolder.Service("sequence", () -> List.of(job("second", "READY", 2)), id -> true, () -> {})
    ));
    var jobs = holder.snapshot();
    assertEquals(List.of("sequence/second", "convert/first"), jobs.stream().map(BackgroundJob::id).toList());
    assertEquals("finished", jobs.getFirst().state());
    assertFalse(jobs.getFirst().canCancel());
    assertTrue(jobs.getLast().canCancel());
    assertNull(jobs.getFirst().progress());
    assertNull(jobs.getFirst().startedAtMs());
    assertEquals("convert", CancelBackgroundJobRequestDTO.fromJson(new JsonObject().put("id", "convert/first")).service());
    assertThrows(IllegalArgumentException.class, () -> CancelBackgroundJobRequestDTO.fromJson(new JsonObject().put("id", "/")));
  }

  @Test void cancelsAllServicesBeforeClosingAndStillClosesAfterFailure() {
    var calls = new ArrayList<String>();
    var holder = new BackgroundJobsHandlersHolder(List.of(
      new BackgroundJobsHandlersHolder.Service("one", () -> List.of(job("a", "running", 1)),
        id -> { calls.add("cancel1"); return true; }, () -> { calls.add("close1"); throw new IllegalStateException(); }),
      new BackgroundJobsHandlersHolder.Service("two", () -> List.of(job("b", "submitted", 2)),
        id -> { calls.add("cancel2"); return true; }, () -> calls.add("close2"))
    ));
    assertThrows(IllegalStateException.class, holder::close);
    assertEquals(List.of("cancel1", "cancel2", "close1", "close2"), calls);
  }

  @Test void incompleteArtifactsStaySeparateAndNeverReplaceWithoutConsent() throws Exception {
    var staging = JobArtifacts.stage(directory, "test");
    var source = Files.writeString(staging.resolve("result.hdf5"), "complete");
    var target = Files.writeString(directory.resolve("result.hdf5"), "old");
    assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> JobArtifacts.publish(source, target, false));
    assertEquals("old", Files.readString(target));
    JobArtifacts.publish(source, target, true);
    assertEquals("complete", Files.readString(target));
    JobArtifacts.remove(staging);
    assertFalse(Files.exists(staging));
  }

  @Test void stopsNativeChildProcessTree() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(Files.isExecutable(Path.of("/bin/sh")));
    var process = new ProcessBuilder("/bin/sh", "-c", "sleep 120 & wait").start();
    try {
      List<ProcessHandle> children = List.of();
      for (int attempt = 0; attempt < 100 && children.isEmpty(); attempt++) {
        children = process.descendants().toList();
        Thread.sleep(10);
      }
      assertFalse(children.isEmpty());
      ProcessTree.stop(process);
      assertTrue(process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS));
      for (var child : children) {
        // Unix can retain a dead child as a zombie until init reaps it.
        if (child.isAlive() && Files.isRegularFile(Path.of("/proc", Long.toString(child.pid()), "stat"))) {
          assertEquals("Z", Files.readString(Path.of("/proc", Long.toString(child.pid()), "stat")).split(" ")[2]);
        } else assertFalse(child.isAlive());
      }
    } finally { ProcessTree.stop(process); }
  }
}
