package ru.itmo.ctlab.hict.hict_server.jobs;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_server.util.ProcessTree;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BackgroundJobsTest {
  @Test void downloadResultNamesDoNotChangeTheirEndpointsAndRemainOptionalForOlderServices() {
    var named = new BackgroundJob.Output("/convert/download/id", "download", null, null, null, null, "assembly.mcool");
    var json = JsonObject.mapFrom(named);
    assertEquals("/convert/download/id", json.getString("filename"));
    assertEquals("assembly.mcool", json.getString("displayName"));
    assertFalse(JsonObject.mapFrom(new BackgroundJob.Output("track.bed", "open_tracks", null)).containsKey("displayName"));
  }
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

  @RepeatedTest(20) void stopsNativeChildProcessTree() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(Files.isExecutable(Path.of("/bin/sh")));
    var process = new ProcessBuilder("/bin/sh", "-c", "sleep 120 & wait").start();
    List<ProcessHandle> children = List.of();
    try {
      for (int attempt = 0; attempt < 100 && children.isEmpty(); attempt++) {
        children = process.descendants().toList();
        Thread.sleep(10);
      }
      assertFalse(children.isEmpty());
      ProcessTree.stop(process);
      assertTrue(process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS));
      for (var child : children) {
        // destroyForcibly requests termination; waiting for the parent does not
        // imply every descendant has already exited on another CPU.
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        boolean exited = hasExited(child);
        while (!exited && System.nanoTime() < deadline) {
          Thread.sleep(10);
          exited = hasExited(child);
        }
        // A terminated process can be reaped between two readings of /proc.
        assertTrue(exited, "Child still running after cancellation: " + child.pid());
      }
    } finally {
      // Retain handles because an exited parent no longer reports its children.
      for (var child : children) child.destroyForcibly();
      ProcessTree.stop(process);
    }
  }

  private static boolean hasExited(ProcessHandle child) {
    if (!child.isAlive()) return true;
    // Unix can retain a terminated child as a zombie until init reaps it.
    var stat = Path.of("/proc", Long.toString(child.pid()), "stat");
    try {
      var status = Files.readString(stat);
      return status.substring(status.lastIndexOf(')') + 2).startsWith("Z ");
    } catch (java.io.IOException ignored) {
      // Linux can report ENOENT or ESRCH while reaping; macOS has no /proc.
      // A failed status read alone is never accepted as proof of termination.
      return !child.isAlive();
    }
  }
}
