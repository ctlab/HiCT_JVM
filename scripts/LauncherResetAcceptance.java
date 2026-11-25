import java.awt.Component;
import java.awt.Container;
import java.awt.Robot;
import java.awt.Window;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import javax.imageio.ImageIO;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JSlider;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.Timer;

/** Runs real Launcher actions/dialogs on the private display supplied by the Python driver. */
class LauncherResetAcceptance {
  static Object launcher;
  static JFrame frame;
  static Path output;
  static Process browserSentinel;
  static Process serverSentinel;

  static Object field(String name) throws Exception {
    var field = launcher.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(launcher);
  }

  static Object invoke(String name) throws Exception {
    var method = launcher.getClass().getDeclaredMethod(name);
    method.setAccessible(true);
    return method.invoke(launcher);
  }

  static <T> T edt(Callable<T> action) throws Exception {
    var future = new FutureTask<>(action);
    SwingUtilities.invokeAndWait(future);
    return future.get();
  }

  static JButton button(Container root, String text) {
    for (Component component : root.getComponents()) {
      if (component instanceof JButton b && b.getText().equals(text)) return b;
      if (component instanceof Container c) {
        var found = button(c, text);
        if (found != null) return found;
      }
    }
    return null;
  }

  static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  static boolean awaitServerVersion(java.net.http.HttpClient client, java.net.http.HttpRequest request)
      throws InterruptedException {
    long deadline = System.nanoTime() + 30_000_000_000L;
    while (System.nanoTime() < deadline) {
      try {
        if (client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding()).statusCode() == 200) return true;
      } catch (java.io.IOException ignored) {
        // The child owns the endpoint once the server is ready.
      }
      Thread.sleep(100);
    }
    return false;
  }

  static Properties settings() throws Exception {
    return edt(() -> (Properties) ((Properties) field("settings")).clone());
  }

  static Properties persisted() throws Exception {
    var result = new Properties();
    try (var input = Files.newInputStream(Path.of(System.getenv("HICT_LAUNCHER_CONFIG")))) {
      result.load(input);
    }
    return result;
  }

  static boolean launchBrowserFixture(String script) throws Exception {
    var type = Class.forName("ru.itmo.ctlab.hict.hict_server.launcher.HictLauncherGui$BrowserBundle");
    var constructor = type.getDeclaredConstructor(String.class, String.class, int.class, Path.class, Path.class, List.class);
    constructor.setAccessible(true);
    var bundle = constructor.newInstance("reset-fixture", "electron", 0, output, Path.of("/bin/sh"), List.of("-c", script));
    var method = launcher.getClass().getDeclaredMethod("tryOpenBundledBrowser", type, String.class);
    method.setAccessible(true);
    return (boolean) method.invoke(launcher, bundle, "http://127.0.0.1:" + System.getenv("WEBUI_PORT") + "/");
  }

  static void confirm(String action, String title, boolean accept, String screenshot) throws Exception {
    JButton target = edt(() -> {
      var b = button(frame, action);
      require(b != null && b.isEnabled() && b.isShowing(), "Missing visible enabled action: " + action);
      var bounds = new java.awt.Rectangle(b.getLocationOnScreen(), b.getSize());
      require(frame.getBounds().contains(bounds), "Action is outside the Launcher window: " + action + " " + bounds);
      return b;
    });
    SwingUtilities.invokeLater(target::doClick);
    JDialog dialog = null;
    long deadline = System.nanoTime() + 10_000_000_000L;
    while (dialog == null && System.nanoTime() < deadline) {
      dialog = edt(() -> {
        for (var window : Window.getWindows()) {
          if (window instanceof JDialog d && d.isShowing() && title.equals(d.getTitle())) return d;
        }
        return null;
      });
      if (dialog == null) Thread.sleep(20);
    }
    require(dialog != null, "Confirmation did not open: " + title);
    final var confirmedDialog = dialog;
    require(edt(() -> confirmedDialog.isModal() && confirmedDialog.getOwner() == frame), "Dialog must be modal and parented");
    Thread.sleep(100);
    ImageIO.write(new Robot().createScreenCapture(edt(() -> frame.getBounds())), "png", output.resolve(screenshot).toFile());
    edt(() -> { button(confirmedDialog, accept ? "OK" : "Cancel").doClick(); return null; });
    edt(() -> null);
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    output = Path.of(args[0]).toRealPath();
    require(output.startsWith("/tmp/"), "Acceptance output must be under /tmp");
    require("1".equals(System.getenv("HICT_TEST_PRIVATE_DISPLAY")), "Use the isolated-display driver");
    List<String> results = new ArrayList<>();
    try {
      edt(() -> {
        var type = Class.forName("ru.itmo.ctlab.hict.hict_server.launcher.HictLauncherGui$LauncherWindow");
        var constructor = type.getDeclaredConstructor(CountDownLatch.class);
        constructor.setAccessible(true);
        launcher = constructor.newInstance(new CountDownLatch(1));
        invoke("showWindow");
        frame = (JFrame) field("frame");
        ((Timer) field("statusTimer")).stop();
        button(frame, "Show Configuration").doClick();
        return null;
      });
      var fields = (Map<String, JTextField>) field("fields");
      var expectedData = output.resolve("retained-data").toString();
      var expectedProcessed = output.resolve("retained-processed").toString();
      edt(() -> {
        fields.get("DATA_DIR").setText(expectedData);
        fields.get("PROCESSED_DIR").setText(expectedProcessed);
        fields.get("TILE_SIZE").setText("1024");
        fields.get("HICT_JAVA_OPTS").setText("-Xmx7g");
        ((JSlider) field("logLevelSlider")).setValue(4);
        button(frame, "Save settings").doClick();
        return null;
      });
      Properties before = settings();
      confirm("Reset JVM settings", "Reset JVM settings", false, "jvm-cancel.png");
      require(settings().equals(before) && persisted().equals(before), "Cancel changed JVM settings");
      results.add("JVM reset cancellation leaves memory and persisted settings unchanged");
      confirm("Reset JVM settings", "Reset JVM settings", true, "jvm-confirm.png");
      Properties after = settings();
      require(expectedData.equals(after.getProperty("DATA_DIR")), "JVM reset changed DATA_DIR");
      require(expectedProcessed.equals(after.getProperty("PROCESSED_DIR")), "JVM reset changed PROCESSED_DIR");
      require("512".equals(after.getProperty("TILE_SIZE")), "JVM reset ignored TILE_SIZE environment default");
      require(after.getProperty("HICT_JAVA_OPTS").isEmpty(), "JVM override not cleared");
      require("WARN".equals(after.getProperty("logLevel")), "JVM reset ignored HICT_LOG_LEVEL environment default");
      require(after.equals(persisted()), "JVM reset did not persist");
      results.add("JVM reset restores defaults, including environment log level, retaining data locations");
      // Owned inert processes verify lifecycle separation without opening a
      // browser profile or risking a user's real server/assembly session.
      browserSentinel = new ProcessBuilder("sleep", "60").start();
      serverSentinel = new ProcessBuilder("sleep", "60").start();
      edt(() -> {
        ((List<Process>) field("browserProcesses")).add(browserSentinel);
        var serverField = launcher.getClass().getDeclaredField("serverProcess");
        serverField.setAccessible(true);
        serverField.set(launcher, serverSentinel);
        return null;
      });
      confirm("Reset WebUI settings and cache", "Reset WebUI", false, "webui-cancel.png");
      require(settings().equals(after) && persisted().equals(after), "Cancel changed WebUI reset flags");
      require(browserSentinel.isAlive() && serverSentinel.isAlive(), "Cancel stopped a process");
      results.add("WebUI reset cancellation leaves settings unchanged");
      confirm("Reset WebUI settings and cache", "Reset WebUI", true, "webui-confirm.png");
      long deadline = System.nanoTime() + 10_000_000_000L;
      while (!"true".equals(settings().getProperty("resetBrowser.tauri")) && System.nanoTime() < deadline) Thread.sleep(20);
      Properties reset = settings();
      require("true".equals(reset.getProperty("resetBrowser.electron")), "Electron reset not queued");
      require("true".equals(reset.getProperty("resetBrowser.tauri")), "Tauri reset not queued");
      require(reset.equals(persisted()), "WebUI reset did not persist");
      reset.remove("resetBrowser.electron");
      reset.remove("resetBrowser.tauri");
      require(reset.equals(after), "WebUI reset modified JVM settings");
      require(!browserSentinel.isAlive() && serverSentinel.isAlive(), "Reset must stop only the browser process");
      results.add("WebUI confirmation persists both engine reset flags without modifying JVM settings");
      results.add("WebUI reset stops a tracked browser sentinel, retaining the server sentinel");
      edt(() -> { frame.setSize(frame.getMinimumSize()); frame.validate(); return null; });
      confirm("Reset JVM settings", "Reset JVM settings", false, "minimum-window-confirmation.png");
      edt(() -> {
        for (var text : List.of("Save settings", "Reset JVM settings", "Reset WebUI settings and cache")) {
          var b = button(frame, text);
          require(frame.getBounds().contains(new java.awt.Rectangle(b.getLocationOnScreen(), b.getSize())),
            "Minimum-size window hides action: " + text);
        }
        button(frame, "Hide Configuration").doClick();
        require(!((Container) field("configurationPanel")).isVisible(), "Configuration did not close");
        button(frame, "Show Configuration").doClick();
        require(expectedData.equals(fields.get("DATA_DIR").getText()), "Toggling configuration lost fields");
        return null;
      });
      Thread.sleep(100);
      ImageIO.write(new Robot().createScreenCapture(edt(() -> frame.getBounds())), "png", output.resolve("minimum-window.png").toFile());
      edt(() -> {
        var lastField = fields.get("HICT_JAVA_OPTS");
        var scroll = (javax.swing.JScrollPane) SwingUtilities.getAncestorOfClass(javax.swing.JScrollPane.class, lastField);
        scroll.getVerticalScrollBar().setValue(scroll.getVerticalScrollBar().getMaximum());
        return null;
      });
      Thread.sleep(100);
      ImageIO.write(new Robot().createScreenCapture(edt(() -> frame.getBounds())), "png", output.resolve("minimum-window-scrolled.png").toFile());
      edt(() -> {
        var lastField = fields.get("HICT_JAVA_OPTS");
        var fieldBounds = new java.awt.Rectangle(lastField.getLocationOnScreen(), lastField.getSize());
        require(frame.getBounds().contains(fieldBounds),
          "Cannot scroll to final field: " + fieldBounds + "; frame=" + frame.getBounds());
        require(lastField.getWidth() >= 160, "Configuration field is too narrow to edit");
        return null;
      });
      ImageIO.write(new Robot().createScreenCapture(edt(() -> frame.getBounds())), "png", output.resolve("minimum-window-scrolled.png").toFile());
      results.add("Reset actions remain inside the minimum-size window; configuration toggles retain fields");
      results.add("Final JVM configuration field is reachable by scrolling at minimum window size");
      require(launchBrowserFixture("exit 0"), "Clean GUI handoff was rejected");
      require("true".equals(persisted().getProperty("resetBrowser.electron")), "Handoff erased an unacknowledged reset");
      results.add("Clean GUI handoff without acknowledgement retains pending reset");
      require(launchBrowserFixture("sleep 2; exit 1"), "Delayed-failure fixture did not launch");
      Thread.sleep(1100);
      require("true".equals(persisted().getProperty("resetBrowser.electron")), "Delayed failure erased pending reset");
      results.add("Reset request survives browser startup followed by delayed failure");
      require(launchBrowserFixture("test \"$HICT_RESET_WEBUI\" = 1 || exit 2; sleep 2; echo HICT_WEBUI_RESET_COMPLETE; sleep 1"),
        "Acknowledgement fixture did not launch");
      require("true".equals(persisted().getProperty("resetBrowser.electron")), "Reset consumed before completion");
      deadline = System.nanoTime() + 10_000_000_000L;
      while (settings().containsKey("resetBrowser.electron") && System.nanoTime() < deadline) Thread.sleep(20);
      require(!persisted().containsKey("resetBrowser.electron"), "Completed reset did not persist");
      require("true".equals(persisted().getProperty("resetBrowser.tauri")), "Electron acknowledgement consumed Tauri reset");
      results.add("Only acknowledged engine reset is consumed and persisted after completion");
      // Exercise the actual owned server after reset checks have released their sentinels.
      serverSentinel.destroyForcibly();
      serverSentinel.waitFor();
      edt(() -> {
        var serverField = launcher.getClass().getDeclaredField("serverProcess");
        serverField.setAccessible(true);
        serverField.set(launcher, null);
        ((javax.swing.JCheckBox) field("openAfterStartCheckbox")).setSelected(false);
        fields.get("HICT_JAVA_OPTS").setText("-Xmx2g");
        fields.get("HICT_BIND_HOST").setText("127.0.0.1");
        invoke("startHiCT");
        return null;
      });
      var client = java.net.http.HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(1)).build();
      var request = java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + System.getenv("VXPORT") + "/version"))
        .timeout(java.time.Duration.ofSeconds(2)).build();
      require(awaitServerVersion(client, request), "Launcher did not start a reachable real server");
      var owned = (Process) field("serverProcess");
      require(owned != null && owned.isAlive(), "Launcher lost ownership of its real server");
      var serverChildren = owned.descendants().toList();
      edt(() -> { invoke("toggleConfiguration"); return null; });
      SwingUtilities.invokeLater(() -> {
        try { ((JButton) field("stopButton")).doClick(); }
        catch (Exception ex) { throw new RuntimeException(ex); }
      });
      deadline = System.nanoTime() + 10_000_000_000L;
      JDialog stopDialog = null;
      while (stopDialog == null && System.nanoTime() < deadline) {
        stopDialog = edt(() -> {
          for (var window : Window.getWindows()) {
            if (window instanceof JDialog dialog && dialog.isShowing() && "Stop HiCT".equals(dialog.getTitle())) return dialog;
          }
          return null;
        });
        if (stopDialog == null) Thread.sleep(20);
      }
      require(stopDialog != null, "Stop confirmation did not appear");
      final var confirmedStop = stopDialog;
      edt(() -> { button(confirmedStop, "Yes").doClick(); return null; });
      require(owned.waitFor(15, java.util.concurrent.TimeUnit.SECONDS), "Launcher Stop left the server alive");
      require(serverChildren.stream().noneMatch(ProcessHandle::isAlive), "Launcher Stop left server descendants alive");
      results.add("Launcher starts the real packaged server, serves its version, and Stop terminates the owned process tree");
      deadline = System.nanoTime() + 10_000_000_000L;
      while (!edt(() -> ((javax.swing.JPanel) field("logCardsPanel")).getComponent(0).isShowing()) && System.nanoTime() < deadline) Thread.sleep(20);
      require(edt(() -> !((javax.swing.JPanel) field("configurationPanel")).isVisible()), "Stop left configuration open");
      require(edt(() -> ((javax.swing.JPanel) field("logCardsPanel")).getComponent(0).isShowing()), "Stop did not restore the simple start card");
      require(edt(() -> !((javax.swing.JTextArea) field("logArea")).isShowing()), "Stop left the log card visible");
      require(edt(() -> ((JButton) field("configureButton")).getText().equals("Show Configuration")), "Stop left the configuration toggle stale");
      require(edt(() -> {
        var start = (JButton) field("startButton");
        return start.isShowing() && start.isEnabled();
      }), "Stop did not restore the Start action");
      ImageIO.write(new Robot().createScreenCapture(edt(() -> frame.getBounds())), "png", output.resolve("after-stop.png").toFile());
      edt(() -> {
        var card = (Container) ((Container) field("logCardsPanel")).getComponent(0);
        var hero = button(card, "Start HiCT");
        require(hero.isShowing() && frame.getBounds().contains(
          new java.awt.Rectangle(hero.getLocationOnScreen(), hero.getSize())), "Large Start button is clipped");
        var scale = field("uiScale");
        var scaled = scale.getClass().getDeclaredMethod("scaled", int.class);
        scaled.setAccessible(true);
        int width = 2 * (int) scaled.invoke(scale, 260);
        int height = 2 * (int) scaled.invoke(scale, 72);
        require(hero.getWidth() >= width && hero.getHeight() >= height,
          "Stopped Start button is smaller than twice the original design size: " + hero.getSize());
        results.add("Stopped Start button meets its doubled design size: "
          + hero.getWidth() + " × " + hero.getHeight() + " px");
        var logs = button(card, "View console log");
        require(logs.isShowing() && logs.getVisibleRect().getSize().equals(logs.getSize()), "Stopped launcher clips console access");
        logs.doClick();
        var console = (javax.swing.JTextArea) field("logArea");
        require(console.isShowing() && console.getText().contains("Stopping HiCT server process"), "Stop log missing");
        button(frame, "Back to start").doClick();
        require(hero.isShowing(), "Cannot return to Start after viewing logs");
        ((Timer) field("statusTimer")).start();
        hero.doClick();
        return null;
      });

      require(awaitServerVersion(client, request), "Launcher could not restart for the unexpected-exit check");
      var crashingServer = edt(() -> (Process) field("serverProcess"));
      require(crashingServer != null && crashingServer.isAlive() && crashingServer.pid() != owned.pid(),
        "Unexpected-exit check must target the fresh Launcher-owned child");
      crashingServer.destroyForcibly();
      require(crashingServer.waitFor(15, java.util.concurrent.TimeUnit.SECONDS), "Test child did not exit");
      int crashCode = crashingServer.exitValue();
      require(crashCode != 0, "Test child reported a successful exit");
      deadline = System.nanoTime() + 15_000_000_000L;
      JButton dismissCrash = null;
      while (dismissCrash == null && System.nanoTime() < deadline) {
        dismissCrash = edt(() -> {
          for (var window : Window.getWindows()) {
            if (window instanceof JDialog dialog && dialog.isShowing() && "HiCT Launcher".equals(dialog.getTitle())) {
              return button(dialog, "OK");
            }
          }
          return null;
        });
        if (dismissCrash == null) Thread.sleep(20);
      }
      require(dismissCrash != null, "Launcher did not report the unexpected server exit");
      var dismiss = dismissCrash;
      edt(() -> {
        dismiss.doClick();
        require(field("lastExitCode") instanceof Integer code && code == crashCode, "Unexpected exit code was lost");
        var logs = button(frame, "View console log");
        require(logs != null && logs.isShowing(), "Unexpected exit hides console access");
        logs.doClick();
        var console = (javax.swing.JTextArea) field("logArea");
        require(console.isShowing() && console.getText().contains("exited with code " + crashCode)
          && console.getText().contains("Stopping HiCT server process"), "Unexpected exit discarded console output");
        return null;
      });
      ImageIO.write(new Robot().createScreenCapture(edt(() -> frame.getBounds())),
        "png", output.resolve("after-unexpected-exit.png").toFile());
      results.add("Unexpected exit of a running server leaves its exit code and prior console output available");
      edt(() -> {
        ((Timer) field("statusTimer")).stop();
        button(frame, "Back to start").doClick();
        fields.get("HICT_JAVA_OPTS").setText("-XX:HictInvalidOptionForTest");
        var card = (Container) ((Container) field("logCardsPanel")).getComponent(0);
        button(card, "Start HiCT").doClick();
        return null;
      });
      deadline = System.nanoTime() + 15_000_000_000L;
      while (!edt(() -> field("lastExitCode") != null && button(frame, "View console log").isShowing())
        && System.nanoTime() < deadline) Thread.sleep(20);
      edt(() -> {
        require(field("lastExitCode") instanceof Integer code && code != 0, "Expected failed child exit");
        var logs = button(frame, "View console log");
        require(logs.isShowing(), "Failed session hides console access");
        logs.doClick();
        var console = (javax.swing.JTextArea) field("logArea");
        require(console.isShowing() && console.getText().contains("Unrecognized VM option")
          && console.getText().contains("exited with code " + field("lastExitCode")), "Failed child diagnostics missing");
        require(console.getText().contains("Stopping HiCT server process"), "Restart discarded previous log");
        return null;
      });
      results.add("Manual stop and failed restart retain console output, exit diagnostics and navigation");
      Files.write(output.resolve("results.txt"), results);
      results.forEach(System.out::println);
    } finally {
      for (var process : new Process[] {browserSentinel, serverSentinel}) {
        if (process != null) {
          process.destroyForcibly();
          process.waitFor();
        }
      }
      if (launcher != null) edt(() -> {
        for (var process : (List<Process>) field("browserProcesses")) process.destroyForcibly();
        if (field("statusTimer") instanceof Timer timer) timer.stop();
        ((ExecutorService) field("backgroundExecutor")).shutdownNow();
        for (var window : Window.getWindows()) window.dispose();
        return null;
      });
    }
  }
}
