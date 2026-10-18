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
    return (boolean) method.invoke(launcher, bundle, "http://127.0.0.1:8189/");
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
