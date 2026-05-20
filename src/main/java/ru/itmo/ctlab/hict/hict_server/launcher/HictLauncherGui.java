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

package ru.itmo.ctlab.hict.hict_server.launcher;

import ru.itmo.ctlab.hict.hict_server.tools.HictCli;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HictLauncherGui {
  private HictLauncherGui() {
  }

  public static void launchAndBlock() throws InterruptedException {
    final var closed = new CountDownLatch(1);
    SwingUtilities.invokeLater(() -> new LauncherWindow(closed).showWindow());
    closed.await();
  }

  private enum PathKind {
    NONE,
    DIRECTORY,
    FILE
  }

  private record ConfigSpec(String key, String label, String defaultValue, PathKind pathKind) {
  }

  private record BrowserBundle(String name, Path root, Path executable) {
  }

  private static final class LauncherWindow {
    private static final DateTimeFormatter LOG_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int MAX_LOG_CHARS = 200_000;
    private static final Pattern JSON_STRING_FIELD_PATTERN = Pattern.compile("\"%s\"\\s*:\\s*\"([^\"]*)\"");

    private static final List<ConfigSpec> CONFIG_SPECS = List.of(
      new ConfigSpec("DATA_DIR", "Data directory", "", PathKind.DIRECTORY),
      new ConfigSpec("PROCESSED_DIR", "Processed/cache directory", "", PathKind.DIRECTORY),
      new ConfigSpec("HICT_BIND_HOST", "Bind host", "127.0.0.1", PathKind.NONE),
      new ConfigSpec("VXPORT", "API port", "5000", PathKind.NONE),
      new ConfigSpec("WEBUI_PORT", "WebUI port", "8080", PathKind.NONE),
      new ConfigSpec("TILE_SIZE", "Tile size", "256", PathKind.NONE),
      new ConfigSpec("MIN_DS_POOL", "Min dataset pool", "4", PathKind.NONE),
      new ConfigSpec("MAX_DS_POOL", "Max dataset pool", "16", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_TOTAL_MAX", "Total worker limit", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_QUEUE_CAPACITY", "Worker queue capacity", "32", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_KEEPALIVE_SECONDS", "Worker keepalive, seconds", "30", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_UI_MIN", "UI workers min", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_UI_MAX", "UI workers max", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_ASSEMBLY_MIN", "Assembly workers min", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_ASSEMBLY_MAX", "Assembly workers max", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_TILE_MIN", "Tile workers min", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_TILE_MAX", "Tile workers max", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_TRACK_MIN", "Track workers min", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_TRACK_MAX", "Track workers max", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_EXPORT_MIN", "Export workers min", "", PathKind.NONE),
      new ConfigSpec("HICT_WORKERS_EXPORT_MAX", "Export workers max", "", PathKind.NONE),
      new ConfigSpec("HICT_TRACK_PRECOMPUTE_JOB_THREADS", "Track precompute job threads", "", PathKind.NONE),
      new ConfigSpec("HICT_TRACK_PRECOMPUTE_WORKER_THREADS", "Track precompute worker threads", "", PathKind.NONE),
      new ConfigSpec("HICT_MATRIX_QUERY_MAX_ELEMENTS", "Max matrix query elements", "", PathKind.NONE),
      new ConfigSpec("HICT_BLOCK_DATA_CACHE_BYTES", "Block data cache bytes", "", PathKind.NONE),
      new ConfigSpec("HICT_BLOCK_META_ROW_CACHE_ROWS", "Block-meta row cache rows", "", PathKind.NONE),
      new ConfigSpec("WEBUI_ROOT", "WebUI root override", "", PathKind.DIRECTORY),
      new ConfigSpec("HICT_TOOLCHAIN_DIR", "Toolchain directory", "", PathKind.DIRECTORY),
      new ConfigSpec("HICT_HICTK_BIN", "External hictk executable", "", PathKind.FILE),
      new ConfigSpec("HICT_COOLER_BIN", "External cooler executable", "", PathKind.FILE),
      new ConfigSpec("HICT_PYTHON_BIN", "External Python executable", "", PathKind.FILE),
      new ConfigSpec("HICT_JAVA_OPTS", "Extra JVM options", "", PathKind.NONE)
    );

    private final CountDownLatch closed;
    private final Path appHome;
    private final Path jarPath;
    private final BrowserBundle browserBundle;
    private final HttpClient httpClient;
    private final ExecutorService backgroundExecutor;
    private final Map<String, JTextField> fields = new LinkedHashMap<>();
    private final Properties settings = new Properties();

    private JFrame frame;
    private JPanel configurationPanel;
    private JTextArea logArea;
    private JLabel apiStatusLabel;
    private JLabel webUiStatusLabel;
    private JLabel processStatusLabel;
    private JLabel addressLabel;
    private JLabel browserStatusLabel;
    private JButton startButton;
    private JButton stopButton;
    private JButton openWebUiButton;
    private JButton configureButton;
    private JCheckBox useBundledBrowserCheckbox;
    private JCheckBox openAfterStartCheckbox;
    private Timer statusTimer;
    private volatile Process serverProcess;
    private volatile boolean closing;

    LauncherWindow(final CountDownLatch closed) {
      this.closed = closed;
      this.appHome = detectAppHome();
      this.jarPath = detectJarPath(this.appHome);
      this.browserBundle = detectBrowserBundle(this.appHome);
      this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(600))
        .build();
      this.backgroundExecutor = Executors.newCachedThreadPool(runnable -> {
        final var thread = new Thread(runnable, "hict-launcher-worker");
        thread.setDaemon(true);
        return thread;
      });
      loadSettings();
    }

    void showWindow() {
      configureLookAndFeel();

      this.frame = new JFrame("HiCT Launcher");
      this.frame.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
      this.frame.setMinimumSize(new Dimension(780, 560));
      this.frame.setPreferredSize(new Dimension(980, 720));
      this.frame.setLayout(new BorderLayout(10, 10));

      this.frame.add(createHeader(), BorderLayout.NORTH);
      this.frame.add(createCenter(), BorderLayout.CENTER);
      this.frame.add(createFooter(), BorderLayout.SOUTH);
      this.frame.addWindowListener(new WindowAdapter() {
        @Override
        public void windowClosing(final WindowEvent e) {
          shutdownAndClose();
        }
      });

      updateBrowserStatus();
      updateStatusLabels(false, false);
      this.statusTimer = new Timer(1_000, ignored -> pollStatusInBackground());
      this.statusTimer.setInitialDelay(0);
      this.statusTimer.start();

      this.frame.pack();
      this.frame.setLocationRelativeTo(null);
      this.frame.setVisible(true);
      appendLog("Launcher ready. DATA_DIR=" + getFieldValue("DATA_DIR"));
      if (this.browserBundle == null) {
        appendLog("No bundled browser payload was found; the system browser will be used.");
      } else {
        appendLog("Bundled browser detected: " + this.browserBundle.name() + " (" + this.browserBundle.executable() + ")");
      }
    }

    private JPanel createHeader() {
      final var panel = new JPanel(new BorderLayout(12, 8));
      panel.setBorder(BorderFactory.createEmptyBorder(10, 12, 0, 12));

      final var title = new JLabel("HiCT portable launcher");
      title.setFont(title.getFont().deriveFont(Font.BOLD, 18f));
      panel.add(title, BorderLayout.WEST);

      final var buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
      this.startButton = new JButton("Start HiCT");
      this.startButton.addActionListener(ignored -> startHiCT());
      this.openWebUiButton = new JButton("Open HiCT WebUI");
      this.openWebUiButton.addActionListener(ignored -> openWebUi());
      this.configureButton = new JButton("Configure");
      this.configureButton.addActionListener(ignored -> toggleConfiguration());
      this.stopButton = new JButton("Stop HiCT");
      this.stopButton.addActionListener(ignored -> stopHiCTInBackground(false));
      buttons.add(this.startButton);
      buttons.add(this.openWebUiButton);
      buttons.add(this.configureButton);
      buttons.add(this.stopButton);
      panel.add(buttons, BorderLayout.EAST);

      final var separator = new JSeparator();
      panel.add(separator, BorderLayout.SOUTH);
      return panel;
    }

    private JPanel createCenter() {
      final var panel = new JPanel(new BorderLayout(8, 8));
      panel.setBorder(BorderFactory.createEmptyBorder(0, 12, 0, 12));

      panel.add(createStatusPanel(), BorderLayout.NORTH);

      final var body = new JPanel(new BorderLayout(8, 8));
      this.configurationPanel = createConfigurationPanel();
      this.configurationPanel.setVisible(false);
      body.add(this.configurationPanel, BorderLayout.NORTH);

      this.logArea = new JTextArea();
      this.logArea.setEditable(false);
      this.logArea.setLineWrap(true);
      this.logArea.setWrapStyleWord(true);
      this.logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
      final var logScroll = new JScrollPane(this.logArea);
      logScroll.setBorder(BorderFactory.createTitledBorder("HiCT server log"));
      body.add(logScroll, BorderLayout.CENTER);

      panel.add(body, BorderLayout.CENTER);
      return panel;
    }

    private JPanel createStatusPanel() {
      final var panel = new JPanel(new GridBagLayout());
      panel.setBorder(BorderFactory.createCompoundBorder(
        BorderFactory.createLineBorder(new Color(210, 215, 220)),
        BorderFactory.createEmptyBorder(8, 8, 8, 8)
      ));
      final var gbc = new GridBagConstraints();
      gbc.insets = new Insets(3, 4, 3, 12);
      gbc.anchor = GridBagConstraints.WEST;

      this.processStatusLabel = new JLabel();
      this.apiStatusLabel = new JLabel();
      this.webUiStatusLabel = new JLabel();
      this.addressLabel = new JLabel();
      this.browserStatusLabel = new JLabel();

      addStatusRow(panel, gbc, 0, "Process:", this.processStatusLabel);
      addStatusRow(panel, gbc, 1, "API:", this.apiStatusLabel);
      addStatusRow(panel, gbc, 2, "WebUI:", this.webUiStatusLabel);
      addStatusRow(panel, gbc, 3, "Addresses:", this.addressLabel);
      addStatusRow(panel, gbc, 4, "Browser:", this.browserStatusLabel);
      return panel;
    }

    private static void addStatusRow(final JPanel panel,
                                     final GridBagConstraints gbc,
                                     final int row,
                                     final String name,
                                     final JLabel value) {
      gbc.gridx = 0;
      gbc.gridy = row;
      gbc.weightx = 0.0;
      gbc.fill = GridBagConstraints.NONE;
      final var label = new JLabel(name);
      label.setFont(label.getFont().deriveFont(Font.BOLD));
      panel.add(label, gbc);

      gbc.gridx = 1;
      gbc.weightx = 1.0;
      gbc.fill = GridBagConstraints.HORIZONTAL;
      panel.add(value, gbc);
    }

    private JPanel createConfigurationPanel() {
      final var outer = new JPanel(new BorderLayout(6, 6));
      outer.setBorder(BorderFactory.createTitledBorder("Configuration"));

      final var form = new JPanel(new GridBagLayout());
      final var gbc = new GridBagConstraints();
      gbc.insets = new Insets(3, 4, 3, 4);
      gbc.anchor = GridBagConstraints.WEST;
      gbc.fill = GridBagConstraints.HORIZONTAL;

      for (int i = 0; i < CONFIG_SPECS.size(); i++) {
        final var spec = CONFIG_SPECS.get(i);
        gbc.gridy = i;
        gbc.gridx = 0;
        gbc.weightx = 0.0;
        final var label = new JLabel(spec.label());
        label.setToolTipText(spec.key());
        form.add(label, gbc);

        gbc.gridx = 1;
        gbc.weightx = 1.0;
        final var field = new JTextField(resolveInitialValue(spec), 32);
        this.fields.put(spec.key(), field);
        form.add(field, gbc);

        gbc.gridx = 2;
        gbc.weightx = 0.0;
        if (spec.pathKind() == PathKind.NONE) {
          form.add(Box.createHorizontalStrut(88), gbc);
        } else {
          final var browse = new JButton("Browse");
          browse.addActionListener(ignored -> browsePath(spec, field));
          form.add(browse, gbc);
        }
      }

      final var optionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 14, 4));
      this.openAfterStartCheckbox = new JCheckBox("Open WebUI after start");
      this.openAfterStartCheckbox.setSelected(getBooleanSetting("openAfterStart", true));
      this.useBundledBrowserCheckbox = new JCheckBox("Use bundled browser when available");
      this.useBundledBrowserCheckbox.setSelected(getBooleanSetting("useBundledBrowser", this.browserBundle != null));
      this.useBundledBrowserCheckbox.setEnabled(this.browserBundle != null);
      optionPanel.add(this.openAfterStartCheckbox);
      optionPanel.add(this.useBundledBrowserCheckbox);

      final var buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 4));
      final var saveButton = new JButton("Save settings");
      saveButton.addActionListener(ignored -> {
        saveSettings();
        appendLog("Settings saved.");
      });
      buttonPanel.add(saveButton);

      outer.add(new JScrollPane(form), BorderLayout.CENTER);
      outer.add(optionPanel, BorderLayout.NORTH);
      outer.add(buttonPanel, BorderLayout.SOUTH);
      return outer;
    }

    private JPanel createFooter() {
      final var panel = new JPanel(new BorderLayout(8, 0));
      panel.setBorder(BorderFactory.createEmptyBorder(0, 12, 10, 12));
      final var hint = new JLabel("Explicit CLI commands still work: hict --help, hict start-server, hict convert --help.");
      hint.setForeground(new Color(90, 96, 104));
      panel.add(hint, BorderLayout.WEST);
      return panel;
    }

    private void toggleConfiguration() {
      this.configurationPanel.setVisible(!this.configurationPanel.isVisible());
      this.configureButton.setText(this.configurationPanel.isVisible() ? "Hide configuration" : "Configure");
      this.frame.revalidate();
      this.frame.repaint();
    }

    private void browsePath(final ConfigSpec spec, final JTextField field) {
      final var chooser = new JFileChooser();
      chooser.setFileSelectionMode(spec.pathKind() == PathKind.DIRECTORY
        ? JFileChooser.DIRECTORIES_ONLY
        : JFileChooser.FILES_ONLY);
      final var current = field.getText();
      if (current != null && !current.isBlank()) {
        chooser.setSelectedFile(Path.of(current).toFile());
      }
      if (chooser.showOpenDialog(this.frame) == JFileChooser.APPROVE_OPTION) {
        field.setText(chooser.getSelectedFile().toPath().toAbsolutePath().normalize().toString());
        if ("DATA_DIR".equals(spec.key()) && getFieldValue("PROCESSED_DIR").isBlank()) {
          fields.get("PROCESSED_DIR").setText(chooser.getSelectedFile().toPath().resolve("processed").toString());
        }
      }
    }

    private void startHiCT() {
      if (isProcessAlive()) {
        appendLog("HiCT is already running.");
        return;
      }
      saveSettings();

      final Path dataDir;
      try {
        dataDir = normalizePath(getFieldValue("DATA_DIR"));
        Files.createDirectories(dataDir);
      } catch (final Exception ex) {
        showError("Cannot prepare DATA_DIR", ex);
        return;
      }

      final var command = buildServerCommand();
      final var processBuilder = new ProcessBuilder(command);
      processBuilder.redirectErrorStream(true);
      processBuilder.directory(dataDir.toFile());
      final var env = processBuilder.environment();
      applyEnvironment(env, dataDir);

      appendLog("Starting HiCT with DATA_DIR=" + dataDir);
      appendLog("Command: " + String.join(" ", command));

      try {
        this.serverProcess = processBuilder.start();
        readProcessOutput(this.serverProcess);
      } catch (final IOException ex) {
        this.serverProcess = null;
        showError("Failed to start HiCT", ex);
        return;
      }

      updateButtons();
      if (this.openAfterStartCheckbox.isSelected()) {
        waitAndOpenWebUi();
      }
    }

    private List<String> buildServerCommand() {
      final var command = new ArrayList<String>();
      command.add(resolveJavaExecutable());
      command.add("-DAUTO_OPEN_BROWSER=false");
      command.add("-DSERVE_WEBUI=true");
      command.addAll(splitCommandLine(getFieldValue("HICT_JAVA_OPTS")));

      if (this.jarPath != null && Files.isRegularFile(this.jarPath)) {
        command.add("-jar");
        command.add(this.jarPath.toString());
      } else {
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(HictCli.class.getName());
      }
      command.add("start-server");
      return command;
    }

    private void applyEnvironment(final Map<String, String> env, final Path dataDir) {
      putIfNotBlank(env, "DATA_DIR", dataDir.toString());
      for (final var spec : CONFIG_SPECS) {
        if ("DATA_DIR".equals(spec.key()) || "HICT_JAVA_OPTS".equals(spec.key())) {
          continue;
        }
        putIfNotBlank(env, spec.key(), getFieldValue(spec.key()));
      }
      env.put("SERVE_WEBUI", "true");
      env.put("AUTO_OPEN_BROWSER", "false");
      env.putIfAbsent("HICT_BIND_HOST", "127.0.0.1");
      env.putIfAbsent("VXPORT", "5000");
      env.putIfAbsent("WEBUI_PORT", "8080");
      env.put("HICT_APP_HOME", this.appHome.toString());
      if (this.jarPath != null) {
        env.put("HICT_JAR_PATH", this.jarPath.toString());
      }

      final var webUiRoot = this.appHome.resolve("webui");
      if (getFieldValue("WEBUI_ROOT").isBlank() && Files.isRegularFile(webUiRoot.resolve("index.html"))) {
        env.put("WEBUI_ROOT", webUiRoot.toString());
      }
      final var platformToolchain = this.appHome.resolve("toolchains").resolve(platformId());
      if (getFieldValue("HICT_TOOLCHAIN_DIR").isBlank() && Files.isRegularFile(platformToolchain.resolve("manifest.json"))) {
        env.put("HICT_TOOLCHAIN_DIR", platformToolchain.toString());
      }
      if (this.browserBundle != null) {
        env.put("HICT_BROWSER_DIR", this.browserBundle.root().toString());
      }
    }

    private void putIfNotBlank(final Map<String, String> env, final String key, final String value) {
      if (value != null && !value.isBlank()) {
        env.put(key, value.trim());
      }
    }

    private void readProcessOutput(final Process process) {
      this.backgroundExecutor.submit(() -> {
        try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            appendLog(line);
          }
        } catch (final IOException ex) {
          appendLog("Failed to read server output: " + ex.getMessage());
        } finally {
          final int exitCode;
          try {
            exitCode = process.waitFor();
            appendLog("HiCT server process exited with code " + exitCode + ".");
          } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            appendLog("HiCT server process output reader was interrupted.");
          }
          if (this.serverProcess == process) {
            this.serverProcess = null;
          }
          SwingUtilities.invokeLater(this::updateButtons);
        }
      });
    }

    private void waitAndOpenWebUi() {
      this.backgroundExecutor.submit(() -> {
        final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (!this.closing && System.nanoTime() < deadline) {
          if (isUrlReachable(webUiUrl())) {
            SwingUtilities.invokeLater(this::openWebUi);
            return;
          }
          sleepQuietly(800);
        }
        appendLog("WebUI did not become reachable within 90 seconds; use Open HiCT WebUI after startup completes.");
      });
    }

    private void openWebUi() {
      final var url = webUiUrl();
      if (this.useBundledBrowserCheckbox.isSelected() && this.browserBundle != null) {
        try {
          new ProcessBuilder(this.browserBundle.executable().toString(), url)
            .directory(this.browserBundle.root().toFile())
            .start();
          appendLog("Opened WebUI in bundled browser: " + url);
          return;
        } catch (final IOException ex) {
          appendLog("Bundled browser failed, falling back to the system browser: " + ex.getMessage());
        }
      }

      try {
        openSystemBrowser(url);
        appendLog("Opened WebUI in the system browser: " + url);
      } catch (final Exception ex) {
        showError("Failed to open WebUI", ex);
      }
    }

    private void stopHiCTInBackground(final boolean closeAfterStop) {
      this.backgroundExecutor.submit(() -> {
        stopHiCT();
        if (closeAfterStop) {
          SwingUtilities.invokeLater(() -> {
            this.frame.dispose();
            this.closed.countDown();
          });
        }
      });
    }

    private void stopHiCT() {
      final var process = this.serverProcess;
      if (process == null) {
        appendLog("No HiCT server process is owned by this launcher.");
        return;
      }

      appendLog("Stopping HiCT server process...");
      process.destroy();
      try {
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
          appendLog("HiCT did not stop gracefully within 5 seconds; terminating it forcibly.");
          process.destroyForcibly();
          process.waitFor(5, TimeUnit.SECONDS);
        }
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        appendLog("Interrupted while stopping HiCT.");
      } finally {
        if (this.serverProcess == process) {
          this.serverProcess = null;
        }
        SwingUtilities.invokeLater(this::updateButtons);
      }
    }

    private void shutdownAndClose() {
      if (this.closing) {
        return;
      }
      this.closing = true;
      saveSettings();
      if (this.statusTimer != null) {
        this.statusTimer.stop();
      }
      stopHiCTInBackground(true);
      this.backgroundExecutor.shutdown();
    }

    private void pollStatusInBackground() {
      this.backgroundExecutor.submit(() -> {
        final var apiReachable = isUrlReachable(apiUrl("/version"));
        final var webUiReachable = isUrlReachable(webUiUrl());
        SwingUtilities.invokeLater(() -> updateStatusLabels(apiReachable, webUiReachable));
      });
    }

    private void updateStatusLabels(final boolean apiReachable, final boolean webUiReachable) {
      this.processStatusLabel.setText(isProcessAlive() ? "running" : "stopped");
      this.processStatusLabel.setForeground(isProcessAlive() ? new Color(20, 118, 55) : new Color(120, 58, 58));
      this.apiStatusLabel.setText(apiReachable ? "reachable" : "not reachable");
      this.apiStatusLabel.setForeground(apiReachable ? new Color(20, 118, 55) : new Color(120, 58, 58));
      this.webUiStatusLabel.setText(webUiReachable ? "reachable" : "not reachable");
      this.webUiStatusLabel.setForeground(webUiReachable ? new Color(20, 118, 55) : new Color(120, 58, 58));
      this.addressLabel.setText(apiUrl("") + "  |  " + webUiUrl());
      updateButtons();
    }

    private void updateBrowserStatus() {
      if (this.browserBundle == null) {
        this.browserStatusLabel.setText("system default browser");
        return;
      }
      this.browserStatusLabel.setText("bundled " + this.browserBundle.name() + " available; system browser fallback enabled");
    }

    private void updateButtons() {
      final var alive = isProcessAlive();
      this.startButton.setEnabled(!alive && !this.closing);
      this.stopButton.setEnabled(alive && !this.closing);
      this.openWebUiButton.setEnabled(!this.closing);
    }

    private boolean isProcessAlive() {
      final var process = this.serverProcess;
      return process != null && process.isAlive();
    }

    private boolean isUrlReachable(final String url) {
      try {
        final var request = HttpRequest.newBuilder(URI.create(url))
          .timeout(Duration.ofMillis(800))
          .GET()
          .build();
        final var response = this.httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        return response.statusCode() >= 200 && response.statusCode() < 500;
      } catch (final Exception ignored) {
        return false;
      }
    }

    private String apiUrl(final String path) {
      return "http://" + clientHost() + ":" + normalizedPort("VXPORT", "5000") + path;
    }

    private String webUiUrl() {
      return "http://" + clientHost() + ":" + normalizedPort("WEBUI_PORT", "8080") + "/";
    }

    private String clientHost() {
      final var bindHost = getFieldValue("HICT_BIND_HOST");
      if (bindHost.isBlank() || "0.0.0.0".equals(bindHost) || "::".equals(bindHost) || "[::]".equals(bindHost)) {
        return "127.0.0.1";
      }
      return bindHost;
    }

    private String normalizedPort(final String key, final String fallback) {
      final var value = getFieldValue(key);
      if (value.isBlank()) {
        return fallback;
      }
      return value.trim();
    }

    private String getFieldValue(final String key) {
      final var field = this.fields.get(key);
      return field == null || field.getText() == null ? "" : field.getText().trim();
    }

    private String resolveInitialValue(final ConfigSpec spec) {
      final var fromSettings = this.settings.getProperty(spec.key());
      if (fromSettings != null) {
        return fromSettings;
      }
      final var fromEnv = System.getenv(spec.key());
      if (fromEnv != null && !fromEnv.isBlank()) {
        return fromEnv;
      }
      return switch (spec.key()) {
        case "DATA_DIR" -> defaultDataDir().toString();
        case "PROCESSED_DIR" -> defaultDataDir().resolve("processed").toString();
        case "WEBUI_ROOT" -> Files.isRegularFile(this.appHome.resolve("webui").resolve("index.html"))
          ? this.appHome.resolve("webui").toString()
          : spec.defaultValue();
        case "HICT_TOOLCHAIN_DIR" -> Files.isRegularFile(this.appHome.resolve("toolchains").resolve(platformId()).resolve("manifest.json"))
          ? this.appHome.resolve("toolchains").resolve(platformId()).toString()
          : spec.defaultValue();
        default -> spec.defaultValue();
      };
    }

    private Path defaultDataDir() {
      final var explicit = firstNonBlank(
        System.getenv("DATA_DIR"),
        System.getenv("HICT_PORTABLE_DATA_DIR")
      );
      if (explicit != null) {
        return normalizePath(explicit);
      }
      return this.appHome;
    }

    private void loadSettings() {
      final var configPath = settingsPath(defaultDataDir());
      if (!Files.isRegularFile(configPath)) {
        return;
      }
      try (var in = Files.newInputStream(configPath)) {
        this.settings.load(in);
      } catch (final IOException ex) {
        appendLogEarly("Could not load launcher settings from " + configPath + ": " + ex.getMessage());
      }
    }

    private void saveSettings() {
      for (final var spec : CONFIG_SPECS) {
        this.settings.setProperty(spec.key(), getFieldValue(spec.key()));
      }
      this.settings.setProperty("openAfterStart", Boolean.toString(this.openAfterStartCheckbox == null || this.openAfterStartCheckbox.isSelected()));
      this.settings.setProperty("useBundledBrowser", Boolean.toString(this.useBundledBrowserCheckbox != null && this.useBundledBrowserCheckbox.isSelected()));
      final Path dataDir;
      try {
        dataDir = normalizePath(getFieldValue("DATA_DIR"));
        Files.createDirectories(dataDir);
      } catch (final Exception ex) {
        appendLog("Could not save launcher settings because DATA_DIR is invalid: " + ex.getMessage());
        return;
      }
      final var configPath = settingsPath(dataDir);
      try (var out = Files.newOutputStream(configPath)) {
        this.settings.store(out, "HiCT portable launcher settings");
      } catch (final IOException ex) {
        appendLog("Could not save launcher settings to " + configPath + ": " + ex.getMessage());
      }
    }

    private boolean getBooleanSetting(final String key, final boolean fallback) {
      final var value = this.settings.getProperty(key);
      if (value == null || value.isBlank()) {
        return fallback;
      }
      return "true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value);
    }

    private Path settingsPath(final Path dataDir) {
      final var explicit = System.getenv("HICT_LAUNCHER_CONFIG");
      if (explicit != null && !explicit.isBlank()) {
        return normalizePath(explicit);
      }
      return dataDir.resolve(".hict-launcher.properties");
    }

    private void appendLog(final String message) {
      final var line = "[" + LocalTime.now().format(LOG_TIME_FORMAT) + "] " + message + System.lineSeparator();
      if (this.logArea == null) {
        appendLogEarly(message);
        return;
      }
      SwingUtilities.invokeLater(() -> {
        this.logArea.append(line);
        final var length = this.logArea.getDocument().getLength();
        if (length > MAX_LOG_CHARS) {
          this.logArea.replaceRange("", 0, length - MAX_LOG_CHARS);
        }
        this.logArea.setCaretPosition(this.logArea.getDocument().getLength());
      });
    }

    private void appendLogEarly(final String message) {
      System.err.println("[HiCT launcher] " + message);
    }

    private void showError(final String title, final Exception ex) {
      appendLog(title + ": " + ex.getMessage());
      SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(
        this.frame,
        title + System.lineSeparator() + ex.getMessage(),
        "HiCT Launcher",
        JOptionPane.ERROR_MESSAGE
      ));
    }

    private static void configureLookAndFeel() {
      try {
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
      } catch (final Exception ignored) {
        // Swing's cross-platform look and feel remains usable.
      }
    }

    private static Path detectAppHome() {
      final var explicit = firstNonBlank(System.getenv("HICT_APP_HOME"), System.getProperty("HICT_APP_HOME"));
      if (explicit != null) {
        return normalizePath(explicit);
      }
      final var jarPath = detectJarPath(null);
      if (jarPath != null) {
        final var parent = jarPath.getParent();
        if (parent != null && "lib".equals(parent.getFileName().toString())) {
          final var appHome = parent.getParent();
          if (appHome != null) {
            return appHome.toAbsolutePath().normalize();
          }
        }
        if (parent != null) {
          return parent.toAbsolutePath().normalize();
        }
      }
      return Path.of(".").toAbsolutePath().normalize();
    }

    private static Path detectJarPath(final Path appHome) {
      final var explicit = firstNonBlank(System.getenv("HICT_JAR_PATH"), System.getProperty("HICT_JAR_PATH"));
      if (explicit != null) {
        final var explicitPath = normalizePath(explicit);
        if (Files.isRegularFile(explicitPath)) {
          return explicitPath;
        }
      }
      if (appHome != null) {
        final var appJar = appHome.resolve("lib").resolve("hict.jar");
        if (Files.isRegularFile(appJar)) {
          return appJar.toAbsolutePath().normalize();
        }
      }
      try {
        final var location = HictCli.class.getProtectionDomain().getCodeSource().getLocation();
        if (location != null && "file".equalsIgnoreCase(location.getProtocol())) {
          final var candidate = Path.of(location.toURI()).toAbsolutePath().normalize();
          if (Files.isRegularFile(candidate) && candidate.getFileName().toString().endsWith(".jar")) {
            return candidate;
          }
        }
      } catch (final Exception ignored) {
        // Gradle/source launches use classpath mode below.
      }
      return null;
    }

    private static BrowserBundle detectBrowserBundle(final Path appHome) {
      final var explicit = firstNonBlank(System.getenv("HICT_BROWSER_DIR"), System.getProperty("HICT_BROWSER_DIR"));
      final var root = explicit == null
        ? appHome.resolve("browsers").resolve(platformId())
        : normalizePath(explicit);
      final var manifest = root.resolve("manifest.json");
      if (!Files.isRegularFile(manifest)) {
        return null;
      }
      try {
        final var manifestText = Files.readString(manifest);
        final var name = Objects.requireNonNullElse(extractJsonString(manifestText, "name"), "Bundled browser");
        final var command = extractJsonString(manifestText, "command");
        if (command == null || command.isBlank()) {
          return null;
        }
        final var executable = root.resolve(command.replace('/', java.io.File.separatorChar)).normalize();
        if (!Files.isRegularFile(executable)) {
          return null;
        }
        return new BrowserBundle(name, root, executable);
      } catch (final IOException ignored) {
        return null;
      }
    }

    private static String extractJsonString(final String text, final String key) {
      final var pattern = Pattern.compile(JSON_STRING_FIELD_PATTERN.pattern().formatted(Pattern.quote(key)));
      final Matcher matcher = pattern.matcher(text);
      if (!matcher.find()) {
        return null;
      }
      return matcher.group(1)
        .replace("\\\\", "\\")
        .replace("\\\"", "\"");
    }

    private static String resolveJavaExecutable() {
      final var javaHome = Path.of(System.getProperty("java.home"));
      final var executableName = isWindows() ? "java.exe" : "java";
      final var candidate = javaHome.resolve("bin").resolve(executableName);
      if (Files.isRegularFile(candidate)) {
        return candidate.toString();
      }
      return executableName;
    }

    private static void openSystemBrowser(final String url) throws IOException {
      if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
        Desktop.getDesktop().browse(URI.create(url));
        return;
      }
      if (isWindows()) {
        new ProcessBuilder("rundll32", "url.dll,FileProtocolHandler", url).start();
        return;
      }
      if (isMac()) {
        new ProcessBuilder("open", url).start();
        return;
      }
      new ProcessBuilder("xdg-open", url).start();
    }

    private static List<String> splitCommandLine(final String value) {
      final var result = new ArrayList<String>();
      if (value == null || value.isBlank()) {
        return result;
      }
      final var current = new StringBuilder();
      boolean inSingleQuote = false;
      boolean inDoubleQuote = false;
      boolean escaping = false;
      for (int i = 0; i < value.length(); i++) {
        final var ch = value.charAt(i);
        if (escaping) {
          current.append(ch);
          escaping = false;
          continue;
        }
        if (ch == '\\') {
          escaping = true;
          continue;
        }
        if (ch == '\'' && !inDoubleQuote) {
          inSingleQuote = !inSingleQuote;
          continue;
        }
        if (ch == '"' && !inSingleQuote) {
          inDoubleQuote = !inDoubleQuote;
          continue;
        }
        if (Character.isWhitespace(ch) && !inSingleQuote && !inDoubleQuote) {
          if (!current.isEmpty()) {
            result.add(current.toString());
            current.setLength(0);
          }
          continue;
        }
        current.append(ch);
      }
      if (!current.isEmpty()) {
        result.add(current.toString());
      }
      return result;
    }

    private static Path normalizePath(final String path) {
      return Path.of(path).toAbsolutePath().normalize();
    }

    private static String firstNonBlank(final String... values) {
      for (final var value : values) {
        if (value != null && !value.isBlank()) {
          return value.trim();
        }
      }
      return null;
    }

    private static String platformId() {
      if (isWindows()) {
        return "windows_x86_64";
      }
      if (isMac()) {
        return "macos_x86_64";
      }
      return "linux_x86_64";
    }

    private static boolean isWindows() {
      return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    private static boolean isMac() {
      final var osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
      return osName.contains("mac") || osName.contains("darwin");
    }

    private static void sleepQuietly(final long millis) {
      try {
        Thread.sleep(millis);
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
