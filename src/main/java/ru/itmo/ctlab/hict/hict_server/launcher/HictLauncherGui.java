/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis, Zakhar Lobanov, Nikita Zheleznov, Pavel Avdeyev, Nikolay Cherkasov and Computer Technologies Laboratory ITMO University team.
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

import ru.itmo.ctlab.hict.hict_server.info.AttributionInfo;
import ru.itmo.ctlab.hict.hict_server.tools.HictCli;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSlider;
import javax.swing.JSeparator;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Cursor;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GraphicsConfiguration;
import java.awt.GraphicsDevice;
import java.awt.GraphicsEnvironment;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Rectangle;
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
import java.util.Comparator;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
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

  private record BrowserBundle(String name, String engine, int priority, Path root, Path executable, List<String> arguments) {
  }

  private enum BrowserMode {
    SYSTEM("system", "System browser"),
    TAURI("tauri", "Tauri"),
    ELECTRON("electron", "Electron");

    private final String wireName;
    private final String label;

    BrowserMode(final String wireName, final String label) {
      this.wireName = wireName;
      this.label = label;
    }

    private static BrowserMode fromWireName(final String value) {
      if (value == null || value.isBlank()) {
        return SYSTEM;
      }
      final var normalized = value.trim().toLowerCase(Locale.ROOT);
      for (final var mode : values()) {
        if (mode.wireName.equals(normalized)) {
          return mode;
        }
      }
      return SYSTEM;
    }
  }

  private static final class LauncherWindow {
    private static final DateTimeFormatter LOG_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int MAX_LOG_CHARS = 200_000;
    private static final Pattern JSON_STRING_FIELD_PATTERN = Pattern.compile("\"%s\"\\s*:\\s*\"([^\"]*)\"");
    private static final double REFERENCE_SCREEN_DIAGONAL = Math.hypot(1920, 1080);
    private static final String[] LOG_LEVELS = {"ERROR", "WARN", "INFO", "DEBUG", "TRACE"};

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
    private final Path browserPayloadRoot;
    private final List<BrowserBundle> browserBundles;
    private final List<Process> browserProcesses;
    private final HttpClient httpClient;
    private final ExecutorService backgroundExecutor;
    private final Map<String, JTextField> fields = new LinkedHashMap<>();
    private final Properties settings = new Properties();
    private final UiScale uiScale;

    private JFrame frame;
    private JPanel configurationPanel;
    private JPanel logCardsPanel;
    private JTextArea logArea;
    private JLabel apiStatusLabel;
    private JLabel webUiStatusLabel;
    private JLabel processStatusLabel;
    private JLabel apiGatewayLabel;
    private JButton webUiLinkButton;
    private JLabel browserStatusLabel;
    private JButton startButton;
    private JButton stopButton;
    private JButton openWebUiButton;
    private JButton configureButton;
    private JButton aboutButton;
    private JRadioButton systemBrowserRadio;
    private JRadioButton tauriBrowserRadio;
    private JRadioButton electronBrowserRadio;
    private JCheckBox openAfterStartCheckbox;
    private JSlider logLevelSlider;
    private JLabel logLevelValueLabel;
    private Timer statusTimer;
    private volatile Process serverProcess;
    private volatile boolean closing;

    LauncherWindow(final CountDownLatch closed) {
      this.closed = closed;
      this.appHome = detectAppHome();
      this.jarPath = detectJarPath(this.appHome);
      this.browserPayloadRoot = detectBrowserPayloadRoot(this.appHome);
      this.browserBundles = detectBrowserBundles(this.browserPayloadRoot);
      this.browserProcesses = new CopyOnWriteArrayList<>();
      this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(600))
        .build();
      this.uiScale = UiScale.detect();
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
      this.frame.setMinimumSize(this.uiScale.windowMinimumSize());
      this.frame.setPreferredSize(this.uiScale.windowPreferredSize());
      this.frame.setLayout(new BorderLayout(this.uiScale.gap(), this.uiScale.gap()));

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
      clampFrameToScreen();
      this.frame.setLocationRelativeTo(null);
      this.frame.setVisible(true);
      appendLauncherLog("Launcher ready. DATA_DIR=" + getFieldValue("DATA_DIR"));
      if (this.browserBundles.isEmpty()) {
        appendLauncherLog("No bundled browser payload was found; the system browser will be used.");
      } else {
        appendLauncherLog("Bundled browsers detected: " + browserBundleNames(this.browserBundles));
      }
    }

    private JPanel createHeader() {
      final var panel = new JPanel(new BorderLayout(this.uiScale.gapLarge(), this.uiScale.gap()));
      panel.setBorder(BorderFactory.createEmptyBorder(this.uiScale.gap(), this.uiScale.gapLarge(), 0, this.uiScale.gapLarge()));

      final var title = new JLabel("HiCT portable launcher");
      title.setFont(title.getFont().deriveFont(Font.BOLD, this.uiScale.titleFontSize()));
      panel.add(title, BorderLayout.WEST);

      final var buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, this.uiScale.gap(), 0));
      this.startButton = new JButton("Start HiCT");
      this.startButton.addActionListener(ignored -> startHiCT());
      styleActionButton(this.startButton, true);
      this.openWebUiButton = new JButton("Re-open WebUI");
      this.openWebUiButton.addActionListener(ignored -> openWebUi());
      this.openWebUiButton.setVisible(false);
      styleActionButton(this.openWebUiButton, false);
      this.configureButton = new JButton("Show Configuration");
      this.configureButton.addActionListener(ignored -> toggleConfiguration());
      styleActionButton(this.configureButton, false);
      this.aboutButton = new JButton("About");
      this.aboutButton.addActionListener(ignored -> showAboutDialog());
      styleActionButton(this.aboutButton, false);
      this.stopButton = new JButton("Stop HiCT");
      this.stopButton.addActionListener(ignored -> stopHiCTInBackground(false));
      styleActionButton(this.stopButton, false);
      buttons.add(this.startButton);
      buttons.add(this.openWebUiButton);
      buttons.add(this.configureButton);
      buttons.add(this.aboutButton);
      buttons.add(this.stopButton);
      panel.add(buttons, BorderLayout.EAST);

      final var separator = new JSeparator();
      panel.add(separator, BorderLayout.SOUTH);
      return panel;
    }

    private void styleActionButton(final JButton button, final boolean primary) {
      button.setFont(button.getFont().deriveFont(
        primary ? Font.BOLD : Font.PLAIN,
        (float) this.uiScale.actionFontSize()
      ));
      button.setMargin(this.uiScale.actionButtonInsets());
      button.setMinimumSize(this.uiScale.actionButtonMinimumSize());
      final var preferred = button.getPreferredSize();
      final var minimum = this.uiScale.actionButtonMinimumSize();
      button.setPreferredSize(new Dimension(
        Math.max(preferred.width, minimum.width),
        Math.max(preferred.height, minimum.height)
      ));
      if (primary) {
        button.setToolTipText("Start the HiCT JVM server and open the configured WebUI browser");
      }
    }

    private void styleHeroButton(final JButton button) {
      button.setFont(button.getFont().deriveFont(Font.BOLD, (float) this.uiScale.primaryButtonFontSize()));
      button.setMargin(this.uiScale.heroButtonInsets());
      button.setMinimumSize(this.uiScale.heroButtonMinimumSize());
      button.setPreferredSize(this.uiScale.heroButtonMinimumSize());
    }

    private JPanel createCenter() {
      final var panel = new JPanel(new BorderLayout(this.uiScale.gap(), this.uiScale.gap()));
      panel.setBorder(BorderFactory.createEmptyBorder(0, this.uiScale.gapLarge(), 0, this.uiScale.gapLarge()));

      panel.add(createStatusPanel(), BorderLayout.NORTH);

      final var body = new JPanel(new BorderLayout(this.uiScale.gap(), this.uiScale.gap()));
      this.configurationPanel = createConfigurationPanel();
      this.configurationPanel.setVisible(false);
      body.add(this.configurationPanel, BorderLayout.NORTH);

      this.logArea = new JTextArea();
      this.logArea.setEditable(false);
      this.logArea.setLineWrap(true);
      this.logArea.setWrapStyleWord(true);
      this.logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, this.uiScale.logFontSize()));
      this.logArea.setRows(this.uiScale.logRows());
      final var logScroll = new JScrollPane(this.logArea);
      logScroll.setBorder(BorderFactory.createTitledBorder("HiCT server log"));
      this.logCardsPanel = new JPanel(new CardLayout());
      this.logCardsPanel.add(createLaunchPlaceholderPanel(), "start");
      this.logCardsPanel.add(logScroll, "log");
      body.add(this.logCardsPanel, BorderLayout.CENTER);

      panel.add(body, BorderLayout.CENTER);
      return panel;
    }

    private JPanel createLaunchPlaceholderPanel() {
      final var panel = new JPanel(new GridBagLayout());
      panel.setBorder(BorderFactory.createCompoundBorder(
        BorderFactory.createTitledBorder("HiCT server log"),
        BorderFactory.createEmptyBorder(this.uiScale.gapLarge(), this.uiScale.gapLarge(), this.uiScale.gapLarge(), this.uiScale.gapLarge())
      ));
      final var content = new JPanel(new GridBagLayout());
      final var gbc = new GridBagConstraints();
      gbc.gridx = 0;
      gbc.gridy = 0;
      gbc.insets = new Insets(0, 0, this.uiScale.gapLarge(), 0);
      final var title = new JLabel("Start HiCT to open the WebUI.");
      title.setFont(title.getFont().deriveFont(Font.BOLD, (float) this.uiScale.heroFontSize()));
      content.add(title, gbc);

      gbc.gridy = 1;
      gbc.insets = new Insets(this.uiScale.gap(), 0, 0, 0);
      final var duplicateStartButton = new JButton("Start HiCT");
      duplicateStartButton.addActionListener(ignored -> startHiCT());
      styleHeroButton(duplicateStartButton);
      content.add(duplicateStartButton, gbc);

      panel.add(content);
      return panel;
    }

    private JPanel createStatusPanel() {
      final var panel = new JPanel(new GridBagLayout());
      panel.setBorder(BorderFactory.createCompoundBorder(
        BorderFactory.createLineBorder(new Color(210, 215, 220)),
        BorderFactory.createEmptyBorder(this.uiScale.gap(), this.uiScale.gap(), this.uiScale.gap(), this.uiScale.gap())
      ));
      final var gbc = new GridBagConstraints();
      gbc.insets = this.uiScale.formInsets();
      gbc.anchor = GridBagConstraints.WEST;

      this.processStatusLabel = new JLabel();
      this.apiStatusLabel = new JLabel();
      this.webUiStatusLabel = new JLabel();
      this.webUiLinkButton = createLinkButton(webUiUrl());
      this.apiGatewayLabel = new JLabel(apiUrl(""));
      this.browserStatusLabel = new JLabel();

      addStatusRow(panel, gbc, 0, "Process:", this.processStatusLabel);
      addStatusRow(panel, gbc, 1, "API:", this.apiStatusLabel);
      addStatusRow(panel, gbc, 2, "WebUI:", this.webUiStatusLabel);
      addStatusRow(panel, gbc, 3, "Links:", createAddressLinksPanel());
      addStatusRow(panel, gbc, 4, "Browser:", this.browserStatusLabel);
      addStatusRow(panel, gbc, 5, "Open in:", createBrowserModePanel());
      return panel;
    }

    private void addStatusRow(final JPanel panel,
                              final GridBagConstraints gbc,
                              final int row,
                              final String name,
                              final Component value) {
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

    private JPanel createAddressLinksPanel() {
      final var panel = new JPanel(new GridBagLayout());
      final var gbc = new GridBagConstraints();
      gbc.insets = new Insets(0, 0, this.uiScale.gapSmall(), this.uiScale.gap());
      gbc.anchor = GridBagConstraints.WEST;
      gbc.gridx = 0;
      gbc.gridy = 0;
      final var webUiLabel = new JLabel("WebUI Link:");
      webUiLabel.setFont(webUiLabel.getFont().deriveFont(Font.BOLD));
      panel.add(webUiLabel, gbc);
      gbc.gridx = 1;
      gbc.weightx = 1.0;
      gbc.fill = GridBagConstraints.HORIZONTAL;
      panel.add(this.webUiLinkButton, gbc);

      gbc.gridx = 0;
      gbc.gridy = 1;
      gbc.weightx = 0.0;
      gbc.fill = GridBagConstraints.NONE;
      final var apiLabel = new JLabel("API Gateway:");
      apiLabel.setFont(apiLabel.getFont().deriveFont(Font.BOLD));
      panel.add(apiLabel, gbc);
      gbc.gridx = 1;
      gbc.weightx = 1.0;
      gbc.fill = GridBagConstraints.HORIZONTAL;
      this.apiGatewayLabel.setForeground(new Color(90, 96, 104));
      this.apiGatewayLabel.setHorizontalAlignment(SwingConstants.LEFT);
      panel.add(this.apiGatewayLabel, gbc);
      return panel;
    }

    private JButton createLinkButton(final String url) {
      final var button = new JButton(url);
      button.setHorizontalAlignment(SwingConstants.LEFT);
      button.setBorderPainted(false);
      button.setContentAreaFilled(false);
      button.setFocusPainted(false);
      button.setOpaque(false);
      button.setMargin(new Insets(0, 0, 0, 0));
      button.setForeground(new Color(24, 95, 180));
      button.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
      button.setToolTipText("Open in the system default browser");
      button.addActionListener(ignored -> openSystemBrowserFromLauncher(button.getText()));
      return button;
    }

    private JPanel createBrowserModePanel() {
      final var panel = new JPanel(new FlowLayout(FlowLayout.LEFT, this.uiScale.gapLarge(), 0));
      final var group = new ButtonGroup();
      this.systemBrowserRadio = new JRadioButton(BrowserMode.SYSTEM.label);
      this.tauriBrowserRadio = new JRadioButton(browserModeLabel(BrowserMode.TAURI));
      this.electronBrowserRadio = new JRadioButton(browserModeLabel(BrowserMode.ELECTRON));

      this.tauriBrowserRadio.setEnabled(hasBrowserMode(BrowserMode.TAURI));
      this.electronBrowserRadio.setEnabled(hasBrowserMode(BrowserMode.ELECTRON));
      this.tauriBrowserRadio.setToolTipText(this.tauriBrowserRadio.isEnabled()
        ? "Use the bundled Tauri/WebView browser"
        : "Tauri browser is not bundled in this package");
      this.electronBrowserRadio.setToolTipText(this.electronBrowserRadio.isEnabled()
        ? "Use the bundled Electron/Chromium browser"
        : "Electron browser is not bundled in this package");

      group.add(this.systemBrowserRadio);
      group.add(this.tauriBrowserRadio);
      group.add(this.electronBrowserRadio);
      panel.add(this.systemBrowserRadio);
      panel.add(this.tauriBrowserRadio);
      panel.add(this.electronBrowserRadio);

      selectBrowserMode(resolveInitialBrowserMode());
      this.systemBrowserRadio.addActionListener(ignored -> onBrowserModeChanged());
      this.tauriBrowserRadio.addActionListener(ignored -> onBrowserModeChanged());
      this.electronBrowserRadio.addActionListener(ignored -> onBrowserModeChanged());
      return panel;
    }

    private JPanel createConfigurationPanel() {
      final var outer = new JPanel(new BorderLayout(6, 6));
      outer.setBorder(BorderFactory.createTitledBorder("Configuration"));

      final var form = new JPanel(new GridBagLayout());
      final var gbc = new GridBagConstraints();
      gbc.insets = this.uiScale.formInsets();
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
        final var field = new JTextField(resolveInitialValue(spec), this.uiScale.fieldColumns());
        this.fields.put(spec.key(), field);
        form.add(field, gbc);

        gbc.gridx = 2;
        gbc.weightx = 0.0;
        if (spec.pathKind() == PathKind.NONE) {
          form.add(Box.createHorizontalStrut(this.uiScale.browseButtonPlaceholderWidth()), gbc);
        } else {
          final var browse = new JButton("Browse");
          browse.addActionListener(ignored -> browsePath(spec, field));
          form.add(browse, gbc);
        }
      }

      final var optionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, this.uiScale.gapLarge(), this.uiScale.gapSmall()));
      this.openAfterStartCheckbox = new JCheckBox("Open WebUI after start");
      this.openAfterStartCheckbox.setSelected(getBooleanSetting("openAfterStart", true));
      optionPanel.add(this.openAfterStartCheckbox);
      optionPanel.add(new JLabel("Verbosity:"));
      this.logLevelSlider = new JSlider(0, LOG_LEVELS.length - 1, resolveInitialLogLevelIndex());
      this.logLevelSlider.setMajorTickSpacing(1);
      this.logLevelSlider.setSnapToTicks(true);
      this.logLevelSlider.setPaintTicks(true);
      this.logLevelSlider.setPaintLabels(true);
      this.logLevelSlider.setLabelTable(logLevelSliderLabels());
      this.logLevelSlider.setToolTipText("Controls HiCT JVM log verbosity for the next server start");
      this.logLevelSlider.setPreferredSize(new Dimension(this.uiScale.scaled(280), this.uiScale.scaled(54)));
      this.logLevelValueLabel = new JLabel(selectedLogLevel());
      this.logLevelValueLabel.setFont(this.logLevelValueLabel.getFont().deriveFont(Font.BOLD));
      this.logLevelSlider.addChangeListener(ignored -> updateLogLevelLabel());
      optionPanel.add(this.logLevelSlider);
      optionPanel.add(this.logLevelValueLabel);

      final var buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT, this.uiScale.gap(), this.uiScale.gapSmall()));
      final var saveButton = new JButton("Save settings");
      saveButton.addActionListener(ignored -> {
        saveSettings();
        appendLauncherLog("Settings saved.");
      });
      buttonPanel.add(saveButton);

      final var formScroll = new JScrollPane(form);
      formScroll.setPreferredSize(this.uiScale.configurationScrollPreferredSize());
      formScroll.setMinimumSize(new Dimension(this.uiScale.minWindowWidth() / 2, this.uiScale.scaled(120)));
      outer.add(formScroll, BorderLayout.CENTER);
      outer.add(optionPanel, BorderLayout.NORTH);
      outer.add(buttonPanel, BorderLayout.SOUTH);
      this.uiScale.applyFonts(outer);
      return outer;
    }

    private Hashtable<Integer, JLabel> logLevelSliderLabels() {
      final var labels = new Hashtable<Integer, JLabel>();
      for (int i = 0; i < LOG_LEVELS.length; i++) {
        labels.put(i, new JLabel(LOG_LEVELS[i]));
      }
      return labels;
    }

    private int resolveInitialLogLevelIndex() {
      final var configured = firstNonBlank(
        this.settings.getProperty("logLevel"),
        System.getenv("HICT_LOG_LEVEL"),
        System.getProperty("HICT_LOG_LEVEL")
      );
      if (configured != null) {
        final var normalized = configured.trim().toUpperCase(Locale.ROOT);
        for (int i = 0; i < LOG_LEVELS.length; i++) {
          if (LOG_LEVELS[i].equals(normalized)) {
            return i;
          }
        }
      }
      return 2;
    }

    private String selectedLogLevel() {
      final var index = this.logLevelSlider == null ? resolveInitialLogLevelIndex() : this.logLevelSlider.getValue();
      return LOG_LEVELS[Math.max(0, Math.min(LOG_LEVELS.length - 1, index))];
    }

    private void updateLogLevelLabel() {
      if (this.logLevelValueLabel != null) {
        this.logLevelValueLabel.setText(selectedLogLevel());
      }
    }

    private JPanel createFooter() {
      final var panel = new JPanel(new BorderLayout(8, 0));
      panel.setBorder(BorderFactory.createEmptyBorder(0, this.uiScale.gapLarge(), this.uiScale.gap(), this.uiScale.gapLarge()));
      final var hint = new JLabel("Explicit CLI commands still work: hict --help, hict start-server, hict convert --help.");
      hint.setForeground(new Color(90, 96, 104));
      panel.add(hint, BorderLayout.WEST);
      return panel;
    }

    private void toggleConfiguration() {
      this.configurationPanel.setVisible(!this.configurationPanel.isVisible());
      this.configureButton.setText(this.configurationPanel.isVisible() ? "Hide Configuration" : "Show Configuration");
      this.frame.revalidate();
      this.frame.repaint();
    }

    private void showAboutDialog() {
      final var tabs = new JTabbedPane();
      tabs.addTab("About", new JScrollPane(createReadOnlyTextArea(aboutText())));
      tabs.addTab("Attribution", new JScrollPane(createReadOnlyTextArea(attributionText())));
      tabs.setPreferredSize(this.uiScale.aboutDialogPreferredSize());
      JOptionPane.showMessageDialog(this.frame, tabs, "About HiCT", JOptionPane.INFORMATION_MESSAGE);
    }

    private JTextArea createReadOnlyTextArea(final String text) {
      final var textArea = new JTextArea(text);
      textArea.setEditable(false);
      textArea.setLineWrap(true);
      textArea.setWrapStyleWord(true);
      textArea.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, this.uiScale.logFontSize()));
      textArea.setBorder(BorderFactory.createEmptyBorder(
        this.uiScale.gap(),
        this.uiScale.gap(),
        this.uiScale.gap(),
        this.uiScale.gap()
      ));
      textArea.setCaretPosition(0);
      return textArea;
    }

    private String aboutText() {
      return "HiCT - Hi-C scaffolding and visualization workstation." + System.lineSeparator()
        + System.lineSeparator()
        + "Authors:" + System.lineSeparator()
        + AttributionInfo.authorsLine() + System.lineSeparator()
        + System.lineSeparator()
        + "License: " + AttributionInfo.licenseLine() + System.lineSeparator()
        + System.lineSeparator()
        + "Portable home: " + this.appHome + System.lineSeparator()
        + "Default data directory: " + defaultDataDir() + System.lineSeparator()
        + System.lineSeparator()
        + "Full third-party notices are kept in package metadata, portable licenses/, runtime/legal, and toolchains/<platform>/share when bundled.";
    }

    private String attributionText() {
      final var builder = new StringBuilder();
      builder.append("Used software").append(System.lineSeparator());
      builder.append(System.lineSeparator());
      for (final var line : AttributionInfo.usedSoftwareLines()) {
        builder.append("- ").append(line).append(System.lineSeparator());
      }
      builder.append(System.lineSeparator());
      builder.append("Optional hictk citation: Rossini R, Paulsen J. Bioinformatics 2024;40(7):btae408.");
      return builder.toString();
    }

    private void clampFrameToScreen() {
      final var bounds = this.uiScale.usableScreenBounds();
      final var preferred = this.frame.getSize();
      final var width = Math.min(preferred.width, Math.max(this.uiScale.minWindowWidth(), bounds.width));
      final var height = Math.min(preferred.height, Math.max(this.uiScale.minWindowHeight(), bounds.height));
      this.frame.setSize(width, height);
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
        showLogPanel();
        appendLauncherLog("HiCT is already running.");
        return;
      }
      showLogPanel();
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

      appendLauncherLog("Starting HiCT with DATA_DIR=" + dataDir);
      appendLauncherLog("Command: " + String.join(" ", command));

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

    private void showLogPanel() {
      if (this.logCardsPanel == null) {
        return;
      }
      final var layout = (CardLayout) this.logCardsPanel.getLayout();
      layout.show(this.logCardsPanel, "log");
      this.logCardsPanel.revalidate();
      this.logCardsPanel.repaint();
    }

    private List<String> buildServerCommand() {
      final var command = new ArrayList<String>();
      command.add(resolveJavaExecutable());
      command.add("-DAUTO_OPEN_BROWSER=false");
      command.add("-DSERVE_WEBUI=true");
      command.addAll(splitCommandLine(getFieldValue("HICT_JAVA_OPTS")));
      command.add("-DHICT_LOG_LEVEL=" + selectedLogLevel());

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
      env.put("HICT_LOG_LEVEL", selectedLogLevel());
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
      if (!this.browserBundles.isEmpty()) {
        env.put("HICT_BROWSER_DIR", this.browserPayloadRoot.toString());
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
            appendServerLog(line);
          }
        } catch (final IOException ex) {
          appendLauncherLog("Failed to read server output: " + ex.getMessage());
        } finally {
          final int exitCode;
          try {
            exitCode = process.waitFor();
            appendLauncherLog("HiCT server process exited with code " + exitCode + ".");
          } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            appendLauncherLog("HiCT server process output reader was interrupted.");
          }
          if (this.serverProcess == process) {
            this.serverProcess = null;
          }
          stopBundledBrowsers("because the HiCT server process exited");
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
        appendLauncherLog("WebUI did not become reachable within 90 seconds; use Re-open WebUI after startup completes.");
      });
    }

    private void openWebUi() {
      final var url = webUiLaunchUrl();
      final var selectedMode = selectedBrowserMode();
      final var bundledCandidates = browserBundlesForMode(selectedMode);
      appendLauncherLog("Opening WebUI. Requested browser mode=" + selectedMode.label + ", url=" + url);
      if (!bundledCandidates.isEmpty()) {
        appendLauncherLog("Bundled browser candidates: " + browserBundleNames(bundledCandidates));
        for (final var bundle : bundledCandidates) {
          if (tryOpenBundledBrowser(bundle, url)) {
            return;
          }
        }
        appendLauncherLog("Selected bundled browser failed, falling back to the system browser.");
      } else if (selectedMode != BrowserMode.SYSTEM) {
        appendLauncherLog(selectedMode.label + " browser is not bundled, falling back to the system browser.");
      }

      try {
        openSystemBrowser(url);
        appendLauncherLog("Opened WebUI in the system browser: " + url);
      } catch (final Exception ex) {
        showError("Failed to open WebUI", ex);
      }
    }

    private boolean tryOpenBundledBrowser(final BrowserBundle bundle, final String url) {
      final var command = new ArrayList<String>();
      command.add(bundle.executable().toString());
      command.addAll(bundle.arguments());
      command.add(url);
      final var processBuilder = new ProcessBuilder(command)
        .directory(bundle.root().toFile())
        .redirectErrorStream(true);
      processBuilder.environment().put("HICT_ELECTRON_URL", url);
      processBuilder.environment().put("HICT_TAURI_URL", url);
      try {
        appendLauncherLog("Starting bundled browser " + bundle.name()
          + " [engine=" + bundle.engine()
          + ", exe=" + bundle.executable()
          + ", cwd=" + bundle.root() + "]");
        final var process = processBuilder.start();
        if (process.waitFor(1_200, TimeUnit.MILLISECONDS)) {
          final var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
          appendLauncherLog("Bundled browser " + bundle.name() + " exited immediately with code " + process.exitValue() + ".");
          if (!output.isBlank()) {
            appendLauncherLog(output);
          }
          if (process.exitValue() == 0) {
            appendLauncherLog("Bundled browser can hand off to a GUI child process and exit cleanly; treating this launch as successful.");
            return true;
          }
          return false;
        }
        registerBrowserProcess(process, bundle.name());
        appendLauncherLog("Opened WebUI in bundled browser " + bundle.name() + ": " + url);
        return true;
      } catch (final IOException ex) {
        appendLauncherLog("Bundled browser " + bundle.name() + " failed to start: " + ex.getMessage()
          + " (pathLength=" + bundle.executable().toString().length() + ")");
        return false;
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        appendLauncherLog("Interrupted while starting bundled browser " + bundle.name() + ".");
        return false;
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
        appendLauncherLog("No HiCT server process is owned by this launcher.");
        stopBundledBrowsers("because Stop HiCT was requested");
        return;
      }

      stopBundledBrowsers("before stopping HiCT");
      appendLauncherLog("Stopping HiCT server process...");
      process.destroy();
      try {
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
          appendLauncherLog("HiCT did not stop gracefully within 5 seconds; terminating it forcibly.");
          process.destroyForcibly();
          process.waitFor(5, TimeUnit.SECONDS);
        }
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        appendLauncherLog("Interrupted while stopping HiCT.");
      } finally {
        if (this.serverProcess == process) {
          this.serverProcess = null;
        }
        SwingUtilities.invokeLater(this::updateButtons);
      }
    }

    private void registerBrowserProcess(final Process process, final String browserName) {
      this.browserProcesses.add(process);
      this.backgroundExecutor.submit(() -> {
        try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            appendBrowserLog(browserName, line);
          }
        } catch (final IOException ex) {
          if (process.isAlive()) {
            appendLauncherLog("Failed to read bundled browser output from " + browserName + ": " + ex.getMessage());
          }
        }
      });
      this.backgroundExecutor.submit(() -> {
        try {
          final var exitCode = process.waitFor();
          appendLauncherLog("Bundled browser " + browserName + " exited with code " + exitCode + ".");
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          appendLauncherLog("Interrupted while waiting for bundled browser " + browserName + " to exit.");
        } finally {
          this.browserProcesses.remove(process);
        }
      });
    }

    private void stopBundledBrowsers(final String reason) {
      final var runningBrowsers = this.browserProcesses.stream()
        .filter(Process::isAlive)
        .toList();
      if (runningBrowsers.isEmpty()) {
        return;
      }

      appendLauncherLog("Stopping " + runningBrowsers.size() + " bundled browser process(es) " + reason + ".");
      for (final var browserProcess : runningBrowsers) {
        browserProcess.destroy();
      }
      for (final var browserProcess : runningBrowsers) {
        try {
          if (!browserProcess.waitFor(2, TimeUnit.SECONDS) && browserProcess.isAlive()) {
            browserProcess.destroyForcibly();
            browserProcess.waitFor(2, TimeUnit.SECONDS);
          }
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          appendLauncherLog("Interrupted while stopping bundled browser processes.");
          return;
        }
      }
      this.browserProcesses.removeIf(process -> !process.isAlive());
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
      this.apiGatewayLabel.setText(apiUrl(""));
      this.webUiLinkButton.setText(webUiUrl());
      updateButtons();
    }

    private void updateBrowserStatus() {
      final var selectedMode = selectedBrowserMode();
      this.browserStatusLabel.setToolTipText(null);
      if (selectedMode == BrowserMode.SYSTEM) {
        this.browserStatusLabel.setText("system default browser");
        return;
      }
      final var candidates = browserBundlesForMode(selectedMode);
      if (candidates.isEmpty()) {
        this.browserStatusLabel.setText(selectedMode.label + " is not bundled; system browser fallback");
        return;
      }
      this.browserStatusLabel.setText(selectedMode.label + " bundled; system browser fallback enabled");
      this.browserStatusLabel.setToolTipText(browserBundleNames(candidates));
    }

    private void updateButtons() {
      final var alive = isProcessAlive();
      this.startButton.setEnabled(!alive && !this.closing);
      this.stopButton.setEnabled(alive && !this.closing);
      this.openWebUiButton.setVisible(alive && !this.closing);
      this.openWebUiButton.setEnabled(alive && !this.closing);
      if (this.frame != null) {
        this.frame.revalidate();
      }
    }

    private void onBrowserModeChanged() {
      this.settings.setProperty("browserMode", selectedBrowserMode().wireName);
      updateBrowserStatus();
      saveSettings();
    }

    private BrowserMode selectedBrowserMode() {
      if (this.tauriBrowserRadio != null && this.tauriBrowserRadio.isSelected()) {
        return BrowserMode.TAURI;
      }
      if (this.electronBrowserRadio != null && this.electronBrowserRadio.isSelected()) {
        return BrowserMode.ELECTRON;
      }
      return BrowserMode.SYSTEM;
    }

    private BrowserMode resolveInitialBrowserMode() {
      final var configuredMode = firstNonBlank(
        this.settings.getProperty("browserMode"),
        System.getenv("HICT_BROWSER_MODE")
      );
      if (configuredMode != null && !configuredMode.isBlank()) {
        final var requested = BrowserMode.fromWireName(configuredMode);
        return hasBrowserMode(requested) ? requested : preferredBrowserMode();
      }
      return preferredBrowserMode();
    }

    private BrowserMode preferredBrowserMode() {
      if (hasBrowserMode(BrowserMode.TAURI)) {
        return BrowserMode.TAURI;
      }
      if (hasBrowserMode(BrowserMode.ELECTRON)) {
        return BrowserMode.ELECTRON;
      }
      return BrowserMode.SYSTEM;
    }

    private void selectBrowserMode(final BrowserMode mode) {
      final var selectableMode = hasBrowserMode(mode) ? mode : BrowserMode.SYSTEM;
      switch (selectableMode) {
        case TAURI -> this.tauriBrowserRadio.setSelected(true);
        case ELECTRON -> this.electronBrowserRadio.setSelected(true);
        case SYSTEM -> this.systemBrowserRadio.setSelected(true);
      }
    }

    private String browserModeLabel(final BrowserMode mode) {
      return hasBrowserMode(mode) ? mode.label : mode.label + " (not bundled)";
    }

    private boolean hasBrowserMode(final BrowserMode mode) {
      return switch (mode) {
        case SYSTEM -> true;
        case TAURI -> this.browserBundles.stream().anyMatch(LauncherWindow::isTauriBundle);
        case ELECTRON -> this.browserBundles.stream().anyMatch(LauncherWindow::isElectronBundle);
      };
    }

    private List<BrowserBundle> browserBundlesForMode(final BrowserMode mode) {
      if (mode == BrowserMode.SYSTEM) {
        return List.of();
      }
      final var selected = this.browserBundles.stream()
        .filter(bundle -> mode == BrowserMode.TAURI ? isTauriBundle(bundle) : isElectronBundle(bundle))
        .toList();
      if (mode != BrowserMode.TAURI || selected.isEmpty()) {
        return selected;
      }
      final var withFallback = new ArrayList<>(selected);
      this.browserBundles.stream()
        .filter(LauncherWindow::isElectronBundle)
        .forEach(withFallback::add);
      return List.copyOf(withFallback);
    }

    private void openSystemBrowserFromLauncher(final String url) {
      try {
        openSystemBrowser(url);
        appendLauncherLog("Opened " + url + " in the system browser.");
      } catch (final IOException ex) {
        showError("Failed to open " + url, ex);
      }
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

    private String webUiLaunchUrl() {
      return webUiUrl() + "?hict_launch=" + System.currentTimeMillis();
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
      this.settings.setProperty("browserMode", selectedBrowserMode().wireName);
      this.settings.setProperty("logLevel", selectedLogLevel());
      final Path dataDir;
      try {
        dataDir = normalizePath(getFieldValue("DATA_DIR"));
        Files.createDirectories(dataDir);
      } catch (final Exception ex) {
        appendLauncherLog("Could not save launcher settings because DATA_DIR is invalid: " + ex.getMessage());
        return;
      }
      final var configPath = settingsPath(dataDir);
      try (var out = Files.newOutputStream(configPath)) {
        this.settings.store(out, "HiCT portable launcher settings");
      } catch (final IOException ex) {
        appendLauncherLog("Could not save launcher settings to " + configPath + ": " + ex.getMessage());
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

    private void appendLauncherLog(final String message) {
      appendLog("Launcher | " + message);
    }

    private void appendServerLog(final String message) {
      appendLog("Server   | " + message);
    }

    private void appendBrowserLog(final String browserName, final String message) {
      appendLog("Browser  | " + browserName + " | " + message);
    }

    private void appendLogEarly(final String message) {
      System.err.println("[HiCT launcher] " + message);
    }

    private void showError(final String title, final Exception ex) {
      appendLauncherLog(title + ": " + ex.getMessage());
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

    private static Path detectBrowserPayloadRoot(final Path appHome) {
      final var explicit = firstNonBlank(System.getenv("HICT_BROWSER_DIR"), System.getProperty("HICT_BROWSER_DIR"));
      return explicit == null
        ? appHome.resolve("browsers").resolve(platformId())
        : normalizePath(explicit);
    }

    private static List<BrowserBundle> detectBrowserBundles(final Path root) {
      final var bundles = new ArrayList<BrowserBundle>();
      final var rootBundle = detectBrowserBundle(root);
      if (rootBundle != null) {
        bundles.add(rootBundle);
      }
      if (Files.isDirectory(root)) {
        try (var children = Files.list(root)) {
          children
            .filter(Files::isDirectory)
            .sorted()
            .map(LauncherWindow::detectBrowserBundle)
            .filter(Objects::nonNull)
            .forEach(bundles::add);
        } catch (final IOException ignored) {
          // No bundled browser is still a valid portable configuration.
        }
      }
      bundles.sort(Comparator.comparingInt(BrowserBundle::priority).thenComparing(BrowserBundle::name));
      return List.copyOf(bundles);
    }

    private static BrowserBundle detectBrowserBundle(final Path root) {
      final var manifest = root.resolve("manifest.json");
      if (!Files.isRegularFile(manifest)) {
        return null;
      }
      try {
        final var manifestText = Files.readString(manifest);
        final var name = Objects.requireNonNullElse(extractJsonString(manifestText, "name"), "Bundled browser");
        final var engine = Objects.requireNonNullElse(extractJsonString(manifestText, "engine"), "unknown");
        final var priority = extractJsonInt(manifestText, "priority", defaultBrowserPriority(engine));
        final var command = extractJsonString(manifestText, "command");
        if (command == null || command.isBlank()) {
          return null;
        }
        final var executable = root.resolve(command.replace('/', java.io.File.separatorChar)).normalize();
        if (!Files.isRegularFile(executable)) {
          return null;
        }
        return new BrowserBundle(name, engine, priority, root, executable, extractJsonStringArray(manifestText, "arguments"));
      } catch (final IOException ignored) {
        return null;
      }
    }

    private static int defaultBrowserPriority(final String engine) {
      final var normalized = engine.toLowerCase(Locale.ROOT);
      if (normalized.contains("tauri")) {
        return 10;
      }
      if (normalized.contains("electron") || normalized.contains("chromium")) {
        return 50;
      }
      return 100;
    }

    private static boolean isTauriBundle(final BrowserBundle bundle) {
      final var normalized = (bundle.engine() + " " + bundle.name()).toLowerCase(Locale.ROOT);
      return normalized.contains("tauri");
    }

    private static boolean isElectronBundle(final BrowserBundle bundle) {
      final var normalized = (bundle.engine() + " " + bundle.name()).toLowerCase(Locale.ROOT);
      return normalized.contains("electron") || normalized.contains("chromium");
    }

    private static String browserBundleNames(final List<BrowserBundle> bundles) {
      return bundles.stream()
        .map(bundle -> bundle.name() + " [" + bundle.engine() + "]")
        .reduce((left, right) -> left + ", " + right)
        .orElse("none");
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

    private static int extractJsonInt(final String text, final String key, final int fallback) {
      final var pattern = Pattern.compile("\"%s\"\\s*:\\s*(-?\\d+)".formatted(Pattern.quote(key)));
      final Matcher matcher = pattern.matcher(text);
      if (!matcher.find()) {
        return fallback;
      }
      try {
        return Integer.parseInt(matcher.group(1));
      } catch (final NumberFormatException ignored) {
        return fallback;
      }
    }

    private static List<String> extractJsonStringArray(final String text, final String key) {
      final var pattern = Pattern.compile("\"%s\"\\s*:\\s*\\[(.*?)]".formatted(Pattern.quote(key)), Pattern.DOTALL);
      final Matcher matcher = pattern.matcher(text);
      if (!matcher.find()) {
        return List.of();
      }
      final var values = new ArrayList<String>();
      final Matcher valueMatcher = Pattern.compile("\"((?:\\\\.|[^\"])*)\"").matcher(matcher.group(1));
      while (valueMatcher.find()) {
        values.add(unescapeJsonString(valueMatcher.group(1)));
      }
      return List.copyOf(values);
    }

    private static String unescapeJsonString(final String value) {
      final var result = new StringBuilder(value.length());
      boolean escaping = false;
      for (int i = 0; i < value.length(); i++) {
        final var ch = value.charAt(i);
        if (!escaping) {
          if (ch == '\\') {
            escaping = true;
          } else {
            result.append(ch);
          }
          continue;
        }
        result.append(switch (ch) {
          case '"', '\\', '/' -> ch;
          case 'b' -> '\b';
          case 'f' -> '\f';
          case 'n' -> '\n';
          case 'r' -> '\r';
          case 't' -> '\t';
          default -> ch;
        });
        escaping = false;
      }
      if (escaping) {
        result.append('\\');
      }
      return result.toString();
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

  private record UiScale(double scale, Rectangle screenBounds) {
    private static UiScale detect() {
      final var screen = detectScreenBounds();
      final double geometryScale = clamp(Math.hypot(screen.getWidth(), screen.getHeight()) / LauncherWindow.REFERENCE_SCREEN_DIAGONAL, 0.95, 1.55);
      final double densityScale = clamp(detectDeviceScaleFactor(), 1.0, 2.25);
      final double scale = clamp(Math.max(geometryScale, densityScale * 0.92), 1.0, 2.0);
      return new UiScale(scale, screen);
    }

    private static Rectangle detectScreenBounds() {
      try {
        final GraphicsEnvironment environment = GraphicsEnvironment.getLocalGraphicsEnvironment();
        final GraphicsDevice device = environment.getDefaultScreenDevice();
        final GraphicsConfiguration configuration = device.getDefaultConfiguration();
        final Rectangle bounds = new Rectangle(configuration.getBounds());
        final Insets insets = java.awt.Toolkit.getDefaultToolkit().getScreenInsets(configuration);
        bounds.x += insets.left;
        bounds.y += insets.top;
        bounds.width = Math.max(640, bounds.width - insets.left - insets.right);
        bounds.height = Math.max(480, bounds.height - insets.top - insets.bottom);
        return bounds;
      } catch (final Exception ignored) {
        return new Rectangle(0, 0, 1280, 720);
      }
    }

    private static double detectDeviceScaleFactor() {
      try {
        final GraphicsEnvironment environment = GraphicsEnvironment.getLocalGraphicsEnvironment();
        final GraphicsConfiguration configuration = environment.getDefaultScreenDevice().getDefaultConfiguration();
        return Math.max(
          configuration.getDefaultTransform().getScaleX(),
          configuration.getDefaultTransform().getScaleY()
        );
      } catch (final Exception ignored) {
        return 1.0;
      }
    }

    private static double clamp(final double value, final double min, final double max) {
      return Math.max(min, Math.min(max, value));
    }

    private int scaled(final int value) {
      return Math.max(1, (int) Math.round(value * this.scale));
    }

    private int gapSmall() {
      return scaled(4);
    }

    private int gap() {
      return scaled(8);
    }

    private int gapLarge() {
      return scaled(12);
    }

    private int titleFontSize() {
      return scaled(18);
    }

    private int actionFontSize() {
      return Math.max(13, scaled(13));
    }

    private int heroFontSize() {
      return Math.max(18, scaled(18));
    }

    private int primaryButtonFontSize() {
      return Math.max(20, scaled(22));
    }

    private int logFontSize() {
      return Math.max(12, scaled(12));
    }

    private int logRows() {
      return this.screenBounds.height < 650 ? 10 : 16;
    }

    private int fieldColumns() {
      if (this.screenBounds.width < 900) {
        return 20;
      }
      if (this.screenBounds.width > 2500) {
        return 42;
      }
      return 32;
    }

    private int browseButtonPlaceholderWidth() {
      return scaled(88);
    }

    private Dimension actionButtonMinimumSize() {
      return new Dimension(scaled(118), scaled(40));
    }

    private Dimension heroButtonMinimumSize() {
      return new Dimension(scaled(260), scaled(72));
    }

    private Insets actionButtonInsets() {
      return new Insets(scaled(8), scaled(14), scaled(8), scaled(14));
    }

    private Insets heroButtonInsets() {
      return new Insets(scaled(14), scaled(24), scaled(14), scaled(24));
    }

    private Dimension aboutDialogPreferredSize() {
      final int width = Math.min(scaled(820), Math.max(scaled(520), this.screenBounds.width - scaled(180)));
      final int height = Math.min(scaled(560), Math.max(scaled(360), this.screenBounds.height - scaled(220)));
      return new Dimension(width, height);
    }

    private Dimension configurationScrollPreferredSize() {
      final int width = Math.min(scaled(820), Math.max(scaled(520), this.screenBounds.width - scaled(160)));
      final int height = Math.max(scaled(180), Math.min(scaled(340), this.screenBounds.height / 3));
      return new Dimension(width, height);
    }

    private int minWindowWidth() {
      return Math.min(scaled(720), Math.max(560, this.screenBounds.width));
    }

    private int minWindowHeight() {
      return Math.min(scaled(500), Math.max(420, this.screenBounds.height));
    }

    private Dimension windowMinimumSize() {
      return new Dimension(minWindowWidth(), minWindowHeight());
    }

    private Dimension windowPreferredSize() {
      final int preferredWidth = Math.min(scaled(980), Math.max(minWindowWidth(), this.screenBounds.width - scaled(80)));
      final int preferredHeight = Math.min(scaled(720), Math.max(minWindowHeight(), this.screenBounds.height - scaled(80)));
      return new Dimension(preferredWidth, preferredHeight);
    }

    private Insets formInsets() {
      return new Insets(scaled(3), scaled(4), scaled(3), scaled(8));
    }

    private Rectangle usableScreenBounds() {
      return new Rectangle(this.screenBounds);
    }

    private void applyFonts(final Component component) {
      final Font font = component.getFont();
      if (font != null) {
        final int size = Math.max(font.getSize(), scaled(font.getSize()));
        component.setFont(font.deriveFont((float) size));
      }
      if (component instanceof Container container) {
        for (final Component child : container.getComponents()) {
          applyFonts(child);
        }
      }
    }
  }
}
