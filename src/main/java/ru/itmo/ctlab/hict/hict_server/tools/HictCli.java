package ru.itmo.ctlab.hict.hict_server.tools;

import io.vertx.core.Launcher;
import hdf.hdf5lib.H5;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeCpuFeatures;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ConversionDirection;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ExternalToolchainManager;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.HictToMcoolExportPipeline;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.HictkConversionPipeline;
import ru.itmo.ctlab.hict.hict_server.MainVerticle;
import ru.itmo.ctlab.hict.hict_server.launcher.HictLauncherGui;

import java.awt.GraphicsEnvironment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.regex.Pattern;

@Command(
  name = "hict",
  mixinStandardHelpOptions = true,
  version = "HiCT JVM",
  description = "HiCT server and conversion CLI.",
  subcommands = {
    HictCli.StartServer.class,
    HictCli.StartApiServer.class,
    HictCli.LauncherGui.class,
    HictCli.Convert.class,
    HictCli.CheckToolchains.class
  }
)
public class HictCli implements Runnable {
  @Option(names = {"-v", "--verbose"}, description = "Enable verbose output.")
  boolean verbose;

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  public static void main(final String[] args) {
    final CommandLine commandLine = new CommandLine(new HictCli())
      .setCaseInsensitiveEnumValuesAllowed(true);

    if (args.length == 0) {
      if (shouldUseLauncherGui()) {
        final int exitCode = commandLine.execute("launcher");
        if (exitCode != 0) {
          System.exit(exitCode);
        }
        return;
      }
      if (System.getProperty("AUTO_OPEN_BROWSER") == null) {
        System.setProperty("AUTO_OPEN_BROWSER", "true");
      }
      commandLine.execute("start-server");
      return;
    }

    final CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
    if (parseResult.hasMatchedOption("verbose")) {
      System.setProperty("HICT_VERBOSE", "true");
    }
    final var subcommand = parseResult.subcommand();
    final String subcommandName = subcommand != null ? subcommand.commandSpec().name() : "";

    final int exitCode = commandLine.execute(args);
    if ("start-server".equals(subcommandName) || "start-api-server".equals(subcommandName)) {
      return;
    }
    System.exit(exitCode);
  }

  private static boolean shouldUseLauncherGui() {
    final var mode = firstNonBlank(
      System.getProperty("HICT_LAUNCHER_MODE"),
      System.getenv("HICT_LAUNCHER_MODE")
    );
    if (!"gui".equalsIgnoreCase(mode)) {
      return false;
    }
    if (GraphicsEnvironment.isHeadless()) {
      System.err.println("HICT_LAUNCHER_MODE=gui was requested, but this Java runtime is headless; starting the server instead.");
      return false;
    }
    return true;
  }

  private static String firstNonBlank(final String... values) {
    for (final var value : values) {
      if (value != null && !value.isBlank()) {
        return value.trim();
      }
    }
    return null;
  }

  @Command(
    name = "start-server",
    mixinStandardHelpOptions = true,
    description = "Start API server with WebUI."
  )
  static class StartServer implements Callable<Integer> {
    @Option(names = "--serve-webui", description = "Whether to serve WebUI (default: true).", defaultValue = "true")
    boolean serveWebUi;

    @Override
    public Integer call() {
      System.setProperty("SERVE_WEBUI", Boolean.toString(serveWebUi));
      Launcher.main(new String[]{"run", MainVerticle.class.getName()});
      return 0;
    }
  }

  @Command(
    name = "start-api-server",
    mixinStandardHelpOptions = true,
    description = "Start API server only (no WebUI)."
  )
  static class StartApiServer implements Callable<Integer> {
    @Override
    public Integer call() {
      System.setProperty("SERVE_WEBUI", "false");
      Launcher.main(new String[]{"run", MainVerticle.class.getName()});
      return 0;
    }
  }

  @Command(
    name = "launcher",
    mixinStandardHelpOptions = true,
    description = "Open the portable graphical launcher."
  )
  static class LauncherGui implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
      if (GraphicsEnvironment.isHeadless()) {
        throw new IllegalStateException("Cannot open the graphical launcher in a headless environment.");
      }
      HictLauncherGui.launchAndBlock();
      return 0;
    }
  }

  @Command(
    name = "check-toolchains",
    mixinStandardHelpOptions = true,
    description = "Verify bundled or configured external conversion and dotplot tools."
  )
  static class CheckToolchains implements Callable<Integer> {
    @Option(names = "--require-hictk", description = "Fail if hictk is not available.")
    boolean requireHictk;

    @Option(names = "--require-dotplot", description = "Fail if no selected dotplot aligner is available.")
    boolean requireDotplot;

    @Option(names = "--require-hdf5-native", description = "Fail if bundled JHDF5/HDF5 native libraries cannot be initialized.")
    boolean requireHdf5Native;

    @Option(names = "--check-available-natives", description = "Smoke-test available native components without failing for absent or CPU-unsupported optional variants.")
    boolean checkAvailableNatives;

    @Option(names = "--quiet", description = "Only print errors.")
    boolean quiet;

    @Override
    public Integer call() {
      final var status = new ExternalToolchainManager().inspect();
      if (!quiet) {
        System.out.println(status.summary());
        System.out.println("Platform: " + status.platform());
        System.out.println("Source: " + status.source());
        System.out.println("hictk: " + valueOrNone(status.hictkCommand()));
        System.out.println("minimap2: " + valueOrNone(status.minimap2Command()));
        System.out.println("mm2-plus AVX2: " + valueOrNone(status.mm2PlusAvx2Command()));
        System.out.println("selected dotplot aligner: " + status.selectedDotplotAligner()
          + " (" + valueOrNone(status.selectedDotplotAlignerCommand()) + ")");
      }
      final var failures = new ArrayList<String>();
      if (requireHictk && !status.hictkAvailable()) {
        failures.add("hictk is unavailable");
      }
      if (requireDotplot && status.selectedDotplotAlignerCommand() == null) {
        failures.add("dotplot aligner is unavailable");
      }
      if (requireHdf5Native) {
        try {
          HDF5LibraryInitializer.initializeHDF5Library();
          final int[] version = new int[3];
          H5.H5get_libversion(version);
          if (!quiet) {
            System.out.println("HDF5 native library: " + version[0] + "." + version[1] + "." + version[2]);
          }
        } catch (final Throwable err) {
          failures.add("HDF5 native library is unavailable: " + err.getClass().getSimpleName() + ": " + err.getMessage());
        }
      }
      if (checkAvailableNatives) {
        smokeHictNativeIfSupported(failures);
        smokeCommandIfPresent("hictk", status.hictkCommand(), List.of("--version"), failures);
        smokeCommandIfPresent("minimap2", status.minimap2Command(), List.of("--version"), failures);
        if (NativeCpuFeatures.supportsAvx2Core()) {
          smokeCommandIfPresent("mm2-plus AVX2", status.mm2PlusAvx2Command(), List.of("--version"), failures);
        } else if (!quiet && status.mm2PlusAvx2Command() != null) {
          System.out.println("mm2-plus AVX2: skipped because this runner does not advertise AVX2");
        }
        if (NativeCpuFeatures.supportsAvx512Core()) {
          smokeCommandIfPresent("mm2-plus AVX-512", status.mm2PlusAvx512Command(), List.of("--version"), failures);
        } else if (!quiet && status.mm2PlusAvx512Command() != null) {
          System.out.println("mm2-plus AVX-512: skipped because this runner does not advertise AVX-512 core features");
        }
      }
      if (!failures.isEmpty()) {
        System.err.println("External toolchain check failed: " + String.join("; ", failures));
        System.err.println(status.summary());
        return 1;
      }
      return 0;
    }

    private static String valueOrNone(final String value) {
      return value == null || value.isBlank() ? "none" : value;
    }

    private void smokeHictNativeIfSupported(final List<String> failures) {
      final var arch = System.getProperty("os.arch", "").toLowerCase(java.util.Locale.ROOT);
      if (arch.equals("aarch64") || arch.equals("arm64")) {
        if (!quiet) {
          System.out.println("HiCT native processing: skipped on ARM runner unless an ARM native-processing payload is selected");
        }
        return;
      }
      if (!hasBundledHictNativeResource()) {
        if (!quiet) {
          System.out.println("HiCT native processing: not bundled for this runtime platform; skipping");
        }
        return;
      }
      final var status = NativeProcessingService.getInstance().setRequestedEnabled(true);
      if (!status.available()) {
        failures.add("HiCT native processing is unavailable: " + status.reason());
      } else if (!quiet) {
        System.out.println("HiCT native processing: " + status.version() + " from " + status.source());
      }
    }

    private boolean hasBundledHictNativeResource() {
      final var os = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT);
      final var arch = System.getProperty("os.arch", "").toLowerCase(java.util.Locale.ROOT);
      final var is64Bit = arch.contains("64") || arch.equals("amd64") || arch.equals("x86_64");
      if (!is64Bit) {
        return false;
      }
      final String platformDirectory;
      if (os.contains("linux")) {
        platformDirectory = "linux_64";
      } else if (os.contains("win")) {
        platformDirectory = "windows_64";
      } else if (os.contains("mac") || os.contains("darwin")) {
        platformDirectory = "macos_64";
      } else {
        return false;
      }
      for (final var variant : NativeCpuFeatures.preferredNativeVariantOrder()) {
        final String libraryBaseName;
        final String variantDirectory;
        switch (variant) {
          case "avx512" -> {
            libraryBaseName = "hict_native_avx512";
            variantDirectory = "avx512";
          }
          case "avx2" -> {
            libraryBaseName = "hict_native";
            variantDirectory = "avx2";
          }
          case "generic" -> {
            libraryBaseName = "hict_native_sse2";
            variantDirectory = "generic";
          }
          default -> {
            continue;
          }
        }
        final var mappedName = System.mapLibraryName(libraryBaseName);
        if (HictCli.class.getResource("/natives/" + platformDirectory + "/" + variantDirectory + "/native/" + mappedName) != null
          || HictCli.class.getResource("/natives/" + platformDirectory + "/" + mappedName) != null) {
          return true;
        }
      }
      return false;
    }

    private void smokeCommandIfPresent(final String label,
                                       final String command,
                                       final List<String> arguments,
                                       final List<String> failures) {
      if (command == null || command.isBlank()) {
        if (!quiet) {
          System.out.println(label + ": not bundled or configured; skipping");
        }
        return;
      }
      final var commandLine = new ArrayList<String>(1 + arguments.size());
      commandLine.add(command);
      commandLine.addAll(arguments);
      try {
        final var process = new ProcessBuilder(commandLine)
          .redirectErrorStream(true)
          .start();
        if (!process.waitFor(15, TimeUnit.SECONDS)) {
          process.destroyForcibly();
          failures.add(label + " timed out");
          return;
        }
        if (process.exitValue() != 0) {
          failures.add(label + " failed with exit code " + process.exitValue());
        } else if (!quiet) {
          System.out.println(label + ": executable smoke test passed");
        }
      } catch (final Exception err) {
        failures.add(label + " could not be executed: " + err.getMessage());
      }
    }
  }

  @Command(
    name = "convert",
    mixinStandardHelpOptions = true,
    description = "Run file converters.",
    subcommands = {
      HictCli.HictToMcool.class,
      HictCli.McoolToHict.class,
      HictCli.HicToMcool.class,
      HictCli.HicToHict.class
    }
  )
  static class Convert implements Runnable {
    @Override
    public void run() {
      CommandLine.usage(this, System.out);
    }
  }

  abstract static class BaseConvert implements Callable<Integer> {
    private static final Pattern OVERALL_PROGRESS_PATTERN = Pattern.compile("Overall progress:\\s*(\\d+)%");
    private static final Pattern LOCAL_PROGRESS_PATTERN = Pattern.compile("^[^\\n]*:\\s*(\\d+)%\\s*\\(");

    @Option(names = {"-i", "--input"}, required = true, description = "Input file path.")
    Path input;

    @Option(names = {"-o", "--output"}, required = true, description = "Output file path.")
    Path output;

    @Option(
      names = "--resolutions",
      split = ",",
      description = "Comma-separated list of resolutions to export (default: all in input)."
    )
    List<Long> resolutions = new ArrayList<>();

    @Option(names = "--chunk-size", defaultValue = "8192", description = "HDF5 chunk size (default: 8192).")
    int chunkSize;

    @Option(names = "--compression", defaultValue = "6", description = "Compression level 0..9 (default: 6).")
    int compression;

    @Option(
      names = "--compression-algorithm",
      defaultValue = "DEFLATE",
      description = "Compression algorithm: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})."
    )
    ConversionOptions.CompressionAlgorithm compressionAlgorithm;


    @Option(
      names = "--parallelism",
      defaultValue = "0",
      description = "Number of worker threads (0 or -1 = auto)."
    )
    int parallelism;

    @Option(
      names = "--all-resolutions",
      defaultValue = "false",
      description = "Export all available resolutions (default: only finest resolution)."
    )
    boolean exportAllResolutions;

    @Option(
      names = "--build-resolution-pyramid",
      defaultValue = "true",
      description = "Build a hictk nice-step resolution pyramid when importing Cooler files (default: ${DEFAULT-VALUE})."
    )
    boolean buildResolutionPyramid;

    @Option(
      names = "--balance-input-coolers",
      defaultValue = "true",
      description = "Balance .cool/.mcool inputs with hictk before importing (default: ${DEFAULT-VALUE})."
    )
    boolean balanceInputCoolers;

    @Option(
      names = "--balance-exported-coolers",
      defaultValue = "true",
      description = "Balance hictk-assisted Cooler exports (default: ${DEFAULT-VALUE})."
    )
    boolean balanceExportedCoolers;

    @Option(
      names = "--export-mode",
      defaultValue = "AUTO",
      description = "Export mode for .hict.hdf5 -> .mcool: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})."
    )
    ConversionOptions.ExportMode exportMode;

    ConversionOptions toOptions(String agpPath, boolean applyAgp) {
      return new ConversionOptions(
        input,
        output,
        resolutions,
        chunkSize,
        compression,
        compressionAlgorithm,
        agpPath,
        applyAgp,
        parallelism,
        exportAllResolutions,
        buildResolutionPyramid,
        balanceInputCoolers,
        balanceExportedCoolers,
        exportMode
      );
    }

    Consumer<String> stdoutLogger() {
      final boolean verboseEnabled = Boolean.parseBoolean(System.getProperty("HICT_VERBOSE", "false"));
      return message -> {
        if (!verboseEnabled) {
          return;
        }
        synchronized (System.out) {
          final var overall = OVERALL_PROGRESS_PATTERN.matcher(message);
          final var local = LOCAL_PROGRESS_PATTERN.matcher(message);
          if (overall.find()) {
            System.out.println("[TOTAL " + overall.group(1) + "%] " + message);
          } else if (local.find()) {
            System.out.println("[LOCAL " + local.group(1) + "%] " + message);
          } else {
            System.out.println(message);
          }
          System.out.flush();
        }
      };
    }

    void initializeHdf5() {
      HDF5LibraryInitializer.initializeHDF5Library();
    }

    HictkConversionPipeline hictkPipeline() {
      return new HictkConversionPipeline(new ExternalToolchainManager());
    }
  }

  @Command(
    name = "hict-to-mcool",
    mixinStandardHelpOptions = true,
    description = "Convert .hict.hdf5 to .mcool."
  )
  static class HictToMcool extends BaseConvert {
    @Option(names = "--agp", description = "AGP file path (optional).")
    String agpPath = ConversionOptions.NO_AGP;

    @Option(names = "--apply-agp", description = "Apply AGP before export.")
    boolean applyAgp;

    @Override
    public Integer call() throws Exception {
      initializeHdf5();
      new HictToMcoolExportPipeline(new ExternalToolchainManager())
        .convert(toOptions(agpPath, applyAgp), stdoutLogger());
      return 0;
    }
  }

  @Command(
    name = "mcool-to-hict",
    mixinStandardHelpOptions = true,
    description = "Convert .mcool to .hict.hdf5."
  )
  static class McoolToHict extends BaseConvert {
    @Override
    public Integer call() throws Exception {
      initializeHdf5();
      final var options = toOptions(ConversionOptions.NO_AGP, false);
      if (options.buildResolutionPyramid() || options.balanceInputCoolers()) {
        final var pipeline = hictkPipeline();
        final var toolchain = pipeline.requireToolchain();
        pipeline.convertCoolerToHictWithPyramid(
          options,
          toolchain,
          stdoutLogger(),
          process -> {
          },
          () -> false
        );
      } else {
        new McoolToHictConverter().convert(options, stdoutLogger());
      }
      return 0;
    }
  }

  @Command(
    name = "hic-to-mcool",
    mixinStandardHelpOptions = true,
    description = "Convert .hic to balanced .mcool through the external hictk pipeline."
  )
  static class HicToMcool extends BaseConvert {
    @Override
    public Integer call() throws Exception {
      final var pipeline = hictkPipeline();
      final var toolchain = pipeline.requireToolchain();
      pipeline.convert(
        ConversionDirection.HIC_TO_MCOOL,
        toOptions(ConversionOptions.NO_AGP, false),
        toolchain,
        stdoutLogger(),
        process -> {
        },
        () -> false
      );
      return 0;
    }
  }

  @Command(
    name = "hic-to-hict",
    mixinStandardHelpOptions = true,
    description = "Convert .hic to .hict.hdf5 through the external hictk pipeline."
  )
  static class HicToHict extends BaseConvert {
    @Override
    public Integer call() throws Exception {
      initializeHdf5();
      final var pipeline = hictkPipeline();
      final var toolchain = pipeline.requireToolchain();
      pipeline.convert(
        ConversionDirection.HIC_TO_HICT,
        toOptions(ConversionOptions.NO_AGP, false),
        toolchain,
        stdoutLogger(),
        process -> {
        },
        () -> false
      );
      return 0;
    }
  }
}
