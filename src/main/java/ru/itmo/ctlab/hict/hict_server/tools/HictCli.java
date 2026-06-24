package ru.itmo.ctlab.hict.hict_server.tools;

import io.vertx.core.Launcher;
import hdf.hdf5lib.H5;
import org.jetbrains.annotations.NotNull;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
    HictCli.CheckToolchains.class,
    HictCli.Toolbox.class
  }
)
public class HictCli implements Runnable {
  private static final Set<String> BOOLEAN_OPTIONS_WITH_OPTIONAL_VALUE = Set.of(
    "--verbose",
    "-v",
    "--serve-webui",
    "--require-hictk",
    "--require-dotplot",
    "--require-hdf5-native",
    "--check-available-natives",
    "--quiet",
    "--all-resolutions",
    "--build-resolution-pyramid",
    "--balance-input-coolers",
    "--balance-exported-coolers",
    "--apply-agp"
  );

  @Option(names = {"-v", "--verbose"}, arity = "0..1", fallbackValue = "true", converter = BooleanOptionConverter.class, description = "Enable verbose output.")
  boolean verbose;

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  public static void main(final String[] args) {
    final CommandLine commandLine = new CommandLine(new HictCli())
      .setCaseInsensitiveEnumValuesAllowed(true)
      .setParameterExceptionHandler(HictCli::handleParameterException)
      .setExecutionExceptionHandler(HictCli::handleExecutionException);

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

    if (isVerboseRequested(args)) {
      System.setProperty("HICT_VERBOSE", "true");
    }
    if ("toolbox".equals(args[0])) {
      System.exit(runToolbox(Arrays.copyOfRange(args, 1, args.length)));
      return;
    }
    if (isToolLikeCommand(args[0])) {
      System.err.println("HiCT does not run external tool names as top-level commands.");
      System.err.println("Did you mean: hict toolbox " + args[0] + " ...");
      System.err.println();
      printToolboxUsage(System.err);
      System.exit(CommandLine.ExitCode.USAGE);
      return;
    }

    final int exitCode = commandLine.execute(normalizeBooleanOptionArgs(args));
    final var parseResult = commandLine.getParseResult();
    final var subcommand = parseResult == null ? null : parseResult.subcommand();
    final String subcommandName = subcommand != null ? subcommand.commandSpec().name() : "";
    if ("start-server".equals(subcommandName) || "start-api-server".equals(subcommandName)) {
      return;
    }
    System.exit(exitCode);
  }

  private static int handleParameterException(final CommandLine.ParameterException ex,
                                              final String[] args) {
    final var commandLine = ex.getCommandLine();
    System.err.println("HiCT could not parse the command line:");
    System.err.println("  " + ex.getMessage());
    printLikelyIntent(args);
    System.err.println();
    System.err.println("Boolean options accept true/false, yes/no, y/n, or 1/0.");
    System.err.println("For negatable options you can also use --no-build-resolution-pyramid, --no-balance-input-coolers, or --no-balance-exported-coolers.");
    System.err.println();
    commandLine.usage(System.err);
    return CommandLine.ExitCode.USAGE;
  }

  private static int handleExecutionException(final Exception ex,
                                              final CommandLine commandLine,
                                              final CommandLine.ParseResult parseResult) {
    System.err.println("HiCT command failed:");
    System.err.println("  " + userFacingMessage(ex));
    if (Boolean.parseBoolean(System.getProperty("HICT_VERBOSE", "false"))) {
      ex.printStackTrace(System.err);
    } else {
      System.err.println("Run again with --verbose for a stack trace.");
    }
    return 1;
  }

  private static void printLikelyIntent(final String[] args) {
    if (args == null || args.length == 0) {
      return;
    }
    if (isToolLikeCommand(args[0])) {
      System.err.println("  Did you mean: hict toolbox " + args[0] + " ...");
    }
  }

  private static boolean isToolLikeCommand(final String raw) {
    final var value = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
    return value.equals("hictk")
      || value.equals("minimap2")
      || value.equals("mm2plus")
      || value.equals("mm2-plus")
      || value.equals("mm2plus-avx2")
      || value.equals("mm2-plus-avx2")
      || value.equals("mm2plus-avx512")
      || value.equals("mm2-plus-avx512");
  }

  private static String userFacingMessage(final Throwable ex) {
    final var message = ex.getMessage();
    if (message != null && !message.isBlank()) {
      return message;
    }
    return ex.getClass().getSimpleName();
  }

  private static boolean isVerboseRequested(final String[] args) {
    for (int i = 0; i < args.length; i++) {
      final var arg = args[i];
      if ("--verbose".equals(arg) || "-v".equals(arg)) {
        if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
          try {
            return parseBooleanOption(args[i + 1]);
          } catch (final CommandLine.TypeConversionException ignored) {
            return true;
          }
        }
        return true;
      }
      if (arg.startsWith("--verbose=")) {
        return parseBooleanOption(arg.substring("--verbose=".length()));
      }
    }
    return false;
  }

  private static String[] normalizeBooleanOptionArgs(final String[] args) {
    final var normalized = new ArrayList<String>(args.length);
    for (int i = 0; i < args.length; i++) {
      final var arg = args[i];
      if (BOOLEAN_OPTIONS_WITH_OPTIONAL_VALUE.contains(arg)
        && i + 1 < args.length
        && !args[i + 1].startsWith("-")) {
        try {
          parseBooleanOption(args[i + 1]);
          normalized.add(arg + "=" + args[i + 1]);
          i++;
          continue;
        } catch (final CommandLine.TypeConversionException ignored) {
          normalized.add(arg);
          normalized.add("true");
          continue;
        }
      }
      normalized.add(arg);
    }
    return normalized.toArray(String[]::new);
  }

  private static boolean parseBooleanOption(final String raw) {
    if (raw == null || raw.isBlank()) {
      return true;
    }
    return switch (raw.trim().toLowerCase(Locale.ROOT)) {
      case "true", "yes", "y", "1", "on" -> true;
      case "false", "no", "n", "0", "off" -> false;
      default -> throw new CommandLine.TypeConversionException(
        "Expected one of true/false, yes/no, y/n, or 1/0, but got '" + raw + "'"
      );
    };
  }

  static class BooleanOptionConverter implements CommandLine.ITypeConverter<Boolean> {
    @Override
    public Boolean convert(final String value) {
      return parseBooleanOption(value);
    }
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
    @Option(names = "--serve-webui", arity = "0..1", fallbackValue = "true", converter = BooleanOptionConverter.class, description = "Whether to serve WebUI (default: true).", defaultValue = "true")
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
    @Option(names = "--require-hictk", arity = "0..1", fallbackValue = "true", converter = BooleanOptionConverter.class, description = "Fail if hictk is not available.")
    boolean requireHictk;

    @Option(names = "--require-dotplot", arity = "0..1", fallbackValue = "true", converter = BooleanOptionConverter.class, description = "Fail if no selected dotplot aligner is available.")
    boolean requireDotplot;

    @Option(names = "--require-hdf5-native", arity = "0..1", fallbackValue = "true", converter = BooleanOptionConverter.class, description = "Fail if bundled JHDF5/HDF5 native libraries cannot be initialized.")
    boolean requireHdf5Native;

    @Option(names = "--check-available-natives", arity = "0..1", fallbackValue = "true", converter = BooleanOptionConverter.class, description = "Smoke-test available native components without failing for absent or CPU-unsupported optional variants.")
    boolean checkAvailableNatives;

    @Option(names = "--quiet", arity = "0..1", fallbackValue = "true", converter = BooleanOptionConverter.class, description = "Only print errors.")
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
    name = "toolbox",
    mixinStandardHelpOptions = true,
    description = {
      "Run bundled or configured external tools without manually extracting the portable package.",
      "Examples:",
      "  hict toolbox hictk --help",
      "  hict toolbox minimap2 --help",
      "  hict toolbox mm2-plus --help"
    }
  )
  static class Toolbox implements Callable<Integer> {
    @Override
    public Integer call() {
      printToolboxUsage(System.out);
      return 0;
    }
  }

  private static int runToolbox(final String[] args) {
    if (args.length == 0 || isHelpArgument(args[0])) {
      printToolboxUsage(System.out);
      return 0;
    }
    final var rawTool = args[0];
    final var tool = normalizeToolName(rawTool);
    final var manager = new ExternalToolchainManager();
    final var status = manager.inspect();
    final String command = switch (tool) {
      case "hictk" -> status.hictkCommand();
      case "minimap2" -> status.minimap2Command();
      case "mm2plus" -> {
        if (NativeCpuFeatures.supportsAvx512Core() && status.mm2PlusAvx512Command() != null && !status.mm2PlusAvx512Command().isBlank()) {
          yield status.mm2PlusAvx512Command();
        }
        if (status.mm2PlusAvx2Command() != null && !status.mm2PlusAvx2Command().isBlank()) {
          yield status.mm2PlusAvx2Command();
        }
        yield status.selectedDotplotAlignerCommand();
      }
      case "mm2plus-avx2" -> status.mm2PlusAvx2Command();
      case "mm2plus-avx512" -> status.mm2PlusAvx512Command();
      default -> null;
    };
    if (command == null || command.isBlank()) {
      System.err.println("HiCT toolbox could not find a bundled or configured executable for '" + rawTool + "'.");
      System.err.println(status.summary());
      if (!status.limitations().isEmpty()) {
        System.err.println("Limitations: " + String.join("; ", status.limitations()));
      }
      printToolboxUsage(System.err);
      return 1;
    }
    printToolNotice(tool, command);
    final var commandLine = new ArrayList<String>(args.length);
    commandLine.add(command);
    commandLine.addAll(Arrays.asList(args).subList(1, args.length));
    try {
      final var process = new ProcessBuilder(commandLine)
        .inheritIO()
        .start();
      return process.waitFor();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("HiCT toolbox command was interrupted.");
      return 130;
    } catch (final Exception e) {
      System.err.println("HiCT toolbox could not run '" + rawTool + "': " + userFacingMessage(e));
      return 1;
    }
  }

  private static boolean isHelpArgument(final String arg) {
    return "-h".equals(arg) || "--help".equals(arg) || "help".equalsIgnoreCase(arg);
  }

  private static String normalizeToolName(final String raw) {
    final var value = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
    return switch (value) {
      case "mm2-plus", "mm2plus", "mm2-plus-auto" -> "mm2plus";
      case "mm2plus-avx2", "mm2-plus-avx2" -> "mm2plus-avx2";
      case "mm2plus-avx512", "mm2-plus-avx512" -> "mm2plus-avx512";
      default -> value;
    };
  }

  private static void printToolboxUsage(final java.io.PrintStream out) {
    out.println("HiCT toolbox");
    out.println();
    out.println("Usage:");
    out.println("  hict toolbox hictk <hictk arguments...>");
    out.println("  hict toolbox minimap2 <minimap2 arguments...>");
    out.println("  hict toolbox mm2-plus <mm2-plus arguments...>");
    out.println("  hict toolbox mm2-plus-avx2 <mm2-plus arguments...>");
    out.println("  hict toolbox mm2-plus-avx512 <mm2-plus arguments...>");
    out.println();
    out.println("The command forwards all remaining arguments to the selected executable.");
  }

  private static void printToolNotice(final String tool, final String command) {
    System.err.println("HiCT toolbox is launching " + command);
    switch (tool) {
      case "hictk" -> {
        System.err.println("hictk is bundled/configured as an optional .hic/.cool tool and is provided by its upstream authors under its own license.");
        System.err.println("Project: https://github.com/paulsengroup/hictk");
        System.err.println("Citation: Rossini R, Paulsen J. Bioinformatics 2024;40(7):btae408.");
      }
      case "minimap2" -> {
        System.err.println("minimap2 is bundled/configured as an optional alignment tool and is provided by its upstream authors under its own license.");
        System.err.println("Project: https://github.com/lh3/minimap2");
        System.err.println("Citation: Li H. Bioinformatics 2018;34(18):3094-3100.");
      }
      default -> {
        System.err.println("mm2-plus is bundled/configured as an optional alignment tool and is provided by its upstream authors under its own license.");
        System.err.println("Project: https://github.com/at-cg/mm2-plus");
        System.err.println("Citation: Ghanshyam Chandra, Md Vasimuddin, Sanchit Misra and Chirag Jain. bioRxiv 2024. doi:10.1101/2024.11.25.625328.");
      }
    }
    System.err.println("Bundled license files, when available, are kept under toolchains/<platform>/share inside the portable package.");
    System.err.println();
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
      description = "Comma-separated list of resolutions to export. Export defaults to the finest available resolution unless --all-resolutions is set."
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
      arity = "0..1",
      fallbackValue = "true",
      converter = BooleanOptionConverter.class,
      defaultValue = "false",
      description = "Export all available resolutions (default: only finest resolution)."
    )
    boolean exportAllResolutions;

    @Option(
      names = "--build-resolution-pyramid",
      arity = "0..1",
      fallbackValue = "true",
      converter = BooleanOptionConverter.class,
      defaultValue = "true",
      negatable = true,
      description = "Build a hictk nice-step resolution pyramid when importing Cooler files (default: ${DEFAULT-VALUE})."
    )
    boolean buildResolutionPyramid;

    @Option(
      names = "--balance-input-coolers",
      arity = "0..1",
      fallbackValue = "true",
      converter = BooleanOptionConverter.class,
      defaultValue = "true",
      negatable = true,
      description = "Balance .cool/.mcool inputs with hictk before importing (default: ${DEFAULT-VALUE})."
    )
    boolean balanceInputCoolers;

    @Option(
      names = "--balance-exported-coolers",
      arity = "0..1",
      fallbackValue = "true",
      converter = BooleanOptionConverter.class,
      defaultValue = "true",
      negatable = true,
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
      final Map<String, Integer> lastLocalProgressByPhase = new HashMap<>();
      final Map<String, Integer> lastOverallProgressByPhase = new HashMap<>();
      return message -> {
        synchronized (System.out) {
          final var overall = OVERALL_PROGRESS_PATTERN.matcher(message);
          final var local = LOCAL_PROGRESS_PATTERN.matcher(message);
          if (verboseEnabled && overall.find()) {
            System.out.println("[TOTAL " + overall.group(1) + "%] " + message);
          } else if (verboseEnabled && local.find()) {
            System.out.println("[LOCAL " + local.group(1) + "%] " + message);
          } else if (verboseEnabled) {
            System.out.println(message);
          } else {
            final var overallForDefault = OVERALL_PROGRESS_PATTERN.matcher(message);
            final var localForDefault = LOCAL_PROGRESS_PATTERN.matcher(message);
            if (overallForDefault.find()) {
              final int percent = Integer.parseInt(overallForDefault.group(1));
              final int previous = lastOverallProgressByPhase.getOrDefault("overall", -1);
              if (percent != previous) {
                lastOverallProgressByPhase.put("overall", percent);
                System.out.println("[TOTAL " + percent + "%] " + message);
              } else {
                return;
              }
            } else if (localForDefault.find()) {
              final int percent = Integer.parseInt(localForDefault.group(1));
              final var phase = progressPhaseKey(message);
              final int previous = lastLocalProgressByPhase.getOrDefault(phase, -1);
              if (percent != previous) {
                lastLocalProgressByPhase.put(phase, percent);
                System.out.println("[LOCAL " + percent + "%] " + message);
              } else {
                return;
              }
            } else if (isImportantConversionMessage(message)) {
              System.out.println(message);
            } else {
              return;
            }
          }
          System.out.flush();
        }
      };
    }

    private static @NotNull String progressPhaseKey(final @NotNull String message) {
      final int colonIndex = message.indexOf(':');
      return colonIndex > 0 ? message.substring(0, colonIndex) : message;
    }

    private static boolean isImportantConversionMessage(final @NotNull String message) {
      return message.startsWith("HICT_")
        || message.startsWith("WARNING:")
        || message.startsWith("ERROR:")
        || message.startsWith("Converting ")
        || message.startsWith("Preparing ")
        || message.startsWith("Applied AGP ")
        || message.startsWith("Applied assembly ")
        || message.startsWith("HiCT native processing:")
        || message.startsWith("hictk-assisted export resources:")
        || message.startsWith("Using HiCT -> Cooler temporary directory:")
        || message.startsWith("HiCT export COO sort batch size=")
        || message.startsWith("Clamped HiCT export COO sort batch size")
        || message.startsWith("Ignoring invalid HiCT export COO sort batch size")
        || message.startsWith("Clamped HiCT export COO merge fan-in")
        || message.startsWith("Ignoring invalid HiCT export COO merge fan-in")
        || message.startsWith("COO sort batch size=")
        || message.startsWith("Merging ")
        || message.startsWith("COO merge pass ")
        || message.startsWith("Merged sorted COO records complete:")
        || message.matches("Resolution \\d+ .*: \\d+% .*")
        || message.startsWith("Running hictk")
        || message.startsWith("Skipping hictk")
        || message.startsWith("Finished ")
        || message.startsWith("Created ")
        || message.contains("memory budget=")
        || message.contains("workers=")
        || message.contains("sortBatchSize=");
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
    description = "Convert .hict.hdf5 to multi-resolution .mcool or single-resolution .cool."
  )
  static class HictToMcool extends BaseConvert {
    @Option(names = "--agp", description = "AGP file path (optional).")
    String agpPath = ConversionOptions.NO_AGP;

    @Option(names = "--apply-agp", arity = "0..1", fallbackValue = "true", converter = BooleanOptionConverter.class, description = "Apply AGP before export.")
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
