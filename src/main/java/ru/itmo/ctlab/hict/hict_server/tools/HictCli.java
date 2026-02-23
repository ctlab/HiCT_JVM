package ru.itmo.ctlab.hict.hict_server.tools;

import io.vertx.core.Launcher;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.HictToMcoolConverter;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;
import ru.itmo.ctlab.hict.hict_server.MainVerticle;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

@Command(
  name = "hict",
  mixinStandardHelpOptions = true,
  version = "HiCT JVM",
  description = "HiCT server and conversion CLI.",
  subcommands = {
    HictCli.StartServer.class,
    HictCli.StartApiServer.class,
    HictCli.Convert.class
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
      CommandLine.usage(commandLine.getCommand(), System.out);
      commandLine.execute("start-server");
      return;
    }

    final CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
    final var subcommand = parseResult.subcommand();
    final String subcommandName = subcommand != null ? subcommand.commandSpec().name() : "";

    final int exitCode = commandLine.execute(args);
    if ("start-server".equals(subcommandName) || "start-api-server".equals(subcommandName)) {
      return;
    }
    System.exit(exitCode);
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
    name = "convert",
    mixinStandardHelpOptions = true,
    description = "Run file converters.",
    subcommands = {
      HictCli.HictToMcool.class,
      HictCli.McoolToHict.class
    }
  )
  static class Convert implements Runnable {
    @Override
    public void run() {
      CommandLine.usage(this, System.out);
    }
  }

  abstract static class BaseConvert implements Callable<Integer> {
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

    @Option(names = "--compression", defaultValue = "0", description = "Compression level 0..9 (default: 0).")
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
        parallelism
      );
    }

    Consumer<String> stdoutLogger() {
      return message -> {
        synchronized (System.out) {
          System.out.println(message);
          System.out.flush();
        }
      };
    }

    void initializeHdf5() {
      HDF5LibraryInitializer.initializeHDF5Library();
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
      new HictToMcoolConverter().convert(toOptions(agpPath, applyAgp), stdoutLogger());
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
      new McoolToHictConverter().convert(toOptions(ConversionOptions.NO_AGP, false), stdoutLogger());
      return 0;
    }
  }
}
