package ru.itmo.ctlab.hict.hict_server.tools;

import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;
import ru.itmo.ctlab.hict.hict_library.converters.HictToMcoolConverter;
import ru.itmo.ctlab.hict.hict_library.converters.McoolToHictConverter;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class ConversionCliLauncher {
  private static final Pattern OVERALL_PROGRESS_PATTERN = Pattern.compile("Overall progress:\\s*(\\d+)%");
  private static final Pattern LOCAL_PROGRESS_PATTERN = Pattern.compile("^[^\\n]*:\\s*(\\d+)%\\s*\\(");

  static {
    HDF5LibraryInitializer.initializeHDF5Library();
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || "help".equals(args[0])) {
      printHelp();
      return;
    }

    final var command = args[0];
    final var parser = new ArgParser(Arrays.copyOfRange(args, 1, args.length));
    final var verbose = parser.flag("verbose");
    final var options = new ConversionOptions(
      Path.of(parser.require("input")),
      Path.of(parser.require("output")),
      parser.listOfLong("resolutions"),
      parser.integer("chunk-size", 8192),
      parser.integer("compression", 6),
      ConversionOptions.CompressionAlgorithm.parse(parser.value("compression-algorithm", "deflate")),
      parser.value("agp", ConversionOptions.NO_AGP),
      parser.flag("apply-agp"),
      parser.integer("parallelism", Runtime.getRuntime().availableProcessors())
    );

    switch (command) {
      case "hict-to-mcool" -> new HictToMcoolConverter().convert(options, stdoutLogger(verbose));
      case "mcool-to-hict" -> new McoolToHictConverter().convert(options, stdoutLogger(verbose));
      default -> throw new IllegalArgumentException("Unknown command: " + command);
    }
  }

  private static Consumer<String> stdoutLogger(final boolean verbose) {
    return message -> {
      if (!verbose) {
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

  private static void printHelp() {
    System.out.println("Usage:");
    System.out.println("  hict-to-mcool --input=<in.hict> --output=<out.mcool> [--resolutions=10000,50000] [--compression=0..9 (default: 6)] [--compression-algorithm=deflate|zstd|lzf] [--chunk-size=8192] [--agp=foo.agp --apply-agp] [--parallelism=N] [--verbose]");
    System.out.println("  mcool-to-hict --input=<in.mcool> --output=<out.hict> [--resolutions=10000,50000] [--compression=0..9 (default: 6)] [--compression-algorithm=deflate|zstd|lzf] [--chunk-size=8192] [--parallelism=N] [--verbose]");
  }

  private record ArgParser(String[] args) {
    String require(String key) {
      final var value = value(key, null);
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("Missing required --" + key);
      }
      return value;
    }

    String value(String key, String defaultValue) {
      final var prefix = "--" + key + "=";
      return Arrays.stream(args).filter(a -> a.startsWith(prefix)).map(a -> a.substring(prefix.length())).findFirst().orElse(defaultValue);
    }

    int integer(String key, int defaultValue) {
      final var value = value(key, String.valueOf(defaultValue));
      return Integer.parseInt(value);
    }

    boolean flag(String key) {
      final var prefix = "--" + key;
      return Arrays.stream(args).anyMatch(prefix::equals);
    }

    List<Long> listOfLong(String key) {
      final var value = value(key, "");
      if (value.isBlank()) {
        return List.of();
      }
      return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isBlank()).map(Long::parseLong).toList();
    }
  }
}
