package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ru.itmo.ctlab.hict.hict_library.util.ProcessingThreads;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/** Adds only thread options actually supported by the bundled command's CLI. */
public final class BundledToolThreads {
  private BundledToolThreads() {}

  public static List<String> hictk(final List<String> command, final Consumer<String> logger) {
    return prepare(command, "hictk", true, logger);
  }

  public static List<String> aligner(final List<String> command, final Consumer<String> logger) {
    return prepare(command, "minimap2", true, logger);
  }

  public static List<String> toolbox(final List<String> command, final String tool, final Consumer<String> logger) {
    return prepare(command, tool, false, logger);
  }

  private static List<String> prepare(final List<String> command, final String tool,
                                      final boolean managed, final Consumer<String> logger) {
    if (command.size() < 2) return command;
    final int separator = command.indexOf("--");
    final int optionsEnd = separator < 0 ? command.size() : separator;
    if (command.subList(1, optionsEnd).stream().anyMatch(arg -> arg.equals("-h") || arg.startsWith("--help")
      || arg.equals("--version") || arg.equals("-V"))) return command;
    final boolean hictk = tool.equals("hictk");
    final String operation = command.get(1);
    if (hictk && !(Set.of("load", "convert", "zoomify", "merge").contains(operation)
      || (operation.equals("balance") && command.size() > 2 && Set.of("ice", "scale").contains(command.get(2))))) {
      return command; // dump, metadata, validate and balance vc have no --threads.
    }
    final var result = new ArrayList<String>();
    int requested = -1;
    boolean explicit = false;
    for (int i = 0; i < command.size(); i++) {
      final String arg = command.get(i);
      if (i > 0 && i < optionsEnd && (arg.equals("-t") || (hictk && arg.equals("--threads")))) {
        if (++i >= optionsEnd) throw new IllegalArgumentException(arg + " requires a thread count");
        requested = ProcessingThreads.parse(command.get(i), arg);
        explicit = true;
      } else if (i > 0 && i < optionsEnd && ((hictk && arg.startsWith("--threads=")) || arg.matches("-t-?\\d+"))) {
        requested = ProcessingThreads.parse(arg.startsWith("--") ? arg.substring(10) : arg.substring(2), "threads");
        explicit = true;
      } else {
        result.add(arg);
      }
    }
    final String key = hictk ? "HICT_HICTK_THREADS" : "HICT_ALIGNMENT_THREADS";
    final int configured = ProcessingThreads.configured(key);
    if (managed && configured > 0) requested = configured;
    final int budget = ProcessingThreads.resolve(requested, key);
    // hictk load/convert require two pipeline threads even on a one-CPU allocation.
    final int minimum = hictk && Set.of("load", "convert").contains(operation) ? 2 : 1;
    final int selected = Math.max(minimum, budget);
    // Keep flags before positional arguments: appending after a variadic
    // --resolutions list changes how CLI11 assigns the input/output filenames.
    final int insertion = hictk ? (operation.equals("balance") ? 3 : 2) : 1;
    result.addAll(insertion, List.of(hictk ? "--threads" : "-t", Integer.toString(selected)));
    logger.accept("HiCT tool threads: tool=" + tool + (hictk ? " command=" + operation : "")
      + ", requested=" + requested + ", selected=" + selected + ", available=" + ProcessingThreads.available()
      + ", JVM-visible=" + Runtime.getRuntime().availableProcessors()
      + ", SLURM_CPUS_PER_TASK=" + System.getenv().getOrDefault("SLURM_CPUS_PER_TASK", "unset")
      + ", explicit=" + explicit);
    if (hictk) {
      switch (operation) {
        case "zoomify", "merge" -> logger.accept("HiCT tool limitation: hictk " + operation
          + " uses one thread for Cooler input; --threads enables parallel work only on supported formats.");
        case "load", "convert" -> logger.accept("HiCT tool limitation: hictk " + operation
          + " uses at most two threads when writing Cooler; its pipeline requires --threads >= 2.");
        case "balance" -> logger.accept("HiCT tool limitation: balancing can be I/O-bound with disk-backed interactions;"
          + " --in-memory requires enough RAM for the entire matrix and is not enabled automatically.");
        default -> { }
      }
    }
    logger.accept("HiCT tool command: " + String.join(" ", result));
    return List.copyOf(result);
  }
}
