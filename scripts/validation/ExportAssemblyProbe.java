import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

/** Uses the real runtime exporters; compatible with pre-fix jars for comparison. */
public class ExportAssemblyProbe {
  public static void main(String[] args) throws Exception {
    final var output = Path.of(args[3]);
    Files.createDirectories(output);
    try (final var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(Path.of(args[0]), 1, 2))) {
      final var contigs = new JsonArray();
      for (final var contig : file.getAssemblyInfo().contigs()) {
        contigs.add(new JsonObject().put("name", contig.descriptor().getContigName())
          .put("length", contig.descriptor().getLengthBp()));
      }
      Files.writeString(output.resolve("source-contigs.json"), contigs.encodePrettily());
      try (final var reader = Files.newBufferedReader(Path.of(args[1]))) { file.importAGP(reader); }
      for (final var value : args[4].split(",")) {
        final long gap = Long.parseLong(value);
        System.setProperty("HICT_EXPORT_GAP_LENGTH_BP", value);
        final var start = System.nanoTime();
        final var agp = file.getAgpProcessor().getAGPStream(gap).collect(Collectors.joining());
        Files.writeString(output.resolve("assembly-gap" + gap + ".agp"), agp);
        if (!args[2].equals("-")) {
          Files.writeString(output.resolve("assembly-gap" + gap + ".fasta"), file.getFastaProcessor().exportAssembly(Path.of(args[2])));
        }
        System.out.printf("AGP + FASTA export: gap=%d bp elapsed=%.3f s%n", gap, (System.nanoTime() - start) / 1e9);
      }
    }
  }
}
