import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

/** Read-only runtime assembly and raw matrix probe, used by the round-trip validation. */
public class QueryAssembly {
  public static void main(String[] args) throws Exception {
    final long resolution = Long.parseLong(args[1]);
    try (final var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(
      Path.of(args[0]), 1, 2, java.util.List.of(resolution)))) {
      if (args.length > 3) {
        try (final var reader = Files.newBufferedReader(Path.of(args[3]))) { file.importAGP(reader); }
      }
      final int order = file.getResolutionToIndex().get(resolution);
      final var contigs = new JsonArray();
      long cursor = 0;
      final var starts = new java.util.ArrayList<Long>();
      for (final var contig : file.getAssemblyInfo().contigs()) {
        final long bins = contig.descriptor().getAtus().get(order).stream().mapToLong(a -> a.getLength()).sum();
        contigs.add(new JsonObject().put("name", contig.descriptor().getContigName())
          .put("length", contig.descriptor().getLengthBp()).put("direction", contig.direction().name())
          .put("startBin", cursor).put("bins", bins));
        starts.add(cursor);
        cursor += bins;
      }
      final var scaffolds = new JsonArray();
      for (final var scaffold : file.getAssemblyInfo().scaffolds()) {
        scaffolds.add(new JsonObject().put("name", scaffold.scaffoldDescriptor().scaffoldName())
          .put("start", scaffold.scaffoldBordersBP().startBP()).put("end", scaffold.scaffoldBordersBP().endBP()));
      }
      final var queries = new JsonArray();
      final var rng = new Random(286495);
      for (int i = 0; i < 128 + starts.size(); i++) {
        final long row = i < starts.size() ? starts.get(i) : rng.nextLong(cursor);
        final long col = i < starts.size() ? row : rng.nextLong(cursor);
        final long endRow = Math.min(cursor, row + 8);
        final long endCol = Math.min(cursor, col + 8);
        final var raw = (MatrixQueries.LongMatrix) file.getMatrixQueries().getSubmatrix(
          ResolutionDescriptor.fromBpResolution(resolution, file), row, col, endRow, endCol, false).matrix();
        final var values = new JsonArray();
        for (final var matrixRow : raw.values()) {
          final var array = new JsonArray();
          for (final long value : matrixRow) array.add(value);
          values.add(array);
        }
        queries.add(new JsonObject().put("row", row).put("col", col).put("values", values));
      }
      Files.writeString(Path.of(args[2]), new JsonObject().put("input", args[0]).put("resolution", resolution)
        .put("contigs", contigs).put("scaffolds", scaffolds).put("queries", queries).encodePrettily());
    }
  }
}
