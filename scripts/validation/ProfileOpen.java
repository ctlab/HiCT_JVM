import java.nio.file.Path;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.Random;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.Initializers;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

/** Read-only opening benchmark with deterministic metadata, weight and raw-query fingerprints. */
public class ProfileOpen {
  public static void main(String[] args) throws Exception {
    long start = System.nanoTime();
    String[] lastStage = {"Starting"};
    long[] lastTime = {start};
    var stages = new JsonArray();
    try (var file = Initializers.withProgressReporter((stage, progress) -> {
      if (!stage.equals(lastStage[0])) {
        long now = System.nanoTime();
        stages.add(new JsonObject().put("stage", lastStage[0]).put("seconds", (now - lastTime[0]) / 1e9));
        System.out.printf("PROFILE %.3fs %s%n", (now - start) / 1e9, stage);
        lastStage[0] = stage;
        lastTime[0] = now;
      }
    }, () -> new ChunkedFile(new ChunkedFile.ChunkedFileOptions(Path.of(args[0]), 1, 2)))) {
      double seconds = (System.nanoTime() - start) / 1e9;
      stages.add(new JsonObject().put("stage", lastStage[0]).put("seconds", (System.nanoTime() - lastTime[0]) / 1e9));
      System.out.printf("PROFILE OPEN COMPLETE %.3fs%n", seconds);
      var digest = MessageDigest.getInstance("SHA-256");
      var buffer = ByteBuffer.allocate(8);
      var contigs = file.getAssemblyInfo().contigs();
      for (var c : contigs) {
        digest.update(c.descriptor().getContigName().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        digest.update(c.direction().name().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (long length : c.descriptor().getLengthBinsAtResolution()) update(digest, buffer, length);
        for (var atus : c.descriptor().getAtus()) for (var atu : atus) {
          update(digest, buffer, atu.getStartIndexInStripeIncl());
          update(digest, buffer, atu.getEndIndexInStripeExcl());
          update(digest, buffer, atu.getDirection().ordinal());
          for (double w : atu.getStripeDescriptor().bin_weights()) update(digest, buffer, Double.doubleToLongBits(w));
        }
      }
      for (var s : file.getAssemblyInfo().scaffolds()) {
        digest.update(s.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
      }
      var random = new Random(20261001);
      int queries = 0;
      for (int i = 1; i < file.getResolutions().length; i++) {
        long size = file.getMatrixSizeBins()[i];
        update(digest, buffer, file.getResolutions()[i]);
        for (int q = 0; q < 12 && size > 0; q++) {
          long row = q == 0 ? 0 : random.nextLong(size);
          long col = q % 2 == 0 ? row : random.nextLong(size);
          var matrix = (MatrixQueries.LongMatrix) file.getMatrixQueries().getSubmatrix(
            ResolutionDescriptor.fromResolutionOrder(i), row, col, Math.min(size, row + 8), Math.min(size, col + 8), false).matrix();
          for (long[] values : matrix.values()) for (long v : values) update(digest, buffer, v);
          queries++;
        }
      }
      var result = new JsonObject().put("input", args[0]).put("openSeconds", seconds)
        .put("resolutions", file.getResolutions().length - 1).put("contigs", contigs.size())
        .put("nonFiniteWeights", file.getCoolerWeightsNaNCount().get()).put("queries", queries)
        .put("sha256", HexFormat.of().formatHex(digest.digest())).put("stages", stages);
      Files.writeString(Path.of(args[1]), result.encodePrettily());
      System.out.println(result.encode());
    }
  }

  private static void update(MessageDigest digest, ByteBuffer buffer, long value) {
    buffer.clear();
    buffer.putLong(value);
    digest.update(buffer.array());
  }
}
