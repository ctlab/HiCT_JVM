package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CancellationException;
import java.util.zip.GZIPOutputStream;
import static org.junit.jupiter.api.Assertions.*;

class ExampleFastaPreparationTest {
  @TempDir Path folder;

  private JsonObject example(String reference, String expected) throws Exception {
    try (var output = new GZIPOutputStream(Files.newOutputStream(folder.resolve("original.fa.gz")))) {
      output.write(reference.getBytes(StandardCharsets.US_ASCII));
    }
    var preparation = new JsonObject().put("filename", "matched.fa")
      .put("sha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(expected.getBytes(StandardCharsets.US_ASCII))))
      .put("records", new JsonArray()
        .add(new JsonObject().put("source", "b.1").put("target", "chrB").put("length", 4))
        .add(new JsonObject().put("source", "a.1").put("target", "chrA").put("length", 5)));
    return new JsonObject().put("files", new JsonArray().add(new JsonObject().put("id", "f").put("filename", "original.fa.gz")))
      .put("openingRecipe", new JsonObject().put("fasta", "f").put("fastaPreparation", preparation));
  }

  @Test void selectsReordersRenamesAndPreservesOriginalCaseAndBases() throws Exception {
    String reference = ">a.1 description\nAaCgt\n>extra\nCCCC\n>b.1\nTG\nca\n";
    String expected = ">chrB\nTGca\n>chrA\nAaCgt\n";
    var example = example(reference, expected);
    var original = Files.readAllBytes(folder.resolve("original.fa.gz"));
    assertEquals("matched.fa", ExampleFastaPreparation.prepare(folder, example, new AtomicBoolean()));
    assertEquals(expected, Files.readString(folder.resolve("matched.fa")));
    assertArrayEquals(original, Files.readAllBytes(folder.resolve("original.fa.gz")));
    Files.writeString(folder.resolve("matched.fa"), "damaged");
    ExampleFastaPreparation.prepare(folder, example, new AtomicBoolean());
    assertEquals(expected, Files.readString(folder.resolve("matched.fa")));
  }

  @Test void failureAndCancellationPreserveExistingOutputAndRemoveTemporaryFiles() throws Exception {
    var example = example(">a.1\nAaCgt\n>b.1\nTGca\n", "incorrect digest");
    Files.writeString(folder.resolve("matched.fa"), "existing");
    assertThrows(java.io.IOException.class, () -> ExampleFastaPreparation.prepare(folder, example, new AtomicBoolean()));
    assertEquals("existing", Files.readString(folder.resolve("matched.fa")));
    assertThrows(CancellationException.class, () -> ExampleFastaPreparation.prepare(folder, example, new AtomicBoolean(true)));
    try (var files = Files.list(folder)) { assertEquals(2, files.count()); }
  }

  @Test void rejectsMissingDuplicateAndLengthMismatchedRecords() throws Exception {
    for (String reference : new String[]{">a.1\nAaCgt\n", ">a.1\nAaCgt\n>b.1\nTGca\n>b.1\nTGca\n", ">a.1\nAaCgt\n>b.1\nTGCAA\n"}) {
      var example = example(reference, ">chrB\nTGca\n>chrA\nAaCgt\n");
      assertThrows(java.io.IOException.class, () -> ExampleFastaPreparation.prepare(folder, example, new AtomicBoolean()));
      assertFalse(Files.exists(folder.resolve("matched.fa")));
    }
  }
}
