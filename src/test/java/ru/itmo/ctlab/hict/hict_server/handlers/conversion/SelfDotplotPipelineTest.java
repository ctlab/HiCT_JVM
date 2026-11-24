package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelfDotplotPipelineTest {
  @TempDir
  Path tempDir;

  @Test
  void pafSamplingWritesBg2PixelsWithoutPythonCooler() throws Exception {
    final var paf = tempDir.resolve("self.paf");
    Files.writeString(
      paf,
      """
        chr1\t1000\t0\t500\t+\tchr1\t1000\t100\t600\t400\t500\t60
        chr1\t1000\t100\t600\t-\tchr2\t800\t50\t550\t400\t500\t60
        tiny\t10\t0\t5\t+\tchr1\t1000\t0\t5\t5\t5\t60
        """
    );
    final var bg2 = tempDir.resolve("self.bg2");
    final var layout = new SelfDotplotPipeline.GeneratedLayout(
      List.of(
        new SelfDotplotPipeline.Chromosome("chr1", 1000L, 0L, 10L),
        new SelfDotplotPipeline.Chromosome("chr2", 800L, 10L, 8L)
      ),
      Map.of(
        "chr1", new SelfDotplotPipeline.Chromosome("chr1", 1000L, 0L, 10L),
        "chr2", new SelfDotplotPipeline.Chromosome("chr2", 800L, 10L, 8L)
      )
    );
    final var rows = SelfDotplotPipeline.writeBg2FromPaf(
      new SelfDotplotPipeline.Options(
        tempDir.resolve("dummy.fa"),
        tempDir,
        "self",
        100,
        "",
        null,
        null,
        17,
        5,
        40,
        false,
        0,
        1,
        1,
        true,
        true,
        100,
        50,
        "",
        ""
      ),
      layout,
      paf,
      bg2,
      ignored -> {
      },
      () -> false
    );

    assertEquals(11L, rows);
    final var text = Files.readString(bg2);
    assertTrue(text.contains("chr1\t0\t100\tchr1\t100\t200\t1"));
    assertTrue(text.contains("chr1\t500\t600\tchr2\t0\t100\t1"));
  }

  @Test
  void pipelineCanUseMinimap2OutputAndHictkWithoutPythonCoolerWhenToolchainExists() throws Exception {
    final var manager = new ExternalToolchainManager();
    final var status = manager.inspect();
    assumeTrue(status.hictkAvailable() && status.minimap2Command() != null,
      "Set HICT_TOOLCHAIN_DIR to a sealed hictk and minimap2 bundle");
    final var toolchain = manager.requireHictkToolchain();
    final var random = new java.util.Random(11);
    final var sequence = new StringBuilder(3000);
    for (int i = 0; i < 3000; i++) sequence.append("ACGT".charAt(random.nextInt(4)));
    final var fasta = Files.writeString(tempDir.resolve("tiny.fa"), ">chr1\n" + sequence + "\n>chr2\n" + sequence + "\n");

    final var output = new SelfDotplotPipeline().generate(
      new SelfDotplotPipeline.Options(
        fasta,
        tempDir,
        "tiny.self",
        25,
        "50,100",
        null,
        null,
        17,
        5,
        40,
        false,
        0,
        1,
        2,
        true,
        true,
        25,
        10,
        "",
        "minimap2"
      ),
      toolchain,
      ignored -> {
      },
      ignored -> {
      },
      () -> false
    );

    assertTrue(Files.isRegularFile(output));
    assertTrue(Files.size(output) > 0L);
    try (var reader = ch.systemsx.cisd.hdf5.HDF5Factory.openForReading(output.toFile())) {
      assertTrue(reader.object().getGroupMembers("/resolutions").containsAll(List.of("25", "50", "100")));
    }
  }

  @Test
  void agpTransformReordersReverseComplementsAndAddsGapsBeforeDotplot() throws Exception {
    final var fasta = tempDir.resolve("raw.fa");
    Files.writeString(
      fasta,
      """
        >ctgA
        ACGTAC
        >ctgB
        TTGGCC
        """
    );
    final var agp = tempDir.resolve("layout.agp");
    Files.writeString(
      agp,
      """
        scf1\t1\t3\t1\tW\tctgA\t1\t3\t+
        scf1\t4\t5\t2\tN\t2\tscaffold\tyes\tpaired-ends
        scf1\t6\t9\t3\tW\tctgB\t2\t5\t-
        """
    );
    final var output = tempDir.resolve("scaffolded.fa");

    SelfDotplotPipeline.applyAgpToFasta(fasta, agp, output, ignored -> {
    }, () -> false);

    assertEquals(
      """
        >scf1
        ACGNNGCCA
        """,
      Files.readString(output)
    );
  }
}
