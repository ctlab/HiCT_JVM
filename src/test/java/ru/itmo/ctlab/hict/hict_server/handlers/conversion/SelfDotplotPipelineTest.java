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
    final var hictk = Path.of("toolchains-dist", "linux_x86_64", "bin", "hictk").toAbsolutePath().normalize();
    assumeTrue(Files.isExecutable(hictk), "bundled test hictk is not available");
    final var fasta = tempDir.resolve("tiny.fa");
    Files.writeString(
      fasta,
      """
        >chr1
        ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT
        ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT
        >chr2
        TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCA
        TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCA
        """
    );
    final var minimap2 = tempDir.resolve("minimap2");
    Files.writeString(
      minimap2,
      """
        #!/usr/bin/env bash
        cat <<'PAF'
        chr1\t128\t0\t100\t+\tchr1\t128\t0\t100\t100\t100\t60
        chr1\t128\t0\t100\t+\tchr2\t128\t0\t100\t100\t100\t60
        PAF
        """
    );
    minimap2.toFile().setExecutable(true, true);

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
        ""
      ),
      new ExternalToolchainManager.ResolvedToolchain(
        "linux_x86_64",
        "test",
        hictk,
        minimap2,
        null,
        null,
        null,
        null,
        List.of(),
        List.of(),
        List.of()
      ),
      ignored -> {
      },
      ignored -> {
      },
      () -> false
    );

    assertTrue(Files.isRegularFile(output));
    assertTrue(Files.size(output) > 0L);
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
