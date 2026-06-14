package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.converters.ConversionOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HicAssemblyConversionIntegrationTest {
  private static final Path HIC = Path.of("/mnt/Models/HiCT/data/DNAZoo/AedesAegypti/AaegL5.0.hic");
  private static final Path ASSEMBLY = Path.of("/mnt/Models/HiCT/data/DNAZoo/AedesAegypti/AaegL5.0.assembly");
  private static final Path FASTA = Path.of("/mnt/Models/HiCT/data/DNAZoo/AedesAegypti/AaegL5.0.fasta.gz");

  @TempDir
  Path tempDir;

  @Test
  void convertsAndOpensHicWithJuiceboxAssemblyAndFasta() throws Exception {
    assumeTrue(
      Boolean.getBoolean("hict.runRealAssemblyConversion")
        || "true".equalsIgnoreCase(System.getenv("HICT_RUN_REAL_ASSEMBLY_CONVERSION")),
      "Set -Dhict.runRealAssemblyConversion=true or HICT_RUN_REAL_ASSEMBLY_CONVERSION=true to run the real sample conversion test"
    );
    assumeTrue(Files.isRegularFile(HIC), "sample .hic file is unavailable");
    assumeTrue(Files.isRegularFile(ASSEMBLY), "sample .assembly file is unavailable");
    assumeTrue(Files.isRegularFile(FASTA), "sample FASTA file is unavailable");

    final var hictk = findLocalHictkBinary();
    assumeTrue(hictk != null, "local hictk binary is unavailable");

    final var output = tempDir.resolve("AGWG.draft.hict.hdf5");
    final var pipeline = new HictkConversionPipeline(new ExternalToolchainManager());
    pipeline.convert(
      ConversionDirection.HIC_TO_HICT,
      new ConversionOptions(
        HIC,
        output,
        List.of(2_500_000L),
        8_192,
        6,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        ASSEMBLY.toString(),
        false,
        Runtime.getRuntime().availableProcessors()
      ),
      new ExternalToolchainManager.ResolvedToolchain(
        "linux_x86_64",
        "test",
        hictk,
        null,
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

    final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(output, 1, 4));
    assertEquals(
      chunkedFile.getAssemblyInfo().contigs().size(),
      chunkedFile.getContigTree().getOrderedContigList().size()
    );
    assertTrue(chunkedFile.getContigTree().getOrderedContigList().size() > 3);
  }

  private static Path findLocalHictkBinary() throws IOException {
    final var tmpRoot = Path.of(System.getProperty("java.io.tmpdir"), "hict-tux");
    assumeTrue(Files.isDirectory(tmpRoot), "local HiCT toolchain staging root is unavailable");
    try (final var candidates = Files.walk(tmpRoot, 10)) {
      return candidates
        .filter(path ->
          Files.isRegularFile(path)
            && path.getFileName().toString().equals("hictk")
            && path.toString().contains("/toolchains/linux_x86_64/bin/")
            && Files.isExecutable(path)
        )
        .findFirst()
        .orElse(null);
    }
  }
}
