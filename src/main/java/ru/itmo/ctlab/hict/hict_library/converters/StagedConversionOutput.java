package ru.itmo.ctlab.hict.hict_library.converters;

import java.io.IOException;
import java.nio.file.*;
import java.util.Locale;

/** Owns one output file until successful conversion can replace the requested destination. */
public final class StagedConversionOutput implements AutoCloseable {
  private final Path destination, temporary;
  private final ConversionOptions options;

  public StagedConversionOutput(ConversionOptions requested) throws IOException {
    destination=requested.outputPath().toAbsolutePath().normalize();
    final var input=requested.inputPath().toAbsolutePath().normalize();
    if(input.equals(destination) || Files.exists(destination)&&Files.isSameFile(input,destination))
      throw new IllegalArgumentException("Conversion output must differ from its input");
    if(Files.isSymbolicLink(destination))throw new IllegalArgumentException("Conversion output must not be a symbolic link");
    Files.createDirectories(destination.getParent());
    temporary=Files.createTempFile(destination.getParent(),".hict-matrix-export-",
      destination.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".cool")?".cool":".mcool");
    options=new ConversionOptions(requested.inputPath(),temporary,requested.resolutions(),requested.chunkSize(),
      requested.compressionLevel(),requested.compressionAlgorithm(),requested.agpPath(),requested.applyAgpBeforeExport(),
      requested.parallelism(),requested.exportAllResolutions(),requested.buildResolutionPyramid(),requested.balanceInputCoolers(),
      requested.balanceExportedCoolers(),requested.exportMode(),requested.approximateRebin());
  }
  public ConversionOptions options(){return options;}
  public void publish() throws IOException {
    try{Files.move(temporary,destination,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);}
    catch(AtomicMoveNotSupportedException ignored){Files.move(temporary,destination,StandardCopyOption.REPLACE_EXISTING);}
  }
  @Override public void close() throws IOException {Files.deleteIfExists(temporary);}
}
