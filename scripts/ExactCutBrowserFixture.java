import java.nio.file.Files;
import java.nio.file.Path;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;

/** Prepare exact library state for UI restoration before the scissors endpoint is enabled. */
class ExactCutBrowserFixture {
  public static void main(String[] args) throws Exception {
    final var data = Path.of(args[0]).toRealPath();
    final var output = Path.of(args[1]).toAbsolutePath();
    if (!data.startsWith("/tmp") || !output.startsWith("/tmp"))
      throw new IllegalArgumentException("Use disposable /tmp fixture paths");
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(data.resolve("tiny-primary.hict.hdf5"), 1, 2))) {
      if (file.getAssemblyInfo().contigs().size() != 1 || file.getAssemblyInfo().contigs().getFirst().descriptor().getLengthBp() != 17)
        throw new IllegalArgumentException("Use the 17 bp reinforcement fixture");
      final int original = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.scaffoldingOperations().splitContigAtBp(original, 5);
      final int left = file.getAssemblyInfo().contigs().getFirst().descriptor().getContigId();
      file.scaffoldingOperations().splitContigAtBp(left, 3);
      file.scaffoldingOperations().reverseSelectionRangeBp(0, 17);
      Files.writeString(output, SessionSnapshotDTO.source(file, "tiny-primary.hict.hdf5", "tiny.fa").encodePrettily(),
        java.nio.file.StandardOpenOption.CREATE_NEW);
    }
  }
}
