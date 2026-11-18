import htsjdk.samtools.*;
import java.nio.file.*;
import java.util.*;

/** Small indexed BAM with gaps, clipping, insertion and a terminal aligned base. */
class BamTrackBrowserFixture {
  public static void main(String[] args) throws Exception {
    Path directory = Path.of(args[0]).toRealPath();
    if (!directory.startsWith("/tmp")) throw new IllegalArgumentException("Use disposable /tmp inputs");
    Path bam = directory.resolve("reads.bam");
    if (Files.exists(bam)) throw new IllegalArgumentException("Use a fresh BAM fixture");
    var header = new SAMFileHeader(); header.setSortOrder(SAMFileHeader.SortOrder.coordinate);
    header.addSequence(new SAMSequenceRecord("tiny", 17));
    try (var writer = new SAMFileWriterFactory().setCreateIndex(true).makeBAMWriter(header, true, bam.toFile())) {
      for (var row : List.of(new String[]{"clipped-insertion", "1", "1S1=1I1X1H", "TAGT"},
        new String[]{"spliced", "2", "2M3N2M", "ACGT"},
        new String[]{"deletion", "10", "2M1D2M", "ACGT"}, new String[]{"terminal", "17", "1M", "C"})) {
        var read = new SAMRecord(header);
        read.setReadName(row[0]); read.setReferenceName("tiny");
        read.setAlignmentStart(Integer.parseInt(row[1])); read.setCigarString(row[2]);
        read.setReadString(row[3]); read.setBaseQualityString("I".repeat(row[3].length()));
        read.setMappingQuality(60); writer.addAlignment(read);
      }
    }
  }
}
