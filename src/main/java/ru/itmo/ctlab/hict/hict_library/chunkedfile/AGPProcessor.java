package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public class AGPProcessor {
  private final @NotNull ChunkedFile chunkedFile;


  public enum AGPGapType {
    SCAFFOLD,
    //a gap between two sequence contigs in a scaffold (superscaffold or ultra-scaffold).
    CONTIG,
    //    an unspanned gap between two sequence contigs.
    CENTROMERE,
    //    a gap inserted for the centromere.
    SHORT_ARM,
    //    a gap inserted at the start of an acrocentric chromosome.
    HETEROCHROMATIN,
    //    a gap inserted for an especially large region of heterochromatic sequence (may also include the centromere).
    TELOMERE,
    //    a gap inserted for the telomere.
    REPEAT,
    //    an unresolvable repeat.
    CONTAMINATION,
//    a gap inserted in place of foreign sequence to maintain the coordinates.
  }

  public enum LinkageEvidence {
    NA,
    //    used when no linkage is being asserted (column 8b is ‘no’)
    PAIRED_ENDS,
    //    paired sequences from the two ends of a DNA fragment, mate-pairs and molecular-barcoding.
    ALIGN_GENUS,
    //    alignment to a reference genome within the same genus.
    ALIGN_XGENUS,
    //    alignment to a reference genome within another genus.
    ALIGN_TRNSCPT,
    //    alignment to a transcript from the same species.
    WITHIN_CLONE,
    //    sequence on both sides of the gap is derived from the same clone, but the gap is not spanned by paired-ends. The adjacent sequence contigs have unknown order and orientation.
    CLONE_CONTIG,
    //    linkage is provided by a clone contig in the tiling path (TPF). For example, a gap where there is a known clone, but there is not yet sequence for that clone.
    MAP,
    //    linkage asserted using a non-sequence based map such as RH, linkage, fingerprint or optical.
    PCR,
    //    PCR using primers on both sides of the gap.
    PROXIMITY_LIGATION,
    //    ligation of segments of DNA that were brought into proximity in chromatin (Hi-C and related technologies).
    STROBE,
    //    strobe sequencing.
    UNSPECIFIED
//    used only for gaps of type contamination and when converting old AGPs that lack a field for linkage evidence into the new format.
  }

  @ToString
  @Getter
  @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
  public static sealed class AGPFileRecord permits ContigAGPRecord, GapAGPRecord {
    private final @NotNull String scaffoldName;
    private final long intraScaffoldStartIncl;
    private final long intraScaffoldEndIncl;
    private final int partNumber;

    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (AGPFileRecord) obj;
      return Objects.equals(this.scaffoldName, that.scaffoldName) &&
        this.intraScaffoldStartIncl == that.intraScaffoldStartIncl &&
        this.intraScaffoldEndIncl == that.intraScaffoldEndIncl &&
        this.partNumber == that.partNumber;
    }

    @Override
    public int hashCode() {
      return Objects.hash(scaffoldName, intraScaffoldStartIncl, intraScaffoldEndIncl, partNumber);
    }
  }

  @Getter
  @ToString
  public static final class GapAGPRecord extends AGPFileRecord {
    private final long gapLength;
    private final @NotNull AGPGapType gapType;
    private final boolean linkage;
    private final @NotNull LinkageEvidence linkageEvidence;

    public GapAGPRecord(
      @NotNull String scaffoldName,
      long intraScaffoldStartIncl,
      long intraScaffoldEndIncl,
      int partNumber,
      long gapLength,
      @NotNull AGPGapType gapType,
      boolean linkage,
      @NotNull LinkageEvidence linkageEvidence
    ) {
      super(
        scaffoldName,
        intraScaffoldStartIncl,
        intraScaffoldEndIncl,
        partNumber
      );
      this.gapLength = gapLength;
      this.gapType = gapType;
      this.linkage = linkage;
      this.linkageEvidence = linkageEvidence;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (GapAGPRecord) obj;
      return this.gapLength == that.gapLength &&
        Objects.equals(this.gapType, that.gapType) &&
        this.linkage == that.linkage &&
        Objects.equals(this.linkageEvidence, that.linkageEvidence);
    }

    @Override
    public int hashCode() {
      return Objects.hash(gapLength, gapType, linkage, linkageEvidence);
    }
  }

  public record ContigAGPRecord(@NotNull String contigName, long intraContigStartBpIncl, long intraContigEndBpIncl){}

}
