package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Following <a href="https://www.ncbi.nlm.nih.gov/assembly/agp/AGP_Specification/">AGP Specification v2.1</a>.
 */
@Slf4j
@RequiredArgsConstructor
public class AGPProcessor {
  private final @NotNull ChunkedFile chunkedFile;

  public Stream<String> getAGPStream(final long unscaffoldedSpacerLength) {
    final var records = this.getAGPRecords(unscaffoldedSpacerLength);
    return records.parallelStream().map(r -> String.format("%s%n", r));
  }

  public @NotNull List<@NotNull AGPFileRecord> getAGPRecords(final long unscaffoldedSpacerLength) {
    assert (unscaffoldedSpacerLength >= 0) : "Spacer length for unscaffolded contigs cannot be negative";
    final var contigTree = this.chunkedFile.getContigTree();
    final var scaffoldTree = this.chunkedFile.getScaffoldTree();

    final var result = new ArrayList<AGPFileRecord>();

    try {
      contigTree.getRootLock().readLock().lock();
      scaffoldTree.getRootLock().readLock().lock();

      final var contigs = contigTree.getOrderedContigList();
      final var scaffolds = scaffoldTree.getScaffoldList();

      final var scaffoldedContigs = groupContigsIntoScaffolds(contigs, scaffolds);

      var positionBp = 1L;

      for (final var sc : scaffoldedContigs) {
        final var scaffold = sc.scaffoldTuple().scaffoldDescriptor();
        assert (scaffold != null || (sc.contigs().size() == 1)) : "Unscaffolded contig must always represent unique unscaffolded segment";
        final var scaffoldName = (scaffold != null) ? scaffold.scaffoldName() : String.format(
          "unscaffolded_%s",
          sc.contigs().get(0).descriptor().getContigName()
        );

        final var spacerLength = ((scaffold != null) ? scaffold.spacerLength() : unscaffoldedSpacerLength);

        int partNumber = 1;

        for (final ContigTree.ContigTuple contigTuple : sc.contigs()) {
          result.add(new ContigAGPRecord(
            scaffoldName,
            positionBp,
            positionBp + contigTuple.descriptor().getLengthBp() - 1,
            partNumber,
            contigTuple.descriptor().getContigNameInSourceFASTA(),
            1 + contigTuple.descriptor().getOffsetInSourceFASTA(),
            contigTuple.descriptor().getOffsetInSourceFASTA() + contigTuple.descriptor().getLengthBp(),
            switch (contigTuple.direction()) {
              case FORWARD -> AGPContigOrientation.PLUS;
              case REVERSED -> AGPContigOrientation.MINUS;
            }
          ));
          positionBp += contigTuple.descriptor().getLengthBp();
          ++partNumber;
          if (((partNumber - 1) / 2 < sc.contigs().size() - 1) && (spacerLength > 0)) {
            result.add(
              new GapAGPRecord(
                scaffoldName,
                positionBp,
                positionBp + spacerLength - 1,
                partNumber,
                spacerLength,
                AGPGapType.SCAFFOLD,
                true,
                LinkageEvidence.PROXIMITY_LIGATION
              )
            );
            positionBp += spacerLength;
            ++partNumber;
          }
        }
      }

    } finally {
      contigTree.getRootLock().readLock().unlock();
      scaffoldTree.getRootLock().readLock().unlock();
    }

    return result;
  }

  protected @NotNull List<@NotNull ScaffoldedContigs> groupContigsIntoScaffolds(
    final @NotNull List<ContigTree.@NotNull ContigTuple> contigs,
    final List<ScaffoldTree.@NotNull ScaffoldTuple> scaffolds
  ) {
    var positionBp = 0L;
    var positionInScaffoldList = 0;

    final List<ScaffoldedContigs> scaffoldedContigs = new ArrayList<>();

    for (final var ctgTuple : contigs) {
      while (positionInScaffoldList < scaffolds.size() && scaffolds.get(positionInScaffoldList).scaffoldBordersBP().endBP() <= positionBp) {
        ++positionInScaffoldList;
      }

      if (
        (positionInScaffoldList < scaffolds.size())
          && (scaffolds.get(positionInScaffoldList).scaffoldDescriptor() != null)
          && (scaffolds.get(positionInScaffoldList).scaffoldBordersBP().startBP() <= positionBp)
          && (positionBp < scaffolds.get(positionInScaffoldList).scaffoldBordersBP().endBP())
      ) {
        final var currentScaffoldTuple = scaffolds.get(positionInScaffoldList);
        assert currentScaffoldTuple.scaffoldDescriptor() != null;
        if (scaffoldedContigs.size() > 0) {
          final var lastTuple = scaffoldedContigs.get(scaffoldedContigs.size() - 1);
          if (currentScaffoldTuple.scaffoldDescriptor().equals(lastTuple.scaffoldTuple().scaffoldDescriptor())) {
            lastTuple.contigs().add(ctgTuple);
            positionBp += ctgTuple.descriptor().getLengthBp();
          } else {
            scaffoldedContigs.add(new ScaffoldedContigs(currentScaffoldTuple, new ArrayList<>(List.of(ctgTuple))));
            positionBp += ctgTuple.descriptor().getLengthBp();
          }
        } else {
          scaffoldedContigs.add(new ScaffoldedContigs(currentScaffoldTuple, new ArrayList<>(List.of(ctgTuple))));
          positionBp += ctgTuple.descriptor().getLengthBp();
        }
      } else {
        scaffoldedContigs.add(new ScaffoldedContigs(
          new ScaffoldTree.ScaffoldTuple(
            null,
            new ScaffoldDescriptor.ScaffoldBordersBP(
              positionBp,
              positionBp + ctgTuple.descriptor().getLengthBp()
            )
          ),
          List.of(ctgTuple)
        ));
        positionBp += ctgTuple.descriptor().getLengthBp();
      }
    }

    return scaffoldedContigs;
  }

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

  public enum AGPContigOrientation {
    PLUS,
    MINUS,
    UNKNOWN,
    IRRELEVANT
  }

  protected record ScaffoldedContigs(
    @NotNull ScaffoldTree.ScaffoldTuple scaffoldTuple,
    @NotNull List<ContigTree.@NotNull ContigTuple> contigs
  ) {
  }

  @Getter
  @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
  public static sealed class AGPFileRecord permits ContigAGPRecord, GapAGPRecord {
    private final @NotNull String scaffoldName;
    private final long interScaffoldStartIncl;
    private final long interScaffoldEndIncl;
    private final int partNumber;

    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (AGPFileRecord) obj;
      return Objects.equals(this.scaffoldName, that.scaffoldName) &&
        this.interScaffoldStartIncl == that.interScaffoldStartIncl &&
        this.interScaffoldEndIncl == that.interScaffoldEndIncl &&
        this.partNumber == that.partNumber;
    }

    @Override
    public int hashCode() {
      return Objects.hash(scaffoldName, interScaffoldStartIncl, interScaffoldEndIncl, partNumber);
    }

    @Override
    public String toString() {
      return String.format(
        "%s\t%d\t%d\t%d",
        this.scaffoldName,
        this.interScaffoldStartIncl,
        this.interScaffoldEndIncl,
        this.partNumber
      );
    }
  }

  @Getter
  public static final class GapAGPRecord extends AGPFileRecord {
    private final long gapLength;
    private final @NotNull AGPGapType gapType;
    private final boolean linkage;
    private final @NotNull LinkageEvidence linkageEvidence;

    public GapAGPRecord(
      final @NotNull String scaffoldName,
      final long interScaffoldStartIncl,
      final long interScaffoldEndIncl,
      final int partNumber,
      final long gapLength,
      final @NotNull AGPGapType gapType,
      final boolean linkage,
      final @NotNull LinkageEvidence linkageEvidence
    ) {
      super(
        scaffoldName,
        interScaffoldStartIncl,
        interScaffoldEndIncl,
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

    @Override
    public String toString() {
      return String.format(
        "%s\tN\t%d\t%s\t%s\t%s",
        super.toString(),
        this.gapLength,
        this.gapType.toString().toLowerCase(),
        this.linkage ? "yes" : "no",
        linkageEvidence.toString().toLowerCase()
      );
    }
  }

  @Getter
  public static final class ContigAGPRecord extends AGPFileRecord {
    private final @NotNull String contigName;
    private final long intraContigStartBpIncl;
    private final long intraContigEndBpIncl;
    private final @NotNull AGPContigOrientation contigOrientation;

    public ContigAGPRecord(
      final @NotNull String scaffoldName,
      final long interScaffoldStartIncl,
      final long interScaffoldEndIncl,
      final int partNumber,
      final @NotNull String contigName,
      final long intraContigStartBpIncl,
      final long intraContigEndBpIncl,
      final @NotNull AGPContigOrientation contigOrientation
    ) {
      super(
        scaffoldName,
        interScaffoldStartIncl,
        interScaffoldEndIncl,
        partNumber
      );
      this.contigName = contigName;
      this.intraContigStartBpIncl = intraContigStartBpIncl;
      this.intraContigEndBpIncl = intraContigEndBpIncl;
      this.contigOrientation = contigOrientation;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (ContigAGPRecord) obj;
      return Objects.equals(this.contigName, that.contigName) &&
        this.intraContigStartBpIncl == that.intraContigStartBpIncl &&
        this.intraContigEndBpIncl == that.intraContigEndBpIncl &&
        Objects.equals(this.contigOrientation, that.contigOrientation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(contigName, intraContigStartBpIncl, intraContigEndBpIncl, contigOrientation);
    }

    @Override
    public String toString() {
      return String.format(
        "%s\tW\t%s\t%d\t%d\t%s",
        super.toString(),
        this.contigName,
        this.intraContigStartBpIncl,
        this.intraContigEndBpIncl,
        switch (this.contigOrientation) {
          case PLUS -> "+";
          case MINUS -> "-";
          case UNKNOWN -> "?";
          case IRRELEVANT -> "na";
        }
      );
    }
  }

}
