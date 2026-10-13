package ru.itmo.ctlab.hict.hict_library.assembly;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import java.util.*;

/** Generated cells are stored in original physical bins, never in edited assembly order. */
public final class SequenceOverlayCompatibility {
  private SequenceOverlayCompatibility() { }
  public static boolean isGenerated(ChunkedFile map) {
    try (var reader = HDF5Factory.openForReading(map.getHdfFilePath().toFile())) {
      if (!reader.object().isGroup("/sequence_analysis")) return false;
      if (!reader.object().hasAttribute("/sequence_analysis", "complete")
          || !reader.bool().getAttr("/sequence_analysis", "complete"))
        throw new IllegalArgumentException("Sequence overlay is incomplete; the currently attached source was retained");
      return true;
    }
  }
  public static void requireCompatible(ChunkedFile primary, ChunkedFile overlay) {
    var left = primary.getOriginalDescriptors(); var right = overlay.getOriginalDescriptors();
    if (!left.keySet().equals(right.keySet())) throw mismatch();
    for (var entry : right.entrySet()) {
      var a = left.get(entry.getKey()); var b = entry.getValue();
      if (!a.getContigNameInSourceFASTA().equals(b.getContigNameInSourceFASTA())
          || a.getOffsetInSourceFASTA() != b.getOffsetInSourceFASTA() || a.getLengthBp() != b.getLengthBp()) throw mismatch();
      for (long resolution : overlay.getResolutions()) {
        if (resolution == 0) continue;
        Integer ai = primary.getResolutionToIndex().get(resolution), bi = overlay.getResolutionToIndex().get(resolution);
        if (ai == null || a.getLengthBinsAtResolution()[ai] != b.getLengthBinsAtResolution()[bi]) throw mismatch();
        var aa = a.getAtus().get(ai); var bb = b.getAtus().get(bi);
        if (aa.size() != bb.size()) throw mismatch();
        for (int i = 0; i < aa.size(); i++) {
          var x = aa.get(i); var y = bb.get(i);
          if (x.getStripeDescriptor().stripeId() != y.getStripeDescriptor().stripeId()
              || x.getStartIndexInStripeIncl() != y.getStartIndexInStripeIncl()
              || x.getEndIndexInStripeExcl() != y.getEndIndexInStripeExcl() || x.getDirection() != y.getDirection()) throw mismatch();
        }
      }
    }
  }
  private static IllegalArgumentException mismatch() {
    return new IllegalArgumentException("This sequence overlay belongs to different source contigs or contact bins. Open its original map or regenerate it. The current source was retained.");
  }
}
