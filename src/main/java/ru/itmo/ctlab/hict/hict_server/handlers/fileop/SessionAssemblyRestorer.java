package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;

/** Prepare an unpublished candidate. Failure restores its checkpoint and never publishes it.
 * File identity and multi-source publication are the responsibility of the session transaction.
 */
public final class SessionAssemblyRestorer {
  private SessionAssemblyRestorer() { }

  public static void restore(ChunkedFile candidate, JsonObject source) throws Exception {
    final var before = candidate.snapshotAssembly();
    try {
      final var agp = source.getString("agp");
      final var hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
        .digest(agp.getBytes(StandardCharsets.UTF_8)));
      require(hash.equals(source.getString("agpSha256")), "agpSha256", "does not match AGP content");
      final var expected = source.getJsonObject("assemblyInfo");
      final var contigs = expected.getJsonArray("contigDescriptors");
      final var scaffolds = expected.getJsonArray("scaffoldDescriptors");
      final var splits = source.getJsonArray("splitEdits");
      if (splits == null || splits.isEmpty()) {
        candidate.importAGP(new StringReader(agp));
      } else {
        for (int index = 0; index < splits.size(); index++) {
          final var edit = splits.getJsonObject(index).mapTo(ChunkedFile.SplitEdit.class);
          final var parent = candidate.getAgpProcessor().resolveSessionComponent(
            edit.parentId(), edit.sourceName(), edit.sourceOffsetBp(), edit.parentLengthBp());
          candidate.scaffoldingOperations().replaySplit(edit, parent);
        }
        final var componentIds = new ArrayList<Integer>(contigs.size());
        for (int index = 0; index < contigs.size(); index++) {
          componentIds.add(contigs.getJsonObject(index).getInteger("contigId"));
        }
        candidate.getAgpProcessor().importSessionRecords(new StringReader(agp), componentIds);
      }
      final var imported = candidate.getAssemblyInfo().contigs();
      require(imported.size() == contigs.size(), "contigDescriptors", "AGP changed the number of components");
      final var replacements = new HashMap<Integer, ContigDescriptor>();
      long length = 0;
      for (int index = 0; index < imported.size(); index++) {
        final var saved = contigs.getJsonObject(index);
        final var tuple = imported.get(index);
        final var actual = tuple.descriptor();
        final var path = "contigDescriptors[" + index + "]";
        // Reusing an ID for a different component would invalidate track/graph references.
        require(saved.getInteger("contigId") == actual.getContigId(), path + ".contigId", "AGP cannot preserve component identity");
        require(saved.getInteger("contigDirection") == tuple.direction().ordinal(), path + ".contigDirection", "AGP changed strand");
        require(saved.getLong("contigLengthBp") == actual.getLengthBp(), path + ".contigLengthBp", "AGP changed component length");
        require(saved.getInteger("contigOffsetInSource") == actual.getOffsetInSourceFASTA(), path + ".contigOffsetInSource", "AGP changed source interval");
        require(saved.getString("contigSourceName").equals(actual.getContigNameInSourceFASTA()), path + ".contigSourceName", "AGP changed source name");
        length = Math.addExact(length, actual.getLengthBp());
        final var bins = new ArrayList<Long>();
        final var presence = new ArrayList<ContigHideType>();
        for (int order = 1; order < candidate.getResolutions().length; order++) {
          final var key = Long.toString(candidate.getResolutions()[order]);
          final long savedBins = saved.getJsonObject("contigLengthBins").getLong(key);
          require(savedBins == actual.getLengthBinsAtResolution()[order], path + ".contigLengthBins." + key, "AGP changed contact-bin ownership");
          bins.add(savedBins);
          final int visibility = saved.getJsonObject("contigPresenceAtResolution").getInteger(key);
          require(visibility >= 0 && visibility < ContigHideType.values().length, path + ".contigPresenceAtResolution." + key, "invalid visibility");
          presence.add(ContigHideType.values()[visibility]);
        }
        final var originalName = saved.getString("contigOriginalName");
        ChunkedFile.validateDisplayName(originalName);
        replacements.put(actual.getContigId(), new ContigDescriptor(actual.getContigId(), originalName,
          actual.getLengthBp(), bins, presence, actual.getAtus().subList(1, actual.getAtus().size()),
          actual.getContigNameInSourceFASTA(), actual.getOffsetInSourceFASTA()));
      }
      candidate.getContigTree().mapDescriptors(d -> replacements.get(d.getContigId()));
      // AGP does not carry empty scaffold spacer defaults or stable scaffold IDs.
      // Restore those from the captured descriptors only after validating component layout.
      final var tree = new ScaffoldTree(length);
      final var ids = new HashSet<Long>();
      long previousEnd = 0;
      for (int index = 0; index < scaffolds.size(); index++) {
        final var saved = scaffolds.getJsonObject(index);
        final var path = "scaffoldDescriptors[" + index + "]";
        final long id = saved.getLong("scaffoldId");
        require(id >= 0 && ids.add(id), path + ".scaffoldId", "invalid or duplicate ID");
        final var borders = saved.getJsonObject("scaffoldBordersBP");
        final long start = borders.getLong("startBP"), end = borders.getLong("endBP");
        require(start >= previousEnd && start < end && end <= length, path + ".scaffoldBordersBP", "overlapping or out-of-bounds scaffold");
        previousEnd = end;
        final long spacer = saved.getLong("spacerLength");
        require(spacer >= 0, path + ".spacerLength", "negative spacer");
        final var name = saved.getString("scaffoldOriginalName");
        ChunkedFile.validateDisplayName(name);
        tree.rescaffold(start, end, ignored -> new ScaffoldDescriptor(id, name, spacer));
      }
      candidate.getScaffoldTree().replaceWith(tree);
      candidate.getContigNameOverrides().clear();
      for (var entry : source.getJsonObject("contigNameOverrides").getMap().entrySet()) {
        final var name = (String) entry.getValue();
        ChunkedFile.validateDisplayName(name);
        candidate.getContigNameOverrides().put(Integer.parseInt(entry.getKey()), name);
      }
      candidate.getScaffoldNameOverrides().clear();
      for (var entry : source.getJsonObject("scaffoldNameOverrides").getMap().entrySet()) {
        final var name = (String) entry.getValue();
        ChunkedFile.validateDisplayName(name);
        candidate.getScaffoldNameOverrides().put(Long.parseLong(entry.getKey()), name);
      }
      require(expected.equals(JsonObject.mapFrom(AssemblyInfoDTO.generateFromChunkedFile(candidate))),
        "assemblyInfo", "restored descriptors differ from the saved assembly");
      final var restored = SessionSnapshotDTO.source(candidate, "", "");
      require(hash.equals(restored.getString("agpSha256")), "agpSha256", "assembly cannot round-trip this AGP without loss");
    } catch (Exception | Error failure) {
      candidate.restoreAssembly(before);
      throw failure;
    }
  }

  /** Prove that AGP reconstruction also preserves native contact-bin lineage. */
  public static void verifyContactBinMapping(ChunkedFile live, ChunkedFile restored) {
    final var expected = live.getAssemblyInfo().contigs();
    final var actual = restored.getAssemblyInfo().contigs();
    require(expected.size() == actual.size(), "contigDescriptors", "component count changed");
    for (int i = 0; i < expected.size(); i++) {
      final var left = expected.get(i).descriptor().getAtus();
      final var right = actual.get(i).descriptor().getAtus();
      require(left.size() == right.size(), "contigDescriptors[" + i + "]", "resolution count changed");
      for (int order = 1; order < left.size(); order++) {
        final var a = ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor.reduce(left.get(order));
        final var b = ru.itmo.ctlab.hict.hict_library.domain.ATUDescriptor.reduce(right.get(order));
        final var field = "contigDescriptors[" + i + "].contactBins[" + order + "]";
        require(a.size() == b.size(), field, "AGP changed native contact-bin mapping");
        for (int j = 0; j < a.size(); j++) {
          var x = a.get(j); var y = b.get(j);
          require(x.getStripeDescriptor().stripeId() == y.getStripeDescriptor().stripeId()
            && x.getStartIndexInStripeIncl() == y.getStartIndexInStripeIncl()
            && x.getEndIndexInStripeExcl() == y.getEndIndexInStripeExcl()
            && x.getDirection() == y.getDirection(), field, "AGP changed native contact-bin mapping");
        }
      }
    }
  }

  private static void require(boolean condition, String field, String message) {
    if (!condition) throw new IllegalArgumentException("Invalid session assembly " + field + ": " + message);
  }
}
