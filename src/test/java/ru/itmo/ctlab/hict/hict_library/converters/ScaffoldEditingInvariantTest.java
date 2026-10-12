package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ScaffoldEditingInvariantTest {
  @TempDir Path dir;
  private ChunkedFile fixture() {
    HDF5LibraryInitializer.initializeHDF5Library();
    var input=dir.resolve("source.mcool"); var output=dir.resolve("map.hict.hdf5");
    try(var w=HDF5Factory.open(input.toFile())) {
      w.object().createGroup("/resolutions");
      for(long res: new long[]{1000,4000,10000}) {
        String p="/resolutions/"+res;
        for(String g: new String[]{"", "/chroms", "/bins", "/pixels", "/indexes"}) w.object().createGroup(p+g);
        w.string().writeArray(p+"/chroms/name",new String[]{"A","B","C"});
        w.int64().writeArray(p+"/chroms/length",new long[]{6000,6000,6000});
        int n=(int)((6000+res-1)/res),size=n*3;
        long[] chr=new long[size],start=new long[size],end=new long[size],offset=new long[size+1];
        double[] weights=new double[size];Arrays.fill(weights,1);
        var rows=new ArrayList<Long>();var cols=new ArrayList<Long>();var counts=new ArrayList<Long>();
        for(int i=0;i<size;i++) {
          chr[i]=i/n;start[i]=(i%n)*res;end[i]=Math.min(6000,start[i]+res);offset[i]=rows.size();
          for(int j=i;j<size;j++){rows.add((long)i);cols.add((long)j);counts.add(1L+i*100+j);}
        }
        offset[size]=rows.size();
        w.int64().writeArray(p+"/bins/chrom",chr);w.int64().writeArray(p+"/bins/start",start);w.int64().writeArray(p+"/bins/end",end);
        w.float64().writeArray(p+"/bins/weight",weights);
        w.int64().writeArray(p+"/indexes/chrom_offset",new long[]{0,n,2*n,3*n});
        w.int64().writeArray(p+"/indexes/bin1_offset",offset);
        w.int64().writeArray(p+"/pixels/bin1_id",rows.stream().mapToLong(Long::longValue).toArray());
        w.int64().writeArray(p+"/pixels/bin2_id",cols.stream().mapToLong(Long::longValue).toArray());
        w.int64().writeArray(p+"/pixels/count",counts.stream().mapToLong(Long::longValue).toArray());
      }
    }
    new McoolToHictConverter().convert(new ConversionOptions(input,output,List.of(1000L,4000L,10000L),64,0,
      ConversionOptions.CompressionAlgorithm.DEFLATE,ConversionOptions.NO_AGP,false,2,false,ConversionOptions.ExportMode.INTERNAL), ignored->{});
    return new ChunkedFile(new ChunkedFile.ChunkedFileOptions(output,1,2));
  }
  @Test void singletonInitializationWholeScaffoldEditsUngroupAndExplicitUnscaffold() {
    try(var f=fixture()) {
      var bp=ResolutionDescriptor.fromResolutionOrder(0);var ops=f.scaffoldingOperations();
      assertEquals(List.of("scf_A","scf_B","scf_C"),f.getScaffoldTree().getScaffoldList().stream().map(s->s.scaffoldDescriptor().scaffoldName()).toList());
      var initial=f.snapshotAssembly();
      f.setContigNameOverride(0,"renamed");
      assertEquals("scf_A",f.getScaffoldTree().getScaffoldList().getFirst().scaffoldDescriptor().scaffoldName());
      ops.scaffoldRegion(0,12000,bp,QueryLengthUnit.BASE_PAIRS,null);
      ops.reverseSelectionRangeBp(6500,7000);
      assertEquals(List.of("B","A","C"),f.getContigTree().getOrderedContigList().stream().map(c->c.descriptor().getContigName()).toList());
      ops.moveSelectionRangeBp(0,1,18000);
      assertEquals(List.of("C","B","A"),f.getContigTree().getOrderedContigList().stream().map(c->c.descriptor().getContigName()).toList());
      ops.unscaffoldRegion(7000,8000,bp,QueryLengthUnit.BASE_PAIRS);
      assertEquals(3,f.getScaffoldTree().getScaffoldList().size());
      ops.unscaffoldAll();assertTrue(f.getScaffoldTree().getScaffoldList().isEmpty());
      f.restoreAssembly(initial);assertEquals(initial,f.snapshotAssembly());
      ops.moveRegionToDebris(10,20,bp,QueryLengthUnit.BASE_PAIRS);
      assertEquals("A",f.getContigTree().getOrderedContigList().getLast().descriptor().getContigName());
      for(int i=1;i<f.getResolutions().length;i++) assertEquals(ContigHideType.HIDDEN,f.getContigTree().getOrderedContigList().getLast().descriptor().getPresenceAtResolution().get(i));
      f.restoreAssembly(initial);assertEquals(initial,f.snapshotAssembly());
    }
  }
  @Test void invalidExportNamesCannotCorruptTheAssembly() {
    try (var f = fixture()) {
      var before = f.snapshotAssembly();
      var id = f.getScaffoldTree().getScaffoldList().getFirst().scaffoldDescriptor().scaffoldId();
      for (String invalid : new String[]{"two words", "tab\tname", "new\nline", "a\u00a0b", "a\u0000b"}) {
        assertThrows(IllegalArgumentException.class, () -> f.setContigNameOverride(0, invalid));
        assertThrows(IllegalArgumentException.class, () -> f.setScaffoldNameOverride(id, invalid));
        assertEquals(before, f.snapshotAssembly());
      }
      f.setContigNameOverride(0, "контig_α");
      assertEquals("контig_α", f.getContigDisplayName(0));
    }
  }

  @Test void fastaCompatibilityUsesSourceIntervalsAfterReversalAndSplit() throws Exception {
    try (var f = fixture()) {
      var path = dir.resolve("source.fasta");
      java.nio.file.Files.writeString(path, ">C\n" + "G".repeat(6000) + "\n>A\n" + "A".repeat(6000) + "\n>B\n" + "C".repeat(6000) + "\n");
      var processor = new ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor(f);
      assertFalse(processor.analyzeLinkCandidate(path).hasWarnings());
      assertTrue(processor.buildSourceNameAliases(path).isEmpty(), "Exact names must win over same-length positional matches");
      var exported = dir.resolve("exported.fasta");
      java.nio.file.Files.writeString(exported, processor.exportAssemblyPreservingGaps(path));
      var sequences = ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor.readSequenceContents(exported);
      assertEquals("A".repeat(6000), sequences.get("scf_A"));
      assertEquals("C".repeat(6000), sequences.get("scf_B"));
      assertEquals("G".repeat(6000), sequences.get("scf_C"));
      f.scaffoldingOperations().reverseSelectionRangeBp(0, 18000);
      f.scaffoldingOperations().splitContigAtBin(1000, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      var report = processor.analyzeLinkCandidate(path);
      assertTrue(report.sourceIntervalsCovered());
      assertFalse(report.hasWarnings());
      java.nio.file.Files.writeString(path, ">A\nA\n>B\nC\n>C\nG\n");
      assertTrue(processor.analyzeLinkCandidate(path).hasWarnings());
      java.nio.file.Files.writeString(path, ">A\nA\n>A\nC\n");
      assertThrows(IllegalArgumentException.class, () -> processor.analyzeLinkCandidate(path));
      assertThrows(IllegalArgumentException.class, () -> ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor.readSequenceContents(path));
    }
  }

  @Test void movementInsideTargetScaffoldSnapsBothTreesToItsStart() {
    try (var f = fixture()) {
      var ops = f.scaffoldingOperations();
      var bp = ResolutionDescriptor.fromResolutionOrder(0);
      ops.scaffoldRegion(0, 12000, bp, QueryLengthUnit.BASE_PAIRS, null);
      ops.moveSelectionRangeBp(12000, 18000, 7000);
      assertEquals(List.of("C", "A", "B"), f.getContigTree().getOrderedContigList().stream()
        .map(c -> c.descriptor().getContigName()).toList());
      var scaffolds = f.getScaffoldTree().getScaffoldList();
      assertEquals(2, scaffolds.size());
      assertEquals(6000, scaffolds.getFirst().scaffoldBordersBP().endBP());
      assertEquals(6000, scaffolds.getLast().scaffoldBordersBP().startBP());
      assertEquals(18000, scaffolds.getLast().scaffoldBordersBP().endBP());
      var before = f.snapshotAssembly();
      ops.moveSelectionRangeBp(6001, 6002, 10000);
      assertEquals(before, f.snapshotAssembly(), "Destination within source scaffold is a no-op");
    }
  }

  @Test void splitPreservesScaffoldSidesAndZeroBinPiecesAtCoarserResolutions() {
    try(var f=fixture()) {
      var bp=ResolutionDescriptor.fromResolutionOrder(0);var ops=f.scaffoldingOperations();var initial=f.snapshotAssembly();
      for(boolean reversed:new boolean[]{false,true}) for(long cut:new long[]{1000,4000}) {
        f.restoreAssembly(initial);ops.scaffoldRegion(0,18000,bp,QueryLengthUnit.BASE_PAIRS,null);
        if(reversed) ops.reverseSelectionRangeBp(0,18000);
        var before=f.snapshotAssembly();
        var original=new HashMap<Long,double[][]>();
        for(long res:new long[]{1000,4000,10000}) {
          int n=(int)f.getContigTree().getLengthInUnits(QueryLengthUnit.BINS,ResolutionDescriptor.fromBpResolution(res,f));
          var m=f.getMatrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(res,f),0,0,n,n,false).matrix();
          double[][] values=new double[n][n];for(int i=0;i<n;i++)for(int j=0;j<n;j++)values[i][j]=m.getAsDouble(i,j);original.put(res,values);
        }
        ops.splitContigAtBin(6000+cut,bp,QueryLengthUnit.BASE_PAIRS);
        var scaffolds=f.getScaffoldTree().getScaffoldList();assertEquals(2,scaffolds.size());
        assertEquals(0,scaffolds.get(0).scaffoldBordersBP().startBP());
        assertEquals(6000+cut,scaffolds.get(0).scaffoldBordersBP().endBP());
        assertEquals(6000+cut,scaffolds.get(1).scaffoldBordersBP().startBP());assertEquals(17000,scaffolds.get(1).scaffoldBordersBP().endBP());
        assertEquals(4,f.getContigTree().getOrderedContigList().size());
        for(long res:new long[]{1000,4000,10000}) {
          int n=original.get(res).length-(res==1000?1:0),removed=6+(int)(cut/1000);
          var matrix=f.getMatrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(res,f),0,0,n,n,false).matrix();
          for(int i=0;i<n;i++)for(int j=0;j<n;j++)assertEquals(original.get(res)[i+(res==1000&&i>=removed?1:0)][j+(res==1000&&j>=removed?1:0)],matrix.getAsDouble(i,j),"resolution="+res+" reversed="+reversed+" cut="+cut);
        }
        var after=f.snapshotAssembly();f.restoreAssembly(before);assertEquals(before,f.snapshotAssembly());f.restoreAssembly(after);assertEquals(after,f.snapshotAssembly());
      }
    }
  }
}
