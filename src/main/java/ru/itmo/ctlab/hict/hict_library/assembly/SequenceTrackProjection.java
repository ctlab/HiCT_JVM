package ru.itmo.ctlab.hict.hict_library.assembly;

import java.util.*;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDirection;

/** Immutable sequence-coordinate projection, separate from all matrix bin coordinates. */
public final class SequenceTrackProjection {
  public record Component(long start, long end, String contig, String scaffold,
                          String source, long sourceStart, boolean reversed) { }
  public record Piece(long start, long end, Long assemblyStart, Long assemblyEnd,
                      String contig, String scaffold, String source, Long sourceStart, Long sourceEnd,
                      boolean reversed, boolean gap) {
    public Piece slice(long from, long to) {
      if (from < start || to > end || from >= to) throw new IllegalArgumentException("Invalid sequence slice");
      long a=from-start,b=to-start;
      return gap ? new Piece(from,to,null,null,null,null,null,null,null,false,true)
        : new Piece(from,to,assemblyStart+a,assemblyStart+b,contig,scaffold,source,
          reversed ? sourceEnd-b : sourceStart+a, reversed ? sourceEnd-a : sourceStart+b,reversed,false);
    }
  }
  private final List<Piece> pieces;
  private final List<Piece> sequencePieces;
  private final long length;
  public SequenceTrackProjection(List<Component> components, List<AssemblySequenceLayout.Gap> gaps) {
    var result=new ArrayList<Piece>(); long assembly=0,display=0; int g=0;
    for(var c:components) {
      if(c.start()!=assembly || c.end()<=c.start()) throw new IllegalArgumentException("Non-contiguous sequence layout");
      long position=c.start();
      while(position<c.end()) {
        while(g<gaps.size() && gaps.get(g).position()==position) {
          long end=Math.addExact(display,gaps.get(g++).length());
          result.add(new Piece(display,end,null,null,null,null,null,null,null,false,true)); display=end;
        }
        long end=c.end();
        if(g<gaps.size()) {
          if(gaps.get(g).position()<position) throw new IllegalArgumentException("Unsorted gaps");
          end=Math.min(end,gaps.get(g).position());
        }
        long count=end-position, offset=position-c.start(), size=c.end()-c.start();
        result.add(new Piece(display,Math.addExact(display,count),position,end,c.contig(),c.scaffold(),c.source(),
          c.sourceStart()+(c.reversed()?size-offset-count:offset),
          c.sourceStart()+(c.reversed()?size-offset:offset+count),c.reversed(),false));
        display=Math.addExact(display,count);position=end;
      }
      assembly=c.end();
    }
    while(g<gaps.size() && gaps.get(g).position()==assembly) {
      long end=Math.addExact(display,gaps.get(g++).length());
      result.add(new Piece(display,end,null,null,null,null,null,null,null,false,true));display=end;
    }
    if(g!=gaps.size())throw new IllegalArgumentException("Gap outside assembly");
    pieces=List.copyOf(result);length=display;
    sequencePieces=pieces.stream().filter(p -> !p.gap()).toList();
  }
  public static SequenceTrackProjection capture(ChunkedFile file, boolean padded) {
    var info=file.getAssemblyInfo(); var components=new ArrayList<Component>();
    var scaffolds=info.scaffolds(); int s=0;long position=0;
    for(var item:info.contigs()) {
      var d=item.descriptor(); long end=Math.addExact(position,d.getLengthBp());
      while(s<scaffolds.size() && scaffolds.get(s).scaffoldBordersBP().endBP()<=position)s++;
      String name=null;
      if(s<scaffolds.size()) {
        var sc=scaffolds.get(s);
        if(sc.scaffoldBordersBP().startBP()<=position && sc.scaffoldDescriptor()!=null)
          name=file.getScaffoldNameOverrides().getOrDefault(sc.scaffoldDescriptor().scaffoldId(),sc.scaffoldDescriptor().scaffoldName());
      }
      components.add(new Component(position,end,file.getContigDisplayName(d.getContigId()),name,d.getContigNameInSourceFASTA(),
        d.getOffsetInSourceFASTA(),item.direction()==ContigDirection.REVERSED));position=end;
    }
    return new SequenceTrackProjection(components,padded
      ? AssemblySequenceLayout.capture(file,AssemblyExportSettings.gapLengthBp()).gaps() : List.of());
  }
  public long length(){return length;}
  /** Map a real assembly base to padded sequence coordinates; joins select the right-hand base. */
  public long sequencePosition(long assemblyBp) {
    if (assemblyBp < 0 || sequencePieces.isEmpty()
      || assemblyBp >= sequencePieces.getLast().assemblyEnd())
      throw new IllegalArgumentException("Assembly base is outside the active sequence");
    int low=0,high=sequencePieces.size();
    while(low<high) {int mid=(low+high)>>>1;
      if(sequencePieces.get(mid).assemblyEnd()<=assemblyBp)low=mid+1;else high=mid;}
    var p=sequencePieces.get(low);
    return p.start()+assemblyBp-p.assemblyStart();
  }
  public List<Piece> window(long start,long end) {
    if(start<0 || end<=start || end>length || end-start>4096)
      throw new IllegalArgumentException("Sequence window must contain 1 to 4096 bases within the active assembly");
    int low=0,high=pieces.size();
    while(low<high) {int mid=(low+high)>>>1;if(pieces.get(mid).end()<=start)low=mid+1;else high=mid;}
    var result=new ArrayList<Piece>();
    for(int i=low;i<pieces.size() && pieces.get(i).start()<end;i++) {
      var p=pieces.get(i);result.add(p.slice(Math.max(start,p.start()),Math.min(end,p.end())));
    }
    return List.copyOf(result);
  }
}
