package ru.itmo.ctlab.hict.hict_library.assembly;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/** Read-only, in-memory line index. No .fai or normalized copy is written beside the source. */
final class IndexedFastaSource {
  private record Stamp(long size, java.nio.file.attribute.FileTime modified, Object key) { }
  private record Run(long start, long offset, long bases, long stride, long lines) {
    long end() { return start + (long) bases * lines; }
    long physical(long position) { long p=position-start; return offset+(p/bases)*stride+p%bases; }
  }
  private static final class Sequence {
    long length;
    final List<Run> runs=new ArrayList<>();
    void line(long offset,long bases,long stride) {
      if (!runs.isEmpty()) {
        Run r=runs.getLast();
        if(r.bases==bases && r.stride==stride && offset==r.offset+r.lines*r.stride) {
          runs.set(runs.size()-1,new Run(r.start,r.offset,bases,stride,r.lines+1));length+=bases;return;
        }
      }
      runs.add(new Run(length,offset,bases,stride,1));length+=bases;
    }
  }
  private final Path path;
  private final Stamp stamp;
  private final LinkedHashMap<String,Sequence> sequences=new LinkedHashMap<>();
  private static Stamp stamp(Path p) throws IOException {
    var a=Files.readAttributes(p,BasicFileAttributes.class);return new Stamp(a.size(),a.lastModifiedTime(),a.fileKey());
  }
  boolean matches(Path p) throws IOException { return path.equals(p.toAbsolutePath().normalize()) && stamp.equals(stamp(p)); }
  void verify() throws IOException { if(!matches(path)) throw new IOException("Linked FASTA changed during export: "+path); }
  IndexedFastaSource(Path input) throws IOException {
    path=input.toAbsolutePath().normalize();stamp=stamp(path);
    // Scan bytes once, retaining only headers and regular-line runs. Even an
    // unwrapped chromosome does not allocate a chromosome-sized text buffer.
    byte[] block=new byte[1<<20];long position=0,lineStart=0,first=-1,last=-1;
    boolean header=false;Sequence current=null;ByteArrayOutputStream name=new ByteArrayOutputStream();
    try(var in=Files.newInputStream(path)) {
      for(int size;(size=in.read(block))!=-1;) {
        if(Thread.currentThread().isInterrupted()) throw new InterruptedIOException("FASTA indexing cancelled");
        for(int i=0;i<size;i++,position++) {
          int b=block[i]&255;
          if(position==lineStart) { header=b=='>';first=-1;last=-1;name.reset(); }
          if(b=='\n') {
            if(header) {
              String h=name.toString(StandardCharsets.UTF_8).substring(1).trim();
              String id=h.split("\\s+",2)[0];
              if(id.isEmpty())throw new IllegalArgumentException("Encountered an empty FASTA header");
              current=new Sequence();if(sequences.putIfAbsent(id,current)!=null)throw new IllegalArgumentException("Duplicate FASTA identifier: "+id);
            } else if(current!=null && first>=0) current.line(first,last-first+1,position+1-lineStart);
            lineStart=position+1;
          } else if(header) name.write(b);
          else if(b>32) { if(b>127)throw new IllegalArgumentException("Non-ASCII FASTA sequence");if(first<0)first=position;last=position; }
        }
      }
      if(position>lineStart) {
        if(header) {
          String h=name.toString(StandardCharsets.UTF_8).substring(1).trim();String id=h.split("\\s+",2)[0];
          if(id.isEmpty()||sequences.putIfAbsent(id,new Sequence())!=null)throw new IllegalArgumentException("Empty or duplicate FASTA identifier: "+id);
        } else if(current!=null && first>=0)current.line(first,last-first+1,position-lineStart);
      }
    }
    if(sequences.isEmpty())throw new IllegalArgumentException("FASTA contains no sequences");verify();
  }
  LinkedHashMap<String,Long> lengths() {
    var result=new LinkedHashMap<String,Long>();sequences.forEach((n,s)->result.put(n,s.length));return result;
  }
  Session open() throws IOException { verify();return new Session(); }
  final class Session implements AutoCloseable {
    private final FileChannel channel;
    private final byte[] data=new byte[64*1024];
    private byte[] physical=new byte[128*1024];
    Session() throws IOException { channel=FileChannel.open(path,StandardOpenOption.READ); }
    void copy(String name,long start,long end,boolean reverse,OutputStream output) throws IOException {
      Sequence sequence=sequences.get(name);
      if(sequence==null)throw new IllegalArgumentException("Linked FASTA does not contain sequence '"+name+"'");
      if(start<0||end<start||end>sequence.length)throw new IllegalArgumentException("Invalid FASTA interval for "+name+": "+start+".."+end);
      long cursor=reverse?end:start;
      while(reverse?cursor>start:cursor<end) {
        if(Thread.currentThread().isInterrupted())throw new InterruptedIOException("FASTA export cancelled");
        long from=reverse?Math.max(start,cursor-data.length):cursor;
        int n=(int)(reverse?cursor-from:Math.min(data.length,end-cursor));
        read(sequence,from,n,data);
        if(reverse)for(int i=0,j=n-1;i<=j;i++,j--) { byte a=complement(data[i]);data[i]=complement(data[j]);data[j]=a; }
        output.write(data,0,n);cursor=reverse?from:cursor+n;
      }
    }
    private void read(Sequence s,long start,int length,byte[] dst) throws IOException {
      int lo=0,hi=s.runs.size();
      while(lo<hi) { int mid=(lo+hi)>>>1;if(s.runs.get(mid).end()<=start)lo=mid+1;else hi=mid; }
      int written=0;
      while(written<length) {
        Run r=s.runs.get(lo++);int n=(int)Math.min(length-written,r.end()-start);
        long begin=r.physical(start),end=r.physical(start+n-1)+1;
        // Irregular/very padded lines may span more than our physical buffer.
        // Read only one logical line in that exceptional case.
        if(end-begin>physical.length) { n=(int)Math.min(n,r.bases-(start-r.start)%r.bases);end=begin+n;lo--; }
        int bytes=Math.toIntExact(end-begin);ByteBuffer buffer=ByteBuffer.wrap(physical,0,bytes);
        while(buffer.hasRemaining()) { int got=channel.read(buffer,begin+buffer.position());if(got<0)throw new EOFException("FASTA truncated during export"); }
        int used=0;
        while(used<n) {
          int count=(int)Math.min(n-used,r.bases-(start+used-r.start)%r.bases);
          System.arraycopy(physical,Math.toIntExact(r.physical(start+used)-begin),dst,written+used,count);used+=count;
        }
        start+=n;written+=n;
      }
    }
    public void close() throws IOException { channel.close();verify(); }
  }
  static byte complement(byte base) {
    int b=base&255;boolean lower=b>='a'&&b<='z';int c=Character.toUpperCase((char)b);
    int out=switch(c) { case 'A'->'T';case 'T'->'A';case 'C'->'G';case 'G'->'C';case 'R'->'Y';case 'Y'->'R';case 'S'->'S';case 'W'->'W';case 'K'->'M';case 'M'->'K';case 'B'->'V';case 'V'->'B';case 'D'->'H';case 'H'->'D';default->'N'; };
    return (byte)(lower?Character.toLowerCase((char)out):out);
  }
}
