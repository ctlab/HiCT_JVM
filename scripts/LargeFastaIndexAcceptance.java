package ru.itmo.ctlab.hict.hict_library.assembly;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Random;

/** Run with -Xmx64m against a gzip FASTA larger than the heap. */
public final class LargeFastaIndexAcceptance {
  public static void main(String[] args) throws Exception {
    long start=System.nanoTime();
    try(var index=new IndexedFastaSource(Path.of(args[0]))) {
      var lengths=index.lengths();
      long bases=lengths.values().stream().mapToLong(Long::longValue).sum();
      if(bases<=Runtime.getRuntime().maxMemory())throw new AssertionError("Input must exceed the Java heap");
      double indexSeconds=(System.nanoTime()-start)/1e9;
      var random=new Random(652);int queries=0;
      start=System.nanoTime();
      try(var reader=index.open()) {
        for(var entry:lengths.entrySet()) {
          for(int i=0;i<4;i++) {
            int length=(int)Math.min(4096,entry.getValue());
            long first=random.nextLong(entry.getValue()-length+1);
            var out=new ByteArrayOutputStream(length);
            reader.copy(entry.getKey(),first,first+length,i%2==1,out);
            if(out.size()!=length)throw new AssertionError("Truncated interval");
            queries++;
          }
        }
      }
      System.out.printf("bases=%d heap=%d indexSeconds=%.3f queries=%d querySeconds=%.3f%n",
        bases,Runtime.getRuntime().maxMemory(),indexSeconds,queries,(System.nanoTime()-start)/1e9);
    }
    // The driver supplies an isolated java.io.tmpdir, so failed indexing must
    // leave it empty, too. No test artifacts are created next to the input.
    var temporary=Path.of(System.getProperty("java.io.tmpdir"));
    var malformed=Files.createTempFile(temporary,"invalid-", ".fa.gz");
    try {
      try(var gzip=new java.util.zip.GZIPOutputStream(Files.newOutputStream(malformed))) {
        gzip.write(">duplicate\nACTG\n>duplicate\nACTG\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
      }
      try(var ignored=new IndexedFastaSource(malformed)) { throw new AssertionError("Accepted duplicate headers"); }
      catch(IllegalArgumentException expected) { }
      Thread.currentThread().interrupt();
      try(var ignored=new IndexedFastaSource(Path.of(args[0]))) { throw new AssertionError("Ignored indexing cancellation"); }
      catch(java.io.InterruptedIOException expected) { }
      finally { Thread.interrupted(); }
    } finally { Files.deleteIfExists(malformed); }
    try(var files=Files.list(temporary)) {
      if(files.findAny().isPresent())throw new AssertionError("Temporary FASTA index leaked on failure or cancellation");
    }
    System.out.println("Temporary-copy cleanup passed for success, invalid input and cancellation.");
  }
}
