package juicebox.tools.utils.norm;

import juicebox.data.ContactRecord;
import juicebox.data.basics.ListOfFloatArrays;
import juicebox.data.iterator.IteratorContainer;

import java.io.*;
import java.util.*;

/** Runs the Juicebox VC_SQRT recipe (NormalizationVectorUpdater.buildVCOrVCSQRT + updateExpectedValueCalculationForChr). */
public class GoldenVcSqrt {
  public static void main(String[] args) throws Exception {
    final var lines = java.nio.file.Files.readAllLines(java.nio.file.Path.of(args[0]));
    final int n = Integer.parseInt(lines.get(0).trim());
    final var records = new ArrayList<ContactRecord>();
    for (int i = 1; i < lines.size(); i++) {
      final var p = lines.get(i).trim().split("\\s+");
      if (p.length < 3) continue;
      records.add(new ContactRecord(Integer.parseInt(p[0]), Integer.parseInt(p[1]), Float.parseFloat(p[2])));
    }
    final var container = new IteratorContainer(n) {
      @Override public Iterator<ContactRecord> getNewContactRecordIterator() { return records.iterator(); }
      @Override public ListOfFloatArrays sparseMultiply(ListOfFloatArrays vector, long vectorLength) { throw new UnsupportedOperationException(); }
      @Override public void clear() {}
    };
    final var nc = new NormalizationCalculations(container);
    final var vc = nc.computeVC();
    final var sqrt = new ListOfFloatArrays(vc.getLength());
    for (int i = 0; i < vc.getLength(); i++) sqrt.set(i, (float) Math.sqrt(vc.get(i)));
    final double factor = nc.getSumFactor(sqrt);
    sqrt.multiplyEverythingBy(factor);
    final var out = new StringBuilder("{\"n\":" + n + ",\"factor\":" + factor + ",\"vector\":[");
    for (int i = 0; i < sqrt.getLength(); i++) {
      if (i > 0) out.append(',');
      final float v = sqrt.get(i);
      out.append(Float.isNaN(v) ? "null" : Float.toString(v));
    }
    out.append("]}");
    System.out.println(out);
  }
}
