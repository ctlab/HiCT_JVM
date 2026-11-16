package ru.itmo.ctlab.hict.hict_library.converters;

import ch.systemsx.cisd.hdf5.*;

/** Writes sorted, unique float64 pixels without changing their values or numeric type. */
final class FloatingPointCoolerPixels {
  private final IHDF5Writer writer;
  private final String root;
  private final long bins;
  private final long[] chromOffsets, offsets, rows, cols;
  private final double[] values;
  private final HDF5IntStorageFeatures ints;
  private int buffered, nextRow;
  private long written, count, previousRow = -1, previousCol = -1;
  private double total, totalCorrection, cis, cisCorrection;

  FloatingPointCoolerPixels(IHDF5Writer writer, String root, long bins, long[] chromOffsets, int chunk,
      HDF5IntStorageFeatures ints, HDF5FloatStorageFeatures floats) {
    this.writer=writer; this.root=root; this.bins=bins; this.chromOffsets=chromOffsets.clone(); this.ints=ints;
    if (bins<0 || chunk<=0 || chromOffsets.length==0 || chromOffsets[chromOffsets.length-1]!=bins)
      throw new IllegalArgumentException("Invalid floating-point Cooler dimensions");
    offsets=new long[Math.toIntExact(bins+1)]; rows=new long[chunk]; cols=new long[chunk]; values=new double[chunk];
    writer.int64().createArray(path("pixels/bin1_id"),0L,chunk,ints);
    writer.int64().createArray(path("pixels/bin2_id"),0L,chunk,ints);
    writer.float64().createArray(path("pixels/count"),0L,chunk,floats);
  }

  void accept(long row,long col,double value) {
    if(row<0 || col<row || col>=bins || !Double.isFinite(value)
      || row<previousRow || row==previousRow && col<=previousCol)
      throw new IllegalArgumentException("Floating-point Cooler pixels must be finite, sorted, unique and inside the upper triangle");
    while(nextRow<=row)offsets[nextRow++]=count;
    previousRow=row; previousCol=col;
    rows[buffered]=row;cols[buffered]=col;values[buffered++]=value;count++;
    double adjusted=value-totalCorrection,next=total+adjusted;
    totalCorrection=(next-total)-adjusted;total=next;
    if(chromosome(row)==chromosome(col)) {
      adjusted=value-cisCorrection;next=cis+adjusted;cisCorrection=(next-cis)-adjusted;cis=next;
    }
    if(!Double.isFinite(total)||!Double.isFinite(cis))throw new IllegalArgumentException("Floating-point Cooler totals exceed float64 range");
    if(buffered==rows.length)flush();
  }

  void finish() {
    flush();while(nextRow<offsets.length)offsets[nextRow++]=count;
    writer.int64().writeArray(path("indexes/bin1_offset"),offsets,ints);
  }
  long count(){return count;}
  double total(){return total;}
  double cis(){return cis;}
  private int chromosome(long bin) {
    int lo=0,hi=chromOffsets.length-1;
    while(lo<hi){int mid=(lo+hi)>>>1;if(chromOffsets[mid]<=bin)lo=mid+1;else hi=mid;}
    return lo-1;
  }
  private void flush() {
    if(buffered==0)return;
    writer.int64().writeArrayBlockWithOffset(path("pixels/bin1_id"),rows,buffered,written);
    writer.int64().writeArrayBlockWithOffset(path("pixels/bin2_id"),cols,buffered,written);
    writer.float64().writeArrayBlockWithOffset(path("pixels/count"),values,buffered,written);
    written+=buffered;buffered=0;
  }
  private String path(String child){return root.equals("/")?"/"+child:root+"/"+child;}
}
