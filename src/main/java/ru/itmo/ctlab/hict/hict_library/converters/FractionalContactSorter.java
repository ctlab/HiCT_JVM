package ru.itmo.ctlab.hict.hict_library.converters;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;

/** Bounded-memory, bounded-file-descriptor sorting and summation of fractional COO records. */
final class FractionalContactSorter implements AutoCloseable {
  @FunctionalInterface interface Consumer { void accept(long row, long col, double value) throws IOException; }
  private record Key(long row, long col) implements Comparable<Key> {
    @Override public int compareTo(Key other) {
      final int rows = Long.compare(row, other.row);
      return rows == 0 ? Long.compare(col, other.col) : rows;
    }
  }
  /** Compensated accumulation reduces order-dependent loss when many fragments merge. */
  private static final class Sum {
    double value, correction;
    void add(double term) {
      final double adjusted = term - correction, next = value + adjusted;
      correction = (next - value) - adjusted; value = next;
      if (!Double.isFinite(value)) throw new IllegalArgumentException("Fractional contact sum exceeds float64 range");
    }
  }
  private final Path directory;
  private final int capacity, fanIn;
  private final BooleanSupplier cancelled;
  private final TreeMap<Key, Sum> pending = new TreeMap<>();
  private List<Path> chunks = new ArrayList<>();
  private boolean finished;

  FractionalContactSorter(Path parent, int capacity, int fanIn, BooleanSupplier cancelled) throws IOException {
    if (capacity <= 0 || fanIn < 2 || fanIn > 64) throw new IllegalArgumentException("Invalid fractional sort bounds");
    this.directory = Files.createTempDirectory(parent, ".hict-fractional-sort-");
    this.capacity = capacity; this.fanIn = fanIn; this.cancelled = cancelled;
  }

  void add(long row, long col, double value) throws IOException {
    check();
    if (finished) throw new IllegalStateException("Fractional sort is already finished");
    if (row < 0 || col < row || !Double.isFinite(value)) throw new IllegalArgumentException("Invalid fractional COO record");
    if (value == 0) return;
    pending.computeIfAbsent(new Key(row, col), ignored -> new Sum()).add(value);
    if (pending.size() >= capacity) flush();
  }

  void finish(Consumer consumer) throws IOException {
    check();
    if (finished) throw new IllegalStateException("Fractional sort is already finished");
    finished = true;
    flush();
    while (chunks.size() > fanIn) {
      final var merged = new ArrayList<Path>();
      for (int first = 0; first < chunks.size(); first += fanIn) {
        final var group = chunks.subList(first, Math.min(chunks.size(), first + fanIn));
        final var output = Files.createTempFile(directory, "merge-", ".coo");
        try (var stream = output(output)) { merge(group, (r, c, v) -> write(stream, r, c, v)); }
        merged.add(output);
        for (var path : group) Files.delete(path);
      }
      chunks = merged;
    }
    merge(chunks, consumer);
  }

  private void flush() throws IOException {
    if (pending.isEmpty()) return;
    check();
    final var path = Files.createTempFile(directory, "chunk-", ".coo");
    try (var stream = output(path)) {
      for (var entry : pending.entrySet()) {
        check();
        if (entry.getValue().value != 0) write(stream, entry.getKey().row, entry.getKey().col, entry.getValue().value);
      }
    }
    chunks.add(path); pending.clear();
  }

  private void merge(List<Path> inputs, Consumer consumer) throws IOException {
    final var opened = new ArrayList<Cursor>();
    final var queue = new PriorityQueue<Cursor>(Comparator.comparing(c -> c.key));
    try {
      for (var path : inputs) {
        check(); final var cursor = new Cursor(path); opened.add(cursor);
        if (cursor.next()) queue.add(cursor);
      }
      Key key = null; Sum sum = new Sum();
      while (!queue.isEmpty()) {
        check(); final var cursor = queue.remove();
        if (key != null && !key.equals(cursor.key)) {
          if (sum.value != 0) consumer.accept(key.row, key.col, sum.value);
          sum = new Sum();
        }
        key = cursor.key; sum.add(cursor.value);
        if (cursor.next()) queue.add(cursor);
      }
      if (key != null && sum.value != 0) consumer.accept(key.row, key.col, sum.value);
    } finally {
      IOException failure = null;
      for (var cursor : opened) try { cursor.close(); } catch (IOException error) { failure = error; }
      if (failure != null) throw failure;
    }
  }

  private void check() {
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new CancellationException();
  }
  private static DataOutputStream output(Path path) throws IOException {
    return new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path), 65536));
  }
  private static void write(DataOutputStream output, long row, long col, double value) throws IOException {
    output.writeLong(row); output.writeLong(col); output.writeDouble(value);
  }
  private static final class Cursor implements AutoCloseable {
    final DataInputStream stream;
    long remaining; Key key; double value;
    Cursor(Path path) throws IOException {
      final long bytes = Files.size(path);
      if (bytes % 24 != 0) throw new EOFException("Truncated fractional COO record");
      remaining = bytes / 24;
      stream = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 65536));
    }
    boolean next() throws IOException {
      if (remaining == 0) return false;
      key = new Key(stream.readLong(), stream.readLong()); value = stream.readDouble(); remaining--; return true;
    }
    @Override public void close() throws IOException { stream.close(); }
  }
  @Override public void close() throws IOException {
    pending.clear();
    try (var paths = Files.walk(directory)) {
      for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
    }
  }
}
