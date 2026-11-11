package ru.itmo.ctlab.hict.hict_library.history;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A map with O(1) checkpoints and an undo log for metadata changes. Reads retain
 * hash-map cost; restoring a branch costs only the number of changed keys on the
 * path, never the number of contigs. Values must be immutable after insertion.
 * Mutations and checkpoint restoration are serialized by the assembly owner.
 */
public final class VersionedMap<K, V> extends AbstractMap<K, V> {
  private final Map<K, V> values = new ConcurrentHashMap<>();
  private Version<K, V> current = new Version<>(this, null, null, null, null);

  public static final class Version<K, V> {
    private final VersionedMap<K, V> owner;
    private final Version<K, V> parent;
    private final K key;
    private final V before, after;
    private final long depth;
    private Version(VersionedMap<K, V> owner, Version<K, V> parent, K key, V before, V after) {
      this.owner = owner; this.parent = parent; this.key = key;
      this.before = before; this.after = after;
      this.depth = parent == null ? 0 : parent.depth + 1;
    }
  }

  public synchronized Version<K, V> snapshot() { return current; }

  public synchronized void restore(Version<K, V> target) {
    if (target.owner != this) throw new IllegalArgumentException("Checkpoint belongs to another map");
    var left = current;
    var right = target;
    var forward = new ArrayDeque<Version<K, V>>();
    while (left != right) {
      if (left.depth >= right.depth) {
        apply(left.key, left.before); left = left.parent;
      } else {
        forward.push(right); right = right.parent;
      }
    }
    while (!forward.isEmpty()) { var change = forward.pop(); apply(change.key, change.after); }
    current = target;
  }

  private void apply(K key, V value) { if (value == null) values.remove(key); else values.put(key, value); }
  @Override public V get(Object key) { return values.get(key); }
  @Override public int size() { return values.size(); }
  @Override public boolean containsKey(Object key) { return values.containsKey(key); }
  @Override public Set<Entry<K, V>> entrySet() { return Collections.unmodifiableMap(values).entrySet(); }
  @Override public synchronized V put(K key, V value) {
    Objects.requireNonNull(key); Objects.requireNonNull(value);
    V before = values.get(key);
    if (Objects.equals(before, value)) return before;
    current = new Version<>(this, current, key, before, value);
    values.put(key, value); return before;
  }
  @Override public synchronized V remove(Object key) {
    V before = values.get(key);
    if (before == null) return null;
    @SuppressWarnings("unchecked") K typedKey = (K) key;
    current = new Version<>(this, current, typedKey, before, null);
    values.remove(key); return before;
  }
  @Override public synchronized void clear() { for (K key : List.copyOf(values.keySet())) remove(key); }
}
