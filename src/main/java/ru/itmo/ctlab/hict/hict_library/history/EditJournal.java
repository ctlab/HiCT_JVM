package ru.itmo.ctlab.hict.hict_library.history;

import java.time.Instant;
import java.util.*;

/** Session journal. The owner serializes access and publishes immutable summaries. */
public final class EditJournal<S> {
  public record Item(long id, String label, String timestamp) {}
  public record Summary(String session, long revision, long currentId, int position,
                        int size, boolean canUndo, boolean canRedo) {}
  private record Entry<S>(Item item, S state) {}
  private final String session = UUID.randomUUID().toString();
  private final ArrayList<Entry<S>> entries = new ArrayList<>();
  private int cursor;
  private long revision, nextId = 1;

  public EditJournal(S initial) { entries.add(new Entry<>(new Item(0, "Initial assembly", Instant.now().toString()), initial)); }
  public S current() { return entries.get(cursor).state(); }
  public Summary summary() { return new Summary(session, revision, entries.get(cursor).item().id(), cursor,
    entries.size(), cursor > 0, cursor + 1 < entries.size()); }
  public List<Item> page(int offset, int limit) {
    if (offset < 0 || limit < 1 || limit > 200) throw new IllegalArgumentException("Invalid journal page (limit 1–200)");
    return entries.subList(Math.min(offset, entries.size()), Math.min(entries.size(), (int)Math.min((long)offset+limit, Integer.MAX_VALUE)))
      .stream().map(Entry::item).toList();
  }
  public void append(String label, S state) {
    entries.subList(cursor + 1, entries.size()).clear();
    entries.add(new Entry<>(new Item(nextId++, label, Instant.now().toString()), state));
    cursor++; revision++;
  }
  public void requireVersion(String expectedSession, long expectedRevision) {
    if (!session.equals(expectedSession) || revision != expectedRevision)
      throw new IllegalStateException("Assembly history changed. Refresh the journal before editing again.");
  }
  public int target(String action, Long id) {
    if ("undo".equals(action)) return Math.max(0, cursor - 1);
    if ("redo".equals(action)) return Math.min(entries.size() - 1, cursor + 1);
    if (!"restore".equals(action) || id == null) throw new IllegalArgumentException("Expected undo, redo or restore with id");
    // IDs are monotonically increasing even after a redo branch is discarded.
    int lo = 0, hi = entries.size() - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1; long candidate = entries.get(mid).item().id();
      if (candidate == id) return mid;
      if (candidate < id) lo = mid + 1; else hi = mid - 1;
    }
    throw new IllegalArgumentException("History entry is no longer available");
  }
  public S stateAt(int index) { return entries.get(index).state(); }
  public void movedTo(int index) { if (cursor != index) { cursor = index; revision++; } }
}
