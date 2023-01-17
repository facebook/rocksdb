package org.rocksdb;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.MemorySession;
import java.lang.foreign.SegmentAllocator;
import java.util.ArrayList;
import java.util.List;

public class FFIAllocator implements SegmentAllocator {

  record SessionArena(MemorySession memorySession, SegmentAllocator segmentAllocator, int index) {}
  private static final long SIZE = 1L << 20;

  private final List<SessionArena> nativeArenas = new ArrayList<>(2);
  private int index = 0;

  public FFIAllocator() {
    synchronized (this) {
      recycleArenas();
    }
  }

  public void close() {
    synchronized (this) {
      for (final SessionArena sessionArena : nativeArenas) {
        sessionArena.memorySession.close();
      }
      nativeArenas.clear();
    }
  }

  private void recycleArenas() {
    final MemorySession memorySession = MemorySession.openConfined();
    final SegmentAllocator arena = SegmentAllocator.newNativeArena(SIZE, SIZE, memorySession);
    final var newest = new SessionArena(memorySession, arena, ++index);
    System.err.println("Add ARENA " + newest.index);
    nativeArenas.add(newest);
    while (nativeArenas.size() > 2) {
      final var oldest = nativeArenas.remove(0);
      System.err.println("Remove ARENA " + oldest.index);
      oldest.memorySession.close();
    }
  }

  @Override public MemorySegment allocate(final long bytesSize,
                                          final long bytesAlignment) {
    synchronized (this) {
      while (true) {
        final var newest = nativeArenas.get(nativeArenas.size() - 1);
        try {
          return newest.segmentAllocator.allocate(bytesSize, bytesAlignment);
        } catch (final OutOfMemoryError e) {
          recycleArenas();
        }
      }
    }
  }
}
