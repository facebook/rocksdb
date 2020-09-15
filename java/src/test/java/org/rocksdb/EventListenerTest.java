package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class EventListenerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  static class TestEventListener extends AbstractEventListener {
    public TestEventListener() {
      super();
    }

    @Override
    public void onFlushCompleted(final RocksDB db,
                                 final FlushJobInfo flushJobInfo) {

    }
  }

  @Test
  public void basicEventListenerTest() throws RocksDBException, InterruptedException {
    final AtomicBoolean wasCalled = new AtomicBoolean();
    AbstractEventListener testEventListener = new AbstractEventListener() {
      @Override
      public void onFlushCompleted(final RocksDB rocksDb,
                                   final FlushJobInfo flushJobInfo) {
        // System.out.println("onFlushCompleted threadId: " + Thread.currentThread().getId());
        assertNotNull(flushJobInfo.getColumnFamilyName());
        assertNotNull(flushJobInfo.getColumnFamilyName());
        assertEquals(FlushReason.MANUAL_FLUSH, flushJobInfo.getFlushReason());
        wasCalled.set(true);
      }
    };
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setListeners(testEventListener);
         final RocksDB db =
             RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      // System.out.println("test threadId: " + Thread.currentThread().getId());
      db.flush(new FlushOptions());
      assertTrue(wasCalled.get());
    }
  }
}
