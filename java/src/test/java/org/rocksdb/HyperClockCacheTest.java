package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HyperClockCacheTest {
  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void newHyperClockCache() throws RocksDBException {
    RocksDB.loadLibrary();
    try (Cache cache = new HyperClockCache(1024 * 1024, 0, 8, false)) {
      BlockBasedTableConfig tableConfing = new BlockBasedTableConfig();
      tableConfing.setBlockCache(cache);
      try (Options options = new Options()) {
        options.setTableFormatConfig(tableConfing);
        options.setCreateIfMissing(true);
        try (RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
          db.put("testKey".getBytes(), "testData".getBytes());
        }
      }
    }
  }
}
