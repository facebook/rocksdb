package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.SizeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class LostOptionsTest {

    @ClassRule
    public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Test
    public void lostOptions() throws RocksDBException {
        try (Options opts = new Options(); Statistics statistics = new Statistics()) {
            opts.setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            opts.setStatistics(statistics);
            opts.setBytesPerSync(SizeUnit.MB);
            BlockBasedTableConfig blockBasedTableConfig = opts.tableFormatConfig() instanceof BlockBasedTableConfig
              ? (BlockBasedTableConfig) opts.tableFormatConfig()
              : new BlockBasedTableConfig();
            blockBasedTableConfig.setFilterPolicy(new BloomFilter(10));
            opts.setTableFormatConfig(blockBasedTableConfig);
            opts.setWriteBufferSize(SizeUnit.MB * 16);
            opts.setLevelCompactionDynamicLevelBytes(true);
            RocksDB db = OptimisticTransactionDB.open(opts, dbFolder.getRoot().getAbsolutePath());
            assertThat(db).isNotNull();


            // we changed this just to see it gets applied
            assertThat(db.getDefaultColumnFamily().getDescriptor().getOptions().writeBufferSize()).isEqualTo(SizeUnit.MB * 16);
            // this throws with options being null
            assertThat(db.getDefaultColumnFamily().getDescriptor().getOptions().tableFormatConfig()).isNotNull();
        }
    }
}
