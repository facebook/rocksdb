package org.rocksdb.test;

public class SMRocksJunitRunner {
  // This list has been copied from Makefile
  private static final String[] TEST_CLASSES = new String[] {"org.rocksdb.BackupEngineOptionsTest",
      "org.rocksdb.BackupEngineTest", "org.rocksdb.BlobOptionsTest",
      "org.rocksdb.BlockBasedTableConfigTest", "org.rocksdb.BuiltinComparatorTest",
      "org.rocksdb.BytewiseComparatorRegressionTest", "org.rocksdb.util.BytewiseComparatorTest",
      "org.rocksdb.util.BytewiseComparatorIntTest", "org.rocksdb.CheckPointTest",
      "org.rocksdb.ClockCacheTest", "org.rocksdb.ColumnFamilyOptionsTest",
      "org.rocksdb.ColumnFamilyTest", "org.rocksdb.CompactionFilterFactoryTest",
      "org.rocksdb.CompactionJobInfoTest", "org.rocksdb.CompactionJobStatsTest",
      "org.rocksdb.CompactionOptionsTest", "org.rocksdb.CompactionOptionsFIFOTest",
      "org.rocksdb.CompactionOptionsUniversalTest", "org.rocksdb.CompactionPriorityTest",
      "org.rocksdb.CompactionStopStyleTest", "org.rocksdb.ComparatorOptionsTest",
      "org.rocksdb.CompressionOptionsTest", "org.rocksdb.CompressionTypesTest",
      "org.rocksdb.DBOptionsTest", "org.rocksdb.DirectSliceTest",
      "org.rocksdb.util.EnvironmentTest", "org.rocksdb.EnvOptionsTest",
      "org.rocksdb.EventListenerTest", "org.rocksdb.IngestExternalFileOptionsTest",
      "org.rocksdb.util.IntComparatorTest", "org.rocksdb.util.JNIComparatorTest",
      "org.rocksdb.FilterTest", "org.rocksdb.FlushTest", "org.rocksdb.InfoLogLevelTest",
      "org.rocksdb.KeyMayExistTest", "org.rocksdb.ConcurrentTaskLimiterTest",
      "org.rocksdb.LoggerTest", "org.rocksdb.LRUCacheTest", "org.rocksdb.MemoryUtilTest",
      "org.rocksdb.MemTableTest", "org.rocksdb.MergeTest", "org.rocksdb.MultiGetManyKeysTest",
      "org.rocksdb.MultiGetTest", "org.rocksdb.MixedOptionsTest",
      "org.rocksdb.MutableColumnFamilyOptionsTest", "org.rocksdb.MutableDBOptionsTest",
      "org.rocksdb.MutableOptionsGetSetTest", "org.rocksdb.NativeComparatorWrapperTest",
      "org.rocksdb.NativeLibraryLoaderTest", "org.rocksdb.OptimisticTransactionTest",
      "org.rocksdb.OptimisticTransactionDBTest", "org.rocksdb.OptimisticTransactionOptionsTest",
      "org.rocksdb.OptionsUtilTest", "org.rocksdb.OptionsTest", "org.rocksdb.PlainTableConfigTest",
      "org.rocksdb.RateLimiterTest", "org.rocksdb.ReadOnlyTest", "org.rocksdb.ReadOptionsTest",
      "org.rocksdb.util.ReverseBytewiseComparatorIntTest", "org.rocksdb.RocksDBTest",
      "org.rocksdb.RocksDBExceptionTest", "org.rocksdb.DefaultEnvTest",
      "org.rocksdb.RocksIteratorTest", "org.rocksdb.RocksMemEnvTest",
      "org.rocksdb.util.SizeUnitTest", "org.rocksdb.SecondaryDBTest", "org.rocksdb.SliceTest",
      "org.rocksdb.SnapshotTest", "org.rocksdb.SstFileManagerTest", "org.rocksdb.SstFileWriterTest",
      "org.rocksdb.SstFileReaderTest", "org.rocksdb.SstPartitionerTest",
      "org.rocksdb.TableFilterTest", "org.rocksdb.TimedEnvTest", "org.rocksdb.TransactionTest",
      "org.rocksdb.TransactionDBTest", "org.rocksdb.TransactionOptionsTest",
      "org.rocksdb.TransactionDBOptionsTest", "org.rocksdb.TransactionLogIteratorTest",
      "org.rocksdb.TtlDBTest", "org.rocksdb.StatisticsTest", "org.rocksdb.StatisticsCollectorTest",
      "org.rocksdb.VerifyChecksumsTest", "org.rocksdb.WalFilterTest",
      "org.rocksdb.WALRecoveryModeTest", "org.rocksdb.WriteBatchHandlerTest",
      "org.rocksdb.WriteBatchTest", "org.rocksdb.WriteBatchThreadedTest",
      "org.rocksdb.WriteOptionsTest", "org.rocksdb.WriteBatchWithIndexTest"};

  public static void main( String[] args) {
    RocksJunitRunner.main(TEST_CLASSES);
  }

}
