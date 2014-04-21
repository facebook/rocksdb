package org.rocksdb;

/**
 * The config for hash linked list memtable representation
 * Such memtable contains a fix-sized array of buckets, where
 * each bucket points to a sorted singly-linked
 * list (or null if the bucket is empty).
 *
 * Note that since this mem-table representation relies on the
 * key prefix, it is required to invoke one of the usePrefixExtractor
 * functions to specify how to extract key prefix given a key.
 * If proper prefix-extractor is not set, then RocksDB will
 * use the default memtable representation (SkipList) instead
 * and post a warning in the LOG.
 */
public class HashLinkedListMemTableConfig extends MemTableConfig {
  public static final long DEFAULT_BUCKET_COUNT = 50000;

  public HashLinkedListMemTableConfig() {
    bucketCount_ = DEFAULT_BUCKET_COUNT;
  }

  /**
   * Set the number of buckets in the fixed-size array used
   * in the hash linked-list mem-table.
   *
   * @param count the number of hash buckets.
   * @return the reference to the current HashLinkedListMemTableConfig.
   */
  public HashLinkedListMemTableConfig setBucketCount(long count) {
    bucketCount_ = count;
    return this;
  }

  /**
   * Returns the number of buckets that will be used in the memtable
   * created based on this config.
   *
   * @return the number of buckets
   */
  public long bucketCount() {
    return bucketCount_;
  }

  @Override protected long newMemTableFactoryHandle() {
    return newMemTableFactoryHandle(bucketCount_);
  }

  private native long newMemTableFactoryHandle(long bucketCount);

  private long bucketCount_;
}
