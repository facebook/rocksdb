package org.rocksdb;

/**
 * The config for skip-list memtable representation.
 */
public class SkipListMemTableConfig extends MemTableConfig {
  public SkipListMemTableConfig() {
  }

  @Override protected long newMemTableFactoryHandle() {
    return newMemTableFactoryHandle0();
  }

  private native long newMemTableFactoryHandle0();
}
