package org.rocksdb.api;

public class ColumnFamilyOptions extends RocksNative {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct ColumnFamilyOptions.
   *
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::ColumnFamilyOptions} in the c++ side.
   */
  public ColumnFamilyOptions() {
    super(newColumnFamilyOptions());
  }

  protected native void nativeClose(long nativeReference);

  private static native long newColumnFamilyOptions();
}
