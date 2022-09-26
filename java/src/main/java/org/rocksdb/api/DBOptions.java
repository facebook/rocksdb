package org.rocksdb.api;

import org.rocksdb.DBOptionsInterface;

public class DBOptions extends RocksNative {
  static {
    RocksDB.loadLibrary();
  }

  protected DBOptions(long nativeReference) {
    super(nativeReference);
  }

  /**
   * Construct DBOptions.
   *
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::DBOptions} in the c++ side.
   */
  public DBOptions() {
    super(newDBOptions());
  }

  protected native void nativeClose(long nativeReference);

  private static native long newDBOptions();
}
