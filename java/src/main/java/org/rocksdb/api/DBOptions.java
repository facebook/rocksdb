package org.rocksdb.api;

import org.rocksdb.DBOptionsInterface;

public class DBOptions extends RocksNative {
  static {
    RocksDB.loadLibrary();
  }

  protected DBOptions(long nativeReference) {
    super(nativeReference);
  }

  protected native void nativeClose(long nativeReference);
}
