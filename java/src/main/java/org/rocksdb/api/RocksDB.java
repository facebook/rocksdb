package org.rocksdb.api;

public class RocksDB extends RocksNative {

  protected RocksDB(long nativeReference) {
    super(nativeReference);
  }

  @Override
  native void nativeClose(long nativeReference);
}
