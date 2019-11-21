package org.rocksdb;

abstract class DbPathSupplierFactory extends RocksObject {
    static {
        RocksDB.loadLibrary();
    }

    protected DbPathSupplierFactory(long nativeHandle) {
        super(nativeHandle);
    }
}
