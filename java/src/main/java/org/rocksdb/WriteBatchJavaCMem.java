package org.rocksdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class WriteBatchJavaCMem extends RocksObject implements WriteBatchInterface {

    private final static int DEFAULT_BUFFER_SIZE = 32768;

    private ByteBuffer buffer;

    public WriteBatchJavaCMem(final int reserved_bytes) {
        super(newWriteBatchJavaCMem(reserved_bytes));
        buffer = ByteBuffer.wrap(new byte[reserved_bytes]).order(ByteOrder.nativeOrder());
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public void put(byte[] key, byte[] value) throws RocksDBException {

    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {

    }

    @Override
    public void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {

    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws RocksDBException {

    }

    @Override
    public void merge(byte[] key, byte[] value) throws RocksDBException {

    }

    @Override
    public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {

    }

    @Override
    public void delete(byte[] key) throws RocksDBException {

    }

    @Override
    public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {

    }

    @Override
    public void delete(ByteBuffer key) throws RocksDBException {

    }

    @Override
    public void delete(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key) throws RocksDBException {

    }

    @Override
    public void singleDelete(byte[] key) throws RocksDBException {

    }

    @Override
    public void singleDelete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {

    }

    @Override
    public void deleteRange(byte[] beginKey, byte[] endKey) throws RocksDBException {

    }

    @Override
    public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey) throws RocksDBException {

    }

    @Override
    public void putLogData(byte[] blob) throws RocksDBException {

    }

    @Override
    public void clear() {

    }

    @Override
    public void setSavePoint() {

    }

    @Override
    public void rollbackToSavePoint() throws RocksDBException {

    }

    @Override
    public void popSavePoint() throws RocksDBException {

    }

    @Override
    public void setMaxBytes(long maxBytes) {

    }

    @Override
    public WriteBatch getWriteBatch() {
        return null;
    }

    @Override
    protected void disposeInternal(long handle) {
        disposeInternalWriteBatchJavaNative(handle);
    }

    private static native long newWriteBatchJavaCMem(final int reserved_bytes);

    private static native void disposeInternalWriteBatchJavaNative(final long handle);

    private static native void flushWriteBatchJavaCMem(
        final long handle, final long position, final byte[] buf);
}
