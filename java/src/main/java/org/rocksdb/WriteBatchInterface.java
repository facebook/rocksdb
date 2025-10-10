// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * <p>Defines the interface for a Write Batch which
 * holds a collection of updates to apply atomically to a DB.</p>
 */
public interface WriteBatchInterface {

    /**
     * Returns the number of updates in the batch.
     *
     * @return number of items in WriteBatch
     */
    int count();

    /**
     * <p>Store the mapping "key-&gt;value" in the database.</p>
     *
     * @param key the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void put(byte[] key, byte[] value) throws RocksDBException;

    /**
     * <p>Store the mapping "key-&gt;value" within given column
     * family.</p>
     *
     * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
     *     instance
     * @param key the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
        throws RocksDBException;

    /**
     * <p>Store the mapping "key-&gt;value" within given column
     * family.</p>
     *
     * @param key the specified key to be inserted. It is using position and limit.
     *     Supports direct buffer only.
     * @param value the value associated with the specified key. It is using position and limit.
     *     Supports direct buffer only.
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void put(final ByteBuffer key, final ByteBuffer value) throws RocksDBException;

    /**
     * <p>Store the mapping "key-&gt;value" within given column
     * family.</p>
     *
     * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
     *     instance
     * @param key the specified key to be inserted. It is using position and limit.
     *     Supports direct buffer only.
     * @param value the value associated with the specified key. It is using position and limit.
     *     Supports direct buffer only.
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void put(ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key, final ByteBuffer value)
        throws RocksDBException;

    /**
     * Sets the database entry for the specified key within the given column family to the
     * wide-column entity defined by the provided columns. If the key already exists in the column
     * family, it will be overwritten.
     *
     * @param columnFamilyHandle The {@link org.rocksdb.ColumnFamilyHandle} instance representing
     *     the column family.
     * @param key The key for which the database entry is to be set.
     * @param columns A {@link java.util.List} of {@link WideColumn} objects representing the
     *     wide-column entity to be set for the key.
     *                Each {@link WideColumn} represents a column containing byte array values.
     *                Upon execution, the wide-column entity defined by these columns will be
     * associated with the key in the database.
     * @throws RocksDBException Thrown if an error occurs while setting the database entry in the
     *     underlying native library.
     */
    void putEntity(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
        final List<WideColumn<byte[]>> columns) throws RocksDBException;

    /**
     * Sets the database entry for the specified key within the given column family to the
     * wide-column entity defined by the provided columns. If the key already exists in the column
     * family, it will be overwritten.
     *
     * @param columnFamilyHandle The {@link org.rocksdb.ColumnFamilyHandle} instance representing
     *     the column family.
     * @param key The key for which the database entry is to be set.
     * @param keyOffset The offset within the {@code key} array indicating the start of the key to
     *     be used. Must be non-negative and no larger than the length of the {@code key} array.
     * @param keyLength The length of the key to be used, starting from the {@code keyOffset}.
     *                  Must be non-negative and no larger than the remaining length of the {@code
     * key} array after {@code keyOffset}.
     * @param columns A {@link java.util.List} of {@link WideColumn} objects representing the
     *     wide-column entity to be set for the key.
     *                Each {@link WideColumn} represents a column containing byte array values.
     *                Upon execution, the wide-column entity defined by these columns will be
     * associated with the key in the database.
     * @throws RocksDBException Thrown if an error occurs while setting the database entry in the
     *     underlying native library.
     */
    void putEntity(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
        final int keyOffset, final int keyLength, final List<WideColumn<byte[]>> columns)
        throws RocksDBException;

    /**
     * Sets the database entry for the specified key within the given column family to the
     * wide-column entity defined by the provided columns. If the key already exists in the column
     * family, it will be overwritten.
     *
     * <p><strong>Note:</strong> All {@link java.nio.ByteBuffer} instances within the provided
     * {@code columns} list must be direct.</p>
     *
     * @param columnFamilyHandle The {@link org.rocksdb.ColumnFamilyHandle} instance representing
     *     the column family.
     * @param key The key for which the database entry is to be set.
     * @param columns A {@link java.util.List} of {@link WideColumn} objects representing the
     *     wide-column entity to be set for the key.
     *                Each {@link WideColumn} represents a column containing byte buffer values.
     *                Upon execution, the wide-column entity defined by these columns will be
     * associated with the key in the database.
     * @throws RocksDBException Thrown if an error occurs while setting the database entry in the
     *     underlying native library.
     */
    void putEntity(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
        final List<WideColumn<ByteBuffer>> columns) throws RocksDBException;

    /**
     * <p>Merge "value" with the existing value of "key" in the database.
     * "key-&gt;merge(existing, value)"</p>
     *
     * @param key the specified key to be merged.
     * @param value the value to be merged with the current value for
     * the specified key.
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void merge(byte[] key, byte[] value) throws RocksDBException;

    /**
     * <p>Merge "value" with the existing value of "key" in given column family.
     * "key-&gt;merge(existing, value)"</p>
     *
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param key the specified key to be merged.
     * @param value the value to be merged with the current value for
     * the specified key.
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
        throws RocksDBException;

    /**
     * <p>If the database contains a mapping for "key", erase it.  Else do nothing.</p>
     *
     * @param key Key to delete within database
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void delete(byte[] key) throws RocksDBException;

    /**
     * <p>If column family contains a mapping for "key", erase it.  Else do nothing.</p>
     *
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param key Key to delete within database
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException;

    /**
     * <p>If column family contains a mapping for "key", erase it.  Else do nothing.</p>
     *
     * @param key Key to delete within database. It is using position and limit.
     *     Supports direct buffer only.
     *
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void delete(final ByteBuffer key) throws RocksDBException;

    /**
     * <p>If column family contains a mapping for "key", erase it.  Else do nothing.</p>
     *
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param key Key to delete within database. It is using position and limit.
     *     Supports direct buffer only.
     *
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void delete(ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key)
        throws RocksDBException;

    /**
     * Remove the database entry for {@code key}. Requires that the key exists
     * and was not overwritten. It is not an error if the key did not exist
     * in the database.
     * <p>
     * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
     * times), then the result of calling SingleDelete() on this key is undefined.
     * SingleDelete() only behaves correctly if there has been only one Put()
     * for this key since the previous call to SingleDelete() for this key.
     * <p>
     * This feature is currently an experimental performance optimization
     * for a very specific workload. It is up to the caller to ensure that
     * SingleDelete is only used for a key that is not deleted using Delete() or
     * written using Merge(). Mixing SingleDelete operations with Deletes and
     * Merges can result in undefined behavior.
     *
     * @param key Key to delete within database
     *
     * @throws RocksDBException thrown if error happens in underlying
     *     native library.
     */
    @Experimental("Performance optimization for a very specific workload")
    void singleDelete(final byte[] key) throws RocksDBException;

    /**
     * Remove the database entry for {@code key}. Requires that the key exists
     * and was not overwritten. It is not an error if the key did not exist
     * in the database.
     * <p>
     * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
     * times), then the result of calling SingleDelete() on this key is undefined.
     * SingleDelete() only behaves correctly if there has been only one Put()
     * for this key since the previous call to SingleDelete() for this key.
     * <p>
     * This feature is currently an experimental performance optimization
     * for a very specific workload. It is up to the caller to ensure that
     * SingleDelete is only used for a key that is not deleted using Delete() or
     * written using Merge(). Mixing SingleDelete operations with Deletes and
     * Merges can result in undefined behavior.
     *
     * @param columnFamilyHandle The column family to delete the key from
     * @param key Key to delete within database
     *
     * @throws RocksDBException thrown if error happens in underlying
     *     native library.
     */
    @Experimental("Performance optimization for a very specific workload")
    void singleDelete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
        throws RocksDBException;

    /**
     * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
     * including "beginKey" and excluding "endKey". a non-OK status on error. It
     * is not an error if no keys exist in the range ["beginKey", "endKey").
     * <p>
     * Delete the database entry (if any) for "key". Returns OK on success, and a
     * non-OK status on error. It is not an error if "key" did not exist in the
     * database.
     *
     * @param beginKey
     *          First key to delete within database (included)
     * @param endKey
     *          Last key to delete within database (excluded)
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void deleteRange(byte[] beginKey, byte[] endKey) throws RocksDBException;

    /**
     * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
     * including "beginKey" and excluding "endKey". a non-OK status on error. It
     * is not an error if no keys exist in the range ["beginKey", "endKey").
     * <p>
     * Delete the database entry (if any) for "key". Returns OK on success, and a
     * non-OK status on error. It is not an error if "key" did not exist in the
     * database.
     *
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param beginKey
     *          First key to delete within database (included)
     * @param endKey
     *          Last key to delete within database (excluded)
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey)
        throws RocksDBException;

    /**
     * Append a blob of arbitrary size to the records in this batch. The blob will
     * be stored in the transaction log but not in any other file. In particular,
     * it will not be persisted to the SST files. When iterating over this
     * WriteBatch, WriteBatch::Handler::LogData will be called with the contents
     * of the blob as it is encountered. Blobs, puts, deletes, and merges will be
     * encountered in the same order in which they were inserted. The blob will
     * NOT consume sequence number(s) and will NOT increase the count of the batch
     * <p>
     * Example application: add timestamps to the transaction log for use in
     * replication.
     *
     * @param blob binary object to be inserted
     * @throws RocksDBException thrown if error happens in underlying native library.
     */
    void putLogData(byte[] blob) throws RocksDBException;

    /**
     * Clear all updates buffered in this batch
     */
    void clear();

    /**
     * Records the state of the batch for future calls to RollbackToSavePoint().
     * May be called multiple times to set multiple save points.
     */
    void setSavePoint();

    /**
     * Remove all entries in this batch (Put, Merge, Delete, PutLogData) since
     * the most recent call to SetSavePoint() and removes the most recent save
     * point.
     *
     * @throws RocksDBException if there is no previous call to SetSavePoint()
     */
    void rollbackToSavePoint() throws RocksDBException;

    /**
     * Pop the most recent save point.
     * <p>
     * That is to say that it removes the last save point,
     * which was set by {@link #setSavePoint()}.
     *
     * @throws RocksDBException If there is no previous call to
     *     {@link #setSavePoint()}, an exception with
     *     {@link Status.Code#NotFound} will be thrown.
     */
    void popSavePoint() throws RocksDBException;

    /**
     * Set the maximum size of the write batch.
     *
     * @param maxBytes the maximum size in bytes.
     */
    void setMaxBytes(long maxBytes);

    /**
     * Get the underlying Write Batch.
     *
     * @return the underlying WriteBatch.
     */
    WriteBatch getWriteBatch();
}
