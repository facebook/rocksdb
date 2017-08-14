// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

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
     */
    void put(byte[] key, byte[] value);

    /**
     * <p>Store the mapping "key-&gt;value" within given column
     * family.</p>
     *
     * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
     *     instance
     * @param key the specified key to be inserted.
     * @param value the value associated with the specified key.
     */
    void put(ColumnFamilyHandle columnFamilyHandle,
                    byte[] key, byte[] value);

    /**
     * <p>Merge "value" with the existing value of "key" in the database.
     * "key-&gt;merge(existing, value)"</p>
     *
     * @param key the specified key to be merged.
     * @param value the value to be merged with the current value for
     * the specified key.
     */
    void merge(byte[] key, byte[] value);

    /**
     * <p>Merge "value" with the existing value of "key" in given column family.
     * "key-&gt;merge(existing, value)"</p>
     *
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param key the specified key to be merged.
     * @param value the value to be merged with the current value for
     * the specified key.
     */
    void merge(ColumnFamilyHandle columnFamilyHandle,
                      byte[] key, byte[] value);

    /**
     * <p>If the database contains a mapping for "key", erase it.  Else do nothing.</p>
     *
     * @param key Key to delete within database
     */
    void remove(byte[] key);

    /**
     * <p>If column family contains a mapping for "key", erase it.  Else do nothing.</p>
     *
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param key Key to delete within database
     */
    void remove(ColumnFamilyHandle columnFamilyHandle, byte[] key);

    /**
     * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
     * including "beginKey" and excluding "endKey". a non-OK status on error. It
     * is not an error if no keys exist in the range ["beginKey", "endKey").
     *
     * Delete the database entry (if any) for "key". Returns OK on success, and a
     * non-OK status on error. It is not an error if "key" did not exist in the
     * database.
     *
     * @param beginKey
     *          First key to delete within database (included)
     * @param endKey
     *          Last key to delete within database (excluded)
     */
    void deleteRange(byte[] beginKey, byte[] endKey);

    /**
     * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
     * including "beginKey" and excluding "endKey". a non-OK status on error. It
     * is not an error if no keys exist in the range ["beginKey", "endKey").
     *
     * Delete the database entry (if any) for "key". Returns OK on success, and a
     * non-OK status on error. It is not an error if "key" did not exist in the
     * database.
     *
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param beginKey
     *          First key to delete within database (included)
     * @param endKey
     *          Last key to delete within database (excluded)
     */
    void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey);

    /**
     * Append a blob of arbitrary size to the records in this batch. The blob will
     * be stored in the transaction log but not in any other file. In particular,
     * it will not be persisted to the SST files. When iterating over this
     * WriteBatch, WriteBatch::Handler::LogData will be called with the contents
     * of the blob as it is encountered. Blobs, puts, deletes, and merges will be
     * encountered in the same order in thich they were inserted. The blob will
     * NOT consume sequence number(s) and will NOT increase the count of the batch
     *
     * Example application: add timestamps to the transaction log for use in
     * replication.
     *
     * @param blob binary object to be inserted
     */
    void putLogData(byte[] blob);

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
}
