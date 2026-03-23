# Write Batch With Index

The WBWI (Write Batch With Index) encapsulates a WriteBatch and an Index into that WriteBatch. The index in use is a Skip List. The purpose of the WBWI is to sit above the DB, and offer the same basic operations as the DB, i.e. Writes - `Put`, `Delete`, and `Merge`, and Reads - `Get`, and `newIterator`.

Write operations on the WBWI are serialized into the WriteBatch (of the WBWI) rather than acting directly on the DB. The WriteBatch can later be written atomically to the DB by calling `db.write(wbwi)`.

Read operations can either be solely against the WriteBatch (e.g. `GetFromBatch`), or they can be read-through operations. A read-through operation, (e.g. `GetFromBatchAndDB`), first tries to read from the WriteBatch, if there is no updated entry in the WriteBatch then it subsequently reads from the DB.

The WriteBatch itself is a thin wrapper around a `std::string` field called `rep`. The WriteBatch can be thought of as append-only. Each new write operation simply appends to the WriteBatch `rep`, see: https://github.com/facebook/rocksdb/blob/main/db/write_batch.cc#L10

The purpose of the Index within the WBWI is to track which keys that have been written to the WriteBatch. The index is a mapping from the key to the offset of the serialized operation within the WriteBatch (i.e. the position within the `rep` string).

This index is used for read-operations against the WriteBatchWithIndex to determine whether a write operation on the WriteBatch has previously occurred for the key that is being written. If there is an entry in the Index the offset into the WriteBatch is used to read the value, if not, then the database can be consulted. The Index allows us to avoid having to scan the WriteBatch when performing `Get` or `Seek` (for iterator) operations.

## Transaction Building Block
The WBWI can be used as a component if one wishes to build Transaction Semantics atop RocksDB. The WBWI by itself isolates the Write Path to a local in-memory store and allows you to [RYOW](https://rocksdb.org/blog/2015/02/27/write-batch-with-index.html) (Read-Your-Own-Writes) before data is atomically written to the database.

It is a key component in RocksDB's Pessimistic and Optimistic Transaction utility classes.

## Modes of Operation

The WBWI has two modes of operation, 1) where all write operations to the same key can be retrieved by iteration, and 2) where only the latest write operation for a key can be retrieved by iteration. Regardless of the mode, all write operations themselves are preserved in the append-only WriteBatch, the mode only controls what is retrievable from the WriteBatch when RYOW.

The mode of operation is controlled by use of the WriteBatchWithIndex constructor argument named `overwrite_key`. The default mode of operation is to allow all write operations to be retrieved (i.e. `overwrite_key=false`).

Not all WBWI functions are supported in both modes, see the table below:

| WBWI Function       | overwrite_key=false | overwrite_key=true |
|---------------------|---------------------|--------------------|
| Put                 | Yes                 | Yes                |
| Delete              | Yes                 | Yes                |
| DeleteRange         | No                  | No                 |
| Merge               | Yes                 | Yes                |
| GetFromBatch        | Yes                 | Yes                |
| GetFromBatchAndDB   | Yes                 | Yes                |
| NewIterator         | One-or-more entries per Key in the batch | One entry per Key in the batch |
| NewIteratorWithBase | Yes                 | Yes                |

For `GetFromBatch` and `GetFromBatchAndDB`, if transaction contains Merge, then they return `Status::MergeInProgress` if no base value is found, and return merged result if base value is found.

## Index Entries when `overwrite_key=false`

TODO

## Index Entries when `overwrite_key=true`

TODO
