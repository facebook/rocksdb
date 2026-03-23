Applications can assign timestamps of custom format to the keys.

Whether user-defined timestamp is enabled or not is a column family setting.

In the current implementation, user-defined timestamp is stored between user key and sequence number, as follows.
```
|user key|timestamp|seqno|type|
|<-------internal key-------->|
```

# APIs
To enable user-defined timestamp feature, applications need to set `ColumnFamilyOptions::comparator` to a timestamp-aware `Comparator` object.
At present, `DB::Get()`, `DB::NewIterator()`, `DB::Put()`, `DB::Delete()`, `DB::SingleDelete()` and `DB::Write()` are supported as part of the `DB` interface whose details can be found in db.h. In addition, we also support user-defined timestamps as part of the `Transaction` interface (transaction.h) for **write-committed** transactions.

At `DB` layer, a user-defined timestamp is a byte array. In contrast, we have made the choice of using `uint64_t` to represent a timestamp used by `Transaction` because it meets the requirements of major use cases and `Transaction` relies on `TransactionDB` which is a layer above `DB`.

### Timestamp-sequence ordering constraint
For any two internal keys of the same user key, (key, ts1, seq1) and (key, ts2, seq2)
- If seq1 < seq2, then ts1 <= ts2
- If ts1 < ts2, then seq1 < seq2

## Timestamp-aware comparator
The `Comparator` class has a few virtual methods related to user-defined timestamps. By implementing these virtual methods, applications can make a subclass of `Comparator` capable of comparing timestamps. For example, the following code snippet shows a comparator capable of comparing timestamps as unsigned 64-bit integers.
```
class ComparatorWithU64TsImpl : public Comparator {
 public:
  // Compare lhs and rhs, taking timestamp, if exists, into consideration.
  int Compare(const Slice& lhs, const Slice& rhs) const override {...}
  // Compare two timestamps ts1 and ts2.
  int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {...}
  // Compare a and b after stripping timestamp from them.
  int CompareWithoutTimestamp(const Slice& a, const Slice& b) const override {...}
  int CompareWithoutTimestamp(const Slice& a, bool a_has_ts, const Slice& b, bool b_has_ts) {...}
};
```

## DB open and column family creation
Currently, whether a column family enables user-defined timestamp is a configuration which is immutable once the column family is created. For the default column family, whether the feature is enabled is determined when the database is first created.
```
Options options;
options.create_if_missing = true;
options.comparator = ComparatorWithU64Ts();
// Create a new db
DB::Open(options, "/path/to/db/dir/", &db);
// Create a new column family
db->CreateColumnFamily(options, "new_cf", &column_family);
```

Some applications may want to delete the most recent data after one RocksDB instance is recovered because these data may not be reliably replicated. To do so, applications can call `DB::OpenAndTrimHistory()`.

## DB::Get()
`DB::Get()` with a timestamp `ts` specified in `ReadOptions` will return the most recent key/value whose timestamp is smaller than or equal to `ts`, in addition to the visibility based on sequence numbers. Note that if the database enables timestamp, then caller of `DB::Get()` 
should always set `ReadOptions.timestamp`.
```
ReadOptions read_opts;
read_opts.timestamp = &ts;
db->Get(read_opts, key, &value);
```
## Iterator on DB
`DB::NewIterator()` with a timestamp `ts` specified in `ReadOptions` will return an iterator. Accessing keys via the iterator will return data whose timestamp is smaller than or equal to `ts`, in addition to the visibility based on sequence numbers. The application should
always call `DB::NewIterator()` with `read_options.timestamp` set.
```
ReadOptions read_opts;
read_opts.timestamp = &ts;
Iterator* it = db->NewIterator(read_opts);
it->SeekToFirst();
it->Next();
it->Prev();
it->SeekToLast();
it->Seek("foo");
it->SeekForPrev("foo");
```
## DB::Put()
When using `DB::Put` with user timestamp, the user needs to pass an additional argument `ts`.
```
db->Put(WriteOptions(), key, ts, value);
```
## DB::Delete()
When using `DB::Delete` with user timestamp, the user needs to pass an additional argument `ts`.
```
db->Delete(WriteOptions(), key, ts);
```
## DB::SingleDelete()
When using `DB::SingleDelete` with user timestamp, the user needs to pass an additional argument `ts`.
```
db->SingleDelete(WriteOptions(), key, ts);
```
## DB::DeleteRange()
When using `DB::DeleteRange` with user timestamp, the user needs to pass an additional argument `ts`.
```
db->DeleteRange(WriteOptions, begin_key, end_key, ts);
```
## DB::Merge()
When using `DB::Merge` with user timestamp, the user needs to pass an additional argument `ts`.
```
db->Merge(WriteOptions(), key, ts, value);
```
## WriteBatch and DB::Write()
When using `DB::Write` API, the user needs to create a `WriteBatch` specifying the size (in bytes) of the timestamps of the default column family.
If timestamp is known when writing the key to the write batch, then user can call `WriteBatch::Put(key, ts, value)`, `WriteBatch::Delete(key, ts)`, `WriteBatch::SingleDelete(key, ts)`.
If timestamp is not known when writing the key to the write batch, then user can call `WriteBatch::Put(key, value)`, `WriteBatch::Delete(key)`, `WriteBatch::SingleDelete(key)`. Later, when the timestamp is known, the user can call `WriteBatch::UpdateTimestamps(ts)` to update the timestamps for all the keys in the write batch.
Once timestamps are written, user can call `DB::Write(batch)` to write to the database.

Remember that applications can enable user-defined timestamps on a per-column-family basis. Therefore, it is possible for a write batch to have some keys from column families enabling user-defined timestamps and other keys from column families without user-defined timestamps. RocksDB internally handles this case. `WriteBatch::UpdateTimestamps(ts, get_ts_sz)` takes a second argument `get_ts_sz` as a callable object which returns the timestamp size given a column family id. `WriteBatch::UpdateTimestamps()` uses `get_ts_sz` to skip keys from column families that disable user-defined timestamp.
```
// Two write batches. The default column family enables timestamp
// and the size of a timestamp is 8 bytes.
WriteBatch wb1(0, 0, 0, /*ts_sz=*/8), wb2(0, 0, 0, /*ts_sz=*/8);
// Timestamp is already known when writing keys to the batch.
wb1.Put(key1, ts1, value1);
db->Write(WriteOptions(), &wb1);

// Timestamp is not known when writing keys to the batch.
wb2.Put(key2, value2);
...
wb.UpdateTimestamps(ts2);
db->Write(WriteOptions(), &wb2);
```
Notes on `WriteBatchWithIndex`: Because user-defined timestamp is not needed for the index that supports the read you own write capabilities, `WriteBatchWithIndex` APIs do not directly support user-defined timestamps like `WriteBatch` do.
Users can instead follow below example code snippet to use `WriteBatchWithIndex` for its read your own write capabilities, as well as using the user-defined timestamp APIs from the `WriteBatch` it contains:
```
WriteBatchWithIndex batch;
batch.Put(key1, value1);
WriteBatch* wb = batch->GetWriteBatch();
wb->UpdateTimestamps(ts);
```

## Transaction::Get()
Similar to `DB::Get()`. `Transaction::Get()` reads from both the transaction's write batch and the database. Data in the transaction's write batch do not have timestamps associated with them because timestamps are assigned only at commit time. Actually, the data in the transaction's write batch is written by this transaction itself, thus is newer than any data in the database.
`Transaction::Get(read_opts)` is non-locking read in the terminology of SQL databases. The argument `read_opts` can take any timestamp for read, but the result of `Transaction::Get()` can be stale and should not be used for subsequent writes in the same transaction because `Transaction::Get()` bypasses validation.
```
std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
ReadOptions read_opts;
read_opts.timestamp = &ts;
txn->Get(read_opts, key, &value);
```

## Transaction::SetReadTimestampForValidation()
Each transaction can have a read timestamp. The transaction will use this timestamp to read data from the database. Any data with timestamp after this read timestamp should be considered invisible to this transaction. This very read timestamp is also used for validation.
```
txn->SetReadTimestampForValidation(/*ts=*/1000);
```

## Transaction::GetForUpdate()
Applications use `GetForUpdate(key)` to read a key and express the intention of writing to the key later. In contrast to `Transaction::Get(read_opts)`, `Transaction::GetForUpdate()` is locking read and should always see most up-to-dated data. Therefore, `GetForUpdate()` also performs validation (after locking the key) to make sure that this calling transaction can proceed only if no other transaction has committed a version of the key tagged with a timestamp equal to or newer than the calling transaction's `read_timestamp_`.

`Transaction::GetForUpdate()` does not get the read timestamp from `ReadOptions` argument. Instead, it always uses the transaction's `read_timestamp` (set by `Transaction::SetReadTimestampForValidation()`) for read.
```
std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
txn->SetReadTimestampForValidation();
txn->GetForUpdate(ReadOptions(), key, &value);
```

## Transaction::GetIterator()
Similar to `DB::NewIterator()`, `Transaction::GetIterator(read_opts)` also creates an iterator to iterate over data both in the transaction's write batch and the database. The data in the transaction's write batch is considered newer than any other data in the database.
The iterator returned by `Transaction::GetIterator()` performs non-locking read, and reads from the database using the timestamp specified via `read_opts.timestamp`. Data returned by `Transaction::GetIterator()` should not be used to determine any subsequent write in the same transaction.
```
std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
std::unique_ptr<Iterator> it(txn->GetIterator());
it->Seek("key");
it->Next();
```

## Transaction::Put()
`Transaction::Put(key, value)` writes {'key'=>'value'} to the transaction's write batch. The data in the write batch won't have timestamp until `Transaction::SetCommitTimestamp()` is called before committing the transaction.
If `read_timestamp` is set, then `Transaction::Put()` also performs validation (after locking 'key') to make sure that no other transaction has committed a newer version of 'key' since `read_timestamp`.
```
std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
txn->Put("key", "value");
```

## Transaction::Delete()
`Transaction::Delete(key)` writes a tombstone of 'key' to the transaction's write batch. The data in the write batch won't have timestamp until `Transaction::SetCommitTimestamp()` is called before committing the transaction.
If `read_timestamp` is set, then `Transaction::Delete()` also performs validation (after locking 'key') to make sure that no other transaction has committed a newer version of 'key' since `read_timestamp`.
```
std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
txn->Delete("key");
```

## Transaction::SingleDelete()
`Transaction::SingleDelete(key)` writes a single-delete tombstone of 'key' to the transaction's write batch. The data in the write batch won't have timestamp until `Transaction::SetCommitTimestamp()` is called before committing the transaction.
If `read_timestamp` is set, then `Transaction::SingleDelete()` also performs validation (after locking 'key') to make sure that no other transaction has committed a newer version of 'key' since `read_timestamp`.
```
std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
txn->SingleDelete("key");
```

## Transaction::SetCommitTimestamp()
If a transaction's write batch includes at least one key for a column family that enables user-defined timestamp, then the transaction must be assigned a commit timestamp in order to commit. `Transaction::SetCommitTimestamp()` should be called before transaction commits. If two-phase commit (2PC) is enabled, then `Transaction::SetCommitTimestamp()` should be called after `Transaction::Prepare()` succeeds.

When transaction commits and writes its write batch to the database, the keys of column families that enable user-defined timestamps will have the specified commit timestamp. The keys of column families that disable user-defined timestamps are unchanged.
```
txn->SetCommitTimestamp(2000);
txn->Commit();
```

## SstFileWriter
We have extended `SstFileWriter` so that we can generate external SST files in which each key can have a user-defined timestamp. This will be useful to support bulk loading with timestamps, which is planned in the future.
```
options.comparator = ComparatorWithU64Ts();
SstFileWriter ssfw(EnvOptions(), options);
ssfw.Open("test.sst");
ssfw.Put("key0", ts, "value0");
ssfw.Put("key1", ts, "value1");
ssfw.Delete("key2", ts2);
ssfw.Finish();
```
# Compaction and garbage collection (GC)
When user-defined timestamp is enabled, compaction normally does not drop a key due to the presence of a newer
version of the same key, i.e. with a higher timestamp, even if there is no snapshot between them.
For example, consider two versions of a key, `foo`:
```
<'foo', ts1, seq1, PUT>
<'foo', ts2, seq2, DELETE>

ts1 < ts2, seq1 < seq2 (timestamp-sequence ordering constraint)
```
In normal case, compaction will preserve both of them, so that we preserve full history of all writes.

Due to space constraint, we sometimes want to 'trim' history older than a certain per-column-family threshold, `full_history_ts_low`.
In the above example, if `ts2 < full_history_ts_low`, then compaction will drop both keys, assuming there is no snapshot between `seq1`
and `seq2`.
`full_history_ts_low` can be set by application and only increase over time. Currently, we repurposed `DB::CompactRange(compact_range_options)` API to set `full_history_ts_low` and trigger a full compaction. In addition, applications can call `DB::IncreaseFullHistoryTsLow()` to increase a column family's `full_history_ts_low` without incurring an immediate full compaction. Future compactions will honor the increased `full_history_ts_low` and trim history when possible.