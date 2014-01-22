// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_ROCKSDB_INCLUDE_DB_H_
#define STORAGE_ROCKSDB_INCLUDE_DB_H_

#include <stdint.h>
#include <stdio.h>
#include <memory>
#include <vector>
#include <string>
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/transaction_log.h"

namespace rocksdb {

using std::unique_ptr;

struct ColumnFamilyHandle;
extern const ColumnFamilyHandle default_column_family;

struct ColumnFamilyDescriptor {
  std::string name;
  ColumnFamilyOptions options;
  ColumnFamilyDescriptor()
      : name(default_column_family_name), options(ColumnFamilyOptions()) {}
  ColumnFamilyDescriptor(const std::string& name,
                         const ColumnFamilyOptions& options)
      : name(name), options(options) {}
};

// Update Makefile if you change these
static const int kMajorVersion = 2;
static const int kMinorVersion = 0;

struct Options;
struct ReadOptions;
struct WriteOptions;
struct FlushOptions;
class WriteBatch;

// Metadata associated with each SST file.
struct LiveFileMetaData {
  std::string name;        // Name of the file
  int level;               // Level at which this file resides.
  size_t size;             // File size in bytes.
  std::string smallestkey; // Smallest user defined key in the file.
  std::string largestkey;  // Largest user defined key in the file.
  SequenceNumber smallest_seqno; // smallest seqno in file
  SequenceNumber largest_seqno;  // largest seqno in file
};

// Abstract handle to particular state of a DB.
// A Snapshot is an immutable object and can therefore be safely
// accessed from multiple threads without any external synchronization.
class Snapshot {
 protected:
  virtual ~Snapshot();
};

// A range of keys
struct Range {
  Slice start;          // Included in the range
  Slice limit;          // Not included in the range

  Range() { }
  Range(const Slice& s, const Slice& l) : start(s), limit(l) { }
};

// A DB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
class DB {
 public:
  // Open the database with the specified "name".
  // Stores a pointer to a heap-allocated database in *dbptr and returns
  // OK on success.
  // Stores nullptr in *dbptr and returns a non-OK status on error.
  // Caller should delete *dbptr when it is no longer needed.
  static Status Open(const Options& options,
                     const std::string& name,
                     DB** dbptr);

  // Open the database for read only. All DB interfaces
  // that modify data, like put/delete, will return error.
  // If the db is opened in read only mode, then no compactions
  // will happen.
  static Status OpenForReadOnly(const Options& options,
      const std::string& name, DB** dbptr,
      bool error_if_log_file_exist = false);

  // Open DB with column families.
  // db_options specify database specific options
  // column_families is the vector of all column families you'd like to open,
  // containing column family name and options. The default column family name
  // is 'default'.
  // If everything is OK, handles will on return be the same size
  // as column_families --- handles[i] will be a handle that you
  // will use to operate on column family column_family[i]
  static Status OpenWithColumnFamilies(
      const DBOptions& db_options, const std::string& name,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle>* handles, DB** dbptr);

  // ListColumnFamilies will open the DB specified by argument name
  // and return the list of all column families in that DB
  // through column_families argument. The ordering of
  // column families in column_families is unspecified.
  static Status ListColumnFamilies(const DBOptions& db_options,
                                   const std::string& name,
                                   std::vector<std::string>* column_families);

  DB() { }
  virtual ~DB();

  // Create a column_family and return the handle of column family
  // through the argument handle.
  virtual Status CreateColumnFamily(const ColumnFamilyOptions& options,
                                    const std::string& column_family_name,
                                    ColumnFamilyHandle* handle);

  // Drop a column family specified by column_family handle.
  // All data related to the column family will be deleted before
  // the function returns.
  // Calls referring to the dropped column family will fail.
  virtual Status DropColumnFamily(const ColumnFamilyHandle& column_family);

  // Set the database entry for "key" to "value".
  // Returns OK on success, and a non-OK status on error.
  // Note: consider setting options.sync = true.
  virtual Status Put(const WriteOptions& options,
                     const ColumnFamilyHandle& column_family, const Slice& key,
                     const Slice& value) = 0;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) {
    return Put(options, default_column_family, key, value);
  }

  // Remove the database entry (if any) for "key".  Returns OK on
  // success, and a non-OK status on error.  It is not an error if "key"
  // did not exist in the database.
  // Note: consider setting options.sync = true.
  virtual Status Delete(const WriteOptions& options,
                        const ColumnFamilyHandle& column_family,
                        const Slice& key) = 0;
  Status Delete(const WriteOptions& options, const Slice& key) {
    return Delete(options, default_column_family, key);
  }

  // Merge the database entry for "key" with "value".  Returns OK on success,
  // and a non-OK status on error. The semantics of this operation is
  // determined by the user provided merge_operator when opening DB.
  // Note: consider setting options.sync = true.
  virtual Status Merge(const WriteOptions& options,
                       const ColumnFamilyHandle& column_family,
                       const Slice& key, const Slice& value) = 0;
  Status Merge(const WriteOptions& options, const Slice& key,
               const Slice& value) {
    return Merge(options, default_column_family, key, value);
  }

  // Apply the specified updates to the database.
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true.
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

  // If the database contains an entry for "key" store the
  // corresponding value in *value and return OK.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() returns true.
  //
  // May return some other Status on an error.
  virtual Status Get(const ReadOptions& options,
                     const ColumnFamilyHandle& column_family, const Slice& key,
                     std::string* value) = 0;
  Status Get(const ReadOptions& options, const Slice& key, std::string* value) {
    return Get(options, default_column_family, key, value);
  }

  // If keys[i] does not exist in the database, then the i'th returned
  // status will be one for which Status::IsNotFound() is true, and
  // (*values)[i] will be set to some arbitrary value (often ""). Otherwise,
  // the i'th returned status will have Status::ok() true, and (*values)[i]
  // will store the value associated with keys[i].
  //
  // (*values) will always be resized to be the same size as (keys).
  // Similarly, the number of returned statuses will be the number of keys.
  // Note: keys will not be "de-duplicated". Duplicate keys will return
  // duplicate values in order.
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values) = 0;
  std::vector<Status> MultiGet(const ReadOptions& options,
                               const std::vector<Slice>& keys,
                               std::vector<std::string>* values) {
    return MultiGet(options, std::vector<ColumnFamilyHandle>(
                                 keys.size(), default_column_family),
                    keys, values);
  }

  // If the key definitely does not exist in the database, then this method
  // returns false, else true. If the caller wants to obtain value when the key
  // is found in memory, a bool for 'value_found' must be passed. 'value_found'
  // will be true on return if value has been set properly.
  // This check is potentially lighter-weight than invoking DB::Get(). One way
  // to make this lighter weight is to avoid doing any IOs.
  // Default implementation here returns true and sets 'value_found' to false
  virtual bool KeyMayExist(const ReadOptions& options,
                           const ColumnFamilyHandle& column_family,
                           const Slice& key, std::string* value,
                           bool* value_found = nullptr) {
    if (value_found != nullptr) {
      *value_found = false;
    }
    return true;
  }
  bool KeyMayExist(const ReadOptions& options, const Slice& key,
                   std::string* value, bool* value_found = nullptr) {
    return KeyMayExist(options, default_column_family, key, value, value_found);
  }

  // Return a heap-allocated iterator over the contents of the database.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // Caller should delete the iterator when it is no longer needed.
  // The returned iterator should be deleted before this db is deleted.
  virtual Iterator* NewIterator(const ReadOptions& options,
                                const ColumnFamilyHandle& column_family) = 0;
  Iterator* NewIterator(const ReadOptions& options) {
    return NewIterator(options, default_column_family);
  }
  // Returns iterators from a consistent database state across multiple
  // column families. Iterators are heap allocated and need to be deleted
  // before the db is deleted
  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle>& column_family,
      std::vector<Iterator*>* iterators) = 0;

  // Return a handle to the current DB state.  Iterators created with
  // this handle will all observe a stable snapshot of the current DB
  // state.  The caller must call ReleaseSnapshot(result) when the
  // snapshot is no longer needed.
  virtual const Snapshot* GetSnapshot() = 0;

  // Release a previously acquired snapshot.  The caller must not
  // use "snapshot" after this call.
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

  // DB implementations can export properties about their state
  // via this method.  If "property" is a valid property understood by this
  // DB implementation, fills "*value" with its current value and returns
  // true.  Otherwise returns false.
  //
  //
  // Valid property names include:
  //
  //  "rocksdb.num-files-at-level<N>" - return the number of files at level <N>,
  //     where <N> is an ASCII representation of a level number (e.g. "0").
  //  "rocksdb.stats" - returns a multi-line string that describes statistics
  //     about the internal operation of the DB.
  //  "rocksdb.sstables" - returns a multi-line string that describes all
  //     of the sstables that make up the db contents.
  virtual bool GetProperty(const ColumnFamilyHandle& column_family,
                           const Slice& property, std::string* value) = 0;
  bool GetProperty(const Slice& property, std::string* value) {
    return GetProperty(default_column_family, property, value);
  }

  // For each i in [0,n-1], store in "sizes[i]", the approximate
  // file system space used by keys in "[range[i].start .. range[i].limit)".
  //
  // Note that the returned sizes measure file system space usage, so
  // if the user data compresses by a factor of ten, the returned
  // sizes will be one-tenth the size of the corresponding user data size.
  //
  // The results may not include the sizes of recently written data.
  virtual void GetApproximateSizes(const ColumnFamilyHandle& column_family,
                                   const Range* range, int n,
                                   uint64_t* sizes) = 0;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
    GetApproximateSizes(default_column_family, range, n, sizes);
  }

  // Compact the underlying storage for the key range [*begin,*end].
  // The actual compaction interval might be superset of [*begin, *end].
  // In particular, deleted and overwritten versions are discarded,
  // and the data is rearranged to reduce the cost of operations
  // needed to access the data.  This operation should typically only
  // be invoked by users who understand the underlying implementation.
  //
  // begin==nullptr is treated as a key before all keys in the database.
  // end==nullptr is treated as a key after all keys in the database.
  // Therefore the following call will compact the entire database:
  //    db->CompactRange(nullptr, nullptr);
  // Note that after the entire database is compacted, all data are pushed
  // down to the last level containing any data. If the total data size
  // after compaction is reduced, that level might not be appropriate for
  // hosting all the files. In this case, client could set reduce_level
  // to true, to move the files back to the minimum level capable of holding
  // the data set or a given level (specified by non-negative target_level).
  virtual void CompactRange(const ColumnFamilyHandle& column_family,
                            const Slice* begin, const Slice* end,
                            bool reduce_level = false,
                            int target_level = -1) = 0;
  void CompactRange(const Slice* begin, const Slice* end,
                    bool reduce_level = false, int target_level = -1) {
    CompactRange(default_column_family, begin, end, reduce_level, target_level);
  }

  // Number of levels used for this DB.
  virtual int NumberLevels(const ColumnFamilyHandle& column_family) = 0;
  int NumberLevels() {
    return NumberLevels(default_column_family);
  }

  // Maximum level to which a new compacted memtable is pushed if it
  // does not create overlap.
  virtual int MaxMemCompactionLevel(
      const ColumnFamilyHandle& column_family) = 0;
  int MaxMemCompactionLevel() {
    return MaxMemCompactionLevel(default_column_family);
  }

  // Number of files in level-0 that would stop writes.
  virtual int Level0StopWriteTrigger(
      const ColumnFamilyHandle& column_family) = 0;
  int Level0StopWriteTrigger() {
    return Level0StopWriteTrigger(default_column_family);
  }

  // Get DB name -- the exact same name that was provided as an argument to
  // DB::Open()
  virtual const std::string& GetName() const = 0;

  // Get Env object from the DB
  virtual Env* GetEnv() const = 0;

  // Get DB Options that we use
  virtual const Options& GetOptions(const ColumnFamilyHandle& column_family)
      const = 0;
  const Options& GetOptions() const {
    return GetOptions(default_column_family);
  }

  // Flush all mem-table data.
  virtual Status Flush(const FlushOptions& options,
                       const ColumnFamilyHandle& column_family) = 0;
  Status Flush(const FlushOptions& options) {
    return Flush(options, default_column_family);
  }

  // Prevent file deletions. Compactions will continue to occur,
  // but no obsolete files will be deleted. Calling this multiple
  // times have the same effect as calling it once.
  virtual Status DisableFileDeletions() = 0;

  // Allow compactions to delete obselete files.
  // If force == true, the call to EnableFileDeletions() will guarantee that
  // file deletions are enabled after the call, even if DisableFileDeletions()
  // was called multiple times before.
  // If force == false, EnableFileDeletions will only enable file deletion
  // after it's been called at least as many times as DisableFileDeletions(),
  // enabling the two methods to be called by two threads concurrently without
  // synchronization -- i.e., file deletions will be enabled only after both
  // threads call EnableFileDeletions()
  virtual Status EnableFileDeletions(bool force = true) = 0;

  // GetLiveFiles followed by GetSortedWalFiles can generate a lossless backup

  // THIS METHOD IS DEPRECATED. Use the GetTableMetaData to get more
  // detailed information on the live files.
  // Retrieve the list of all files in the database. The files are
  // relative to the dbname and are not absolute paths. The valid size of the
  // manifest file is returned in manifest_file_size. The manifest file is an
  // ever growing file, but only the portion specified by manifest_file_size is
  // valid for this snapshot.
  // Setting flush_memtable to true does Flush before recording the live files.
  // Setting flush_memtable to false is useful when we don't want to wait for
  // flush which may have to wait for compaction to complete taking an
  // indeterminate time. But this will have to use GetSortedWalFiles after
  // GetLiveFiles to compensate for memtables missed in this snapshot due to the
  // absence of Flush, by WAL files to recover the database consistently later
  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) = 0;

  // Retrieve the sorted list of all wal files with earliest file first
  virtual Status GetSortedWalFiles(VectorLogPtr& files) = 0;

  // The sequence number of the most recent transaction.
  virtual SequenceNumber GetLatestSequenceNumber() const = 0;

  // Sets iter to an iterator that is positioned at a write-batch containing
  // seq_number. If the sequence number is non existent, it returns an iterator
  // at the first available seq_no after the requested seq_no
  // Returns Status::OK if iterator is valid
  // Must set WAL_ttl_seconds or WAL_size_limit_MB to large values to
  // use this api, else the WAL files will get
  // cleared aggressively and the iterator might keep getting invalid before
  // an update is read.
  virtual Status GetUpdatesSince(SequenceNumber seq_number,
                                 unique_ptr<TransactionLogIterator>* iter) = 0;

  // Delete the file name from the db directory and update the internal state to
  // reflect that. Supports deletion of sst and log files only. 'name' must be
  // path relative to the db directory. eg. 000001.sst, /archive/000003.log
  virtual Status DeleteFile(std::string name) = 0;

  // Returns a list of all table files with their level, start key
  // and end key
  virtual void GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {}

  // Sets the globally unique ID created at database creation time by invoking
  // Env::GenerateUniqueId(), in identity. Returns Status::OK if identity could
  // be set properly
  virtual Status GetDbIdentity(std::string& identity) = 0;

 private:
  // No copying allowed
  DB(const DB&);
  void operator=(const DB&);
};

// Destroy the contents of the specified database.
// Be very careful using this method.
Status DestroyDB(const std::string& name, const Options& options);

// If a DB cannot be opened, you may attempt to call this method to
// resurrect as much of the contents of the database as possible.
// Some data may be lost, so be careful when calling this function
// on a database that contains important information.
Status RepairDB(const std::string& dbname, const Options& options);

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_DB_H_
