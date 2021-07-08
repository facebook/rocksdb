//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_garbage.h"
#include "db/dbformat.h"
#include "db/wal_edit.h"
#include "memory/arena.h"
#include "rocksdb/cache.h"
#include "table/table_reader.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed. The number should be forward compatible so
// users can down-grade RocksDB safely. A future Tag is ignored by doing '&'
// between Tag and kTagSafeIgnoreMask field.
enum Tag : uint32_t {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9,
  kMinLogNumberToKeep = 10,

  // these are new formats divergent from open source leveldb
  kNewFile2 = 100,
  kNewFile3 = 102,
  kNewFile4 = 103,      // 4th (the latest) format version of adding files
  kColumnFamily = 200,  // specify column family for version edit
  kColumnFamilyAdd = 201,
  kColumnFamilyDrop = 202,
  kMaxColumnFamily = 203,

  kInAtomicGroup = 300,

  kBlobFileAddition = 400,
  kBlobFileGarbage,

  // Mask for an unidentified tag from the future which can be safely ignored.
  kTagSafeIgnoreMask = 1 << 13,

  // Forward compatible (aka ignorable) records
  kDbId,
  kBlobFileAddition_DEPRECATED,
  kBlobFileGarbage_DEPRECATED,
  kWalAddition,
  kWalDeletion,
  kFullHistoryTsLow,
  kWalAddition2,
  kWalDeletion2,
};

enum NewFileCustomTag : uint32_t {
  kTerminate = 1,  // The end of customized fields
  kNeedCompaction = 2,
  // Since Manifest is not entirely forward-compatible, we currently encode
  // kMinLogNumberToKeep as part of NewFile as a hack. This should be removed
  // when manifest becomes forward-compatible.
  kMinLogNumberToKeepHack = 3,
  kOldestBlobFileNumber = 4,
  kOldestAncesterTime = 5,
  kFileCreationTime = 6,
  kFileChecksum = 7,
  kFileChecksumFuncName = 8,
  kTemperature = 9,

  // If this bit for the custom tag is set, opening DB should fail if
  // we don't know this field.
  kCustomTagNonSafeIgnoreMask = 1 << 6,

  // Forward incompatible (aka unignorable) fields
  kPathId,
};

class VersionSet;

constexpr uint64_t kFileNumberMask = 0x3FFFFFFFFFFFFFFF;
constexpr uint64_t kUnknownOldestAncesterTime = 0;
constexpr uint64_t kUnknownFileCreationTime = 0;

extern uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id);

// A copyable structure contains information needed to read data from an SST
// file. It can contain a pointer to a table reader opened for the file, or
// file number and size, which can be used to create a new table reader for it.
// The behavior is undefined when a copied of the structure is used when the
// file is not in any live version any more.
struct FileDescriptor {
  // Table reader in table_reader_handle
  TableReader* table_reader;
  uint64_t packed_number_and_path_id;
  uint64_t file_size;  // File size in bytes
  SequenceNumber smallest_seqno;  // The smallest seqno in this file
  SequenceNumber largest_seqno;   // The largest seqno in this file

  FileDescriptor() : FileDescriptor(0, 0, 0) {}

  FileDescriptor(uint64_t number, uint32_t path_id, uint64_t _file_size)
      : FileDescriptor(number, path_id, _file_size, kMaxSequenceNumber, 0) {}

  FileDescriptor(uint64_t number, uint32_t path_id, uint64_t _file_size,
                 SequenceNumber _smallest_seqno, SequenceNumber _largest_seqno)
      : table_reader(nullptr),
        packed_number_and_path_id(PackFileNumberAndPathId(number, path_id)),
        file_size(_file_size),
        smallest_seqno(_smallest_seqno),
        largest_seqno(_largest_seqno) {}

  FileDescriptor(const FileDescriptor& fd) { *this = fd; }

  FileDescriptor& operator=(const FileDescriptor& fd) {
    table_reader = fd.table_reader;
    packed_number_and_path_id = fd.packed_number_and_path_id;
    file_size = fd.file_size;
    smallest_seqno = fd.smallest_seqno;
    largest_seqno = fd.largest_seqno;
    return *this;
  }

  uint64_t GetNumber() const {
    return packed_number_and_path_id & kFileNumberMask;
  }
  uint32_t GetPathId() const {
    return static_cast<uint32_t>(
        packed_number_and_path_id / (kFileNumberMask + 1));
  }
  uint64_t GetFileSize() const { return file_size; }
};

struct FileSampledStats {
  FileSampledStats() : num_reads_sampled(0) {}
  FileSampledStats(const FileSampledStats& other) { *this = other; }
  FileSampledStats& operator=(const FileSampledStats& other) {
    num_reads_sampled = other.num_reads_sampled.load();
    return *this;
  }

  // number of user reads to this file.
  mutable std::atomic<uint64_t> num_reads_sampled;
};

struct FileMetaData {
  FileDescriptor fd;
  InternalKey smallest;            // Smallest internal key served by table
  InternalKey largest;             // Largest internal key served by table

  // Needs to be disposed when refs becomes 0.
  Cache::Handle* table_reader_handle = nullptr;

  FileSampledStats stats;

  // Stats for compensating deletion entries during compaction

  // File size compensated by deletion entry.
  // This is updated in Version::UpdateAccumulatedStats() first time when the
  // file is created or loaded.  After it is updated (!= 0), it is immutable.
  uint64_t compensated_file_size = 0;
  // These values can mutate, but they can only be read or written from
  // single-threaded LogAndApply thread
  uint64_t num_entries = 0;     // the number of entries.
  uint64_t num_deletions = 0;   // the number of deletion entries.
  uint64_t raw_key_size = 0;    // total uncompressed key size.
  uint64_t raw_value_size = 0;  // total uncompressed value size.

  int refs = 0;  // Reference count

  bool being_compacted = false;       // Is this file undergoing compaction?
  bool init_stats_from_file = false;  // true if the data-entry stats of this
                                      // file has initialized from file.

  bool marked_for_compaction = false;  // True if client asked us nicely to
                                       // compact this file.
  Temperature temperature = Temperature::kUnknown;

  // Used only in BlobDB. The file number of the oldest blob file this SST file
  // refers to. 0 is an invalid value; BlobDB numbers the files starting from 1.
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;

  // The file could be the compaction output from other SST files, which could
  // in turn be outputs for compact older SST files. We track the memtable
  // flush timestamp for the oldest SST file that eventually contribute data
  // to this file. 0 means the information is not available.
  uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;

  // Unix time when the SST file is created.
  uint64_t file_creation_time = kUnknownFileCreationTime;

  // File checksum
  std::string file_checksum = kUnknownFileChecksum;

  // File checksum function name
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;

  FileMetaData() = default;

  FileMetaData(uint64_t file, uint32_t file_path_id, uint64_t file_size,
               const InternalKey& smallest_key, const InternalKey& largest_key,
               const SequenceNumber& smallest_seq,
               const SequenceNumber& largest_seq, bool marked_for_compact,
               uint64_t oldest_blob_file, uint64_t _oldest_ancester_time,
               uint64_t _file_creation_time, const std::string& _file_checksum,
               const std::string& _file_checksum_func_name)
      : fd(file, file_path_id, file_size, smallest_seq, largest_seq),
        smallest(smallest_key),
        largest(largest_key),
        marked_for_compaction(marked_for_compact),
        oldest_blob_file_number(oldest_blob_file),
        oldest_ancester_time(_oldest_ancester_time),
        file_creation_time(_file_creation_time),
        file_checksum(_file_checksum),
        file_checksum_func_name(_file_checksum_func_name) {
    TEST_SYNC_POINT_CALLBACK("FileMetaData::FileMetaData", this);
  }

  // REQUIRED: Keys must be given to the function in sorted order (it expects
  // the last key to be the largest).
  void UpdateBoundaries(const Slice& key, const Slice& value,
                        SequenceNumber seqno, ValueType value_type);

  // Unlike UpdateBoundaries, ranges do not need to be presented in any
  // particular order.
  void UpdateBoundariesForRange(const InternalKey& start,
                                const InternalKey& end, SequenceNumber seqno,
                                const InternalKeyComparator& icmp) {
    if (smallest.size() == 0 || icmp.Compare(start, smallest) < 0) {
      smallest = start;
    }
    if (largest.size() == 0 || icmp.Compare(largest, end) < 0) {
      largest = end;
    }
    fd.smallest_seqno = std::min(fd.smallest_seqno, seqno);
    fd.largest_seqno = std::max(fd.largest_seqno, seqno);
  }

  // Try to get oldest ancester time from the class itself or table properties
  // if table reader is already pinned.
  // 0 means the information is not available.
  uint64_t TryGetOldestAncesterTime() {
    if (oldest_ancester_time != kUnknownOldestAncesterTime) {
      return oldest_ancester_time;
    } else if (fd.table_reader != nullptr &&
               fd.table_reader->GetTableProperties() != nullptr) {
      return fd.table_reader->GetTableProperties()->creation_time;
    }
    return kUnknownOldestAncesterTime;
  }

  uint64_t TryGetFileCreationTime() {
    if (file_creation_time != kUnknownFileCreationTime) {
      return file_creation_time;
    } else if (fd.table_reader != nullptr &&
               fd.table_reader->GetTableProperties() != nullptr) {
      return fd.table_reader->GetTableProperties()->file_creation_time;
    }
    return kUnknownFileCreationTime;
  }
};

// A compressed copy of file meta data that just contain minimum data needed
// to server read operations, while still keeping the pointer to full metadata
// of the file in case it is needed.
struct FdWithKeyRange {
  FileDescriptor fd;
  FileMetaData* file_metadata;  // Point to all metadata
  Slice smallest_key;    // slice that contain smallest key
  Slice largest_key;     // slice that contain largest key

  FdWithKeyRange()
      : fd(),
        file_metadata(nullptr),
        smallest_key(),
        largest_key() {
  }

  FdWithKeyRange(FileDescriptor _fd, Slice _smallest_key, Slice _largest_key,
                 FileMetaData* _file_metadata)
      : fd(_fd),
        file_metadata(_file_metadata),
        smallest_key(_smallest_key),
        largest_key(_largest_key) {}
};

// Data structure to store an array of FdWithKeyRange in one level
// Actual data is guaranteed to be stored closely
struct LevelFilesBrief {
  size_t num_files;
  FdWithKeyRange* files;
  LevelFilesBrief() {
    num_files = 0;
    files = nullptr;
  }
};

// The state of a DB at any given time is referred to as a Version.
// Any modification to the Version is considered a Version Edit. A Version is
// constructed by joining a sequence of Version Edits. Version Edits are written
// to the MANIFEST file.
class VersionEdit {
 public:
  void Clear();

  void SetDBId(const std::string& db_id) {
    has_db_id_ = true;
    db_id_ = db_id;
  }
  bool HasDbId() const { return has_db_id_; }
  const std::string& GetDbId() const { return db_id_; }

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  bool HasComparatorName() const { return has_comparator_; }
  const std::string& GetComparatorName() const { return comparator_; }

  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  bool HasLogNumber() const { return has_log_number_; }
  uint64_t GetLogNumber() const { return log_number_; }

  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  bool HasPrevLogNumber() const { return has_prev_log_number_; }
  uint64_t GetPrevLogNumber() const { return prev_log_number_; }

  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  bool HasNextFile() const { return has_next_file_number_; }
  uint64_t GetNextFile() const { return next_file_number_; }

  void SetMaxColumnFamily(uint32_t max_column_family) {
    has_max_column_family_ = true;
    max_column_family_ = max_column_family;
  }
  bool HasMaxColumnFamily() const { return has_max_column_family_; }
  uint32_t GetMaxColumnFamily() const { return max_column_family_; }

  void SetMinLogNumberToKeep(uint64_t num) {
    has_min_log_number_to_keep_ = true;
    min_log_number_to_keep_ = num;
  }
  bool HasMinLogNumberToKeep() const { return has_min_log_number_to_keep_; }
  uint64_t GetMinLogNumberToKeep() const { return min_log_number_to_keep_; }

  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  bool HasLastSequence() const { return has_last_sequence_; }
  SequenceNumber GetLastSequence() const { return last_sequence_; }

  // Delete the specified table file from the specified level.
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.emplace(level, file);
  }

  // Retrieve the table files deleted as well as their associated levels.
  using DeletedFiles = std::set<std::pair<int, uint64_t>>;
  const DeletedFiles& GetDeletedFiles() const { return deleted_files_; }

  // Add the specified table file at the specified level.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  // REQUIRES: "oldest_blob_file_number" is the number of the oldest blob file
  // referred to by this file if any, kInvalidBlobFileNumber otherwise.
  void AddFile(int level, uint64_t file, uint32_t file_path_id,
               uint64_t file_size, const InternalKey& smallest,
               const InternalKey& largest, const SequenceNumber& smallest_seqno,
               const SequenceNumber& largest_seqno, bool marked_for_compaction,
               uint64_t oldest_blob_file_number, uint64_t oldest_ancester_time,
               uint64_t file_creation_time, const std::string& file_checksum,
               const std::string& file_checksum_func_name) {
    assert(smallest_seqno <= largest_seqno);
    new_files_.emplace_back(
        level, FileMetaData(file, file_path_id, file_size, smallest, largest,
                            smallest_seqno, largest_seqno,
                            marked_for_compaction, oldest_blob_file_number,
                            oldest_ancester_time, file_creation_time,
                            file_checksum, file_checksum_func_name));
  }

  void AddFile(int level, const FileMetaData& f) {
    assert(f.fd.smallest_seqno <= f.fd.largest_seqno);
    new_files_.emplace_back(level, f);
  }

  // Retrieve the table files added as well as their associated levels.
  using NewFiles = std::vector<std::pair<int, FileMetaData>>;
  const NewFiles& GetNewFiles() const { return new_files_; }

  // Add a new blob file.
  void AddBlobFile(uint64_t blob_file_number, uint64_t total_blob_count,
                   uint64_t total_blob_bytes, std::string checksum_method,
                   std::string checksum_value) {
    blob_file_additions_.emplace_back(
        blob_file_number, total_blob_count, total_blob_bytes,
        std::move(checksum_method), std::move(checksum_value));
  }

  void AddBlobFile(BlobFileAddition blob_file_addition) {
    blob_file_additions_.emplace_back(std::move(blob_file_addition));
  }

  // Retrieve all the blob files added.
  using BlobFileAdditions = std::vector<BlobFileAddition>;
  const BlobFileAdditions& GetBlobFileAdditions() const {
    return blob_file_additions_;
  }

  void SetBlobFileAdditions(BlobFileAdditions blob_file_additions) {
    assert(blob_file_additions_.empty());
    blob_file_additions_ = std::move(blob_file_additions);
  }

  // Add garbage for an existing blob file.  Note: intentionally broken English
  // follows.
  void AddBlobFileGarbage(uint64_t blob_file_number,
                          uint64_t garbage_blob_count,
                          uint64_t garbage_blob_bytes) {
    blob_file_garbages_.emplace_back(blob_file_number, garbage_blob_count,
                                     garbage_blob_bytes);
  }

  void AddBlobFileGarbage(BlobFileGarbage blob_file_garbage) {
    blob_file_garbages_.emplace_back(std::move(blob_file_garbage));
  }

  // Retrieve all the blob file garbage added.
  using BlobFileGarbages = std::vector<BlobFileGarbage>;
  const BlobFileGarbages& GetBlobFileGarbages() const {
    return blob_file_garbages_;
  }

  void SetBlobFileGarbages(BlobFileGarbages blob_file_garbages) {
    assert(blob_file_garbages_.empty());
    blob_file_garbages_ = std::move(blob_file_garbages);
  }

  // Add a WAL (either just created or closed).
  // AddWal and DeleteWalsBefore cannot be called on the same VersionEdit.
  void AddWal(WalNumber number, WalMetadata metadata = WalMetadata()) {
    assert(NumEntries() == wal_additions_.size());
    wal_additions_.emplace_back(number, std::move(metadata));
  }

  // Retrieve all the added WALs.
  const WalAdditions& GetWalAdditions() const { return wal_additions_; }

  bool IsWalAddition() const { return !wal_additions_.empty(); }

  // Delete a WAL (either directly deleted or archived).
  // AddWal and DeleteWalsBefore cannot be called on the same VersionEdit.
  void DeleteWalsBefore(WalNumber number) {
    assert((NumEntries() == 1) == !wal_deletion_.IsEmpty());
    wal_deletion_ = WalDeletion(number);
  }

  const WalDeletion& GetWalDeletion() const { return wal_deletion_; }

  bool IsWalDeletion() const { return !wal_deletion_.IsEmpty(); }

  bool IsWalManipulation() const {
    size_t entries = NumEntries();
    return (entries > 0) && ((entries == wal_additions_.size()) ||
                             (entries == !wal_deletion_.IsEmpty()));
  }

  // Number of edits
  size_t NumEntries() const {
    return new_files_.size() + deleted_files_.size() +
           blob_file_additions_.size() + blob_file_garbages_.size() +
           wal_additions_.size() + !wal_deletion_.IsEmpty();
  }

  void SetColumnFamily(uint32_t column_family_id) {
    column_family_ = column_family_id;
  }
  uint32_t GetColumnFamily() const { return column_family_; }

  // set column family ID by calling SetColumnFamily()
  void AddColumnFamily(const std::string& name) {
    assert(!is_column_family_drop_);
    assert(!is_column_family_add_);
    assert(NumEntries() == 0);
    is_column_family_add_ = true;
    column_family_name_ = name;
  }

  // set column family ID by calling SetColumnFamily()
  void DropColumnFamily() {
    assert(!is_column_family_drop_);
    assert(!is_column_family_add_);
    assert(NumEntries() == 0);
    is_column_family_drop_ = true;
  }

  bool IsColumnFamilyManipulation() const {
    return is_column_family_add_ || is_column_family_drop_;
  }

  bool IsColumnFamilyAdd() const { return is_column_family_add_; }

  bool IsColumnFamilyDrop() const { return is_column_family_drop_; }

  void MarkAtomicGroup(uint32_t remaining_entries) {
    is_in_atomic_group_ = true;
    remaining_entries_ = remaining_entries;
  }
  bool IsInAtomicGroup() const { return is_in_atomic_group_; }
  uint32_t GetRemainingEntries() const { return remaining_entries_; }

  bool HasFullHistoryTsLow() const { return !full_history_ts_low_.empty(); }
  const std::string& GetFullHistoryTsLow() const {
    assert(HasFullHistoryTsLow());
    return full_history_ts_low_;
  }
  void SetFullHistoryTsLow(std::string full_history_ts_low) {
    assert(!full_history_ts_low.empty());
    full_history_ts_low_ = std::move(full_history_ts_low);
  }

  // return true on success.
  bool EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString(bool hex_key = false) const;
  std::string DebugJSON(int edit_num, bool hex_key = false) const;

 private:
  friend class ReactiveVersionSet;
  friend class VersionEditHandlerBase;
  friend class ListColumnFamiliesHandler;
  friend class VersionEditHandler;
  friend class VersionEditHandlerPointInTime;
  friend class DumpManifestHandler;
  friend class VersionSet;
  friend class Version;
  friend class AtomicGroupReadBuffer;

  bool GetLevel(Slice* input, int* level, const char** msg);

  const char* DecodeNewFile4From(Slice* input);

  int max_level_ = 0;
  std::string db_id_;
  std::string comparator_;
  uint64_t log_number_ = 0;
  uint64_t prev_log_number_ = 0;
  uint64_t next_file_number_ = 0;
  uint32_t max_column_family_ = 0;
  // The most recent WAL log number that is deleted
  uint64_t min_log_number_to_keep_ = 0;
  SequenceNumber last_sequence_ = 0;
  bool has_db_id_ = false;
  bool has_comparator_ = false;
  bool has_log_number_ = false;
  bool has_prev_log_number_ = false;
  bool has_next_file_number_ = false;
  bool has_max_column_family_ = false;
  bool has_min_log_number_to_keep_ = false;
  bool has_last_sequence_ = false;

  DeletedFiles deleted_files_;
  NewFiles new_files_;

  BlobFileAdditions blob_file_additions_;
  BlobFileGarbages blob_file_garbages_;

  WalAdditions wal_additions_;
  WalDeletion wal_deletion_;

  // Each version edit record should have column_family_ set
  // If it's not set, it is default (0)
  uint32_t column_family_ = 0;
  // a version edit can be either column_family add or
  // column_family drop. If it's column family add,
  // it also includes column family name.
  bool is_column_family_drop_ = false;
  bool is_column_family_add_ = false;
  std::string column_family_name_;

  bool is_in_atomic_group_ = false;
  uint32_t remaining_entries_ = 0;

  std::string full_history_ts_low_;
};

}  // namespace ROCKSDB_NAMESPACE
