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
#include "db/dbformat.h"
#include "memory/arena.h"
#include "rocksdb/cache.h"
#include "util/autovector.h"
#include "db/value_log.h"

namespace rocksdb {

class VersionSet;
class ColumnFamilyData;

const uint64_t kFileNumberMask = 0x3FFFFFFFFFFFFFFF;

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

  FileDescriptor(const FileDescriptor& fd) { *this=fd; }

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
  Cache::Handle* table_reader_handle;

  FileSampledStats stats;

  // Stats for compensating deletion entries during compaction

  // File size compensated by deletion entry.
  // This is updated in Version::UpdateAccumulatedStats() first time when the
  // file is created or loaded.  After it is updated (!= 0), it is immutable.
  uint64_t compensated_file_size;
  // These values can mutate, but they can only be read or written from
  // single-threaded LogAndApply thread
  uint64_t num_entries;            // the number of entries.
  uint64_t num_deletions;          // the number of deletion entries.
  uint64_t raw_key_size;           // total uncompressed key size.
  uint64_t raw_value_size;         // total uncompressed value size.

  int refs;  // Reference count

  bool being_compacted;        // Is this file undergoing compaction?
  bool init_stats_from_file;   // true if the data-entry stats of this file
                               // has initialized from file.

  bool marked_for_compaction;  // True if client asked us nicely to compact this
                               // file.
  // add earliest_ref to FileMetaData
  // for each ring: filenumber of the oldest value referred to in this SST, or
  // HIGH-VALUE if no reference
  std::vector<uint64_t> indirect_ref_0;
  // value of 0 means 'omitted', i. e. this file was created without a value for
  // this field
  // The SSTs are chained off the VLogRings on doubly-linked lists, one
  // chain-pair per ring.  The SST is chained for as long as it is current,
  // i. e. part of the current version
  std::vector<FileMetaData*> ringfwdchain;
  std::vector<FileMetaData*> ringbwdchain;
  // The value log for the CF this file is in.  It's a shame to waste 8 bytes,
  // but it's just too hard to get a pointer to the ColumnFamilyData down to all
  // the routines that need it
  std::shared_ptr<VLog> vlog;
  // The level of this file, needed so that Active Recycling can put the file
  // back to the same level
  int level;
  // average value of indirect_ref_0 for the parents (i. e. higher level) of
  // this file, 0 if no parents, and the ring it is in (62/2 bits)
  VLogRingRefFileno avgparentfileno;

  FileMetaData()
      : table_reader_handle(nullptr),
        compensated_file_size(0),
        num_entries(0),
        num_deletions(0),
        raw_key_size(0),
        raw_value_size(0),
        refs(0),
        being_compacted(false),
        init_stats_from_file(false),
        marked_for_compaction(false),
       	indirect_ref_0(std::vector<uint64_t>()),  // 0 means 'omitted'
        ringfwdchain(std::vector<FileMetaData*>()),
        ringbwdchain(std::vector<FileMetaData*>()),
        vlog(nullptr),   // vlog is filled in when we add the file to a CF
        level(-1),
        avgparentfileno(0) {}

  // REQUIRED: Keys must be given to the function in sorted order (it expects
  // the last key to be the largest).
  void UpdateBoundaries(const Slice& key, SequenceNumber seqno) {
    if (smallest.size() == 0) {
      smallest.DecodeFrom(key);
    }
    largest.DecodeFrom(key);
    fd.smallest_seqno = std::min(fd.smallest_seqno, seqno);
    fd.largest_seqno = std::max(fd.largest_seqno, seqno);
  }

  // After the last kv has been written to the file, install the earliest refs
  // that were found in the file, one for each ring (0 means no ref)
  void InstallRef0(int outputlevel, const std::vector<uint64_t> &earliestref,
                   ColumnFamilyData *cfd);

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

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetMaxColumnFamily(uint32_t max_column_family) {
    has_max_column_family_ = true;
    max_column_family_ = max_column_family;
  }
  void SetMinLogNumberToKeep(uint64_t num) {
    has_min_log_number_to_keep_ = true;
    min_log_number_to_keep_ = num;
  }

  bool has_log_number() { return has_log_number_; }

  uint64_t log_number() { return log_number_; }

  bool has_next_file_number() const { return has_next_file_number_; }

  uint64_t next_file_number() const { return next_file_number_; }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint32_t file_path_id,
               uint64_t file_size, const InternalKey& smallest,
               const InternalKey& largest, const SequenceNumber& smallest_seqno,
               const SequenceNumber& largest_seqno,
               bool marked_for_compaction,
	       const std::vector<uint64_t>& indirect_ref_0 = std::vector<uint64_t>(),
               const uint64_t avgparentfileno = 0) {
    assert(smallest_seqno <= largest_seqno);
    FileMetaData f;
    f.fd = FileDescriptor(file, file_path_id, file_size, smallest_seqno,
                          largest_seqno);
    f.smallest = smallest;
    f.largest = largest;
    f.fd.smallest_seqno = smallest_seqno;
    f.fd.largest_seqno = largest_seqno;
    f.marked_for_compaction = marked_for_compaction;
    // older code doesn't know about indirect_ref; use 'omitted' (0) then
    f.indirect_ref_0=indirect_ref_0;  // set the earliest refs, if any
    f.level = level;  // save level # in the metadata so VLogRing can get to it
    f.avgparentfileno = avgparentfileno;  // higher-level information
    new_files_.emplace_back(level, std::move(f));
  }

  void AddFile(int level, const FileMetaData& f) {
    assert(f.fd.smallest_seqno <= f.fd.largest_seqno);
    new_files_.emplace_back(level, f);
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert({level, file});
  }
  void DeleteFile(int level, FileMetaData *f) {
    // At this point we are committed to deleting the input file f, but that's
    // a ways in the future.  We are holding the mutex for compaction/copy, but
    // we are still before the lock point on &w (in LogAndApply) at which
    // writers get queued.  We mustn't UnCurrent f right now, until we know that
    // we have processed, for manifest purposes, the SST actions that justify
    // this deletion. So, we enqueue f on retiring_files_ (analogous to
    // new_files_ for added files).  As the edits are applied we will move
    // these files to retiredfiles in the rep, and will UnCurrent them in SaveTo
    // If the file was never installed, the sizes may be different
    assert(f->indirect_ref_0.size()==f->ringfwdchain.size());
    assert(f->indirect_ref_0.size()==f->ringbwdchain.size());
    retiring_files_.push_back(f);
    DeleteFile(level, f->fd.GetNumber());
  }


  // Number of edits
  size_t NumEntries() { return new_files_.size() + deleted_files_.size(); }

  bool IsColumnFamilyManipulation() {
    return is_column_family_add_ || is_column_family_drop_;
  }

  void SetColumnFamily(uint32_t column_family_id) {
    column_family_ = column_family_id;
  }

  void SetVLogStats(std::vector<VLogRingRestartInfo>& vstats) {
    vlog_additions = vstats;
  }

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

  // return true on success.
  bool EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  const char* DecodeNewFile4From(Slice* input);

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  const DeletedFileSet& GetDeletedFiles() { return deleted_files_; }
  const std::vector<std::pair<int, FileMetaData>>& GetNewFiles() {
    return new_files_;
  }

  const std::vector<FileMetaData*>& GetRetiringFiles() {
    return retiring_files_;
  }
  std::vector<VLogRingRestartInfo>& VLogAdditions() { return vlog_additions; }

  void MarkAtomicGroup(uint32_t remaining_entries) {
    is_in_atomic_group_ = true;
    remaining_entries_ = remaining_entries;
  }

  std::string DebugString(bool hex_key = false) const;
  std::string DebugJSON(int edit_num, bool hex_key = false) const;

 private:
  friend class ReactiveVersionSet;
  friend class VersionSet;
  friend class Version;
  friend class AtomicGroupReadBuffer;

  bool GetLevel(Slice* input, int* level, const char** msg);

  int max_level_;
  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  uint32_t max_column_family_;
  // The most recent WAL log number that is deleted
  uint64_t min_log_number_to_keep_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;
  bool has_max_column_family_;
  bool has_min_log_number_to_keep_;

  DeletedFileSet deleted_files_;
  std::vector<std::pair<int, FileMetaData>> new_files_;
  // Files (from compaction/copy) that we will need to mark UnCurrent in SaveTo.
  // Files deleted during recovery don't go here.
  std::vector<FileMetaData*> retiring_files_;
  // files and bytes added/removed, one set per ring.
  std::vector<VLogRingRestartInfo> vlog_additions;


  // Each version edit record should have column_family_ set
  // If it's not set, it is default (0)
  uint32_t column_family_;
  // a version edit can be either column_family add or
  // column_family drop. If it's column family add,
  // it also includes column family name.
  bool is_column_family_drop_;
  bool is_column_family_add_;
  std::string column_family_name_;

  bool is_in_atomic_group_;
  uint32_t remaining_entries_;
};

}  // namespace rocksdb
