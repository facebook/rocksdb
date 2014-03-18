//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <set>
#include <utility>
#include <vector>
#include "rocksdb/cache.h"
#include "db/dbformat.h"

namespace rocksdb {

class VersionSet;

struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table
  bool being_compacted;       // Is this file undergoing compaction?
  SequenceNumber smallest_seqno;// The smallest seqno in this file
  SequenceNumber largest_seqno; // The largest seqno in this file

  // Needs to be disposed when refs becomes 0.
  Cache::Handle* table_reader_handle;

  FileMetaData(uint64_t number, uint64_t file_size) :
      refs(0), allowed_seeks(1 << 30), number(number), file_size(file_size),
      being_compacted(false), table_reader_handle(nullptr) {
  }
  FileMetaData() : FileMetaData(0, 0) { }
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetVersionNumber() {
    has_version_number_ = true;
    version_number_ = kManifestVersion;
  }
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

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest,
               const SequenceNumber& smallest_seqno,
               const SequenceNumber& largest_seqno) {
    assert(smallest_seqno <= largest_seqno);
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    f.smallest_seqno = smallest_seqno;
    f.largest_seqno = largest_seqno;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert({level, file});
  }

  // Number of edits
  int NumEntries() {
    return new_files_.size() + deleted_files_.size();
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString(bool hex_key = false) const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t>> DeletedFileSet;

  bool GetLevel(Slice* input, int* level, const char** msg);

  int max_level_;
  uint32_t version_number_;
  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_version_number_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  DeletedFileSet deleted_files_;
  std::vector<std::pair<int, FileMetaData> > new_files_;

  enum {
    kManifestVersion = 1
  };
};

}  // namespace rocksdb
