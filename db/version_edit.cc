//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/blob/blob_index.h"
#include "db/version_set.h"
#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

}  // anonymous namespace

uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id) {
  assert(number <= kFileNumberMask);
  return number | (path_id * (kFileNumberMask + 1));
}

void FileMetaData::UpdateBoundaries(const Slice& key, const Slice& value,
                                    SequenceNumber seqno,
                                    ValueType value_type) {
  if (smallest.size() == 0) {
    smallest.DecodeFrom(key);
  }
  largest.DecodeFrom(key);
  fd.smallest_seqno = std::min(fd.smallest_seqno, seqno);
  fd.largest_seqno = std::max(fd.largest_seqno, seqno);

  if (value_type == kTypeBlobIndex) {
    BlobIndex blob_index;
    const Status s = blob_index.DecodeFrom(value);
    if (!s.ok()) {
      return;
    }

    if (blob_index.IsInlined()) {
      return;
    }

    if (blob_index.HasTTL()) {
      return;
    }

    // Paranoid check: this should not happen because BlobDB numbers the blob
    // files starting from 1.
    if (blob_index.file_number() == kInvalidBlobFileNumber) {
      return;
    }

    if (oldest_blob_file_number == kInvalidBlobFileNumber ||
        oldest_blob_file_number > blob_index.file_number()) {
      oldest_blob_file_number = blob_index.file_number();
    }
  }
}

void VersionEdit::Clear() {
  max_level_ = 0;
  db_id_.clear();
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  next_file_number_ = 0;
  max_column_family_ = 0;
  min_log_number_to_keep_ = 0;
  last_sequence_ = 0;
  has_db_id_ = false;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_max_column_family_ = false;
  has_min_log_number_to_keep_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
  blob_file_additions_.clear();
  blob_file_garbages_.clear();
  wal_additions_.clear();
  wal_deletion_.Reset();
  column_family_ = 0;
  is_column_family_add_ = false;
  is_column_family_drop_ = false;
  column_family_name_.clear();
  is_in_atomic_group_ = false;
  remaining_entries_ = 0;
  full_history_ts_low_.clear();
}

bool VersionEdit::EncodeTo(std::string* dst) const {
  if (has_db_id_) {
    PutVarint32(dst, kDbId);
    PutLengthPrefixedSlice(dst, db_id_);
  }
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32Varint64(dst, kLogNumber, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32Varint64(dst, kPrevLogNumber, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32Varint64(dst, kNextFileNumber, next_file_number_);
  }
  if (has_max_column_family_) {
    PutVarint32Varint32(dst, kMaxColumnFamily, max_column_family_);
  }
  if (has_last_sequence_) {
    PutVarint32Varint64(dst, kLastSequence, last_sequence_);
  }
  for (const auto& deleted : deleted_files_) {
    PutVarint32Varint32Varint64(dst, kDeletedFile, deleted.first /* level */,
                                deleted.second /* file number */);
  }

  bool min_log_num_written = false;
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    if (!f.smallest.Valid() || !f.largest.Valid()) {
      return false;
    }
    PutVarint32(dst, kNewFile4);
    PutVarint32Varint64(dst, new_files_[i].first /* level */, f.fd.GetNumber());
    PutVarint64(dst, f.fd.GetFileSize());
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
    PutVarint64Varint64(dst, f.fd.smallest_seqno, f.fd.largest_seqno);
    // Customized fields' format:
    // +-----------------------------+
    // | 1st field's tag (varint32)  |
    // +-----------------------------+
    // | 1st field's size (varint32) |
    // +-----------------------------+
    // |    bytes for 1st field      |
    // |  (based on size decoded)    |
    // +-----------------------------+
    // |                             |
    // |          ......             |
    // |                             |
    // +-----------------------------+
    // | last field's size (varint32)|
    // +-----------------------------+
    // |    bytes for last field     |
    // |  (based on size decoded)    |
    // +-----------------------------+
    // | terminating tag (varint32)  |
    // +-----------------------------+
    //
    // Customized encoding for fields:
    //   tag kPathId: 1 byte as path_id
    //   tag kNeedCompaction:
    //        now only can take one char value 1 indicating need-compaction
    //
    PutVarint32(dst, NewFileCustomTag::kOldestAncesterTime);
    std::string varint_oldest_ancester_time;
    PutVarint64(&varint_oldest_ancester_time, f.oldest_ancester_time);
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:VarintOldestAncesterTime",
                             &varint_oldest_ancester_time);
    PutLengthPrefixedSlice(dst, Slice(varint_oldest_ancester_time));

    PutVarint32(dst, NewFileCustomTag::kFileCreationTime);
    std::string varint_file_creation_time;
    PutVarint64(&varint_file_creation_time, f.file_creation_time);
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:VarintFileCreationTime",
                             &varint_file_creation_time);
    PutLengthPrefixedSlice(dst, Slice(varint_file_creation_time));

    PutVarint32(dst, NewFileCustomTag::kFileChecksum);
    PutLengthPrefixedSlice(dst, Slice(f.file_checksum));

    PutVarint32(dst, NewFileCustomTag::kFileChecksumFuncName);
    PutLengthPrefixedSlice(dst, Slice(f.file_checksum_func_name));

    if (f.max_timestamp != kDisableUserTimestamp) {
      if (f.min_timestamp.size() != f.max_timestamp.size()) {
        assert(false);
        return false;
      }
      PutVarint32(dst, NewFileCustomTag::kMinTimestamp);
      PutLengthPrefixedSlice(dst, Slice(f.min_timestamp));
      PutVarint32(dst, NewFileCustomTag::kMaxTimestamp);
      PutLengthPrefixedSlice(dst, Slice(f.max_timestamp));
    }
    if (f.fd.GetPathId() != 0) {
      PutVarint32(dst, NewFileCustomTag::kPathId);
      char p = static_cast<char>(f.fd.GetPathId());
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (f.temperature != Temperature::kUnknown) {
      PutVarint32(dst, NewFileCustomTag::kTemperature);
      char p = static_cast<char>(f.temperature);
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (f.marked_for_compaction) {
      PutVarint32(dst, NewFileCustomTag::kNeedCompaction);
      char p = static_cast<char>(1);
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (has_min_log_number_to_keep_ && !min_log_num_written) {
      PutVarint32(dst, NewFileCustomTag::kMinLogNumberToKeepHack);
      std::string varint_log_number;
      PutFixed64(&varint_log_number, min_log_number_to_keep_);
      PutLengthPrefixedSlice(dst, Slice(varint_log_number));
      min_log_num_written = true;
    }
    if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
      PutVarint32(dst, NewFileCustomTag::kOldestBlobFileNumber);
      std::string oldest_blob_file_number;
      PutVarint64(&oldest_blob_file_number, f.oldest_blob_file_number);
      PutLengthPrefixedSlice(dst, Slice(oldest_blob_file_number));
    }
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:NewFile4:CustomizeFields",
                             dst);

    PutVarint32(dst, NewFileCustomTag::kTerminate);
  }

  for (const auto& blob_file_addition : blob_file_additions_) {
    PutVarint32(dst, kBlobFileAddition);
    blob_file_addition.EncodeTo(dst);
  }

  for (const auto& blob_file_garbage : blob_file_garbages_) {
    PutVarint32(dst, kBlobFileGarbage);
    blob_file_garbage.EncodeTo(dst);
  }

  for (const auto& wal_addition : wal_additions_) {
    PutVarint32(dst, kWalAddition2);
    std::string encoded;
    wal_addition.EncodeTo(&encoded);
    PutLengthPrefixedSlice(dst, encoded);
  }

  if (!wal_deletion_.IsEmpty()) {
    PutVarint32(dst, kWalDeletion2);
    std::string encoded;
    wal_deletion_.EncodeTo(&encoded);
    PutLengthPrefixedSlice(dst, encoded);
  }

  // 0 is default and does not need to be explicitly written
  if (column_family_ != 0) {
    PutVarint32Varint32(dst, kColumnFamily, column_family_);
  }

  if (is_column_family_add_) {
    PutVarint32(dst, kColumnFamilyAdd);
    PutLengthPrefixedSlice(dst, Slice(column_family_name_));
  }

  if (is_column_family_drop_) {
    PutVarint32(dst, kColumnFamilyDrop);
  }

  if (is_in_atomic_group_) {
    PutVarint32(dst, kInAtomicGroup);
    PutVarint32(dst, remaining_entries_);
  }

  if (HasFullHistoryTsLow()) {
    PutVarint32(dst, kFullHistoryTsLow);
    PutLengthPrefixedSlice(dst, full_history_ts_low_);
  }
  return true;
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return dst->Valid();
  } else {
    return false;
  }
}

bool VersionEdit::GetLevel(Slice* input, int* level, const char** /*msg*/) {
  uint32_t v = 0;
  if (GetVarint32(input, &v)) {
    *level = v;
    if (max_level_ < *level) {
      max_level_ = *level;
    }
    return true;
  } else {
    return false;
  }
}

const char* VersionEdit::DecodeNewFile4From(Slice* input) {
  const char* msg = nullptr;
  int level = 0;
  FileMetaData f;
  uint64_t number = 0;
  uint32_t path_id = 0;
  uint64_t file_size = 0;
  SequenceNumber smallest_seqno = 0;
  SequenceNumber largest_seqno = kMaxSequenceNumber;
  if (GetLevel(input, &level, &msg) && GetVarint64(input, &number) &&
      GetVarint64(input, &file_size) && GetInternalKey(input, &f.smallest) &&
      GetInternalKey(input, &f.largest) &&
      GetVarint64(input, &smallest_seqno) &&
      GetVarint64(input, &largest_seqno)) {
    // See comments in VersionEdit::EncodeTo() for format of customized fields
    while (true) {
      uint32_t custom_tag = 0;
      Slice field;
      if (!GetVarint32(input, &custom_tag)) {
        return "new-file4 custom field";
      }
      if (custom_tag == kTerminate) {
        if (f.min_timestamp.size() != f.max_timestamp.size()) {
          assert(false);
          return "new-file4 custom field timestamp size mismatch error";
        }
        break;
      }
      if (!GetLengthPrefixedSlice(input, &field)) {
        return "new-file4 custom field length prefixed slice error";
      }
      switch (custom_tag) {
        case kPathId:
          if (field.size() != 1) {
            return "path_id field wrong size";
          }
          path_id = field[0];
          if (path_id > 3) {
            return "path_id wrong vaue";
          }
          break;
        case kOldestAncesterTime:
          if (!GetVarint64(&field, &f.oldest_ancester_time)) {
            return "invalid oldest ancester time";
          }
          break;
        case kFileCreationTime:
          if (!GetVarint64(&field, &f.file_creation_time)) {
            return "invalid file creation time";
          }
          break;
        case kFileChecksum:
          f.file_checksum = field.ToString();
          break;
        case kFileChecksumFuncName:
          f.file_checksum_func_name = field.ToString();
          break;
        case kNeedCompaction:
          if (field.size() != 1) {
            return "need_compaction field wrong size";
          }
          f.marked_for_compaction = (field[0] == 1);
          break;
        case kMinLogNumberToKeepHack:
          // This is a hack to encode kMinLogNumberToKeep in a
          // forward-compatible fashion.
          if (!GetFixed64(&field, &min_log_number_to_keep_)) {
            return "deleted log number malformatted";
          }
          has_min_log_number_to_keep_ = true;
          break;
        case kOldestBlobFileNumber:
          if (!GetVarint64(&field, &f.oldest_blob_file_number)) {
            return "invalid oldest blob file number";
          }
          break;
        case kTemperature:
          if (field.size() != 1) {
            return "temperature field wrong size";
          } else {
            Temperature casted_field = static_cast<Temperature>(field[0]);
            if (casted_field <= Temperature::kCold) {
              f.temperature = casted_field;
            }
          }
          break;
        case kMinTimestamp:
          f.min_timestamp = field.ToString();
          break;
        case kMaxTimestamp:
          f.max_timestamp = field.ToString();
          break;
        default:
          if ((custom_tag & kCustomTagNonSafeIgnoreMask) != 0) {
            // Should not proceed if cannot understand it
            return "new-file4 custom field not supported";
          }
          break;
      }
    }
  } else {
    return "new-file4 entry";
  }
  f.fd =
      FileDescriptor(number, path_id, file_size, smallest_seqno, largest_seqno);
  new_files_.push_back(std::make_pair(level, f));
  return nullptr;
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
#ifndef NDEBUG
  bool ignore_ignorable_tags = false;
  TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:IgnoreIgnorableTags",
                           &ignore_ignorable_tags);
#endif
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag = 0;

  // Temporary storage for parsing
  int level = 0;
  FileMetaData f;
  Slice str;
  InternalKey key;
  while (msg == nullptr && GetVarint32(&input, &tag)) {
#ifndef NDEBUG
    if (ignore_ignorable_tags && tag > kTagSafeIgnoreMask) {
      tag = kTagSafeIgnoreMask;
    }
#endif
    switch (tag) {
      case kDbId:
        if (GetLengthPrefixedSlice(&input, &str)) {
          db_id_ = str.ToString();
          has_db_id_ = true;
        } else {
          msg = "db id";
        }
        break;
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kMaxColumnFamily:
        if (GetVarint32(&input, &max_column_family_)) {
          has_max_column_family_ = true;
        } else {
          msg = "max column family";
        }
        break;

      case kMinLogNumberToKeep:
        if (GetVarint64(&input, &min_log_number_to_keep_)) {
          has_min_log_number_to_keep_ = true;
        } else {
          msg = "min log number to kee";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level, &msg) &&
            GetInternalKey(&input, &key)) {
          // we don't use compact pointers anymore,
          // but we should not fail if they are still
          // in manifest
        } else {
          if (!msg) {
            msg = "compaction pointer";
          }
        }
        break;

      case kDeletedFile: {
        uint64_t number = 0;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          if (!msg) {
            msg = "deleted file";
          }
        }
        break;
      }

      case kNewFile: {
        uint64_t number = 0;
        uint64_t file_size = 0;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          f.fd = FileDescriptor(number, 0, file_size);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file entry";
          }
        }
        break;
      }
      case kNewFile2: {
        uint64_t number = 0;
        uint64_t file_size = 0;
        SequenceNumber smallest_seqno = 0;
        SequenceNumber largest_seqno = kMaxSequenceNumber;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest) &&
            GetVarint64(&input, &smallest_seqno) &&
            GetVarint64(&input, &largest_seqno)) {
          f.fd = FileDescriptor(number, 0, file_size, smallest_seqno,
                                largest_seqno);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file2 entry";
          }
        }
        break;
      }

      case kNewFile3: {
        uint64_t number = 0;
        uint32_t path_id = 0;
        uint64_t file_size = 0;
        SequenceNumber smallest_seqno = 0;
        SequenceNumber largest_seqno = kMaxSequenceNumber;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint32(&input, &path_id) && GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest) &&
            GetVarint64(&input, &smallest_seqno) &&
            GetVarint64(&input, &largest_seqno)) {
          f.fd = FileDescriptor(number, path_id, file_size, smallest_seqno,
                                largest_seqno);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file3 entry";
          }
        }
        break;
      }

      case kNewFile4: {
        msg = DecodeNewFile4From(&input);
        break;
      }

      case kBlobFileAddition:
      case kBlobFileAddition_DEPRECATED: {
        BlobFileAddition blob_file_addition;
        const Status s = blob_file_addition.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        AddBlobFile(std::move(blob_file_addition));
        break;
      }

      case kBlobFileGarbage:
      case kBlobFileGarbage_DEPRECATED: {
        BlobFileGarbage blob_file_garbage;
        const Status s = blob_file_garbage.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        AddBlobFileGarbage(std::move(blob_file_garbage));
        break;
      }

      case kWalAddition: {
        WalAddition wal_addition;
        const Status s = wal_addition.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        wal_additions_.emplace_back(std::move(wal_addition));
        break;
      }

      case kWalAddition2: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "WalAddition not prefixed by length";
          break;
        }

        WalAddition wal_addition;
        const Status s = wal_addition.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        wal_additions_.emplace_back(std::move(wal_addition));
        break;
      }

      case kWalDeletion: {
        WalDeletion wal_deletion;
        const Status s = wal_deletion.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        wal_deletion_ = std::move(wal_deletion);
        break;
      }

      case kWalDeletion2: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "WalDeletion not prefixed by length";
          break;
        }

        WalDeletion wal_deletion;
        const Status s = wal_deletion.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        wal_deletion_ = std::move(wal_deletion);
        break;
      }

      case kColumnFamily:
        if (!GetVarint32(&input, &column_family_)) {
          if (!msg) {
            msg = "set column family id";
          }
        }
        break;

      case kColumnFamilyAdd:
        if (GetLengthPrefixedSlice(&input, &str)) {
          is_column_family_add_ = true;
          column_family_name_ = str.ToString();
        } else {
          if (!msg) {
            msg = "column family add";
          }
        }
        break;

      case kColumnFamilyDrop:
        is_column_family_drop_ = true;
        break;

      case kInAtomicGroup:
        is_in_atomic_group_ = true;
        if (!GetVarint32(&input, &remaining_entries_)) {
          if (!msg) {
            msg = "remaining entries";
          }
        }
        break;

      case kFullHistoryTsLow:
        if (!GetLengthPrefixedSlice(&input, &str)) {
          msg = "full_history_ts_low";
        } else if (str.empty()) {
          msg = "full_history_ts_low: empty";
        } else {
          full_history_ts_low_.assign(str.data(), str.size());
        }
        break;

      default:
        if (tag & kTagSafeIgnoreMask) {
          // Tag from future which can be safely ignored.
          // The next field must be the length of the entry.
          uint32_t field_len;
          if (!GetVarint32(&input, &field_len) ||
              static_cast<size_t>(field_len) > input.size()) {
            if (!msg) {
              msg = "safely ignoreable tag length error";
            }
          } else {
            input.remove_prefix(static_cast<size_t>(field_len));
          }
        } else {
          msg = "unknown tag";
        }
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString(bool hex_key) const {
  std::string r;
  r.append("VersionEdit {");
  if (has_db_id_) {
    r.append("\n  DB ID: ");
    r.append(db_id_);
  }
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFileNumber: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_max_column_family_) {
    r.append("\n  MaxColumnFamily: ");
    AppendNumberTo(&r, max_column_family_);
  }
  if (has_min_log_number_to_keep_) {
    r.append("\n  MinLogNumberToKeep: ");
    AppendNumberTo(&r, min_log_number_to_keep_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (const auto& deleted_file : deleted_files_) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, deleted_file.first);
    r.append(" ");
    AppendNumberTo(&r, deleted_file.second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetNumber());
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetFileSize());
    r.append(" ");
    r.append(f.smallest.DebugString(hex_key));
    r.append(" .. ");
    r.append(f.largest.DebugString(hex_key));
    if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
      r.append(" blob_file:");
      AppendNumberTo(&r, f.oldest_blob_file_number);
    }
    if (f.min_timestamp != kDisableUserTimestamp) {
      assert(f.max_timestamp != kDisableUserTimestamp);
      r.append(" min_timestamp:");
      r.append(Slice(f.min_timestamp).ToString(true));
      r.append(" max_timestamp:");
      r.append(Slice(f.max_timestamp).ToString(true));
    }
    r.append(" oldest_ancester_time:");
    AppendNumberTo(&r, f.oldest_ancester_time);
    r.append(" file_creation_time:");
    AppendNumberTo(&r, f.file_creation_time);
    r.append(" file_checksum:");
    r.append(Slice(f.file_checksum).ToString(true));
    r.append(" file_checksum_func_name: ");
    r.append(f.file_checksum_func_name);
    if (f.temperature != Temperature::kUnknown) {
      r.append(" temperature: ");
      // Maybe change to human readable format whenthe feature becomes
      // permanent
      r.append(ToString(static_cast<int>(f.temperature)));
    }
  }

  for (const auto& blob_file_addition : blob_file_additions_) {
    r.append("\n  BlobFileAddition: ");
    r.append(blob_file_addition.DebugString());
  }

  for (const auto& blob_file_garbage : blob_file_garbages_) {
    r.append("\n  BlobFileGarbage: ");
    r.append(blob_file_garbage.DebugString());
  }

  for (const auto& wal_addition : wal_additions_) {
    r.append("\n  WalAddition: ");
    r.append(wal_addition.DebugString());
  }

  if (!wal_deletion_.IsEmpty()) {
    r.append("\n  WalDeletion: ");
    r.append(wal_deletion_.DebugString());
  }

  r.append("\n  ColumnFamily: ");
  AppendNumberTo(&r, column_family_);
  if (is_column_family_add_) {
    r.append("\n  ColumnFamilyAdd: ");
    r.append(column_family_name_);
  }
  if (is_column_family_drop_) {
    r.append("\n  ColumnFamilyDrop");
  }
  if (is_in_atomic_group_) {
    r.append("\n  AtomicGroup: ");
    AppendNumberTo(&r, remaining_entries_);
    r.append(" entries remains");
  }
  if (HasFullHistoryTsLow()) {
    r.append("\n FullHistoryTsLow: ");
    r.append(Slice(full_history_ts_low_).ToString(hex_key));
  }
  r.append("\n}\n");
  return r;
}

std::string VersionEdit::DebugJSON(int edit_num, bool hex_key) const {
  JSONWriter jw;
  jw << "EditNumber" << edit_num;

  if (has_db_id_) {
    jw << "DB ID" << db_id_;
  }
  if (has_comparator_) {
    jw << "Comparator" << comparator_;
  }
  if (has_log_number_) {
    jw << "LogNumber" << log_number_;
  }
  if (has_prev_log_number_) {
    jw << "PrevLogNumber" << prev_log_number_;
  }
  if (has_next_file_number_) {
    jw << "NextFileNumber" << next_file_number_;
  }
  if (has_max_column_family_) {
    jw << "MaxColumnFamily" << max_column_family_;
  }
  if (has_min_log_number_to_keep_) {
    jw << "MinLogNumberToKeep" << min_log_number_to_keep_;
  }
  if (has_last_sequence_) {
    jw << "LastSeq" << last_sequence_;
  }

  if (!deleted_files_.empty()) {
    jw << "DeletedFiles";
    jw.StartArray();

    for (const auto& deleted_file : deleted_files_) {
      jw.StartArrayedObject();
      jw << "Level" << deleted_file.first;
      jw << "FileNumber" << deleted_file.second;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!new_files_.empty()) {
    jw << "AddedFiles";
    jw.StartArray();

    for (size_t i = 0; i < new_files_.size(); i++) {
      jw.StartArrayedObject();
      jw << "Level" << new_files_[i].first;
      const FileMetaData& f = new_files_[i].second;
      jw << "FileNumber" << f.fd.GetNumber();
      jw << "FileSize" << f.fd.GetFileSize();
      jw << "SmallestIKey" << f.smallest.DebugString(hex_key);
      jw << "LargestIKey" << f.largest.DebugString(hex_key);
      if (f.min_timestamp != kDisableUserTimestamp) {
        assert(f.max_timestamp != kDisableUserTimestamp);
        jw << "MinTimestamp" << Slice(f.min_timestamp).ToString(true);
        jw << "MaxTimestamp" << Slice(f.max_timestamp).ToString(true);
      }
      jw << "OldestAncesterTime" << f.oldest_ancester_time;
      jw << "FileCreationTime" << f.file_creation_time;
      jw << "FileChecksum" << Slice(f.file_checksum).ToString(true);
      jw << "FileChecksumFuncName" << f.file_checksum_func_name;
      if (f.temperature != Temperature::kUnknown) {
        jw << "temperature" << ToString(static_cast<int>(f.temperature));
      }
      if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
        jw << "OldestBlobFile" << f.oldest_blob_file_number;
      }
      if (f.temperature != Temperature::kUnknown) {
        // Maybe change to human readable format whenthe feature becomes
        // permanent
        jw << "Temperature" << static_cast<int>(f.temperature);
      }
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!blob_file_additions_.empty()) {
    jw << "BlobFileAdditions";

    jw.StartArray();

    for (const auto& blob_file_addition : blob_file_additions_) {
      jw.StartArrayedObject();
      jw << blob_file_addition;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!blob_file_garbages_.empty()) {
    jw << "BlobFileGarbages";

    jw.StartArray();

    for (const auto& blob_file_garbage : blob_file_garbages_) {
      jw.StartArrayedObject();
      jw << blob_file_garbage;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!wal_additions_.empty()) {
    jw << "WalAdditions";

    jw.StartArray();

    for (const auto& wal_addition : wal_additions_) {
      jw.StartArrayedObject();
      jw << wal_addition;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!wal_deletion_.IsEmpty()) {
    jw << "WalDeletion";
    jw.StartObject();
    jw << wal_deletion_;
    jw.EndObject();
  }

  jw << "ColumnFamily" << column_family_;

  if (is_column_family_add_) {
    jw << "ColumnFamilyAdd" << column_family_name_;
  }
  if (is_column_family_drop_) {
    jw << "ColumnFamilyDrop" << column_family_name_;
  }
  if (is_in_atomic_group_) {
    jw << "AtomicGroup" << remaining_entries_;
  }

  if (HasFullHistoryTsLow()) {
    jw << "FullHistoryTsLow" << Slice(full_history_ts_low_).ToString(hex_key);
  }

  jw.EndObject();

  return jw.Get();
}

}  // namespace ROCKSDB_NAMESPACE
