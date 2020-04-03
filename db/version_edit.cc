//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
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
};

// Mask for an identified tag from the future which can be safely ignored.
uint32_t kTagSafeIgnoreMask = 1 << 13;

enum CustomTag : uint32_t {
  kTerminate = 1,  // The end of customized fields
  kNeedCompaction = 2,
  // Since Manifest is not entirely currently forward-compatible, and the only
  // forward-compatible part is the CutsomtTag of kNewFile, we currently encode
  // kMinLogNumberToKeep as part of a CustomTag as a hack. This should be
  // removed when manifest becomes forward-comptabile.
  kMinLogNumberToKeepHack = 3,
  kPathId = 65,
  // add earliest_ref field in Edit record
  kIndirectRef0 = 70,  // indicates the oldest ref to VLog in this file
  kValueLogStats = 71,  // files, frag, and bytes deleted/added
  kAvgFileRef = 72,  // indicator of age in VLog of overlapping files in next level
};
// If this bit for the custom tag is set, opening DB should fail if
// we don't know this field.
uint32_t kCustomTagNonSafeIgnoreMask = 1 << 6;

uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id) {
  assert(number <= kFileNumberMask);
  return number | (path_id * (kFileNumberMask + 1));
}

void VersionEdit::Clear() {
  comparator_.clear();
  max_level_ = 0;
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  max_column_family_ = 0;
  min_log_number_to_keep_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  has_max_column_family_ = false;
  has_min_log_number_to_keep_ = false;
  deleted_files_.clear();
  new_files_.clear();
  column_family_ = 0;
  is_column_family_add_ = 0;
  is_column_family_drop_ = 0;
  column_family_name_.clear();
  is_in_atomic_group_ = false;
  remaining_entries_ = 0;
  vlog_additions.clear();  // no additions to rings
}

bool VersionEdit::EncodeTo(std::string* dst) const {
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
  if (has_last_sequence_) {
    PutVarint32Varint64(dst, kLastSequence, last_sequence_);
  }
  if (has_max_column_family_) {
    PutVarint32Varint32(dst, kMaxColumnFamily, max_column_family_);
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
    bool has_customized_fields = false;
    if (f.marked_for_compaction || has_min_log_number_to_keep_ ||
        f.indirect_ref_0.size() || f.avgparentfileno) {
      PutVarint32(dst, kNewFile4);
      has_customized_fields = true;
    } else if (f.fd.GetPathId() == 0) {
      // Use older format to make sure user can roll back the build if they
      // don't config multiple DB paths.
      PutVarint32(dst, kNewFile2);
    } else {
      PutVarint32(dst, kNewFile3);
    }
    PutVarint32Varint64(dst, new_files_[i].first /* level */, f.fd.GetNumber());
    if (f.fd.GetPathId() != 0 && !has_customized_fields) {
      // kNewFile3
      PutVarint32(dst, f.fd.GetPathId());
    }
    PutVarint64(dst, f.fd.GetFileSize());
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
    PutVarint64Varint64(dst, f.fd.smallest_seqno, f.fd.largest_seqno);
    if (has_customized_fields) {
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
      //   tag kIndirectRef0:
      //        varint32  length of rest of record
      //        varint32 n - the number of rings for this cf
      //        varint64[n] earliest file referred to in each ring
      //   tag kValueLogStats
      //        varint32  length of rest of record (not used)
      //        varint32 b - number of rings for this cf
      //      for each ring:
      //        varint64 number of bytes added to size
      //        varint64 number of bytes of fragmentation added - may be negative
      //        varint32 m - number of file edits
      //        varint64[m]  file edits, a sequence of closed intervals of
      //          start,end file numbers.  If first start=0 it means delete
      //          files up to end
      //
      if (f.fd.GetPathId() != 0) {
        PutVarint32(dst, CustomTag::kPathId);
        char p = static_cast<char>(f.fd.GetPathId());
        PutLengthPrefixedSlice(dst, Slice(&p, 1));
      }
      if (f.marked_for_compaction) {
        PutVarint32(dst, CustomTag::kNeedCompaction);
        char p = static_cast<char>(1);
        PutLengthPrefixedSlice(dst, Slice(&p, 1));
      }
      if (has_min_log_number_to_keep_ && !min_log_num_written) {
        PutVarint32(dst, CustomTag::kMinLogNumberToKeepHack);
        std::string varint_log_number;
        PutFixed64(&varint_log_number, min_log_number_to_keep_);
        PutLengthPrefixedSlice(dst, Slice(varint_log_number));
        min_log_num_written = true;
      }
      if (f.indirect_ref_0.size()) {
	// write out the record type
        PutVarint32(dst, CustomTag::kIndirectRef0);
	// init length to length of # refs
        uint32_t totallength = VarintLength(f.indirect_ref_0.size());
	// add in length of refs themselves
        for(auto ref : f.indirect_ref_0)totallength += VarintLength(ref);
        PutVarint32(dst, totallength);  // write out total field length
	// write out number of refs
        PutVarint32(dst, (uint32_t)f.indirect_ref_0.size());
        for(auto ref : f.indirect_ref_0)PutVarint64(dst, ref);  // write refs
      }
      if (f.avgparentfileno) {
	// write out the record type
        PutVarint32(dst, CustomTag::kAvgFileRef);
	// write out total field length
        PutVarint32(dst, VarintLength(f.avgparentfileno));
	// write out the data: average file position
        PutVarint64(dst, f.avgparentfileno);
      }
      TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:NewFile4:CustomizeFields",
                               dst);

      PutVarint32(dst, CustomTag::kTerminate);
    }
  }

  // 0 is default and does not need to be explicitly written
  if (column_family_ != 0) {
    PutVarint32Varint32(dst, kColumnFamily, column_family_);
  }

  // write the CF information for indirect values: the active file numbers for
  // each ring, and the amount of fragmentation in those file
  if(vlog_additions.size()) {   // if vlog changes are marked...
    // Build up the record
    PutVarint32(dst, CustomTag::kValueLogStats);  // write out the record type
    std::string totalrcd;  // where we build up the whole thing
    // start with # of structs
    PutVarint32(&totalrcd, (uint32_t)vlog_additions.size());
    for(auto add : vlog_additions) {   // for each addition
      std::string addstring;  // place to build a single struct
      // NOTE size and frag are signed; we cast to unsigned for the interface
      PutVarint64(&addstring, (uint64_t)add.size);  // bytes added
      PutVarint64(&addstring, (uint64_t)add.frag);   // frag added
      // # files-added values
      PutVarint32(&addstring, (uint32_t)add.valid_files.size());
      // files-added intervals
      for(auto fno : add.valid_files)PutVarint64(&addstring, (uint64_t)fno);
      totalrcd.append(addstring);  // accumulate all the rings
    }
    PutVarint32(dst, (uint32_t)totalrcd.size());  // write out the record length
    dst->append(totalrcd);  // write out the accumulated record
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
  uint32_t v;
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
  int level;
  FileMetaData f;
  uint64_t number;
  uint32_t path_id = 0;
  uint64_t file_size;
  SequenceNumber smallest_seqno;
  SequenceNumber largest_seqno;
  // Since this is the only forward-compatible part of the code, we hack new
  // extension into this record. When we do, we set this boolean to distinguish
  // the record from the normal NewFile records.
  if (GetLevel(input, &level, &msg) && GetVarint64(input, &number) &&
      GetVarint64(input, &file_size) && GetInternalKey(input, &f.smallest) &&
      GetInternalKey(input, &f.largest) &&
      GetVarint64(input, &smallest_seqno) &&
      GetVarint64(input, &largest_seqno)) {
    // See comments in VersionEdit::EncodeTo() for format of customized fields
    while (true) {
      uint32_t custom_tag;
      Slice field;
      if (!GetVarint32(input, &custom_tag)) {
        return "new-file4 custom field";
      }
      if (custom_tag == kTerminate) {
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
        case kIndirectRef0:
          uint32_t nrings;   // number of rings in this field
          if(!GetVarint32(&field,&nrings))return("indirect ref no # values");
          f.indirect_ref_0.clear();  // empty the return area
          uint64_t earlyref;  // place to decode the ref # for each ring
          while(nrings--){
            if(!GetVarint64(&field,&earlyref))return("indirect ref no value");
            f.indirect_ref_0.push_back(earlyref);
          }
          f.level = level;  // If there's a ref0, there needs to be a level too
          break;
        case kAvgFileRef:
          uint64_t avgparentfileno;   // number of rings in this field
          if(!GetVarint64(&field,&avgparentfileno))return("avg parent ref no value");
          f.avgparentfileno = avgparentfileno;  // install into meta
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
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
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

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
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
        uint64_t number;
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
        uint64_t number;
        uint64_t file_size;
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
        uint64_t number;
        uint64_t file_size;
        SequenceNumber smallest_seqno;
        SequenceNumber largest_seqno;
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
        uint64_t number;
        uint32_t path_id;
        uint64_t file_size;
        SequenceNumber smallest_seqno;
        SequenceNumber largest_seqno;
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
      case kValueLogStats:
        {
        vlog_additions.clear();   // init to no rings
        uint32_t nrings;
        GetVarint32(&input, &nrings);   // read & discard record length
        // read # rings; for each ring...
        for(GetVarint32(&input, &nrings); nrings; --nrings) {
          uint64_t size; GetVarint64(&input, &size);  // get size
          uint64_t frag; GetVarint64(&input, &frag);   // get frag
          std::vector<uint64_t> files;   // get vector of files
          uint32_t nfiles;
          // read # files.  for each...
          for(GetVarint32(&input, &nfiles); nfiles; --nfiles) {
            uint64_t fileno;
	    GetVarint64(&input, &fileno);
	    files.push_back(fileno);  // append file#
          }
	  // create a new ring entry.  sizes must be signed, even though the
	  // file interface demands unsigned
          vlog_additions.emplace_back(files,(int64_t)size,(int64_t)frag);
        }
        break;
        }

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
  if (has_min_log_number_to_keep_) {
    r.append("\n  MinLogNumberToKeep: ");
    AppendNumberTo(&r, min_log_number_to_keep_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
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
  if (has_max_column_family_) {
    r.append("\n  MaxColumnFamily: ");
    AppendNumberTo(&r, max_column_family_);
  }
  if (is_in_atomic_group_) {
    r.append("\n  AtomicGroup: ");
    AppendNumberTo(&r, remaining_entries_);
    r.append(" entries remains");
  }
  r.append("\n}\n");
  return r;
}

std::string VersionEdit::DebugJSON(int edit_num, bool hex_key) const {
  JSONWriter jw;
  jw << "EditNumber" << edit_num;

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
  if (has_last_sequence_) {
    jw << "LastSeq" << last_sequence_;
  }
  if(vlog_additions.size()){
    jw << "VLogAdditions";
    jw.StartArray();

    for (size_t i = 0; i < vlog_additions.size(); i++) {
      jw.StartArrayedObject();
      jw << "AddedBytes" << vlog_additions[i].size;
      jw << "AddedFrag" << vlog_additions[i].frag;
      jw << "RingFiles";
      jw.StartArray();
      for (size_t j = 0; j < vlog_additions[i].valid_files.size(); j++) {
        jw <<vlog_additions[i].valid_files[j];
      }
      jw.EndArray();
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!deleted_files_.empty()) {
    jw << "DeletedFiles";
    jw.StartArray();

    for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
         iter != deleted_files_.end();
         ++iter) {
      jw.StartArrayedObject();
      jw << "Level" << iter->first;
      jw << "FileNumber" << iter->second;
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
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  jw << "ColumnFamily" << column_family_;

  if (is_column_family_add_) {
    jw << "ColumnFamilyAdd" << column_family_name_;
  }
  if (is_column_family_drop_) {
    jw << "ColumnFamilyDrop" << column_family_name_;
  }
  if (has_max_column_family_) {
    jw << "MaxColumnFamily" << max_column_family_;
  }
  if (has_min_log_number_to_keep_) {
    jw << "MinLogNumberToKeep" << min_log_number_to_keep_;
  }
  if (is_in_atomic_group_) {
    jw << "AtomicGroup" << remaining_entries_;
  }

  jw.EndObject();

  return jw.Get();
}

// After the last kv has been written to the file, install the earliest refs
// that were found in the file, one for each ring (0 means no ref).  Also
// install other stuff needed in a new file: vlog pointer and level.  Files
// created during compaction go through AddFile, but not detailed creation
// therein
void FileMetaData::InstallRef0(int outputlevel,
    const std::vector<uint64_t> &earliestref,
    ColumnFamilyData *cfd) {
  indirect_ref_0 = earliestref;
  // keep the chain fields lockstep in size with indirect_ref
  ringfwdchain.resize(earliestref.size(),nullptr);
  ringbwdchain.resize(earliestref.size(),nullptr);
  vlog = cfd->vlog();
  level = outputlevel;
}

}  // namespace rocksdb
