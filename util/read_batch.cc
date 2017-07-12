// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// IteratorBatch::rep_ :=
//     count: fixed32
//     data: record[count]
// record :=
//     timestamp:fixed64 kIteratorValid       varint32
//     timestamp:fixed64 kIteratorSeekToFirst varint32
//     timestamp:fixed64 kIteratorSeekToLast  varint32
//     timestamp:fixed64 kIteratorSeek        varint32
//     timestamp:fixed64 kIteratorSeekForPrev varint32
//     timestamp:fixed64 kIteratorNext        varint32
//     timestamp:fixed64 kIteratorPrev        varint32
//     timestamp:fixed64 kIteratorKey         varint32
//     timestamp:fixed64 kIteratorValue       varint32
//     timestamp:fixed64 kIteratorStatus      varint32
//     timestamp:fixed64 kIteratorGetProperty varint32
//
// ReadBatch holds a collection of reads to apply sequentially to a DB.
// Similar to write_batch
//
// ReadBatch::rep_ :=
//     count: fixed32 (always 1 for now)
//     data: record[count]
// record :=
//     timestamp:fixed64 kIteratorValid  varint32 varstring
//     timestamp:fixed64 kNewIterator varint32 varint32
//
// TraceableOp is an operation applied to db
//
// TraceableOp::rep_ :=
//     timestamp:fixed64 varint32

#include "util/read_batch.h"

#include "db/db_impl.h"
#include "util/coding.h"

namespace rocksdb {

namespace {

enum ReadOpType : unsigned char {
  kGet = 0,
  kNewIterator,
};

enum IteratorOpType : unsigned char {
  kIteratorValid,
  kIteratorSeekToFirst,
  kIteratorSeekToLast,
  kIteratorSeek,
  kIteratorSeekForPrev,
  kIteratorNext,
  kIteratorPrev,
  kIteratorKey,
  kIteratorValue,
  kIteratorStatus,
  kIteratorGetProperty,
};

}  // end anonymous namespace

// ReadBatchBase
ReadBatchBase::ReadBatchBase(size_t reserved_bytes) {
  rep_.reserve((reserved_bytes > kHeader) ? reserved_bytes : kHeader);
  rep_.resize(kHeader);
}

ReadBatchBase::~ReadBatchBase() {}

int ReadBatchBase::Count() { return DecodeFixed32(rep_.data()); }

void ReadBatchBase::SetCount(int n) { EncodeFixed32(&rep_[0], n); }

// ReadBatch
void ReadBatch::Get(uint32_t column_family_id, const Slice& key) {
  SetCount(Count() + 1);
  rep_.push_back(static_cast<unsigned char>(kGet));
  PutVarint32(&rep_, column_family_id);
  PutLengthPrefixedSlice(&rep_, key);
}

void ReadBatch::NewIterator(uint32_t column_family_id, uint32_t iter_id) {
  SetCount(Count() + 1);
  rep_.push_back(static_cast<unsigned char>(kNewIterator));
  PutVarint32(&rep_, column_family_id);
  PutVarint32(&rep_, iter_id);
}

Status ReadBatch::Execute(
    DBImpl* db,
    std::unordered_map<uint32_t, ColumnFamilyHandle*>& handle_map) const {
  Slice input(rep_);
  if (input.size() < ReadBatch::kHeader) {
    return Status::Corruption("malformed ReadBatch (too small)");
  }
  Status s;
  uint32_t column_family_id, iter_id;
  Slice key;
  PinnableSlice value;
  while (s.ok() && !input.empty()) {
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kGet: {
        if (!GetVarint32(&input, &column_family_id) ||
            !GetLengthPrefixedSlice(&input, &key)) {
          return Status::Corruption("bad ReadBatch Get");
        }
        if (handle_map.find(column_family_id) == handle_map.end()) {
          return Status::Corruption("bad ReadBatch Column Family ID");
        }
        s = db->Get(ReadOptions(), handle_map[column_family_id], key, &value);
        break;
      }
      case kNewIterator: {
        if (!GetVarint32(&input, &column_family_id) ||
            !GetVarint32(&input, &iter_id)) {
          return Status::Corruption("bad ReadBatch Get");
        }
        if (handle_map.find(column_family_id) == handle_map.end()) {
          return Status::Corruption("bad ReadBatch Column Family ID");
        }
        auto iter =
            db->NewIterator(ReadOptions(), handle_map[column_family_id]);
        delete iter;
        break;
      }
      default:
        break;
    }
  }
  return s;
}

// IteraterBatch
void IteratorBatch::Valid(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorValid));
}

void IteratorBatch::SeekToFirst(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorSeekToFirst));
}

void IteratorBatch::SeekToLast(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorSeekToLast));
}

void IteratorBatch::Seek(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorSeek));
}

void IteratorBatch::SeekForPrev(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorSeekForPrev));
}

void IteratorBatch::Next(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorNext));
}

void IteratorBatch::Prev(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorPrev));
}

void IteratorBatch::Key(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorKey));
}

void IteratorBatch::Value(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorValue));
}

void IteratorBatch::Status(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorStatus));
}

void IteratorBatch::GetProperty(uint64_t timestamp) {
  SetCount(Count() + 1);
  PutFixed64(&rep_, timestamp);
  rep_.push_back(static_cast<unsigned char>(kIteratorGetProperty));
}

}  // namespace rocksdb
