//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeDeletion varstring
//    kTypeSingleDeletion varstring
//    kTypeMerge varstring varstring
//    kTypeColumnFamilyValue varint32 varstring varstring
//    kTypeColumnFamilyDeletion varint32 varstring varstring
//    kTypeColumnFamilySingleDeletion varint32 varstring varstring
//    kTypeColumnFamilyMerge varint32 varstring varstring
//    kTypeBeginPrepareXID varstring
//    kTypeEndPrepareXID
//    kTypeCommitXID varstring
//    kTypeRollbackXID varstring
//    kTypeNoop
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "rocksdb/write_batch.h"

#include <map>
#include <stack>
#include <stdexcept>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/snapshot_impl.h"
#include "db/write_batch_internal.h"
#include "rocksdb/merge_operator.h"
#include "util/coding.h"
#include "util/perf_context_imp.h"
#include "util/statistics.h"

namespace rocksdb {

// anon namespace for file-local types
namespace {

enum ContentFlags : uint32_t {
  DEFERRED = 1 << 0,
  HAS_PUT = 1 << 1,
  HAS_DELETE = 1 << 2,
  HAS_SINGLE_DELETE = 1 << 3,
  HAS_MERGE = 1 << 4,
  HAS_BEGIN_PREPARE = 1 << 5,
  HAS_END_PREPARE = 1 << 6,
  HAS_COMMIT = 1 << 7,
  HAS_ROLLBACK = 1 << 8,
};

struct BatchContentClassifier : public WriteBatch::Handler {
  uint32_t content_flags = 0;

  Status PutCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_PUT;
    return Status::OK();
  }

  Status DeleteCF(uint32_t, const Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE;
    return Status::OK();
  }

  Status SingleDeleteCF(uint32_t, const Slice&) override {
    content_flags |= ContentFlags::HAS_SINGLE_DELETE;
    return Status::OK();
  }

  Status MergeCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_MERGE;
    return Status::OK();
  }

  Status MarkBeginPrepare() override {
    content_flags |= ContentFlags::HAS_BEGIN_PREPARE;
    return Status::OK();
  }

  Status MarkEndPrepare(const Slice&) override {
    content_flags |= ContentFlags::HAS_END_PREPARE;
    return Status::OK();
  }

  Status MarkCommit(const Slice&) override {
    content_flags |= ContentFlags::HAS_COMMIT;
    return Status::OK();
  }

  Status MarkRollback(const Slice&) override {
    content_flags |= ContentFlags::HAS_ROLLBACK;
    return Status::OK();
  }
};

}  // anon namespace


struct SavePoint {
  size_t size;  // size of rep_
  int count;    // count of elements in rep_
  uint32_t content_flags;
};

struct SavePoints {
  std::stack<SavePoint> stack;
};

WriteBatch::WriteBatch(size_t reserved_bytes)
    : save_points_(nullptr), content_flags_(0), rep_() {
  rep_.reserve((reserved_bytes > WriteBatchInternal::kHeader) ?
    reserved_bytes : WriteBatchInternal::kHeader);
  rep_.resize(WriteBatchInternal::kHeader);
}

WriteBatch::WriteBatch(const std::string& rep)
    : save_points_(nullptr),
      content_flags_(ContentFlags::DEFERRED),
      rep_(rep) {}

WriteBatch::WriteBatch(const WriteBatch& src)
    : save_points_(src.save_points_),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)),
      rep_(src.rep_) {}

WriteBatch::WriteBatch(WriteBatch&& src)
    : save_points_(std::move(src.save_points_)),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)),
      rep_(std::move(src.rep_)) {}

WriteBatch& WriteBatch::operator=(const WriteBatch& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(src);
  }
  return *this;
}

WriteBatch& WriteBatch::operator=(WriteBatch&& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(std::move(src));
  }
  return *this;
}

WriteBatch::~WriteBatch() { delete save_points_; }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Handler::LogData(const Slice& blob) {
  // If the user has not specified something to do with blobs, then we ignore
  // them.
}

bool WriteBatch::Handler::Continue() {
  return true;
}

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(WriteBatchInternal::kHeader);

  content_flags_.store(0, std::memory_order_relaxed);

  if (save_points_ != nullptr) {
    while (!save_points_->stack.empty()) {
      save_points_->stack.pop();
    }
  }
}

int WriteBatch::Count() const {
  return WriteBatchInternal::Count(this);
}

uint32_t WriteBatch::ComputeContentFlags() const {
  auto rv = content_flags_.load(std::memory_order_relaxed);
  if ((rv & ContentFlags::DEFERRED) != 0) {
    BatchContentClassifier classifier;
    Iterate(&classifier);
    rv = classifier.content_flags;

    // this method is conceptually const, because it is performing a lazy
    // computation that doesn't affect the abstract state of the batch.
    // content_flags_ is marked mutable so that we can perform the
    // following assignment
    content_flags_.store(rv, std::memory_order_relaxed);
  }
  return rv;
}

bool WriteBatch::HasPut() const {
  return (ComputeContentFlags() & ContentFlags::HAS_PUT) != 0;
}

bool WriteBatch::HasDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE) != 0;
}

bool WriteBatch::HasSingleDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_SINGLE_DELETE) != 0;
}

bool WriteBatch::HasMerge() const {
  return (ComputeContentFlags() & ContentFlags::HAS_MERGE) != 0;
}

bool ReadKeyFromWriteBatchEntry(Slice* input, Slice* key, bool cf_record) {
  assert(input != nullptr && key != nullptr);
  // Skip tag byte
  input->remove_prefix(1);

  if (cf_record) {
    // Skip column_family bytes
    uint32_t cf;
    if (!GetVarint32(input, &cf)) {
      return false;
    }
  }

  // Extract key
  return GetLengthPrefixedSlice(input, key);
}

bool WriteBatch::HasBeginPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_BEGIN_PREPARE) != 0;
}

bool WriteBatch::HasEndPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_END_PREPARE) != 0;
}

bool WriteBatch::HasCommit() const {
  return (ComputeContentFlags() & ContentFlags::HAS_COMMIT) != 0;
}

bool WriteBatch::HasRollback() const {
  return (ComputeContentFlags() & ContentFlags::HAS_ROLLBACK) != 0;
}

Status ReadRecordFromWriteBatch(Slice* input, char* tag,
                                uint32_t* column_family, Slice* key,
                                Slice* value, Slice* blob, Slice* xid) {
  assert(key != nullptr && value != nullptr);
  *tag = (*input)[0];
  input->remove_prefix(1);
  *column_family = 0;  // default
  switch (*tag) {
    case kTypeColumnFamilyValue:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Put");
      }
    // intentional fallthrough
    case kTypeValue:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch Put");
      }
      break;
    case kTypeColumnFamilyDeletion:
    case kTypeColumnFamilySingleDeletion:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Delete");
      }
    // intentional fallthrough
    case kTypeDeletion:
    case kTypeSingleDeletion:
      if (!GetLengthPrefixedSlice(input, key)) {
        return Status::Corruption("bad WriteBatch Delete");
      }
      break;
    case kTypeColumnFamilyMerge:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Merge");
      }
    // intentional fallthrough
    case kTypeMerge:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch Merge");
      }
      break;
    case kTypeLogData:
      assert(blob != nullptr);
      if (!GetLengthPrefixedSlice(input, blob)) {
        return Status::Corruption("bad WriteBatch Blob");
      }
      break;
    case kTypeNoop:
    case kTypeBeginPrepareXID:
      break;
    case kTypeEndPrepareXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad EndPrepare XID");
      }
      break;
    case kTypeCommitXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad Commit XID");
      }
      break;
    case kTypeRollbackXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad Rollback XID");
      }
      break;
    default:
      return Status::Corruption("unknown WriteBatch tag");
  }
  return Status::OK();
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < WriteBatchInternal::kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(WriteBatchInternal::kHeader);
  Slice key, value, blob, xid;
  int found = 0;
  Status s;
  while (s.ok() && !input.empty() && handler->Continue()) {
    char tag = 0;
    uint32_t column_family = 0;  // default

    s = ReadRecordFromWriteBatch(&input, &tag, &column_family, &key, &value,
                                 &blob, &xid);
    if (!s.ok()) {
      return s;
    }

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_PUT));
        s = handler->PutCF(column_family, key, value);
        found++;
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_DELETE));
        s = handler->DeleteCF(column_family, key);
        found++;
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_SINGLE_DELETE));
        s = handler->SingleDeleteCF(column_family, key);
        found++;
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_MERGE));
        s = handler->MergeCF(column_family, key, value);
        found++;
        break;
      case kTypeLogData:
        handler->LogData(blob);
        break;
      case kTypeBeginPrepareXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BEGIN_PREPARE));
        handler->MarkBeginPrepare();
        break;
      case kTypeEndPrepareXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_END_PREPARE));
        handler->MarkEndPrepare(xid);
        break;
      case kTypeCommitXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_COMMIT));
        handler->MarkCommit(xid);
        break;
      case kTypeRollbackXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_ROLLBACK));
        handler->MarkRollback(xid);
        break;
      case kTypeNoop:
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (!s.ok()) {
    return s;
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

size_t WriteBatchInternal::GetFirstOffset(WriteBatch* b) {
  return WriteBatchInternal::kHeader;
}

void WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                             const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);
}

void WriteBatch::Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) {
  WriteBatchInternal::Put(this, GetColumnFamilyID(column_family), key, value);
}

void WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                             const SliceParts& key, const SliceParts& value) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  PutLengthPrefixedSliceParts(&b->rep_, value);
  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);
}

void WriteBatch::Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                     const SliceParts& value) {
  WriteBatchInternal::Put(this, GetColumnFamilyID(column_family), key, value);
}

void WriteBatchInternal::InsertNoop(WriteBatch* b) {
  b->rep_.push_back(static_cast<char>(kTypeNoop));
}

void WriteBatchInternal::MarkEndPrepare(WriteBatch* b, const Slice& xid) {
  // a manually constructed batch can only contain one prepare section
  assert(b->rep_[12] == static_cast<char>(kTypeNoop));

  // all savepoints up to this point are cleared
  if (b->save_points_ != nullptr) {
    while (!b->save_points_->stack.empty()) {
      b->save_points_->stack.pop();
    }
  }

  // rewrite noop as begin marker
  b->rep_[12] = static_cast<char>(kTypeBeginPrepareXID);
  b->rep_.push_back(static_cast<char>(kTypeEndPrepareXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_END_PREPARE |
                              ContentFlags::HAS_BEGIN_PREPARE,
                          std::memory_order_relaxed);
}

void WriteBatchInternal::MarkCommit(WriteBatch* b, const Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeCommitXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_COMMIT,
                          std::memory_order_relaxed);
}

void WriteBatchInternal::MarkRollback(WriteBatch* b, const Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeRollbackXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_ROLLBACK,
                          std::memory_order_relaxed);
}

void WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                const Slice& key) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);
}

void WriteBatch::Delete(ColumnFamilyHandle* column_family, const Slice& key) {
  WriteBatchInternal::Delete(this, GetColumnFamilyID(column_family), key);
}

void WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                const SliceParts& key) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);
}

void WriteBatch::Delete(ColumnFamilyHandle* column_family,
                        const SliceParts& key) {
  WriteBatchInternal::Delete(this, GetColumnFamilyID(column_family), key);
}

void WriteBatchInternal::SingleDelete(WriteBatch* b, uint32_t column_family_id,
                                      const Slice& key) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeSingleDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilySingleDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);
}

void WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                              const Slice& key) {
  WriteBatchInternal::SingleDelete(this, GetColumnFamilyID(column_family), key);
}

void WriteBatchInternal::SingleDelete(WriteBatch* b, uint32_t column_family_id,
                                      const SliceParts& key) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeSingleDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilySingleDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);
}

void WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                              const SliceParts& key) {
  WriteBatchInternal::SingleDelete(this, GetColumnFamilyID(column_family), key);
}

void WriteBatchInternal::Merge(WriteBatch* b, uint32_t column_family_id,
                               const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeMerge));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyMerge));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);
}

void WriteBatch::Merge(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) {
  WriteBatchInternal::Merge(this, GetColumnFamilyID(column_family), key, value);
}

void WriteBatchInternal::Merge(WriteBatch* b, uint32_t column_family_id,
                               const SliceParts& key,
                               const SliceParts& value) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeMerge));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyMerge));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  PutLengthPrefixedSliceParts(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);
}

void WriteBatch::Merge(ColumnFamilyHandle* column_family,
                       const SliceParts& key,
                       const SliceParts& value) {
  WriteBatchInternal::Merge(this, GetColumnFamilyID(column_family),
                            key, value);
}

void WriteBatch::PutLogData(const Slice& blob) {
  rep_.push_back(static_cast<char>(kTypeLogData));
  PutLengthPrefixedSlice(&rep_, blob);
}

void WriteBatch::SetSavePoint() {
  if (save_points_ == nullptr) {
    save_points_ = new SavePoints();
  }
  // Record length and count of current batch of writes.
  save_points_->stack.push(SavePoint{
      GetDataSize(), Count(), content_flags_.load(std::memory_order_relaxed)});
}

Status WriteBatch::RollbackToSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  SavePoint savepoint = save_points_->stack.top();
  save_points_->stack.pop();

  assert(savepoint.size <= rep_.size());
  assert(savepoint.count <= Count());

  if (savepoint.size == rep_.size()) {
    // No changes to rollback
  } else if (savepoint.size == 0) {
    // Rollback everything
    Clear();
  } else {
    rep_.resize(savepoint.size);
    WriteBatchInternal::SetCount(this, savepoint.count);
    content_flags_.store(savepoint.content_flags, std::memory_order_relaxed);
  }

  return Status::OK();
}

class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  ColumnFamilyMemTables* const cf_mems_;
  FlushScheduler* const flush_scheduler_;
  const bool ignore_missing_column_families_;
  const uint64_t recovering_log_number_;
  // log number that all Memtables inserted into should reference
  uint64_t log_number_ref_;
  DBImpl* db_;
  const bool concurrent_memtable_writes_;
  bool* has_valid_writes_;
  typedef std::map<MemTable*, MemTablePostProcessInfo> MemPostInfoMap;
  MemPostInfoMap mem_post_info_map_;
  // current recovered transaction we are rebuilding (recovery)
  WriteBatch* rebuilding_trx_;

  // cf_mems should not be shared with concurrent inserters
  MemTableInserter(SequenceNumber sequence, ColumnFamilyMemTables* cf_mems,
                   FlushScheduler* flush_scheduler,
                   bool ignore_missing_column_families,
                   uint64_t recovering_log_number, DB* db,
                   bool concurrent_memtable_writes,
                   bool* has_valid_writes = nullptr)
      : sequence_(sequence),
        cf_mems_(cf_mems),
        flush_scheduler_(flush_scheduler),
        ignore_missing_column_families_(ignore_missing_column_families),
        recovering_log_number_(recovering_log_number),
        log_number_ref_(0),
        db_(reinterpret_cast<DBImpl*>(db)),
        concurrent_memtable_writes_(concurrent_memtable_writes),
        has_valid_writes_(has_valid_writes),
        rebuilding_trx_(nullptr) {
    assert(cf_mems_);
  }

  void set_log_number_ref(uint64_t log) { log_number_ref_ = log; }

  SequenceNumber get_final_sequence() { return sequence_; }

  void PostProcess() {
    for (auto& pair : mem_post_info_map_) {
      pair.first->BatchPostProcess(pair.second);
    }
  }

  bool SeekToColumnFamily(uint32_t column_family_id, Status* s) {
    // If we are in a concurrent mode, it is the caller's responsibility
    // to clone the original ColumnFamilyMemTables so that each thread
    // has its own instance.  Otherwise, it must be guaranteed that there
    // is no concurrent access
    bool found = cf_mems_->Seek(column_family_id);
    if (!found) {
      if (ignore_missing_column_families_) {
        *s = Status::OK();
      } else {
        *s = Status::InvalidArgument(
            "Invalid column family specified in write batch");
      }
      return false;
    }
    if (recovering_log_number_ != 0 &&
        recovering_log_number_ < cf_mems_->GetLogNumber()) {
      // This is true only in recovery environment (recovering_log_number_ is
      // always 0 in
      // non-recovery, regular write code-path)
      // * If recovering_log_number_ < cf_mems_->GetLogNumber(), this means that
      // column
      // family already contains updates from this log. We can't apply updates
      // twice because of update-in-place or merge workloads -- ignore the
      // update
      *s = Status::OK();
      return false;
    }

    if (has_valid_writes_ != nullptr) {
      *has_valid_writes_ = true;
    }

    if (log_number_ref_ > 0) {
      cf_mems_->GetMemTable()->RefLogContainingPrepSection(log_number_ref_);
    }

    return true;
  }

  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                       const Slice& value) override {
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::Put(rebuilding_trx_, column_family_id, key, value);
      return Status::OK();
    }

    Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }

    MemTable* mem = cf_mems_->GetMemTable();
    auto* moptions = mem->GetMemTableOptions();
    if (!moptions->inplace_update_support) {
      mem->Add(sequence_, kTypeValue, key, value, concurrent_memtable_writes_,
               get_post_process_info(mem));
    } else if (moptions->inplace_callback == nullptr) {
      assert(!concurrent_memtable_writes_);
      mem->Update(sequence_, key, value);
      RecordTick(moptions->statistics, NUMBER_KEYS_UPDATED);
    } else {
      assert(!concurrent_memtable_writes_);
      if (mem->UpdateCallback(sequence_, key, value)) {
      } else {
        // key not found in memtable. Do sst get, update, add
        SnapshotImpl read_from_snapshot;
        read_from_snapshot.number_ = sequence_;
        ReadOptions ropts;
        ropts.snapshot = &read_from_snapshot;

        std::string prev_value;
        std::string merged_value;

        auto cf_handle = cf_mems_->GetColumnFamilyHandle();
        if (cf_handle == nullptr) {
          cf_handle = db_->DefaultColumnFamily();
        }
        Status s = db_->Get(ropts, cf_handle, key, &prev_value);

        char* prev_buffer = const_cast<char*>(prev_value.c_str());
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());
        auto status = moptions->inplace_callback(s.ok() ? prev_buffer : nullptr,
                                                 s.ok() ? &prev_size : nullptr,
                                                 value, &merged_value);
        if (status == UpdateStatus::UPDATED_INPLACE) {
          // prev_value is updated in-place with final value.
          mem->Add(sequence_, kTypeValue, key, Slice(prev_buffer, prev_size));
          RecordTick(moptions->statistics, NUMBER_KEYS_WRITTEN);
        } else if (status == UpdateStatus::UPDATED) {
          // merged_value contains the final value.
          mem->Add(sequence_, kTypeValue, key, Slice(merged_value));
          RecordTick(moptions->statistics, NUMBER_KEYS_WRITTEN);
        }
      }
    }
    // Since all Puts are logged in trasaction logs (if enabled), always bump
    // sequence number. Even if the update eventually fails and does not result
    // in memtable add/update.
    sequence_++;
    CheckMemtableFull();
    return Status::OK();
  }

  Status DeleteImpl(uint32_t column_family_id, const Slice& key,
                    ValueType delete_type) {
    MemTable* mem = cf_mems_->GetMemTable();
    mem->Add(sequence_, delete_type, key, Slice(), concurrent_memtable_writes_,
             get_post_process_info(mem));
    sequence_++;
    CheckMemtableFull();
    return Status::OK();
  }

  virtual Status DeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
      return Status::OK();
    }

    Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }

    return DeleteImpl(column_family_id, key, kTypeDeletion);
  }

  virtual Status SingleDeleteCF(uint32_t column_family_id,
                                const Slice& key) override {
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::SingleDelete(rebuilding_trx_, column_family_id, key);
      return Status::OK();
    }

    Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }

    return DeleteImpl(column_family_id, key, kTypeSingleDeletion);
  }

  virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
    assert(!concurrent_memtable_writes_);
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::Merge(rebuilding_trx_, column_family_id, key, value);
      return Status::OK();
    }

    Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }

    MemTable* mem = cf_mems_->GetMemTable();
    auto* moptions = mem->GetMemTableOptions();
    bool perform_merge = false;

    if (moptions->max_successive_merges > 0 && db_ != nullptr) {
      LookupKey lkey(key, sequence_);

      // Count the number of successive merges at the head
      // of the key in the memtable
      size_t num_merges = mem->CountSuccessiveMergeEntries(lkey);

      if (num_merges >= moptions->max_successive_merges) {
        perform_merge = true;
      }
    }

    if (perform_merge) {
      // 1) Get the existing value
      std::string get_value;

      // Pass in the sequence number so that we also include previous merge
      // operations in the same batch.
      SnapshotImpl read_from_snapshot;
      read_from_snapshot.number_ = sequence_;
      ReadOptions read_options;
      read_options.snapshot = &read_from_snapshot;

      auto cf_handle = cf_mems_->GetColumnFamilyHandle();
      if (cf_handle == nullptr) {
        cf_handle = db_->DefaultColumnFamily();
      }
      db_->Get(read_options, cf_handle, key, &get_value);
      Slice get_value_slice = Slice(get_value);

      // 2) Apply this merge
      auto merge_operator = moptions->merge_operator;
      assert(merge_operator);

      std::string new_value;

      Status merge_status = MergeHelper::TimedFullMerge(
          merge_operator, key, &get_value_slice, {value}, &new_value,
          moptions->info_log, moptions->statistics, Env::Default());

      if (!merge_status.ok()) {
        // Failed to merge!
        // Store the delta in memtable
        perform_merge = false;
      } else {
        // 3) Add value to memtable
        mem->Add(sequence_, kTypeValue, key, new_value);
      }
    }

    if (!perform_merge) {
      // Add merge operator to memtable
      mem->Add(sequence_, kTypeMerge, key, value);
    }

    sequence_++;
    CheckMemtableFull();
    return Status::OK();
  }

  void CheckMemtableFull() {
    if (flush_scheduler_ != nullptr) {
      auto* cfd = cf_mems_->current();
      assert(cfd != nullptr);
      if (cfd->mem()->ShouldScheduleFlush() &&
          cfd->mem()->MarkFlushScheduled()) {
        // MarkFlushScheduled only returns true if we are the one that
        // should take action, so no need to dedup further
        flush_scheduler_->ScheduleFlush(cfd);
      }
    }
  }

  Status MarkBeginPrepare() override {
    assert(rebuilding_trx_ == nullptr);
    assert(db_);

    if (recovering_log_number_ != 0) {
      // during recovery we rebuild a hollow transaction
      // from all encountered prepare sections of the wal
      if (db_->allow_2pc() == false) {
        return Status::NotSupported(
            "WAL contains prepared transactions. Open with "
            "TransactionDB::Open().");
      }

      // we are now iterating through a prepared section
      rebuilding_trx_ = new WriteBatch();
      if (has_valid_writes_ != nullptr) {
        *has_valid_writes_ = true;
      }
    } else {
      // in non-recovery we ignore prepare markers
      // and insert the values directly. making sure we have a
      // log for each insertion to reference.
      assert(log_number_ref_ > 0);
    }

    return Status::OK();
  }

  Status MarkEndPrepare(const Slice& name) override {
    assert(db_);
    assert((rebuilding_trx_ != nullptr) == (recovering_log_number_ != 0));

    if (recovering_log_number_ != 0) {
      assert(db_->allow_2pc());
      db_->InsertRecoveredTransaction(recovering_log_number_, name.ToString(),
                                      rebuilding_trx_);
      rebuilding_trx_ = nullptr;
    } else {
      assert(rebuilding_trx_ == nullptr);
      assert(log_number_ref_ > 0);
    }

    return Status::OK();
  }

  Status MarkCommit(const Slice& name) override {
    assert(db_);

    Status s;

    if (recovering_log_number_ != 0) {
      // in recovery when we encounter a commit marker
      // we lookup this transaction in our set of rebuilt transactions
      // and commit.
      auto trx = db_->GetRecoveredTransaction(name.ToString());

      // the log contaiting the prepared section may have
      // been released in the last incarnation because the
      // data was flushed to L0
      if (trx != nullptr) {
        // at this point individual CF lognumbers will prevent
        // duplicate re-insertion of values.
        assert(log_number_ref_ == 0);
        // all insertes must reference this trx log number
        log_number_ref_ = trx->log_number_;
        s = trx->batch_->Iterate(this);
        log_number_ref_ = 0;

        if (s.ok()) {
          db_->DeleteRecoveredTransaction(name.ToString());
        }
        if (has_valid_writes_ != nullptr) {
          *has_valid_writes_ = true;
        }
      }
    } else {
      // in non recovery we simply ignore this tag
    }

    return s;
  }

  Status MarkRollback(const Slice& name) override {
    assert(db_);

    if (recovering_log_number_ != 0) {
      auto trx = db_->GetRecoveredTransaction(name.ToString());

      // the log containing the transactions prep section
      // may have been released in the previous incarnation
      // because we knew it had been rolled back
      if (trx != nullptr) {
        db_->DeleteRecoveredTransaction(name.ToString());
      }
    } else {
      // in non recovery we simply ignore this tag
    }

    return Status::OK();
  }

 private:
  MemTablePostProcessInfo* get_post_process_info(MemTable* mem) {
    if (!concurrent_memtable_writes_) {
      // No need to batch counters locally if we don't use concurrent mode.
      return nullptr;
    }
    return &mem_post_info_map_[mem];
  }
};

// This function can only be called in these conditions:
// 1) During Recovery()
// 2) During Write(), in a single-threaded write thread
// 3) During Write(), in a concurrent context where memtables has been cloned
// The reason is that it calls memtables->Seek(), which has a stateful cache
Status WriteBatchInternal::InsertInto(
    const autovector<WriteThread::Writer*>& writers, SequenceNumber sequence,
    ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
    bool ignore_missing_column_families, uint64_t log_number, DB* db,
    bool concurrent_memtable_writes) {
  MemTableInserter inserter(sequence, memtables, flush_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes);
  for (size_t i = 0; i < writers.size(); i++) {
    auto w = writers[i];
    if (!w->ShouldWriteToMemtable()) {
      continue;
    }
    inserter.set_log_number_ref(w->log_ref);
    w->status = w->batch->Iterate(&inserter);
    if (!w->status.ok()) {
      return w->status;
    }
  }
  return Status::OK();
}

Status WriteBatchInternal::InsertInto(WriteThread::Writer* writer,
                                      ColumnFamilyMemTables* memtables,
                                      FlushScheduler* flush_scheduler,
                                      bool ignore_missing_column_families,
                                      uint64_t log_number, DB* db,
                                      bool concurrent_memtable_writes) {
  MemTableInserter inserter(WriteBatchInternal::Sequence(writer->batch),
                            memtables, flush_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes);
  assert(writer->ShouldWriteToMemtable());
  inserter.set_log_number_ref(writer->log_ref);
  Status s = writer->batch->Iterate(&inserter);
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }
  return s;
}

Status WriteBatchInternal::InsertInto(
    const WriteBatch* batch, ColumnFamilyMemTables* memtables,
    FlushScheduler* flush_scheduler, bool ignore_missing_column_families,
    uint64_t log_number, DB* db, bool concurrent_memtable_writes,
    SequenceNumber* last_seq_used, bool* has_valid_writes) {
  MemTableInserter inserter(WriteBatchInternal::Sequence(batch), memtables,
                            flush_scheduler, ignore_missing_column_families,
                            log_number, db, concurrent_memtable_writes,
                            has_valid_writes);
  Status s = batch->Iterate(&inserter);
  if (last_seq_used != nullptr) {
    *last_seq_used = inserter.get_final_sequence();
  }
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }
  return s;
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= WriteBatchInternal::kHeader);
  b->rep_.assign(contents.data(), contents.size());
  b->content_flags_.store(ContentFlags::DEFERRED, std::memory_order_relaxed);
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= WriteBatchInternal::kHeader);
  dst->rep_.append(src->rep_.data() + WriteBatchInternal::kHeader,
    src->rep_.size() - WriteBatchInternal::kHeader);
  dst->content_flags_.store(
      dst->content_flags_.load(std::memory_order_relaxed) |
          src->content_flags_.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
}

size_t WriteBatchInternal::AppendedByteSize(size_t leftByteSize,
                                            size_t rightByteSize) {
  if (leftByteSize == 0 || rightByteSize == 0) {
    return leftByteSize + rightByteSize;
  } else {
    return leftByteSize + rightByteSize - WriteBatchInternal::kHeader;
  }
}

}  // namespace rocksdb
