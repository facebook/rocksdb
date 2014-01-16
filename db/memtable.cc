//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"

#include <memory>

#include "db/dbformat.h"
#include "db/merge_context.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/murmurhash.h"
#include "util/statistics_imp.h"

namespace std {
template <>
struct hash<rocksdb::Slice> {
  size_t operator()(const rocksdb::Slice& slice) const {
    return MurmurHash(slice.data(), slice.size(), 0);
  }
};
}

namespace rocksdb {

MemTable::MemTable(const InternalKeyComparator& cmp, const Options& options)
    : comparator_(cmp),
      refs_(0),
      arena_impl_(options.arena_block_size),
      table_(options.memtable_factory->CreateMemTableRep(comparator_,
                                                         &arena_impl_)),
      flush_in_progress_(false),
      flush_completed_(false),
      file_number_(0),
      first_seqno_(0),
      mem_next_logfile_number_(0),
      mem_logfile_number_(0),
      locks_(options.inplace_update_support ? options.inplace_update_num_locks
                                            : 0) {}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() {
  return arena_impl_.ApproximateMemoryUsage() +
         table_->ApproximateMemoryUsage();
}

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  MemTableIterator(MemTableRep* table, const ReadOptions& options)
    : iter_() {
    if (options.prefix) {
      iter_.reset(table->GetPrefixIterator(*options.prefix));
    } else if (options.prefix_seek) {
      iter_.reset(table->GetDynamicPrefixIterator());
    } else {
      iter_.reset(table->GetIterator());
    }
  }

  virtual bool Valid() const { return iter_->Valid(); }
  virtual void Seek(const Slice& k) { iter_->Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_->SeekToFirst(); }
  virtual void SeekToLast() { iter_->SeekToLast(); }
  virtual void Next() { iter_->Next(); }
  virtual void Prev() { iter_->Prev(); }
  virtual Slice key() const {
    return GetLengthPrefixedSlice(iter_->key());
  }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_->key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  std::unique_ptr<MemTableRep::Iterator> iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator(const ReadOptions& options) {
  return new MemTableIterator(table_.get(), options);
}

port::RWMutex* MemTable::GetLock(const Slice& key) {
  return &locks_[std::hash<Slice>()(key) % locks_.size()];
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_impl_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == (unsigned)encoded_len);
  table_->Insert(buf);

  // The first sequence number inserted into the memtable
  assert(first_seqno_ == 0 || s > first_seqno_);
  if (first_seqno_ == 0) {
    first_seqno_ = s;
  }
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s,
                   MergeContext& merge_context, const Options& options) {
  Slice memkey = key.memtable_key();
  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetIterator(key.user_key()));
  iter->Seek(memkey.data());

  bool merge_in_progress = s->IsMergeInProgress();
  auto merge_operator = options.merge_operator.get();
  auto logger = options.info_log;
  std::string merge_result;

  for (; iter->Valid(); iter->Next()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength-8]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
        Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          if (options.inplace_update_support) {
            GetLock(key.user_key())->ReadLock();
          }
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          *s = Status::OK();
          if (merge_in_progress) {
            assert(merge_operator);
          if (!merge_operator->FullMerge(key.user_key(), &v,
                                         merge_context.GetOperands(), value,
                                         logger.get())) {
              RecordTick(options.statistics.get(), NUMBER_MERGE_FAILURES);
              *s = Status::Corruption("Error: Could not perform merge.");
            }
          } else {
            value->assign(v.data(), v.size());
          }
          if (options.inplace_update_support) {
            GetLock(key.user_key())->Unlock();
          }
          return true;
        }
        case kTypeDeletion: {
          if (merge_in_progress) {
            assert(merge_operator);
            *s = Status::OK();
          if (!merge_operator->FullMerge(key.user_key(), nullptr,
                                         merge_context.GetOperands(), value,
                                         logger.get())) {
              RecordTick(options.statistics.get(), NUMBER_MERGE_FAILURES);
              *s = Status::Corruption("Error: Could not perform merge.");
            }
          } else {
            *s = Status::NotFound();
          }
          return true;
        }
        case kTypeMerge: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          merge_in_progress = true;
          merge_context.PushOperand(v);
          while(merge_context.GetNumOperands() >= 2) {
            // Attempt to associative merge. (Returns true if successful)
          if (merge_operator->PartialMerge(key.user_key(),
                                           merge_context.GetOperand(0),
                                           merge_context.GetOperand(1),
                                           &merge_result, logger.get())) {
              merge_context.PushPartialMergeResult(merge_result);
            } else {
              // Stack them because user can't associative merge
              break;
            }
          }
          break;
        }
        case kTypeLogData:
          assert(false);
          break;
      }
    } else {
      // exit loop if user key does not match
      break;
    }
  }

  // No change to value, since we have not yet found a Put/Delete

  if (merge_in_progress) {
    *s = Status::MergeInProgress("");
  }
  return false;
}

bool MemTable::Update(SequenceNumber seq, ValueType type,
                      const Slice& key,
                      const Slice& value) {
  LookupKey lkey(key, seq);
  Slice memkey = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetIterator(lkey.user_key()));
  iter->Seek(memkey.data());

  if (iter->Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength-8]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
        Slice(key_ptr, key_length - 8), lkey.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          uint32_t vlength;
          GetVarint32Ptr(key_ptr + key_length,
                         key_ptr + key_length+5, &vlength);
          // Update value, if newValue size  <= curValue size
          if (value.size() <= vlength) {
            char* p = EncodeVarint32(const_cast<char*>(key_ptr) + key_length,
                                     value.size());
            WriteLock wl(GetLock(lkey.user_key()));
            memcpy(p, value.data(), value.size());
            assert(
              (p + value.size()) - entry ==
              (unsigned) (VarintLength(key_length) +
                          key_length +
                          VarintLength(value.size()) +
                          value.size())
            );
            return true;
          }
        }
        default:
          // If the latest value is kTypeDeletion, kTypeMerge or kTypeLogData
          // then we probably don't have enough space to update in-place
          // Maybe do something later
          // Return false, and do normal Add()
          return false;
      }
    }
  }

  // Key doesn't exist
  return false;
}

size_t MemTable::CountSuccessiveMergeEntries(const LookupKey& key) {
  Slice memkey = key.memtable_key();

  // A total ordered iterator is costly for some memtablerep (prefix aware
  // reps). By passing in the user key, we allow efficient iterator creation.
  // The iterator only needs to be ordered within the same user key.
  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetIterator(key.user_key()));
  iter->Seek(memkey.data());

  size_t num_successive_merges = 0;

  for (; iter->Valid(); iter->Next()) {
    const char* entry = iter->key();
    uint32_t key_length;
    const char* iter_key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (!comparator_.comparator.user_comparator()->Compare(
        Slice(iter_key_ptr, key_length - 8), key.user_key()) == 0) {
      break;
    }

    const uint64_t tag = DecodeFixed64(iter_key_ptr + key_length - 8);
    if (static_cast<ValueType>(tag & 0xff) != kTypeMerge) {
      break;
    }

    ++num_successive_merges;
  }

  return num_successive_merges;
}

}  // namespace rocksdb
