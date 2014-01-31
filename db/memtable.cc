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
#include "rocksdb/slice_transform.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/murmurhash.h"
#include "util/mutexlock.h"
#include "util/perf_context_imp.h"
#include "util/statistics.h"
#include "util/stop_watch.h"

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
      arena_(options.arena_block_size),
      table_(options.memtable_factory->CreateMemTableRep(comparator_, &arena_)),
      flush_in_progress_(false),
      flush_completed_(false),
      file_number_(0),
      first_seqno_(0),
      mem_next_logfile_number_(0),
      mem_logfile_number_(0),
      locks_(options.inplace_update_support ? options.inplace_update_num_locks
                                            : 0),
      prefix_extractor_(options.prefix_extractor) {
  if (prefix_extractor_ && options.memtable_prefix_bloom_bits > 0) {
    prefix_bloom_.reset(new DynamicBloom(options.memtable_prefix_bloom_bits,
                                         options.memtable_prefix_bloom_probes));
  }
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() {
  return arena_.ApproximateMemoryUsage() + table_->ApproximateMemoryUsage();
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key1,
                                        const char* prefix_len_key2) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice k1 = GetLengthPrefixedSlice(prefix_len_key1);
  Slice k2 = GetLengthPrefixedSlice(prefix_len_key2);
  return comparator.Compare(k1, k2);
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key,
                                        const Slice& key)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  return comparator.Compare(a, key);
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  MemTableIterator(const MemTable& mem, const ReadOptions& options)
      : mem_(mem), iter_(), dynamic_prefix_seek_(false), valid_(false) {
    if (options.prefix) {
      iter_.reset(mem_.table_->GetPrefixIterator(*options.prefix));
    } else if (options.prefix_seek) {
      dynamic_prefix_seek_ = true;
      iter_.reset(mem_.table_->GetDynamicPrefixIterator());
    } else {
      iter_.reset(mem_.table_->GetIterator());
    }
  }

  virtual bool Valid() const { return valid_; }
  virtual void Seek(const Slice& k) {
    if (dynamic_prefix_seek_ && mem_.prefix_bloom_ &&
        !mem_.prefix_bloom_->MayContain(
          mem_.prefix_extractor_->Transform(ExtractUserKey(k)))) {
      valid_ = false;
      return;
    }
    iter_->Seek(k, nullptr);
    valid_ = iter_->Valid();
  }
  virtual void SeekToFirst() {
    iter_->SeekToFirst();
    valid_ = iter_->Valid();
  }
  virtual void SeekToLast() {
    iter_->SeekToLast();
    valid_ = iter_->Valid();
  }
  virtual void Next() {
    assert(Valid());
    iter_->Next();
    valid_ = iter_->Valid();
  }
  virtual void Prev() {
    assert(Valid());
    iter_->Prev();
    valid_ = iter_->Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return GetLengthPrefixedSlice(iter_->key());
  }
  virtual Slice value() const {
    assert(Valid());
    Slice key_slice = GetLengthPrefixedSlice(iter_->key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  const MemTable& mem_;
  std::shared_ptr<MemTableRep::Iterator> iter_;
  bool dynamic_prefix_seek_;
  bool valid_;

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator(const ReadOptions& options) {
  return new MemTableIterator(*this, options);
}

port::RWMutex* MemTable::GetLock(const Slice& key) {
  return &locks_[std::hash<Slice>()(key) % locks_.size()];
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key, /* user key */
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
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == (unsigned)encoded_len);
  table_->Insert(buf);

  if (prefix_bloom_) {
    assert(prefix_extractor_);
    prefix_bloom_->Add(prefix_extractor_->Transform(key));
  }

  // The first sequence number inserted into the memtable
  assert(first_seqno_ == 0 || s > first_seqno_);
  if (first_seqno_ == 0) {
    first_seqno_ = s;
  }
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s,
                   MergeContext& merge_context, const Options& options) {
  StopWatchNano memtable_get_timer(options.env, false);
  StartPerfTimer(&memtable_get_timer);

  Slice mem_key = key.memtable_key();
  Slice user_key = key.user_key();

  std::unique_ptr<MemTableRep::Iterator> iter;
  if (prefix_bloom_ &&
      !prefix_bloom_->MayContain(prefix_extractor_->Transform(user_key))) {
    // iter is null if prefix bloom says the key does not exist
  } else {
    iter.reset(table_->GetIterator(user_key));
    iter->Seek(key.internal_key(), mem_key.data());
  }

  bool merge_in_progress = s->IsMergeInProgress();
  auto merge_operator = options.merge_operator.get();
  auto logger = options.info_log;
  std::string merge_result;

  bool found_final_value = false;
  for (; !found_final_value && iter && iter->Valid(); iter->Next()) {
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
    uint32_t key_length = 0;
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
          found_final_value = true;
          break;
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
          found_final_value = true;
          break;
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
        default:
          assert(false);
          break;
      }
    } else {
      // exit loop if user key does not match
      break;
    }
  }

  // No change to value, since we have not yet found a Put/Delete

  if (!found_final_value && merge_in_progress) {
    *s = Status::MergeInProgress("");
  }
  BumpPerfTime(&perf_context.get_from_memtable_time, &memtable_get_timer);
  BumpPerfCount(&perf_context.get_from_memtable_count);
  return found_final_value;
}

void MemTable::Update(SequenceNumber seq,
                      const Slice& key,
                      const Slice& value) {
  LookupKey lkey(key, seq);
  Slice mem_key = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
    table_->GetIterator(lkey.user_key()));
  iter->Seek(lkey.internal_key(), mem_key.data());

  if (iter->Valid()) {
    // entry format is:
    //    key_length  varint32
    //    userkey  char[klength-8]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
        Slice(key_ptr, key_length - 8), lkey.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice prev_value = GetLengthPrefixedSlice(key_ptr + key_length);
          uint32_t prev_size = prev_value.size();
          uint32_t new_size = value.size();

          // Update value, if new value size  <= previous value size
          if (new_size <= prev_size ) {
            char* p = EncodeVarint32(const_cast<char*>(key_ptr) + key_length,
                                     new_size);
            WriteLock wl(GetLock(lkey.user_key()));
            memcpy(p, value.data(), new_size);
            assert(
              (p + new_size) - entry ==
              (unsigned) (VarintLength(key_length) +
                          key_length +
                          VarintLength(new_size) +
                          new_size)
            );
            // no need to update bloom, as user key does not change.
            return;
          }
        }
        default:
          // If the latest value is kTypeDeletion, kTypeMerge or kTypeLogData
          // we don't have enough space for update inplace
            Add(seq, kTypeValue, key, value);
            return;
      }
    }
  }

  // key doesn't exist
  Add(seq, kTypeValue, key, value);
}

bool MemTable::UpdateCallback(SequenceNumber seq,
                              const Slice& key,
                              const Slice& delta,
                              const Options& options) {
  LookupKey lkey(key, seq);
  Slice memkey = lkey.memtable_key();

  std::shared_ptr<MemTableRep::Iterator> iter(
    table_->GetIterator(lkey.user_key()));
  iter->Seek(lkey.internal_key(), memkey.data());

  if (iter->Valid()) {
    // entry format is:
    //    key_length  varint32
    //    userkey  char[klength-8]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
        Slice(key_ptr, key_length - 8), lkey.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice prev_value = GetLengthPrefixedSlice(key_ptr + key_length);
          uint32_t  prev_size = prev_value.size();

          char* prev_buffer = const_cast<char*>(prev_value.data());
          uint32_t  new_prev_size = prev_size;

          std::string str_value;
          WriteLock wl(GetLock(lkey.user_key()));
          auto status = options.inplace_callback(prev_buffer, &new_prev_size,
                                                    delta, &str_value);
          if (status == UpdateStatus::UPDATED_INPLACE) {
            // Value already updated by callback.
            assert(new_prev_size <= prev_size);
            if (new_prev_size < prev_size) {
              // overwrite the new prev_size
              char* p = EncodeVarint32(const_cast<char*>(key_ptr) + key_length,
                                       new_prev_size);
              if (VarintLength(new_prev_size) < VarintLength(prev_size)) {
                // shift the value buffer as well.
                memcpy(p, prev_buffer, new_prev_size);
              }
            }
            RecordTick(options.statistics.get(), NUMBER_KEYS_UPDATED);
            return true;
          } else if (status == UpdateStatus::UPDATED) {
            Add(seq, kTypeValue, key, Slice(str_value));
            RecordTick(options.statistics.get(), NUMBER_KEYS_WRITTEN);
            return true;
          } else if (status == UpdateStatus::UPDATE_FAILED) {
            // No action required. Return.
            return true;
          }
        }
        default:
          break;
      }
    }
  }
  // If the latest value is not kTypeValue
  // or key doesn't exist
  return false;
}

size_t MemTable::CountSuccessiveMergeEntries(const LookupKey& key) {
  Slice memkey = key.memtable_key();

  // A total ordered iterator is costly for some memtablerep (prefix aware
  // reps). By passing in the user key, we allow efficient iterator creation.
  // The iterator only needs to be ordered within the same user key.
  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetIterator(key.user_key()));
  iter->Seek(key.internal_key(), memkey.data());

  size_t num_successive_merges = 0;

  for (; iter->Valid(); iter->Next()) {
    const char* entry = iter->key();
    uint32_t key_length = 0;
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
