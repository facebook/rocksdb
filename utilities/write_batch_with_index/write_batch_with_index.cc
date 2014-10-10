//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/comparator.h"
#include "db/column_family.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace rocksdb {

class ReadableWriteBatch : public WriteBatch {
 public:
  explicit ReadableWriteBatch(size_t reserved_bytes = 0)
      : WriteBatch(reserved_bytes) {}
  // Retrieve some information from a write entry in the write batch, given
  // the start offset of the write entry.
  Status GetEntryFromDataOffset(size_t data_offset, WriteType* type, Slice* Key,
                                Slice* value, Slice* blob) const;
};

// Key used by skip list, as the binary searchable index of WriteBatchWithIndex.
struct WriteBatchIndexEntry {
  WriteBatchIndexEntry(size_t o, uint32_t c)
      : offset(o), column_family(c), search_key(nullptr) {}
  WriteBatchIndexEntry(const Slice* sk, uint32_t c)
      : offset(0), column_family(c), search_key(sk) {}

  size_t offset;           // offset of an entry in write batch's string buffer.
  uint32_t column_family;  // column family of the entry
  const Slice* search_key;  // if not null, instead of reading keys from
                            // write batch, use it to compare. This is used
                            // for lookup key.
};

class WriteBatchEntryComparator {
 public:
  WriteBatchEntryComparator(const Comparator* default_comparator,
                            const ReadableWriteBatch* write_batch)
      : default_comparator_(default_comparator), write_batch_(write_batch) {}
  // Compare a and b. Return a negative value if a is less than b, 0 if they
  // are equal, and a positive value if a is greater than b
  int operator()(const WriteBatchIndexEntry* entry1,
                 const WriteBatchIndexEntry* entry2) const;

  int CompareKey(uint32_t column_family, const Slice& key1,
                 const Slice& key2) const;

  void SetComparatorForCF(uint32_t column_family_id,
                          const Comparator* comparator) {
    cf_comparator_map_[column_family_id] = comparator;
  }

 private:
  const Comparator* default_comparator_;
  std::unordered_map<uint32_t, const Comparator*> cf_comparator_map_;
  const ReadableWriteBatch* write_batch_;
};

typedef SkipList<WriteBatchIndexEntry*, const WriteBatchEntryComparator&>
    WriteBatchEntrySkipList;

class WBWIIteratorImpl : public WBWIIterator {
 public:
  WBWIIteratorImpl(uint32_t column_family_id,
                   WriteBatchEntrySkipList* skip_list,
                   const ReadableWriteBatch* write_batch)
      : column_family_id_(column_family_id),
        skip_list_iter_(skip_list),
        write_batch_(write_batch),
        valid_(false) {}

  virtual ~WBWIIteratorImpl() {}

  virtual bool Valid() const override { return valid_; }

  virtual void SeekToFirst() {
    valid_ = true;
    WriteBatchIndexEntry search_entry(nullptr, column_family_id_);
    skip_list_iter_.Seek(&search_entry);
    ReadEntry();
  }

  virtual void SeekToLast() {
    valid_ = true;
    WriteBatchIndexEntry search_entry(nullptr, column_family_id_ + 1);
    skip_list_iter_.Seek(&search_entry);
    if (!skip_list_iter_.Valid()) {
      skip_list_iter_.SeekToLast();
    } else {
      skip_list_iter_.Prev();
    }
    ReadEntry();
  }

  virtual void Seek(const Slice& key) override {
    valid_ = true;
    WriteBatchIndexEntry search_entry(&key, column_family_id_);
    skip_list_iter_.Seek(&search_entry);
    ReadEntry();
  }

  virtual void Next() override {
    skip_list_iter_.Next();
    ReadEntry();
  }

  virtual void Prev() override {
    skip_list_iter_.Prev();
    ReadEntry();
  }

  virtual const WriteEntry& Entry() const override { return current_; }

  virtual Status status() const override { return status_; }

  const WriteBatchIndexEntry* GetRawEntry() const {
    return skip_list_iter_.key();
  }

 private:
  uint32_t column_family_id_;
  WriteBatchEntrySkipList::Iterator skip_list_iter_;
  const ReadableWriteBatch* write_batch_;
  Status status_;
  bool valid_;
  WriteEntry current_;

  void ReadEntry() {
    if (!status_.ok() || !skip_list_iter_.Valid()) {
      valid_ = false;
      return;
    }
    const WriteBatchIndexEntry* iter_entry = skip_list_iter_.key();
    if (iter_entry == nullptr ||
        iter_entry->column_family != column_family_id_) {
      valid_ = false;
      return;
    }
    Slice blob;
    status_ = write_batch_->GetEntryFromDataOffset(
        iter_entry->offset, &current_.type, &current_.key, &current_.value,
        &blob);
    if (!status_.ok()) {
      valid_ = false;
    } else if (current_.type != kPutRecord && current_.type != kDeleteRecord &&
               current_.type != kMergeRecord) {
      valid_ = false;
      status_ = Status::Corruption("write batch index is corrupted");
    }
  }
};

struct WriteBatchWithIndex::Rep {
  Rep(const Comparator* index_comparator, size_t reserved_bytes = 0,
      bool overwrite_key = false)
      : write_batch(reserved_bytes),
        comparator(index_comparator, &write_batch),
        skip_list(comparator, &arena),
        overwrite_key(overwrite_key),
        last_entry_offset(0) {}
  ReadableWriteBatch write_batch;
  WriteBatchEntryComparator comparator;
  Arena arena;
  WriteBatchEntrySkipList skip_list;
  bool overwrite_key;
  size_t last_entry_offset;

  // Remember current offset of internal write batch, which is used as
  // the starting offset of the next record.
  void SetLastEntryOffset() { last_entry_offset = write_batch.GetDataSize(); }

  // In overwrite mode, find the existing entry for the same key and update it
  // to point to the current entry.
  // Return true if the key is found and updated.
  bool UpdateExistingEntry(ColumnFamilyHandle* column_family, const Slice& key);
  bool UpdateExistingEntryWithCfId(uint32_t column_family_id, const Slice& key);

  // Add the recent entry to the update.
  // In overwrite mode, if key already exists in the index, update it.
  void AddOrUpdateIndex(ColumnFamilyHandle* column_family, const Slice& key);
  void AddOrUpdateIndex(const Slice& key);

  // Allocate an index entry pointing to the last entry in the write batch and
  // put it to skip list.
  void AddNewEntry(uint32_t column_family_id);
};

bool WriteBatchWithIndex::Rep::UpdateExistingEntry(
    ColumnFamilyHandle* column_family, const Slice& key) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  return UpdateExistingEntryWithCfId(cf_id, key);
}

bool WriteBatchWithIndex::Rep::UpdateExistingEntryWithCfId(
    uint32_t column_family_id, const Slice& key) {
  if (!overwrite_key) {
    return false;
  }

  WBWIIteratorImpl iter(column_family_id, &skip_list, &write_batch);
  iter.Seek(key);
  if (!iter.Valid()) {
    return false;
  }
  if (comparator.CompareKey(column_family_id, key, iter.Entry().key) != 0) {
    return false;
  }
  WriteBatchIndexEntry* non_const_entry =
      const_cast<WriteBatchIndexEntry*>(iter.GetRawEntry());
  non_const_entry->offset = last_entry_offset;
  return true;
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(
    ColumnFamilyHandle* column_family, const Slice& key) {
  if (!UpdateExistingEntry(column_family, key)) {
    uint32_t cf_id = GetColumnFamilyID(column_family);
    const auto* cf_cmp = GetColumnFamilyUserComparator(column_family);
    if (cf_cmp != nullptr) {
      comparator.SetComparatorForCF(cf_id, cf_cmp);
    }
    AddNewEntry(cf_id);
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(const Slice& key) {
  if (!UpdateExistingEntryWithCfId(0, key)) {
    AddNewEntry(0);
  }
}

void WriteBatchWithIndex::Rep::AddNewEntry(uint32_t column_family_id) {
    auto* mem = arena.Allocate(sizeof(WriteBatchIndexEntry));
    auto* index_entry =
        new (mem) WriteBatchIndexEntry(last_entry_offset, column_family_id);
    skip_list.Insert(index_entry);
  }

Status ReadableWriteBatch::GetEntryFromDataOffset(size_t data_offset,
                                                  WriteType* type, Slice* Key,
                                                  Slice* value,
                                                  Slice* blob) const {
  if (type == nullptr || Key == nullptr || value == nullptr ||
      blob == nullptr) {
    return Status::InvalidArgument("Output parameters cannot be null");
  }

  if (data_offset >= GetDataSize()) {
    return Status::InvalidArgument("data offset exceed write batch size");
  }
  Slice input = Slice(rep_.data() + data_offset, rep_.size() - data_offset);
  char tag;
  uint32_t column_family;
  Status s =
      ReadRecordFromWriteBatch(&input, &tag, &column_family, Key, value, blob);

  switch (tag) {
    case kTypeColumnFamilyValue:
    case kTypeValue:
      *type = kPutRecord;
      break;
    case kTypeColumnFamilyDeletion:
    case kTypeDeletion:
      *type = kDeleteRecord;
      break;
    case kTypeColumnFamilyMerge:
    case kTypeMerge:
      *type = kMergeRecord;
      break;
    case kTypeLogData:
      *type = kLogDataRecord;
      break;
    default:
      return Status::Corruption("unknown WriteBatch tag");
  }
  return Status::OK();
}

WriteBatchWithIndex::WriteBatchWithIndex(
    const Comparator* default_index_comparator, size_t reserved_bytes,
    bool overwrite_key)
    : rep(new Rep(default_index_comparator, reserved_bytes, overwrite_key)) {}

WriteBatchWithIndex::~WriteBatchWithIndex() { delete rep; }

WriteBatch* WriteBatchWithIndex::GetWriteBatch() { return &rep->write_batch; }

WBWIIterator* WriteBatchWithIndex::NewIterator() {
  return new WBWIIteratorImpl(0, &(rep->skip_list), &rep->write_batch);
}

WBWIIterator* WriteBatchWithIndex::NewIterator(
    ColumnFamilyHandle* column_family) {
  return new WBWIIteratorImpl(GetColumnFamilyID(column_family),
                              &(rep->skip_list), &rep->write_batch);
}

void WriteBatchWithIndex::Put(ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  rep->write_batch.Put(column_family, key, value);
  rep->AddOrUpdateIndex(column_family, key);
}

void WriteBatchWithIndex::Put(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  rep->write_batch.Put(key, value);
  rep->AddOrUpdateIndex(key);
}

void WriteBatchWithIndex::Merge(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  rep->write_batch.Merge(column_family, key, value);
  rep->AddOrUpdateIndex(column_family, key);
}

void WriteBatchWithIndex::Merge(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  rep->write_batch.Merge(key, value);
  rep->AddOrUpdateIndex(key);
}

void WriteBatchWithIndex::PutLogData(const Slice& blob) {
  rep->write_batch.PutLogData(blob);
}

void WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                 const Slice& key) {
  rep->SetLastEntryOffset();
  rep->write_batch.Delete(column_family, key);
  rep->AddOrUpdateIndex(column_family, key);
}

void WriteBatchWithIndex::Delete(const Slice& key) {
  rep->SetLastEntryOffset();
  rep->write_batch.Delete(key);
  rep->AddOrUpdateIndex(key);
}

int WriteBatchEntryComparator::operator()(
    const WriteBatchIndexEntry* entry1,
    const WriteBatchIndexEntry* entry2) const {
  if (entry1->column_family > entry2->column_family) {
    return 1;
  } else if (entry1->column_family < entry2->column_family) {
    return -1;
  }

  Status s;
  Slice key1, key2;
  if (entry1->search_key == nullptr) {
    Slice value, blob;
    WriteType write_type;
    s = write_batch_->GetEntryFromDataOffset(entry1->offset, &write_type, &key1,
                                             &value, &blob);
    if (!s.ok()) {
      return 1;
    }
  } else {
    key1 = *(entry1->search_key);
  }
  if (entry2->search_key == nullptr) {
    Slice value, blob;
    WriteType write_type;
    s = write_batch_->GetEntryFromDataOffset(entry2->offset, &write_type, &key2,
                                             &value, &blob);
    if (!s.ok()) {
      return -1;
    }
  } else {
    key2 = *(entry2->search_key);
  }

  int cmp = CompareKey(entry1->column_family, key1, key2);
  if (cmp != 0) {
    return cmp;
  } else if (entry1->offset > entry2->offset) {
    return 1;
  } else if (entry1->offset < entry2->offset) {
    return -1;
  }
  return 0;
}

int WriteBatchEntryComparator::CompareKey(uint32_t column_family,
                                          const Slice& key1,
                                          const Slice& key2) const {
  auto comparator_for_cf = cf_comparator_map_.find(column_family);
  if (comparator_for_cf != cf_comparator_map_.end()) {
    return comparator_for_cf->second->Compare(key1, key2);
  } else {
    return default_comparator_->Compare(key1, key2);
  }
}

}  // namespace rocksdb
