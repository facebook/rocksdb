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
namespace {
class ReadableWriteBatch : public WriteBatch {
 public:
  explicit ReadableWriteBatch(size_t reserved_bytes = 0)
      : WriteBatch(reserved_bytes) {}
  // Retrieve some information from a write entry in the write batch, given
  // the start offset of the write entry.
  Status GetEntryFromDataOffset(size_t data_offset, WriteType* type, Slice* Key,
                                Slice* value, Slice* blob) const;
};
}  // namespace

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
  WriteBatchEntryComparator(const Comparator* comparator,
                            const ReadableWriteBatch* write_batch)
      : comparator_(comparator), write_batch_(write_batch) {}
  // Compare a and b. Return a negative value if a is less than b, 0 if they
  // are equal, and a positive value if a is greater than b
  int operator()(const WriteBatchIndexEntry* entry1,
                 const WriteBatchIndexEntry* entry2) const;

 private:
  const Comparator* comparator_;
  const ReadableWriteBatch* write_batch_;
};

typedef SkipList<WriteBatchIndexEntry*, const WriteBatchEntryComparator&>
    WriteBatchEntrySkipList;

struct WriteBatchWithIndex::Rep {
  Rep(const Comparator* index_comparator, size_t reserved_bytes = 0)
      : write_batch(reserved_bytes),
        comparator(index_comparator, &write_batch),
        skip_list(comparator, &arena) {}
  ReadableWriteBatch write_batch;
  WriteBatchEntryComparator comparator;
  Arena arena;
  WriteBatchEntrySkipList skip_list;

  WriteBatchIndexEntry* GetEntry(ColumnFamilyHandle* column_family) {
    return GetEntryWithCfId(GetColumnFamilyID(column_family));
  }

  WriteBatchIndexEntry* GetEntryWithCfId(uint32_t column_family_id) {
    auto* mem = arena.Allocate(sizeof(WriteBatchIndexEntry));
    auto* index_entry = new (mem)
        WriteBatchIndexEntry(write_batch.GetDataSize(), column_family_id);
    return index_entry;
  }
};

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

  virtual const WriteEntry& Entry() const override { return current_; }

  virtual Status status() const override { return status_; }

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

WriteBatchWithIndex::WriteBatchWithIndex(const Comparator* index_comparator,
                                         size_t reserved_bytes)
    : rep(new Rep(index_comparator, reserved_bytes)) {}

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
  auto* index_entry = rep->GetEntry(column_family);
  rep->write_batch.Put(column_family, key, value);
  rep->skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::Put(const Slice& key, const Slice& value) {
  auto* index_entry = rep->GetEntryWithCfId(0);
  rep->write_batch.Put(key, value);
  rep->skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::Merge(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  auto* index_entry = rep->GetEntry(column_family);
  rep->write_batch.Merge(column_family, key, value);
  rep->skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::Merge(const Slice& key, const Slice& value) {
  auto* index_entry = rep->GetEntryWithCfId(0);
  rep->write_batch.Merge(key, value);
  rep->skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::PutLogData(const Slice& blob) {
  rep->write_batch.PutLogData(blob);
}

void WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                 const Slice& key) {
  auto* index_entry = rep->GetEntry(column_family);
  rep->write_batch.Delete(column_family, key);
  rep->skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::Delete(const Slice& key) {
  auto* index_entry = rep->GetEntryWithCfId(0);
  rep->write_batch.Delete(key);
  rep->skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                 const SliceParts& key) {
  auto* index_entry = rep->GetEntry(column_family);
  rep->write_batch.Delete(column_family, key);
  rep->skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::Delete(const SliceParts& key) {
  auto* index_entry = rep->GetEntryWithCfId(0);
  rep->write_batch.Delete(key);
  rep->skip_list.Insert(index_entry);
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

  int cmp = comparator_->Compare(key1, key2);
  if (cmp != 0) {
    return cmp;
  } else if (entry1->offset > entry2->offset) {
    return 1;
  } else if (entry1->offset < entry2->offset) {
    return -1;
  }
  return 0;
}

}  // namespace rocksdb
