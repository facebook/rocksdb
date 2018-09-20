//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

#include <unordered_map>
#include "db/column_family.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "memtable/skiplist.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

class Env;
class Logger;
class Statistics;

static std::unordered_map<std::string, const WriteBatchEntryIndexFactory*>&
GetWriteBatchEntryIndexFactoryMap() {
  static std::unordered_map<std::string, const WriteBatchEntryIndexFactory*>
         factoryMap;
  return factoryMap;
};

Status ReadableWriteBatch::GetEntryFromDataOffset(size_t data_offset,
                                                  WriteType* type, Slice* Key,
                                                  Slice* value, Slice* blob,
                                                  Slice* xid) const {
  if (type == nullptr || Key == nullptr || value == nullptr ||
      blob == nullptr || xid == nullptr) {
    return Status::InvalidArgument("Output parameters cannot be null");
  }

  if (data_offset == GetDataSize()) {
    // reached end of batch.
    return Status::NotFound();
  }

  if (data_offset > GetDataSize()) {
    return Status::InvalidArgument("data offset exceed write batch size");
  }
  Slice input = Slice(rep_.data() + data_offset, rep_.size() - data_offset);
  char tag;
  uint32_t column_family;
  Status s = ReadRecordFromWriteBatch(&input, &tag, &column_family, Key, value,
                                      blob, xid);

  switch (tag) {
    case kTypeColumnFamilyValue:
    case kTypeValue:
      *type = kPutRecord;
      break;
    case kTypeColumnFamilyDeletion:
    case kTypeDeletion:
      *type = kDeleteRecord;
      break;
    case kTypeColumnFamilySingleDeletion:
    case kTypeSingleDeletion:
      *type = kSingleDeleteRecord;
      break;
    case kTypeColumnFamilyRangeDeletion:
    case kTypeRangeDeletion:
      *type = kDeleteRangeRecord;
      break;
    case kTypeColumnFamilyMerge:
    case kTypeMerge:
      *type = kMergeRecord;
      break;
    case kTypeLogData:
      *type = kLogDataRecord;
      break;
    case kTypeNoop:
    case kTypeBeginPrepareXID:
    case kTypeBeginPersistedPrepareXID:
    case kTypeBeginUnprepareXID:
    case kTypeEndPrepareXID:
    case kTypeCommitXID:
    case kTypeRollbackXID:
      *type = kXIDRecord;
      break;
    default:
      return Status::Corruption("unknown WriteBatch tag ",
                                ToString(static_cast<unsigned int>(tag)));
  }
  return Status::OK();
}

WriteBatchWithIndexInternal::Result WriteBatchWithIndexInternal::GetFromBatch(
    const ImmutableDBOptions& immuable_db_options, WriteBatchWithIndex* batch,
    ColumnFamilyHandle* column_family, const Slice& key,
    MergeContext* merge_context, const Comparator* cmp,
    std::string* value, bool overwrite_key, Status* s) {
  *s = Status::OK();
  WriteBatchWithIndexInternal::Result result =
      WriteBatchWithIndexInternal::Result::kNotFound;

  if (cmp == nullptr) {
    return result;
  }

  WBWIIterator::IteratorStorage iter;
  batch->NewIterator(column_family, iter, true);

  // We want to iterate in the reverse order that the writes were added to the
  // batch.  Since we don't have a reverse iterator, we must seek past the end.
  // TODO(agiardullo): consider adding support for reverse iteration
  iter->Seek(key);
  while (iter->Valid()) {
    const WriteEntry entry = iter->Entry();
    if (cmp->Compare(entry.key, key) != 0) {
      break;
    }

    iter->Next();
  }

  if (!(*s).ok()) {
    return WriteBatchWithIndexInternal::Result::kError;
  }

  if (!iter->Valid()) {
    // Read past end of results.  Reposition on last result.
    iter->SeekToLast();
  } else {
    iter->Prev();
  }

  Slice entry_value;
  while (iter->Valid()) {
    const WriteEntry entry = iter->Entry();
    if (cmp->Compare(entry.key, key) != 0) {
      // Unexpected error or we've reached a different next key
      break;
    }

    switch (entry.type) {
      case kPutRecord: {
        result = WriteBatchWithIndexInternal::Result::kFound;
        entry_value = entry.value;
        break;
      }
      case kMergeRecord: {
        result = WriteBatchWithIndexInternal::Result::kMergeInProgress;
        merge_context->PushOperand(entry.value);
        break;
      }
      case kDeleteRecord:
      case kSingleDeleteRecord: {
        result = WriteBatchWithIndexInternal::Result::kDeleted;
        break;
      }
      case kLogDataRecord:
      case kXIDRecord: {
        // ignore
        break;
      }
      default: {
        result = WriteBatchWithIndexInternal::Result::kError;
        *s = Status::Corruption("Unexpected entry in WriteBatchWithIndex:",
                                ToString(entry.type));
        break;
      }
    }
    if (result == WriteBatchWithIndexInternal::Result::kFound ||
        result == WriteBatchWithIndexInternal::Result::kDeleted ||
        result == WriteBatchWithIndexInternal::Result::kError) {
      // We can stop iterating once we find a PUT or DELETE
      break;
    }
    if (result == WriteBatchWithIndexInternal::Result::kMergeInProgress &&
        overwrite_key == true) {
      // Since we've overwritten keys, we do not know what other operations are
      // in this batch for this key, so we cannot do a Merge to compute the
      // result.  Instead, we will simply return MergeInProgress.
      break;
    }

    iter->Prev();
  }

  if (s->ok()) {
    if (result == WriteBatchWithIndexInternal::Result::kFound ||
        result == WriteBatchWithIndexInternal::Result::kDeleted) {
      // Found a Put or Delete.  Merge if necessary.
      if (merge_context->GetNumOperands() > 0) {
        const MergeOperator* merge_operator;

        if (column_family != nullptr) {
          auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
          merge_operator = cfh->cfd()->ioptions()->merge_operator;
        } else {
          *s = Status::InvalidArgument("Must provide a column_family");
          result = WriteBatchWithIndexInternal::Result::kError;
          return result;
        }
        Statistics* statistics = immuable_db_options.statistics.get();
        Env* env = immuable_db_options.env;
        Logger* logger = immuable_db_options.info_log.get();

        if (merge_operator) {
          *s = MergeHelper::TimedFullMerge(merge_operator, key, &entry_value,
                                           merge_context->GetOperands(), value,
                                           logger, statistics, env);
        } else {
          *s = Status::InvalidArgument("Options::merge_operator must be set");
        }
        if (s->ok()) {
          result = WriteBatchWithIndexInternal::Result::kFound;
        } else {
          result = WriteBatchWithIndexInternal::Result::kError;
        }
      } else {  // nothing to merge
        if (result == WriteBatchWithIndexInternal::Result::kFound) {  // PUT
          value->assign(entry_value.data(), entry_value.size());
        }
      }
    }
  }

  return result;
}

Slice WriteBatchKeyExtractor::operator()(
    const WriteBatchIndexEntry* entry) const {
  if (entry->search_key == nullptr) {
    return Slice(write_batch_->Data().data() + entry->key_offset,
                 entry->key_size);
  } else {
    return *(entry->search_key);
  }
}

template<bool OverwriteKey>
struct WriteBatchEntryComparator {
  int operator()(WriteBatchIndexEntry* l, WriteBatchIndexEntry* r) const {
    int cmp = c->Compare(extractor(l), extractor(r));
    // unnecessary comp offset if overwrite key
    if (OverwriteKey || cmp != 0) {
      return cmp;
    }
    if (l->offset > r->offset) {
      return 1;
    }
    if (l->offset < r->offset) {
      return -1;
    }
    return 0;
  }
  WriteBatchKeyExtractor extractor;
  const Comparator* c;
};

template<bool OverwriteKey>
class WriteBatchEntrySkipListIndex : public WriteBatchEntryIndex {
protected:
  typedef WriteBatchEntryComparator<OverwriteKey> EntryComparator;
  typedef SkipList<WriteBatchIndexEntry*, const EntryComparator&> Index;
  EntryComparator comparator_;
  Index index_;
  bool overwrite_key_;

  class SkipListIterator : public WriteBatchEntryIndex::Iterator {
   public:
    SkipListIterator(Index* index) : iter_(index) {}
    typename Index::Iterator iter_;

   public:
    virtual bool Valid() const override {
      return iter_.Valid();
    }
    virtual void SeekToFirst() override {
      iter_.SeekToFirst();
    }
    virtual void SeekToLast() override {
      iter_.SeekToLast();
    }
    virtual void Seek(WriteBatchIndexEntry* target) override {
      iter_.Seek(target);
    }
    virtual void SeekForPrev(WriteBatchIndexEntry* target) override {
      iter_.SeekForPrev(target);
    }
    virtual void Next() override {
      iter_.Next();
    }
    virtual void Prev() override {
      iter_.Prev();
    }
    virtual WriteBatchIndexEntry* key() const override {
      return iter_.key();
    }
  };

 public:
  WriteBatchEntrySkipListIndex(WriteBatchKeyExtractor e, const Comparator* c,
                               Arena* a)
      : comparator_({e, c}),
        index_(comparator_, a) {
  }

  virtual Iterator* NewIterator() override {
    return new SkipListIterator(&index_);
  }
  virtual void NewIterator(IteratorStorage& storage, bool /*ephemeral*/) override {
    static_assert(sizeof(SkipListIterator) <= sizeof storage.buffer,
                  "Need larger buffer for SkipListIterator");
    storage.iter = new (storage.buffer) SkipListIterator(&index_);
  }
  virtual bool Upsert(WriteBatchIndexEntry* key) override {
    if (OverwriteKey) {
      Slice search_key = comparator_.extractor(key);
      WriteBatchIndexEntry search_entry(&search_key, key->column_family);
      typename Index::Iterator iter(&index_);
      iter.Seek(&search_entry);
      if (iter.Valid() &&
          comparator_.c->Compare(search_key,
                                 comparator_.extractor(iter.key())) == 0) {
        // found, replace
        std::swap(iter.key()->offset, key->offset);
        return false;
      }
    }
    index_.Insert(key);
    return true;
  }
};

WriteBatchEntryIndexContext::~WriteBatchEntryIndexContext() {}
WriteBatchEntryIndexFactory::~WriteBatchEntryIndexFactory() {}
WriteBatchEntryIndexContext*
WriteBatchEntryIndexFactory::NewContext(Arena*) const { return nullptr; }

const WriteBatchEntryIndexFactory* skip_list_WriteBatchEntryIndexFactory() {
  class SkipListIndexFactory : public WriteBatchEntryIndexFactory {
   public:
    WriteBatchEntryIndex* New(WriteBatchEntryIndexContext* /*ctx*/,
                              WriteBatchKeyExtractor e,
                              const Comparator* c, Arena* a,
                              bool overwite_key) const override {
      if (overwite_key) {
        typedef WriteBatchEntrySkipListIndex<true> index_t;
        return new (a->AllocateAligned(sizeof(index_t))) index_t(e, c, a);
      } else {
        typedef WriteBatchEntrySkipListIndex<false> index_t;
        return new (a->AllocateAligned(sizeof(index_t))) index_t(e, c, a);
      }
    }
    const char* Name() const override final { return "skip_list"; }
  };
  static SkipListIndexFactory factory;
  return &factory;
}

WriteBatchEntryIndexFactoryRegister::WriteBatchEntryIndexFactoryRegister(
    const char* name, const WriteBatchEntryIndexFactory* factory) {
  auto ib = GetWriteBatchEntryIndexFactoryMap().emplace(name, factory);
  assert(ib.second);
  if (!ib.second) {
    fprintf(stderr,
      "ERROR: duplicate MemTable name: %s, DLL may be loaded multi times\n",
      name);
    abort();
  }
}

const WriteBatchEntryIndexFactory*
   GetWriteBatchEntryIndexFactory(const char* name) {
  auto& factoryMap = GetWriteBatchEntryIndexFactoryMap();
  auto find = factoryMap.find(name);
  if (find == factoryMap.end()) {
    return nullptr;
  }
  return find->second;
}

ROCKSDB_REGISTER_WRITE_BATCH_WITH_INDEX(skip_list);

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
