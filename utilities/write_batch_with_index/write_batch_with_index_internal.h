// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef ROCKSDB_LITE

#include <limits>
#include <string>
#include <vector>

#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace rocksdb {

class MergeContext;
struct Options;

// Key used by skip list, as the binary searchable index of WriteBatchWithIndex.
struct WriteBatchIndexEntry {
  WriteBatchIndexEntry(size_t o, uint32_t c, size_t ko, size_t ksz)
      : offset(o),
        column_family(c),
        key_offset(ko),
        key_size(ksz),
        search_key(nullptr) {}
  WriteBatchIndexEntry(const Slice* sk, uint32_t c)
      : offset(0),
        column_family(c),
        key_offset(0),
        key_size(0),
        search_key(sk) {}

  size_t offset;           // offset of an entry in write batch's string buffer.
  uint32_t column_family;  // column family of the entry.
  size_t key_offset;       // offset of the key in write batch's string buffer.
  size_t key_size;         // size of the key.

  const Slice* search_key;  // if not null, instead of reading keys from
                            // write batch, use it to compare. This is used
                            // for lookup key.
};

class ReadableWriteBatch : public WriteBatch {
 public:
  explicit ReadableWriteBatch(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : WriteBatch(reserved_bytes, max_bytes) {}
  // Retrieve some information from a write entry in the write batch, given
  // the start offset of the write entry.
  Status GetEntryFromDataOffset(size_t data_offset, WriteType* type, Slice* Key,
                                Slice* value, Slice* blob, Slice* xid) const;
};

class WriteBatchWithIndexInternal {
 public:
  enum Result { kFound, kDeleted, kNotFound, kMergeInProgress, kError };

  // If batch contains a value for key, store it in *value and return kFound.
  // If batch contains a deletion for key, return Deleted.
  // If batch contains Merge operations as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operands to *operands,
  //   and return kMergeInProgress
  // If batch does not contain this key, return kNotFound
  // Else, return kError on error with error Status stored in *s.
  static WriteBatchWithIndexInternal::Result GetFromBatch(
      const ImmutableDBOptions& ioptions, WriteBatchWithIndex* batch,
      ColumnFamilyHandle* column_family, const Slice& key,
      MergeContext* merge_context, const Comparator* cmp,
      std::string* value, bool overwrite_key, Status* s);
};

class WriteBatchKeyExtractor {
 public:
  WriteBatchKeyExtractor(const ReadableWriteBatch* write_batch)
          : write_batch_(write_batch) {
  }

  Slice operator()(const WriteBatchIndexEntry* entry) const;

 private:
  const ReadableWriteBatch* write_batch_;
};

class WriteBatchEntryIndex {
 public:
  virtual ~WriteBatchEntryIndex() {}

  class Iterator {
   public:
    virtual ~Iterator() {}
    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(WriteBatchIndexEntry* target) = 0;
    virtual void SeekForPrev(WriteBatchIndexEntry* target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual WriteBatchIndexEntry* key() const = 0;
  };
  typedef WBIteratorStorage<Iterator, 24> IteratorStorage;

  virtual Iterator* NewIterator() = 0;
  // sizeof(iterator) size must less or equal than 24
  // INCLUDE virtual table pointer
  virtual void NewIterator(IteratorStorage& storage, bool ephemeral) = 0;
  // return true if insert success
  // assign key->offset to exists entry's offset otherwise
  virtual bool Upsert(WriteBatchIndexEntry* key) = 0;
};

class WriteBatchEntryIndexContext {
 public:
  virtual ~WriteBatchEntryIndexContext();
};

class WriteBatchEntryIndexFactory {
 public:
  // object MUST allocated from arena, allow return nullptr
  // context will not delete, only by calling destructor
  virtual WriteBatchEntryIndexContext* NewContext(Arena*) const;
  virtual WriteBatchEntryIndex* New(WriteBatchEntryIndexContext* ctx,
                                    WriteBatchKeyExtractor e,
                                    const Comparator* c, Arena* a,
                                    bool overwrite_key) const = 0;
  virtual ~WriteBatchEntryIndexFactory();
  virtual const char* Name() const = 0;
};

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
