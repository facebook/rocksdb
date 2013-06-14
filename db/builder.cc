// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/merge_helper.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/stop_watch.h"

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  const EnvOptions& soptions,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta,
                  const Comparator* user_comparator,
                  const SequenceNumber newest_snapshot,
                  const SequenceNumber earliest_seqno_in_memtable) {
  Status s;
  meta->file_size = 0;
  meta->smallest_seqno = meta->largest_seqno = 0;
  iter->SeekToFirst();

  // If the sequence number of the smallest entry in the memtable is
  // smaller than the most recent snapshot, then we do not trigger
  // removal of duplicate/deleted keys as part of this builder.
  bool purge = options.purge_redundant_kvs_while_flush;
  if (earliest_seqno_in_memtable <= newest_snapshot) {
    purge = false;
  }

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    unique_ptr<WritableFile> file;
    s = env->NewWritableFile(fname, &file, soptions);
    if (!s.ok()) {
      return s;
    }
    TableBuilder* builder = new TableBuilder(options, file.get(), 0);

    // the first key is the smallest key
    Slice key = iter->key();
    meta->smallest.DecodeFrom(key);
    meta->smallest_seqno = GetInternalKeySeqno(key);
    meta->largest_seqno = meta->smallest_seqno;

    MergeHelper merge(user_comparator, options.merge_operator,
                      options.info_log.get(),
                      true /* internal key corruption is not ok */);

    if (purge) {
      ParsedInternalKey ikey;
      // Ugly walkaround to avoid compiler error for release build
      // TODO: find a clean way to treat in memory key corruption
      ikey.type = kTypeValue;
      ParsedInternalKey prev_ikey;
      std::string prev_value;
      std::string prev_key;

      // Ugly walkaround to avoid compiler error for release build
      // TODO: find a clean way to treat in memory key corruption
      auto ok __attribute__((unused)) = ParseInternalKey(key, &ikey);
      // in-memory key corruption is not ok;
      assert(ok);

      if (ikey.type == kTypeMerge) {
        // merge values if the first entry is of merge type
        merge.MergeUntil(iter,  0 /* don't worry about snapshot */);
        prev_key.assign(merge.key().data(), merge.key().size());
        ok = ParseInternalKey(Slice(prev_key), &prev_ikey);
        assert(ok);
        prev_value.assign(merge.value().data(), merge.value().size());
      } else {
        // store first key-value
        prev_key.assign(key.data(), key.size());
        prev_value.assign(iter->value().data(), iter->value().size());
        ok = ParseInternalKey(Slice(prev_key), &prev_ikey);
        assert(ok);
        assert(prev_ikey.sequence >= earliest_seqno_in_memtable);
        iter->Next();
      }

      while (iter->Valid()) {
        bool iterator_at_next = false;
        ParsedInternalKey this_ikey;
        Slice key = iter->key();
        ok = ParseInternalKey(key, &this_ikey);
        assert(ok);
        assert(this_ikey.sequence >= earliest_seqno_in_memtable);

        if (user_comparator->Compare(prev_ikey.user_key, this_ikey.user_key)) {
          // This key is different from previous key.
          // Output prev key and remember current key
          builder->Add(Slice(prev_key), Slice(prev_value));
          if (this_ikey.type == kTypeMerge) {
            merge.MergeUntil(iter, 0 /* don't worry about snapshot */);
            iterator_at_next = true;
            prev_key.assign(merge.key().data(), merge.key().size());
            ok = ParseInternalKey(Slice(prev_key), &prev_ikey);
            assert(ok);
            prev_value.assign(merge.value().data(), merge.value().size());
          } else {
            prev_key.assign(key.data(), key.size());
            prev_value.assign(iter->value().data(), iter->value().size());
            ok = ParseInternalKey(Slice(prev_key), &prev_ikey);
            assert(ok);
          }
        } else {
          // seqno within the same key are in decreasing order
          assert(this_ikey.sequence < prev_ikey.sequence);
          // This key is an earlier version of the same key in prev_key.
          // Skip current key.
        }

        if (!iterator_at_next) iter->Next();
      }
      // output last key
      builder->Add(Slice(prev_key), Slice(prev_value));
      meta->largest.DecodeFrom(Slice(prev_key));
      SequenceNumber seqno = GetInternalKeySeqno(Slice(prev_key));
      meta->smallest_seqno = std::min(meta->smallest_seqno, seqno);
      meta->largest_seqno = std::max(meta->largest_seqno, seqno);

    } else {
      for (; iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        meta->largest.DecodeFrom(key);
        builder->Add(key, iter->value());
        SequenceNumber seqno = GetInternalKeySeqno(key);
        meta->smallest_seqno = std::min(meta->smallest_seqno, seqno);
        meta->largest_seqno = std::max(meta->largest_seqno, seqno);
      }
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
      if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok() && !options.disableDataSync) {
      if (options.use_fsync) {
        StopWatch sw(env, options.statistics, TABLE_SYNC_MICROS);
        s = file->Fsync();
      } else {
        StopWatch sw(env, options.statistics, TABLE_SYNC_MICROS);
        s = file->Sync();
      }
    }
    if (s.ok()) {
      s = file->Close();
    }

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              soptions,
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
