// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/version_set.h"
#include "memory/arena.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

// Advance iterator to the first entry of the next distinct user key.
// This avoids considering older versions (smaller sequence numbers) of
// the same user key after we have already inspected its most recent entry.
inline void SkipToNextUserKey(InternalIterator* it) {
  if (!it->Valid()) return;
  Slice uk = ExtractUserKey(it->key());
  std::string user_key_copy(uk.data(), uk.size());
  do {
    it->Next();
    if (!it->Valid()) break;
    Slice cur = ExtractUserKey(it->key());
    if (cur.size() != user_key_copy.size() ||
        memcmp(cur.data(), user_key_copy.data(), user_key_copy.size()) != 0) {
      break;
    }
  } while (true);
}

// Check if any key exists with the given prefix.
// - Enables prefix_same_as_start when a prefix extractor is configured.
// - Primarily relies on table filters and index blocks to avoid data block
//   reads when possible; data blocks may still be read if filters indicate a
//   potential hit.
// - Snapshot-aware: ignores entries newer than the snapshot sequence number.
// - Skips tombstones and older versions by advancing to the next user key.
// - Uses the extracted (effective) prefix only to bound iteration; existence
//   is determined against the full user-requested prefix.
// - Early-aborts on first visible non-deletion match in mem, immutable
//   memtables, or SST layers.
// - Reads sequence/type from the internal-key footer to avoid full
//   ParseInternalKey overhead in hot loops.
Status DBImpl::PrefixExists(const ReadOptions& options,
                            ColumnFamilyHandle* column_family,
                            const Slice& prefix) {
  if (column_family == nullptr) {
    column_family = DefaultColumnFamily();
  }

  // column_family is guaranteed to be ColumnFamilyHandleImpl by the DB
  // interface
  auto cfh = static_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  if (cfd == nullptr) {
    return Status::InvalidArgument("Column family not found");
  }

  // SuperVersion and options needed for prefix_extractor and iterators
  SuperVersion* sv = GetAndRefSuperVersion(cfd);
  if (sv == nullptr) {
    return Status::InvalidArgument("Column family not found");
  }

  const SliceTransform* prefix_extractor =
      sv->mutable_cf_options.prefix_extractor.get();

  // If no prefix extractor is configured, we'll still benefit from:
  // 1. Early abort on first key match
  // 2. No data block loading
  // We'll use the input as a search key and check if found keys start with it.
  Slice actual_prefix = prefix;
  // requested_prefix is the exact prefix the caller asked for. When an
  // extractor exists, actual_prefix is the extractor's Transform(prefix) and
  // is only used to bound iteration. Any match must still start with
  // requested_prefix to return OK.
  const Slice& requested_prefix = prefix;  // full user-specified prefix
  if (prefix_extractor != nullptr) {
    // Validate that the prefix is in the domain
    if (!prefix_extractor->InDomain(prefix)) {
      ReturnAndCleanupSuperVersion(cfd, sv);
      return Status::NotFound();
    }
    // Extract the actual prefix
    actual_prefix = prefix_extractor->Transform(prefix);
  }

  // Create an internal key for searching
  std::string internal_key_str;
  AppendInternalKey(
      &internal_key_str,
      ParsedInternalKey(actual_prefix, kMaxSequenceNumber, kTypeValue));
  Slice internal_key(internal_key_str);

  // Use a local Arena for any temporary allocations needed by iterators
  Arena arena;

  // Use prefix_same_as_start when an extractor exists to constrain seeks
  ReadOptions ro = options;
  if (prefix_extractor != nullptr) {
    ro.prefix_same_as_start = true;
    ro.total_order_seek = false;
  }

  // Step 1: Check mutable memtable
  // Snapshot-aware: skip entries with sequence > snapshot; the first visible
  // version decides existence (non-deletion => OK, deletion => move to next
  // user key). Sequence/type are decoded from the internal-key footer to avoid
  // full parsing overhead. Iterators allocated from an Arena must be destroyed
  // to run their destructors. We wrap them with ScopedArenaPtr so their
  // destructors run and release any cache pins before we release the
  // SuperVersion.
  if (sv->mem != nullptr) {
    InternalIterator* mem_iter_ptr =
        sv->mem->NewIterator(ro, nullptr, &arena, prefix_extractor, false);
    ScopedArenaPtr<InternalIterator> mem_iter(mem_iter_ptr);
    mem_iter->Seek(internal_key);
    SequenceNumber snap_seq =
        ro.snapshot ? ro.snapshot->GetSequenceNumber() : kMaxSequenceNumber;
    while (mem_iter->Valid()) {
      Slice found_user_key = ExtractUserKey(mem_iter->key());
      // Stop when we leave the effective (extracted) prefix domain
      if (!(found_user_key.size() >= actual_prefix.size() &&
            memcmp(found_user_key.data(), actual_prefix.data(),
                   actual_prefix.size()) == 0)) {
        break;
      }
      uint64_t footer = ExtractInternalKeyFooter(mem_iter->key());
      SequenceNumber seq = footer >> 8;
      ValueType vt = static_cast<ValueType>(footer & 0xff);
      {
        if (seq > snap_seq) {
          // Not visible yet at snapshot; advance within same user key
          mem_iter->Next();
          continue;
        }
        // First visible version at/under snapshot_seq decides existence
        if (vt != kTypeDeletion && vt != kTypeSingleDeletion &&
            found_user_key.size() >= requested_prefix.size() &&
            memcmp(found_user_key.data(), requested_prefix.data(),
                   requested_prefix.size()) == 0) {
          mem_iter.reset();
          ReturnAndCleanupSuperVersion(cfd, sv);
          return Status::OK();
        }
        // Visible deletion: skip to next distinct user key
        SkipToNextUserKey(mem_iter.get());
        continue;
      }
    }
  }

  // Step 2: Check immutable memtables
  // Snapshot-aware with the same visibility rules as memtable. Sequence/type
  // are decoded from the footer as well. Multiple imm memtables may exist;
  // each iterator is wrapped so cleanup runs deterministically on early return.
  if (sv->imm != nullptr) {
    std::vector<InternalIterator*> imm_iters_raw;
    sv->imm->AddIterators(ro, nullptr, prefix_extractor, &imm_iters_raw,
                          &arena);
    for (auto raw : imm_iters_raw) {
      ScopedArenaPtr<InternalIterator> iter(raw);
      iter->Seek(internal_key);
      SequenceNumber snap_seq =
          ro.snapshot ? ro.snapshot->GetSequenceNumber() : kMaxSequenceNumber;
      while (iter->Valid()) {
        Slice found_user_key = ExtractUserKey(iter->key());
        if (!(found_user_key.size() >= actual_prefix.size() &&
              memcmp(found_user_key.data(), actual_prefix.data(),
                     actual_prefix.size()) == 0)) {
          break;
        }
        uint64_t footer = ExtractInternalKeyFooter(iter->key());
        SequenceNumber seq = footer >> 8;
        ValueType vt = static_cast<ValueType>(footer & 0xff);
        {
          if (seq > snap_seq) {
            iter->Next();
            continue;
          }
          if (vt != kTypeDeletion && vt != kTypeSingleDeletion &&
              found_user_key.size() >= requested_prefix.size() &&
              memcmp(found_user_key.data(), requested_prefix.data(),
                     requested_prefix.size()) == 0) {
            iter.reset();
            ReturnAndCleanupSuperVersion(cfd, sv);
            return Status::OK();
          }
          SkipToNextUserKey(iter.get());
          continue;
        }
      }
    }
  }

  // Step 3: Check SST files using filter policies and index blocks
  // Filter policies (bloom, ribbon, etc.) eliminate files that don't contain
  // the prefix. Index blocks are consulted first; data blocks may still be
  // read when the filter indicates a potential match.
  Version* current = sv->current;
  if (current == nullptr) {
    ReturnAndCleanupSuperVersion(cfd, sv);
    return Status::NotFound();
  }

  // Create an iterator to seek through SST files.
  // Use the overload that refs its own SuperVersion so we can explicitly
  // return the TLS-acquired SuperVersion before iterating. The SST scan is
  // also snapshot-aware and uses the extracted prefix only to bound iteration;
  // matches are verified against the requested prefix. Sequence/type are read
  // via footer decoding.
  ScopedArenaPtr<InternalIterator> iter_guard(
      NewInternalIterator(ro, &arena, kMaxSequenceNumber, column_family,
                          /*allow_unprepared_value=*/true));
  InternalIterator* iter = iter_guard.get();

  // Return the TLS SuperVersion now; the SST iterator holds its own ref.
  ReturnAndCleanupSuperVersion(cfd, sv);

  iter->Seek(internal_key);
  SequenceNumber snap_seq =
      ro.snapshot ? ro.snapshot->GetSequenceNumber() : kMaxSequenceNumber;
  while (iter->Valid()) {
    Slice found_user_key = ExtractUserKey(iter->key());
    if (!(found_user_key.size() >= actual_prefix.size() &&
          memcmp(found_user_key.data(), actual_prefix.data(),
                 actual_prefix.size()) == 0)) {
      break;
    }
    uint64_t footer = ExtractInternalKeyFooter(iter->key());
    SequenceNumber seq = footer >> 8;
    ValueType vt = static_cast<ValueType>(footer & 0xff);
    {
      if (seq > snap_seq) {
        iter->Next();
        continue;
      }
      if (vt != kTypeDeletion && vt != kTypeSingleDeletion &&
          found_user_key.size() >= requested_prefix.size() &&
          memcmp(found_user_key.data(), requested_prefix.data(),
                 requested_prefix.size()) == 0) {
        // Ensure iterator cleanups (including SV release) run before return.
        iter_guard.reset();
        return Status::OK();
      }
      SkipToNextUserKey(iter);
      continue;
    }
  }

  // Ensure SST iterator cleanups run before return.
  iter_guard.reset();
  return Status::NotFound();
}

// Batched prefix existence checks.
// - Enables prefix_same_as_start when a prefix extractor is configured.
// - Reuses a single SuperVersion and shared iterators (mem/imm/SST).
// - Snapshot-aware across all layers: entries newer than the snapshot are
//   skipped; the first visible version decides existence.
// - Uses extracted (effective) prefixes to bound iteration and the full
//   requested prefixes to determine existence per item (exact per-requested-
//   prefix semantics within each effective-prefix group).
// - Reads sequence/type from internal-key footers in hot loops to avoid full
//   ParseInternalKey overhead.
// - Mem/imm iterators are Arena-backed and wrapped with ScopedArenaPtr so
//   destructors run and release cache pins before exit.
// - The SST iterator is created via NewInternalIterator() overload that refs
//   its own SuperVersion; we then release the TLS SuperVersion at function end.
// - Optionally sorts and deduplicates effective prefixes when
//   prefixes_sorted == false, then maps results back to original order.
void DBImpl::PrefixExistsMulti(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               bool prefixes_sorted, size_t num_prefixes,
                               const Slice* prefixes, Status* statuses) {
  if (column_family == nullptr) {
    column_family = DefaultColumnFamily();
  }

  auto cfh = static_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  if (cfd == nullptr) {
    for (size_t i = 0; i < num_prefixes; ++i) {
      statuses[i] = Status::InvalidArgument("Column family not found");
    }
    return;
  }

  // Prepare SuperVersion once, needed for options and iterator reuse
  SuperVersion* sv = GetAndRefSuperVersion(cfd);
  if (sv == nullptr) {
    for (size_t i = 0; i < num_prefixes; ++i) {
      statuses[i] = Status::InvalidArgument("Column family not found");
    }
    return;
  }

  const SliceTransform* prefix_extractor =
      sv->mutable_cf_options.prefix_extractor.get();

  // Preprocess prefixes: domain check and transform
  std::vector<Slice> actual_prefixes;
  actual_prefixes.reserve(num_prefixes);
  std::vector<std::string> transformed_storage;  // own storage for transformed
  transformed_storage.reserve(num_prefixes);

  for (size_t i = 0; i < num_prefixes; ++i) {
    if (prefix_extractor != nullptr) {
      // Validate the requested prefix is in extractor domain before Transform.
      if (!prefix_extractor->InDomain(prefixes[i])) {
        // Not in domain: cannot exist under extracted namespace.
        statuses[i] = Status::NotFound();
        // Placeholder slice to keep vector sizes aligned; will be ignored.
        actual_prefixes.emplace_back(Slice());
      } else {
        // Transform the requested prefix to the effective prefix. If the
        // transform returns a Slice that aliases the input memory, we can
        // safely reference it directly (the caller-owned input outlives this
        // call). Otherwise, make an owned copy to ensure stability. This avoids
        // an allocation/copy in the common case where the extractor returns a
        // sub-slice of the input.
        Slice transformed = prefix_extractor->Transform(prefixes[i]);
        const char* base = prefixes[i].data();
        const size_t base_len = prefixes[i].size();
        const char* tdata = transformed.data();
        const size_t tlen = transformed.size();
        const bool aliases_input =
            tdata >= base && (tdata + tlen) <= (base + base_len);
        if (aliases_input) {
          actual_prefixes.emplace_back(transformed);
        } else {
          transformed_storage.emplace_back(tdata, tlen);
          actual_prefixes.emplace_back(transformed_storage.back());
        }
        // Mark as pending; will be set to OK/NotFound per requested prefix.
        statuses[i] = Status::OK();
      }
    } else {
      actual_prefixes.emplace_back(prefixes[i]);
      statuses[i] = Status::OK();
    }
  }

  // Prepare processing order with optional sorting/deduplication.
  struct Item {
    size_t idx;
    Slice pfx;  // actual effective prefix
  };
  std::vector<Item> items;
  items.reserve(num_prefixes);
  for (size_t i = 0; i < num_prefixes; ++i) {
    if (statuses[i].ok()) {
      items.push_back({i, actual_prefixes[i]});
    }
  }

  if (!prefixes_sorted) {
    std::stable_sort(items.begin(), items.end(),
                     [](const Item& a, const Item& b) {
                       int r = a.pfx.compare(b.pfx);
                       return r < 0;
                     });
  }

  // Local arena for iterators
  Arena arena;

  // Build shared iterators once, using prefix_same_as_start when extractor
  // exists
  ReadOptions ro = options;
  if (prefix_extractor != nullptr) {
    ro.prefix_same_as_start = true;
    ro.total_order_seek = false;
  }

  // Build shared iterators once
  InternalIterator* mem_iter = nullptr;
  ScopedArenaPtr<InternalIterator> mem_iter_guard;
  if (sv->mem != nullptr) {
    mem_iter =
        sv->mem->NewIterator(ro, nullptr, &arena, prefix_extractor, false);
    mem_iter_guard.reset(mem_iter);
  }

  std::vector<InternalIterator*> imm_iters;
  std::vector<ScopedArenaPtr<InternalIterator>> imm_iters_guards;
  if (sv->imm != nullptr) {
    sv->imm->AddIterators(ro, nullptr, prefix_extractor, &imm_iters, &arena);
    imm_iters_guards.reserve(imm_iters.size());
    for (auto* p : imm_iters) {
      imm_iters_guards.emplace_back(p);
    }
  }

  Version* current = sv->current;
  ScopedArenaPtr<InternalIterator> sst_iter;
  if (current != nullptr) {
    sst_iter.reset(NewInternalIterator(ro, &arena, kMaxSequenceNumber,
                                       column_family,
                                       /*allow_unprepared_value=*/true));
  }

  // Process prefixes in selected order, with deduplication across equal
  // effective prefixes. Within each group, compute results per requested
  // prefix: a match only if found key starts with the exact requested prefix.
  // Track per-item matches and stop scanning the group early once all are
  // satisfied or the iterator exits the effective-prefix range.
  size_t pos = 0;
  while (pos < items.size()) {
    size_t start = pos;
    const Slice& p = items[pos].pfx;
    // group equal prefixes
    while (pos < items.size() && items[pos].pfx.compare(p) == 0) {
      ++pos;
    }

    // Build internal key for this prefix
    std::string ikey_str;
    AppendInternalKey(&ikey_str,
                      ParsedInternalKey(p, kMaxSequenceNumber, kTypeValue));
    Slice ikey(ikey_str);

    // Track per-item matches in this group
    const size_t group_size = pos - start;
    std::vector<unsigned char> matched(group_size, 0);
    size_t remaining = group_size;

    // Check mutable memtable
    if (mem_iter) {
      mem_iter->Seek(ikey);
      SequenceNumber snap_seq =
          ro.snapshot ? ro.snapshot->GetSequenceNumber() : kMaxSequenceNumber;
      while (mem_iter->Valid()) {
        Slice fuk = ExtractUserKey(mem_iter->key());
        // Bound by effective prefix without calling Transform(found_key)
        if (!(fuk.size() >= p.size() &&
              memcmp(fuk.data(), p.data(), p.size()) == 0)) {
          break;
        }
        uint64_t footer = ExtractInternalKeyFooter(mem_iter->key());
        SequenceNumber seq = footer >> 8;
        ValueType vt = static_cast<ValueType>(footer & 0xff);
        {
          if (seq > snap_seq) {
            mem_iter->Next();
            continue;
          }
          if (vt != kTypeDeletion && vt != kTypeSingleDeletion) {
            // Assign per requested prefix in this group
            for (size_t j = 0; j < group_size; ++j) {
              if (matched[j]) continue;
              size_t idx = items[start + j].idx;
              const Slice& req = prefixes[idx];
              if (fuk.size() >= req.size() &&
                  memcmp(fuk.data(), req.data(), req.size()) == 0) {
                statuses[idx] = Status::OK();
                matched[j] = 1;
                if (--remaining == 0) break;
              }
            }
            if (remaining == 0) break;
          }
          SkipToNextUserKey(mem_iter);
          continue;
        }
      }
    }

    // Check immutable memtables
    if (remaining > 0 && !imm_iters.empty()) {
      for (auto& guard : imm_iters_guards) {
        InternalIterator* iter = guard.get();
        iter->Seek(ikey);
        SequenceNumber snap_seq =
            ro.snapshot ? ro.snapshot->GetSequenceNumber() : kMaxSequenceNumber;
        while (iter->Valid()) {
          Slice fuk = ExtractUserKey(iter->key());
          // Bound by effective prefix without calling Transform(found_key)
          if (!(fuk.size() >= p.size() &&
                memcmp(fuk.data(), p.data(), p.size()) == 0)) {
            break;
          }
          uint64_t footer = ExtractInternalKeyFooter(iter->key());
          SequenceNumber seq = footer >> 8;
          ValueType vt = static_cast<ValueType>(footer & 0xff);
          {
            if (seq > snap_seq) {
              iter->Next();
              continue;
            }
            if (vt != kTypeDeletion && vt != kTypeSingleDeletion) {
              for (size_t j = 0; j < group_size; ++j) {
                if (matched[j]) continue;
                size_t idx = items[start + j].idx;
                const Slice& req = prefixes[idx];
                if (fuk.size() >= req.size() &&
                    memcmp(fuk.data(), req.data(), req.size()) == 0) {
                  statuses[idx] = Status::OK();
                  matched[j] = 1;
                  if (--remaining == 0) break;
                }
              }
              if (remaining == 0) break;
            }
            SkipToNextUserKey(iter);
            continue;
          }
        }
        if (remaining == 0) break;
      }
    }

    // Check SST files (index blocks first, snapshot-aware)
    if (remaining > 0 && sst_iter) {
      sst_iter->Seek(ikey);
      SequenceNumber snap_seq =
          ro.snapshot ? ro.snapshot->GetSequenceNumber() : kMaxSequenceNumber;
      while (sst_iter->Valid()) {
        Slice fuk = ExtractUserKey(sst_iter->key());
        if (!(fuk.size() >= p.size() &&
              memcmp(fuk.data(), p.data(), p.size()) == 0)) {
          break;
        }
        uint64_t footer = ExtractInternalKeyFooter(sst_iter->key());
        SequenceNumber seq = footer >> 8;
        ValueType vt = static_cast<ValueType>(footer & 0xff);
        {
          if (seq > snap_seq) {
            sst_iter->Next();
            continue;
          }
          if (vt != kTypeDeletion && vt != kTypeSingleDeletion) {
            for (size_t j = 0; j < group_size; ++j) {
              if (matched[j]) continue;
              size_t idx = items[start + j].idx;
              const Slice& req = prefixes[idx];
              if (fuk.size() >= req.size() &&
                  memcmp(fuk.data(), req.data(), req.size()) == 0) {
                statuses[idx] = Status::OK();
                matched[j] = 1;
                if (--remaining == 0) break;
              }
            }
            if (remaining == 0) break;
          }
          SkipToNextUserKey(sst_iter.get());
          continue;
        }
      }
    }

    // Assign NotFound to any unmatched requested prefixes in this group
    if (remaining > 0) {
      for (size_t j = 0; j < group_size; ++j) {
        if (!matched[j]) {
          statuses[items[start + j].idx] = Status::NotFound();
        }
      }
    }
  }

  // Ensure SST iterator cleanups run before return.
  if (sst_iter) {
    sst_iter.reset();
  }
  // Now release the TLS-acquired SuperVersion.
  ReturnAndCleanupSuperVersion(cfd, sv);
}

}  // namespace ROCKSDB_NAMESPACE
