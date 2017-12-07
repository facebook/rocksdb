// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util/transaction_test_util.h"

#include <inttypes.h>
#include <algorithm>
#include <numeric>
#include <string>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/random.h"
#include "util/string_util.h"

namespace rocksdb {

RandomTransactionInserter::RandomTransactionInserter(
    Random64* rand, const WriteOptions& write_options,
    const ReadOptions& read_options, uint64_t num_keys, uint16_t num_sets)
    : rand_(rand),
      write_options_(write_options),
      read_options_(read_options),
      num_keys_(num_keys),
      num_sets_(num_sets),
      txn_id_(0) {}

RandomTransactionInserter::~RandomTransactionInserter() {
  if (txn_ != nullptr) {
    delete txn_;
  }
  if (optimistic_txn_ != nullptr) {
    delete optimistic_txn_;
  }
}

bool RandomTransactionInserter::TransactionDBInsert(
    TransactionDB* db, const TransactionOptions& txn_options) {
  txn_ = db->BeginTransaction(write_options_, txn_options, txn_);
  bool take_snapshot = rand_->OneIn(2);
  if (take_snapshot) {
    txn_->SetSnapshot();
    read_options_.snapshot = txn_->GetSnapshot();
  }
  auto res = DoInsert(nullptr, txn_, false);
  if (take_snapshot) {
    read_options_.snapshot = nullptr;
  }
  return res;
}

bool RandomTransactionInserter::OptimisticTransactionDBInsert(
    OptimisticTransactionDB* db,
    const OptimisticTransactionOptions& txn_options) {
  optimistic_txn_ =
      db->BeginTransaction(write_options_, txn_options, optimistic_txn_);

  return DoInsert(nullptr, optimistic_txn_, true);
}

bool RandomTransactionInserter::DBInsert(DB* db) {
  return DoInsert(db, nullptr, false);
}

Status RandomTransactionInserter::DBGet(
    DB* db, Transaction* txn, ReadOptions& read_options, uint16_t set_i,
    uint64_t ikey, bool get_for_update, uint64_t* int_value,
    std::string* full_key, bool* unexpected_error) {
  Status s;
  // four digits and zero end char
  char prefix_buf[5];
  // Pad prefix appropriately so we can iterate over each set
  snprintf(prefix_buf, sizeof(prefix_buf), "%.4u", set_i + 1);
  // key format:  [SET#][random#]
  std::string skey = ToString(ikey);
  Slice base_key(skey);
  *full_key = std::string(prefix_buf) + base_key.ToString();
  Slice key(*full_key);

  std::string value;
  if (txn != nullptr) {
    if (get_for_update) {
      s = txn->GetForUpdate(read_options, key, &value);
    } else {
      s = txn->Get(read_options, key, &value);
    }
  } else {
    s = db->Get(read_options, key, &value);
  }

  if (s.ok()) {
    // Found key, parse its value
    *int_value = std::stoull(value);
    if (*int_value == 0 || *int_value == ULONG_MAX) {
      *unexpected_error = true;
      fprintf(stderr, "Get returned unexpected value: %s\n", value.c_str());
      s = Status::Corruption();
    }
  } else if (s.IsNotFound()) {
    // Have not yet written to this key, so assume its value is 0
    *int_value = 0;
    s = Status::OK();
  }
  return s;
}

bool RandomTransactionInserter::DoInsert(DB* db, Transaction* txn,
                                         bool is_optimistic) {
  Status s;
  WriteBatch batch;

  // pick a random number to use to increment a key in each set
  uint64_t incr = (rand_->Next() % 100) + 1;
  bool unexpected_error = false;

  std::vector<uint16_t> set_vec(num_sets_);
  std::iota(set_vec.begin(), set_vec.end(), 0);
  std::random_shuffle(set_vec.begin(), set_vec.end(),
                      [&](uint64_t r) { return rand_->Uniform(r); });
  // For each set, pick a key at random and increment it
  for (uint16_t set_i : set_vec) {
    uint64_t int_value = 0;
    std::string full_key;
    uint64_t rand_key = rand_->Next() % num_keys_;
    const bool get_for_update = txn ? rand_->OneIn(2) : false;
    s = DBGet(db, txn, read_options_, set_i, rand_key, get_for_update,
              &int_value, &full_key, &unexpected_error);
    Slice key(full_key);
    if (!s.ok()) {
      // Optimistic transactions should never return non-ok status here.
      // Non-optimistic transactions may return write-coflict/timeout errors.
      if (is_optimistic || !(s.IsBusy() || s.IsTimedOut() || s.IsTryAgain())) {
        fprintf(stderr, "Get returned an unexpected error: %s\n",
                s.ToString().c_str());
        unexpected_error = true;
      }
      break;
    }

    if (s.ok()) {
      // Increment key
      std::string sum = ToString(int_value + incr);
      if (txn != nullptr) {
        s = txn->Put(key, sum);
        if (!get_for_update && (s.IsBusy() || s.IsTimedOut())) {
          // If the initial get was not for update, then the key is not locked
          // before put and put could fail due to concurrent writes.
          break;
        } else if (!s.ok()) {
          // Since we did a GetForUpdate, Put should not fail.
          fprintf(stderr, "Put returned an unexpected error: %s\n",
                  s.ToString().c_str());
          unexpected_error = true;
        }
      } else {
        batch.Put(key, sum);
      }
      bytes_inserted_ += key.size() + sum.size();
    }
  }

  if (s.ok()) {
    if (txn != nullptr) {
      std::hash<std::thread::id> hasher;
      char name[64];
      snprintf(name, 64, "txn%zu-%d", hasher(std::this_thread::get_id()),
               txn_id_++);
      assert(strlen(name) < 64 - 1);
      if (!is_optimistic && !rand_->OneIn(10)) {
        // also try commit without prpare
        txn->SetName(name);
        s = txn->Prepare();
        assert(s.ok());
      }
      // TODO(myabandeh): enable this when WritePreparedTxnDB::RollbackPrepared
      // is updated to handle in-the-middle rollbacks.
      if (!rand_->OneIn(0)) {
        s = txn->Commit();
      } else {
        // Also try 5% rollback
        s = txn->Rollback();
        assert(s.ok());
      }
      assert(is_optimistic || s.ok());

      if (!s.ok()) {
        if (is_optimistic) {
          // Optimistic transactions can have write-conflict errors on commit.
          // Any other error is unexpected.
          if (!(s.IsBusy() || s.IsTimedOut() || s.IsTryAgain())) {
            unexpected_error = true;
          }
        } else {
          // Non-optimistic transactions should only fail due to expiration
          // or write failures.  For testing purproses, we do not expect any
          // write failures.
          if (!s.IsExpired()) {
            unexpected_error = true;
          }
        }

        if (unexpected_error) {
          fprintf(stderr, "Commit returned an unexpected error: %s\n",
                  s.ToString().c_str());
        }
      }
    } else {
      s = db->Write(write_options_, &batch);
      if (!s.ok()) {
        unexpected_error = true;
        fprintf(stderr, "Write returned an unexpected error: %s\n",
                s.ToString().c_str());
      }
    }
  } else {
    if (txn != nullptr) {
      assert(txn->Rollback().ok());
    }
  }

  if (s.ok()) {
    success_count_++;
  } else {
    failure_count_++;
  }

  last_status_ = s;

  // return success if we didn't get any unexpected errors
  return !unexpected_error;
}

// Verify that the sum of the keys in each set are equal
Status RandomTransactionInserter::Verify(DB* db, uint16_t num_sets,
                                         uint64_t num_keys_per_set,
                                         bool take_snapshot, Random64* rand) {
  uint64_t prev_total = 0;
  uint32_t prev_i = 0;
  bool prev_assigned = false;

  ReadOptions roptions;
  if (take_snapshot) {
    roptions.snapshot = db->GetSnapshot();
  }

  std::vector<uint16_t> set_vec(num_sets);
  std::iota(set_vec.begin(), set_vec.end(), 0);
  if (rand) {
    std::random_shuffle(set_vec.begin(), set_vec.end(),
                        [&](uint64_t r) { return rand->Uniform(r); });
  }
  // For each set of keys with the same prefix, sum all the values
  for (uint16_t set_i : set_vec) {
    // four digits and zero end char
    char prefix_buf[5];
    snprintf(prefix_buf, sizeof(prefix_buf), "%.4u", set_i + 1);
    uint64_t total = 0;

    // Use either point lookup or iterator. Point lookups are slower so we use
    // it less often.
    if (num_keys_per_set != 0 && rand && rand->OneIn(10)) {  // use point lookup
      ReadOptions read_options;
      for (uint64_t k = 0; k < num_keys_per_set; k++) {
        std::string dont_care;
        uint64_t int_value = 0;
        bool unexpected_error = false;
        const bool FOR_UPDATE = false;
        Status s = DBGet(db, nullptr, roptions, set_i, k, FOR_UPDATE,
                         &int_value, &dont_care, &unexpected_error);
        assert(s.ok());
        assert(!unexpected_error);
        total += int_value;
      }
    } else {  // user iterators
      Iterator* iter = db->NewIterator(roptions);
      for (iter->Seek(Slice(prefix_buf, 4)); iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        // stop when we reach a different prefix
        if (key.ToString().compare(0, 4, prefix_buf) != 0) {
          break;
        }
        Slice value = iter->value();
        uint64_t int_value = std::stoull(value.ToString());
        if (int_value == 0 || int_value == ULONG_MAX) {
          fprintf(stderr, "Iter returned unexpected value: %s\n",
                  value.ToString().c_str());
          return Status::Corruption();
        }
        total += int_value;
      }
      delete iter;
    }

    if (prev_assigned && total != prev_total) {
      fprintf(stdout,
              "RandomTransactionVerify found inconsistent totals. "
              "Set[%" PRIu32 "]: %" PRIu64 ", Set[%" PRIu32 "]: %" PRIu64 " \n",
              prev_i, prev_total, set_i, total);
      return Status::Corruption();
    }
    prev_total = total;
    prev_i = set_i;
    prev_assigned = true;
  }
  if (take_snapshot) {
    db->ReleaseSnapshot(roptions.snapshot);
  }

  return Status::OK();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
