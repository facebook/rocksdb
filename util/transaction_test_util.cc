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

  return DoInsert(nullptr, txn_, false);
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

bool RandomTransactionInserter::DoInsert(DB* db, Transaction* txn,
                                         bool is_optimistic) {
  Status s;
  WriteBatch batch;
  std::string value;

  // pick a random number to use to increment a key in each set
  uint64_t incr = (rand_->Next() % 100) + 1;

  bool unexpected_error = false;

  // For each set, pick a key at random and increment it
  for (uint8_t i = 0; i < num_sets_; i++) {
    uint64_t int_value = 0;
    char prefix_buf[5];
    // prefix_buf needs to be large enough to hold a uint16 in string form

    // key format:  [SET#][random#]
    std::string rand_key = ToString(rand_->Next() % num_keys_);
    Slice base_key(rand_key);

    // Pad prefix appropriately so we can iterate over each set
    snprintf(prefix_buf, sizeof(prefix_buf), "%.4u", i + 1);
    std::string full_key = std::string(prefix_buf) + base_key.ToString();
    Slice key(full_key);

    if (txn != nullptr) {
      s = txn->GetForUpdate(read_options_, key, &value);
    } else {
      s = db->Get(read_options_, key, &value);
    }

    if (s.ok()) {
      // Found key, parse its value
      int_value = std::stoull(value);

      if (int_value == 0 || int_value == ULONG_MAX) {
        unexpected_error = true;
        fprintf(stderr, "Get returned unexpected value: %s\n", value.c_str());
        s = Status::Corruption();
      }
    } else if (s.IsNotFound()) {
      // Have not yet written to this key, so assume its value is 0
      int_value = 0;
      s = Status::OK();
    } else {
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
        if (!s.ok()) {
          // Since we did a GetForUpdate, Put should not fail.
          fprintf(stderr, "Put returned an unexpected error: %s\n",
                  s.ToString().c_str());
          unexpected_error = true;
        }
      } else {
        batch.Put(key, sum);
      }
    }
  }

  if (s.ok()) {
    if (txn != nullptr) {
      std::hash<std::thread::id> hasher;
      char name[64];
      snprintf(name, 64, "txn%zu-%d", hasher(std::this_thread::get_id()),
               txn_id_++);
      assert(strlen(name) < 64 - 1);
      txn->SetName(name);
      s = txn->Prepare();
      s = txn->Commit();

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
      txn->Rollback();
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

Status RandomTransactionInserter::Verify(DB* db, uint16_t num_sets) {
  uint64_t prev_total = 0;

  // For each set of keys with the same prefix, sum all the values
  for (uint32_t i = 0; i < num_sets; i++) {
    char prefix_buf[6];
    snprintf(prefix_buf, sizeof(prefix_buf), "%.4u", i + 1);
    uint64_t total = 0;

    Iterator* iter = db->NewIterator(ReadOptions());

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

    if (i > 0) {
      if (total != prev_total) {
        fprintf(stderr,
                "RandomTransactionVerify found inconsistent totals. "
                "Set[%" PRIu32 "]: %" PRIu64 ", Set[%" PRIu32 "]: %" PRIu64
                " \n",
                i - 1, prev_total, i, total);
        return Status::Corruption();
      }
    }
    prev_total = total;
  }

  return Status::OK();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
