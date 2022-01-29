//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#include "db_stress_tool/multi_ops_txns_stress.h"

#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/defer.h"
#ifndef NDEBUG
#include "utilities/fault_injection_fs.h"
#endif  // NDEBUG

namespace ROCKSDB_NAMESPACE {

// TODO: move these to gflags.
DEFINE_int32(ub_c, 1000, "(Exclusive) upper bound of C");
DEFINE_int32(ub_a, 1000, "(Exclusive) upper bound of A");

DEFINE_int32(delay_snapshot_read_one_in, 0,
             "With a chance of 1/N, inject a random delay between taking "
             "snapshot and read.");

std::string MultiOpsTxnsStressTest::Record::EncodePrimaryKey(uint32_t a) {
  char buf[8];
  EncodeFixed32(buf, kPrimaryIndexId);
  std::reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, a);
  std::reverse(buf + 4, buf + 8);
  return std::string(buf, sizeof(buf));
}

std::string MultiOpsTxnsStressTest::Record::EncodeSecondaryKey(uint32_t c) {
  char buf[8];
  EncodeFixed32(buf, kSecondaryIndexId);
  std::reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, c);
  std::reverse(buf + 4, buf + 8);
  return std::string(buf, sizeof(buf));
}

std::string MultiOpsTxnsStressTest::Record::EncodeSecondaryKey(uint32_t c,
                                                               uint32_t a) {
  char buf[12];
  EncodeFixed32(buf, kSecondaryIndexId);
  std::reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, c);
  EncodeFixed32(buf + 8, a);
  std::reverse(buf + 4, buf + 8);
  std::reverse(buf + 8, buf + 12);
  return std::string(buf, sizeof(buf));
}

std::tuple<Status, uint32_t, uint32_t>
MultiOpsTxnsStressTest::Record::DecodePrimaryIndexValue(
    Slice primary_index_value) {
  if (primary_index_value.size() != 8) {
    return std::tuple<Status, uint32_t, uint32_t>{Status::Corruption(""), 0, 0};
  }
  uint32_t b = 0;
  uint32_t c = 0;
  if (!GetFixed32(&primary_index_value, &b) ||
      !GetFixed32(&primary_index_value, &c)) {
    assert(false);
    return std::tuple<Status, uint32_t, uint32_t>{Status::Corruption(""), 0, 0};
  }
  return std::tuple<Status, uint32_t, uint32_t>{Status::OK(), b, c};
}

std::pair<Status, uint32_t>
MultiOpsTxnsStressTest::Record::DecodeSecondaryIndexValue(
    Slice secondary_index_value) {
  if (secondary_index_value.size() != 4) {
    return std::make_pair(Status::Corruption(""), 0);
  }
  uint32_t crc = 0;
  bool result __attribute__((unused)) =
      GetFixed32(&secondary_index_value, &crc);
  assert(result);
  return std::make_pair(Status::OK(), crc);
}

std::pair<std::string, std::string>
MultiOpsTxnsStressTest::Record::EncodePrimaryIndexEntry() const {
  std::string primary_index_key = EncodePrimaryKey();
  std::string primary_index_value = EncodePrimaryIndexValue();
  return std::make_pair(primary_index_key, primary_index_value);
}

std::string MultiOpsTxnsStressTest::Record::EncodePrimaryKey() const {
  return EncodePrimaryKey(a_);
}

std::string MultiOpsTxnsStressTest::Record::EncodePrimaryIndexValue() const {
  char buf[8];
  EncodeFixed32(buf, b_);
  EncodeFixed32(buf + 4, c_);
  return std::string(buf, sizeof(buf));
}

std::pair<std::string, std::string>
MultiOpsTxnsStressTest::Record::EncodeSecondaryIndexEntry() const {
  std::string secondary_index_key;
  char buf[12];
  EncodeFixed32(buf, kSecondaryIndexId);
  std::reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, c_);
  EncodeFixed32(buf + 8, a_);
  std::reverse(buf + 4, buf + 8);
  std::reverse(buf + 8, buf + 12);
  secondary_index_key.assign(buf, sizeof(buf));

  // Secondary index value is always 4-byte crc32 of the secondary key
  std::string secondary_index_value;
  uint32_t crc = crc32c::Value(buf, sizeof(buf));
  PutFixed32(&secondary_index_value, crc);
  return std::make_pair(secondary_index_key, secondary_index_value);
}

std::string MultiOpsTxnsStressTest::Record::EncodeSecondaryKey() const {
  char buf[12];
  EncodeFixed32(buf, kSecondaryIndexId);
  std::reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, c_);
  EncodeFixed32(buf + 8, a_);
  std::reverse(buf + 4, buf + 8);
  std::reverse(buf + 8, buf + 12);
  return std::string(buf, sizeof(buf));
}

Status MultiOpsTxnsStressTest::Record::DecodePrimaryIndexEntry(
    Slice primary_index_key, Slice primary_index_value) {
  if (primary_index_key.size() != 8) {
    assert(false);
    return Status::Corruption("Primary index key length is not 8");
  }

  const char* const index_id_buf = primary_index_key.data();
  uint32_t index_id =
      static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[0])) << 24;
  index_id += static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[1]))
              << 16;
  index_id += static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[2]))
              << 8;
  index_id +=
      static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[3]));
  primary_index_key.remove_prefix(sizeof(uint32_t));
  if (index_id != kPrimaryIndexId) {
    std::ostringstream oss;
    oss << "Unexpected primary index id: " << index_id;
    return Status::Corruption(oss.str());
  }

  const char* const buf = primary_index_key.data();
  a_ = static_cast<uint32_t>(static_cast<unsigned char>(buf[0])) << 24;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 16;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 8;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[3]));

  if (primary_index_value.size() != 8) {
    return Status::Corruption("Primary index value length is not 8");
  }
  GetFixed32(&primary_index_value, &b_);
  GetFixed32(&primary_index_value, &c_);
  return Status::OK();
}

Status MultiOpsTxnsStressTest::Record::DecodeSecondaryIndexEntry(
    Slice secondary_index_key, Slice secondary_index_value) {
  if (secondary_index_key.size() != 12) {
    return Status::Corruption("Secondary index key length is not 12");
  }
  uint32_t crc =
      crc32c::Value(secondary_index_key.data(), secondary_index_key.size());

  const char* const index_id_buf = secondary_index_key.data();
  uint32_t index_id =
      static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[0])) << 24;
  index_id += static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[1]))
              << 16;
  index_id += static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[2]))
              << 8;
  index_id +=
      static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[3]));
  secondary_index_key.remove_prefix(sizeof(uint32_t));
  if (index_id != kSecondaryIndexId) {
    std::ostringstream oss;
    oss << "Unexpected secondary index id: " << index_id;
    return Status::Corruption(oss.str());
  }

  const char* const buf = secondary_index_key.data();
  assert(secondary_index_key.size() == 8);
  c_ = static_cast<uint32_t>(static_cast<unsigned char>(buf[0])) << 24;
  c_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 16;
  c_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 8;
  c_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[3]));

  a_ = static_cast<uint32_t>(static_cast<unsigned char>(buf[4])) << 24;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[5])) << 16;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[6])) << 8;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[7]));

  if (secondary_index_value.size() != 4) {
    return Status::Corruption("Secondary index value length is not 4");
  }
  uint32_t val = 0;
  GetFixed32(&secondary_index_value, &val);
  if (val != crc) {
    std::ostringstream oss;
    oss << "Secondary index key checksum mismatch, stored: " << val
        << ", recomputed: " << crc;
    return Status::Corruption(oss.str());
  }
  return Status::OK();
}

void MultiOpsTxnsStressTest::FinishInitDb(SharedState* shared) {
  if (FLAGS_enable_compaction_filter) {
    // TODO (yanqin) enable compaction filter
  }
  if (FLAGS_destroy_db_initially) {
    ReopenAndPreloadDb(shared);
  } else {
    ScanExistingDb(FLAGS_threads, /*lb_a=*/0, FLAGS_ub_a, /*lb_c=*/0,
                   FLAGS_ub_c);
  }
}

void MultiOpsTxnsStressTest::ReopenAndPreloadDb(SharedState* shared) {
  (void)shared;
#ifndef ROCKSDB_LITE
  std::vector<ColumnFamilyDescriptor> cf_descs;
  for (const auto* handle : column_families_) {
    cf_descs.emplace_back(handle->GetName(), ColumnFamilyOptions(options_));
  }
  CancelAllBackgroundWork(db_, /*wait=*/true);
  for (auto* handle : column_families_) {
    delete handle;
  }
  column_families_.clear();
  delete db_;
  db_ = nullptr;
  txn_db_ = nullptr;

  TransactionDBOptions txn_db_opts;
  txn_db_opts.skip_concurrency_control = true;  // speed-up preloading
  Status s = TransactionDB::Open(options_, txn_db_opts, FLAGS_db, cf_descs,
                                 &column_families_, &txn_db_);
  if (s.ok()) {
    db_ = txn_db_;
  } else {
    fprintf(stderr, "Failed to open db: %s\n", s.ToString().c_str());
    exit(1);
  }

  PreloadDb(shared, FLAGS_threads, /*lb_a=*/0, FLAGS_ub_a, /*lb_c=*/0,
            FLAGS_ub_c);

  // Reopen
  CancelAllBackgroundWork(db_, /*wait=*/true);
  for (auto* handle : column_families_) {
    delete handle;
  }
  column_families_.clear();
  s = db_->Close();
  if (!s.ok()) {
    fprintf(stderr, "Error during closing db: %s\n", s.ToString().c_str());
    exit(1);
  }
  delete db_;
  db_ = nullptr;
  txn_db_ = nullptr;

  Open();
#endif  // !ROCKSDB_LITE
}

// Used for point-lookup transaction
Status MultiOpsTxnsStressTest::TestGet(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  uint32_t a = 0;
  uint32_t pos = 0;
  std::tie(a, pos) = ChooseExistingA(thread);
  return PointLookupTxn(thread, read_opts, a);
}

// Not used.
std::vector<Status> MultiOpsTxnsStressTest::TestMultiGet(
    ThreadState* /*thread*/, const ReadOptions& /*read_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  return std::vector<Status>{Status::NotSupported()};
}

Status MultiOpsTxnsStressTest::TestPrefixScan(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  (void)thread;
  (void)read_opts;
  (void)rand_column_families;
  (void)rand_keys;
  return Status::OK();
}

// Given a key K, this creates an iterator which scans to K and then
// does a random sequence of Next/Prev operations.
Status MultiOpsTxnsStressTest::TestIterate(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  uint32_t c = 0;
  uint32_t pos = 0;
  std::tie(c, pos) = ChooseExistingC(thread);
  return RangeScanTxn(thread, read_opts, c);
}

// Not intended for use.
Status MultiOpsTxnsStressTest::TestPut(ThreadState* /*thread*/,
                                       WriteOptions& /*write_opts*/,
                                       const ReadOptions& /*read_opts*/,
                                       const std::vector<int>& /*cf_ids*/,
                                       const std::vector<int64_t>& /*keys*/,
                                       char (&value)[100],
                                       std::unique_ptr<MutexLock>& /*lock*/) {
  (void)value;
  return Status::NotSupported();
}

// Not intended for use.
Status MultiOpsTxnsStressTest::TestDelete(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {
  return Status::NotSupported();
}

// Not intended for use.
Status MultiOpsTxnsStressTest::TestDeleteRange(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {
  return Status::NotSupported();
}

void MultiOpsTxnsStressTest::TestIngestExternalFile(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
}

void MultiOpsTxnsStressTest::TestCompactRange(
    ThreadState* thread, int64_t /*rand_key*/, const Slice& /*start_key*/,
    ColumnFamilyHandle* column_family) {
  // TODO (yanqin).
  // May use GetRangeHash() for validation before and after DB::CompactRange()
  // completes.
  (void)thread;
  (void)column_family;
}

Status MultiOpsTxnsStressTest::TestBackupRestore(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
  return Status::OK();
}

Status MultiOpsTxnsStressTest::TestCheckpoint(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
  return Status::OK();
}

#ifndef ROCKSDB_LITE
Status MultiOpsTxnsStressTest::TestApproximateSize(
    ThreadState* thread, uint64_t iteration,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)iteration;
  (void)rand_column_families;
  return Status::OK();
}
#endif  // !ROCKSDB_LITE

Status MultiOpsTxnsStressTest::TestCustomOperations(
    ThreadState* thread, const std::vector<int>& rand_column_families) {
  (void)rand_column_families;
  // Randomly choose from 0, 1, and 2.
  // TODO (yanqin) allow user to configure probability of each operation.
  uint32_t rand = thread->rand.Uniform(3);
  Status s;
  if (0 == rand) {
    // Update primary key.
    uint32_t old_a = 0;
    uint32_t pos = 0;
    std::tie(old_a, pos) = ChooseExistingA(thread);
    uint32_t new_a = GenerateNextA(thread);
    s = PrimaryKeyUpdateTxn(thread, old_a, pos, new_a);
  } else if (1 == rand) {
    // Update secondary key.
    uint32_t old_c = 0;
    uint32_t pos = 0;
    std::tie(old_c, pos) = ChooseExistingC(thread);
    int count = 0;
    uint32_t new_c = ChooseRandomC(thread);
    while (new_c == old_c && count < 10) {
      ++count;
      new_c = ChooseRandomC(thread);
    }
    if (count >= 10) {
      // If we reach here, it means our random number generator has a serious
      // problem.
      std::terminate();
    }
    s = SecondaryKeyUpdateTxn(thread, old_c, pos, new_c);
  } else if (2 == rand) {
    // Update primary index value.
    uint32_t a = 0;
    uint32_t pos = 0;
    std::tie(a, pos) = ChooseExistingA(thread);
    s = UpdatePrimaryIndexValueTxn(thread, a, /*b_delta=*/1);
  } else {
    // Should never reach here.
    assert(false);
  }
  return s;
}

Status MultiOpsTxnsStressTest::PrimaryKeyUpdateTxn(ThreadState* thread,
                                                   uint32_t old_a,
                                                   uint32_t old_a_pos,
                                                   uint32_t new_a) {
#ifdef ROCKSDB_LITE
  (void)thread;
  (void)old_a;
  (void)new_a;
  return Status::NotSupported();
#else
  std::string old_pk = Record::EncodePrimaryKey(old_a);
  std::string new_pk = Record::EncodePrimaryKey(new_a);
  Transaction* txn = nullptr;
  WriteOptions wopts;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);
  txn->SetSnapshotOnNextOperation(/*notifier=*/nullptr);

  const Defer cleanup([&s, thread, txn, this]() {
    if (s.ok()) {
      // Two gets, one for existing pk, one for locking potential new pk.
      thread->stats.AddGets(/*ngets=*/2, /*nfounds=*/1);
      thread->stats.AddDeletes(1);
      thread->stats.AddBytesForWrites(
          /*nwrites=*/2,
          Record::kPrimaryIndexEntrySize + Record::kSecondaryIndexEntrySize);
      thread->stats.AddSingleDeletes(1);
      return;
    }
    if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    } else if (s.IsBusy()) {
      // ignore.
    } else {
      thread->stats.AddErrors(1);
    }
    RollbackTxn(txn).PermitUncheckedError();
  });

  ReadOptions ropts;
  ropts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  std::string value;
  s = txn->GetForUpdate(ropts, old_pk, &value);
  if (!s.ok()) {
    return s;
  }
  std::string empty_value;
  s = txn->GetForUpdate(ropts, new_pk, &empty_value);
  if (s.ok()) {
    assert(!empty_value.empty());
    s = Status::Busy();
    return s;
  }

  auto result = Record::DecodePrimaryIndexValue(value);
  s = std::get<0>(result);
  if (!s.ok()) {
    return s;
  }
  uint32_t b = std::get<1>(result);
  uint32_t c = std::get<2>(result);

  ColumnFamilyHandle* cf = db_->DefaultColumnFamily();
  s = txn->Delete(cf, old_pk, /*assume_tracked=*/true);
  if (!s.ok()) {
    return s;
  }
  s = txn->Put(cf, new_pk, value, /*assume_tracked=*/true);
  if (!s.ok()) {
    return s;
  }

  auto* wb = txn->GetWriteBatch();
  assert(wb);

  std::string old_sk = Record::EncodeSecondaryKey(c, old_a);
  s = wb->SingleDelete(old_sk);
  if (!s.ok()) {
    return s;
  }

  Record record(new_a, b, c);
  std::string new_sk;
  std::string new_crc;
  std::tie(new_sk, new_crc) = record.EncodeSecondaryIndexEntry();
  s = wb->Put(new_sk, new_crc);
  if (!s.ok()) {
    return s;
  }

  s = CommitTxn(txn);

  if (s.ok()) {
    const uint32_t num_a = FLAGS_ub_a - 0;
    const uint32_t num_a_per_thread = (num_a % FLAGS_threads)
                                          ? (num_a / FLAGS_threads + 1)
                                          : (num_a / FLAGS_threads);
    uint32_t idx = old_a / num_a_per_thread;
    assert(new_a / num_a_per_thread == idx);
    assert(idx == thread->tid);
    {
      auto& existing_a_set = existing_a_sets_[idx];
      auto it = existing_a_set.find(old_a);
      assert(it != existing_a_set.end());
      existing_a_set.erase(it);

      it = existing_a_set.find(new_a);
      assert(existing_a_set.end() == it);
      existing_a_set.insert(new_a);
    }
    {
      auto& existing_a_vec = existing_a_vecs_[idx];
      existing_a_vec[old_a_pos] = new_a;
    }
    {
      auto& next_a_set = next_a_sets_[idx];
      auto it = next_a_set.find(old_a);
      assert(next_a_set.end() == it);
      next_a_set.insert(old_a);
    }
  }
  return s;
#endif  // !ROCKSDB_LITE
}

Status MultiOpsTxnsStressTest::SecondaryKeyUpdateTxn(ThreadState* thread,
                                                     uint32_t old_c,
                                                     uint32_t old_c_pos,
                                                     uint32_t new_c) {
#ifdef ROCKSDB_LITE
  (void)thread;
  (void)old_c;
  (void)new_c;
  return Status::NotSupported();
#else
  Transaction* txn = nullptr;
  WriteOptions wopts;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);

  Iterator* it = nullptr;
  long iterations = 0;
  const Defer cleanup([&s, thread, &it, txn, this, &iterations]() {
    delete it;
    if (s.ok()) {
      thread->stats.AddIterations(iterations);
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/1);
      thread->stats.AddSingleDeletes(1);
      thread->stats.AddBytesForWrites(
          /*nwrites=*/2,
          Record::kPrimaryIndexEntrySize + Record::kSecondaryIndexEntrySize);
      return;
    } else if (s.IsBusy() || s.IsTimedOut() || s.IsTryAgain() ||
               s.IsMergeInProgress()) {
      // ww-conflict detected, or
      // lock cannot be acquired, or
      // memtable history is not large enough for conflict checking, or
      // Merge operation cannot be resolved.
      // TODO (yanqin) add stats for other cases?
    } else if (s.IsNotFound()) {
      // ignore.
    } else {
      thread->stats.AddErrors(1);
    }
    RollbackTxn(txn).PermitUncheckedError();
  });

  // TODO (yanqin) try SetSnapshotOnNextOperation(). We currently need to take
  // a snapshot here because we will later verify that point lookup in the
  // primary index using GetForUpdate() returns the same value for 'c' as the
  // iterator. The iterator does not need a snapshot though, because it will be
  // assigned the current latest (published) sequence in the db, which will be
  // no smaller than the snapshot created here. The GetForUpdate will perform
  // ww conflict checking to ensure GetForUpdate() (using the snapshot) sees
  // the same data as this iterator.
  txn->SetSnapshot();
  std::string old_sk_prefix = Record::EncodeSecondaryKey(old_c);
  std::string iter_ub_str = Record::EncodeSecondaryKey(old_c + 1);
  Slice iter_ub = iter_ub_str;
  ReadOptions ropts;
  if (thread->rand.OneIn(2)) {
    ropts.snapshot = txn->GetSnapshot();
  }
  ropts.total_order_seek = true;
  ropts.iterate_upper_bound = &iter_ub;
  ropts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  it = txn->GetIterator(ropts);

  assert(it);
  it->Seek(old_sk_prefix);
  if (!it->Valid()) {
    s = Status::NotFound();
    return s;
  }
  auto* wb = txn->GetWriteBatch();
  assert(wb);

  do {
    ++iterations;
    Record record;
    s = record.DecodeSecondaryIndexEntry(it->key(), it->value());
    if (!s.ok()) {
      VerificationAbort(thread->shared, "Cannot decode secondary key", s);
      break;
    }
    // At this point, record.b is not known yet, thus we need to access
    // primary index.
    std::string pk = Record::EncodePrimaryKey(record.a_value());
    std::string value;
    ReadOptions read_opts;
    read_opts.rate_limiter_priority =
        FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
    read_opts.snapshot = txn->GetSnapshot();
    s = txn->GetForUpdate(read_opts, pk, &value);
    if (s.IsBusy() || s.IsTimedOut() || s.IsTryAgain() ||
        s.IsMergeInProgress()) {
      // Write conflict, or cannot acquire lock, or memtable size is not large
      // enough, or merge cannot be resolved.
      break;
    } else if (!s.ok()) {
      // We can also fail verification here.
      VerificationAbort(thread->shared, "pk should exist, but does not", s);
      break;
    }
    auto result = Record::DecodePrimaryIndexValue(value);
    s = std::get<0>(result);
    if (!s.ok()) {
      VerificationAbort(thread->shared, "Cannot decode primary index value", s);
      break;
    }
    uint32_t b = std::get<1>(result);
    uint32_t c = std::get<2>(result);
    if (c != old_c) {
      std::ostringstream oss;
      oss << "c in primary index does not match secondary index: " << c
          << " != " << old_c;
      s = Status::Corruption();
      VerificationAbort(thread->shared, oss.str(), s);
      break;
    }
    Record new_rec(record.a_value(), b, new_c);
    std::string new_primary_index_value = new_rec.EncodePrimaryIndexValue();
    ColumnFamilyHandle* cf = db_->DefaultColumnFamily();
    s = txn->Put(cf, pk, new_primary_index_value, /*assume_tracked=*/true);
    if (!s.ok()) {
      break;
    }
    std::string old_sk = it->key().ToString(/*hex=*/false);
    std::string new_sk;
    std::string new_crc;
    std::tie(new_sk, new_crc) = new_rec.EncodeSecondaryIndexEntry();
    s = wb->SingleDelete(old_sk);
    if (!s.ok()) {
      break;
    }
    s = wb->Put(new_sk, new_crc);
    if (!s.ok()) {
      break;
    }

    it->Next();
  } while (it->Valid());

  if (!s.ok()) {
    return s;
  }

  s = CommitTxn(txn);

  if (s.ok()) {
    const uint32_t num_c = FLAGS_ub_c - 0;
    const uint32_t num_c_per_thread = (num_c % FLAGS_threads)
                                          ? (num_c / FLAGS_threads + 1)
                                          : (num_c / FLAGS_threads);
    uint32_t idx = old_c / num_c_per_thread;
    assert(new_c / num_c_per_thread == idx);
    assert(idx == thread->tid);
    {
      auto& existing_c_set = existing_c_sets_[idx];
      auto iter = existing_c_set.find(old_c);
      assert(iter != existing_c_set.end());
      existing_c_set.erase(iter);

      iter = existing_c_set.find(new_c);
      if (iter == existing_c_set.end()) {
        existing_c_set.insert(new_c);
      }
    }
    {
      auto& existing_c_vec = existing_c_vecs_[idx];
      existing_c_vec[old_c_pos] = new_c;
    }
  }

  return s;
#endif  // !ROCKSDB_LITE
}

Status MultiOpsTxnsStressTest::UpdatePrimaryIndexValueTxn(ThreadState* thread,
                                                          uint32_t a,
                                                          uint32_t b_delta) {
#ifdef ROCKSDB_LITE
  (void)thread;
  (void)a;
  (void)b_delta;
  return Status::NotSupported();
#else
  std::string pk_str = Record::EncodePrimaryKey(a);
  Transaction* txn = nullptr;
  WriteOptions wopts;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);

  const Defer cleanup([&s, thread, txn, this]() {
    if (s.ok()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/1);
      thread->stats.AddBytesForWrites(
          /*nwrites=*/1, /*nbytes=*/Record::kPrimaryIndexEntrySize);
      return;
    }
    if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    } else if (s.IsInvalidArgument()) {
      // ignored.
    } else if (s.IsBusy() || s.IsTimedOut() || s.IsTryAgain() ||
               s.IsMergeInProgress()) {
      // ignored.
    } else {
      thread->stats.AddErrors(1);
    }
    RollbackTxn(txn).PermitUncheckedError();
  });
  ReadOptions ropts;
  ropts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  std::string value;
  s = txn->GetForUpdate(ropts, pk_str, &value);
  if (!s.ok()) {
    return s;
  }
  auto result = Record::DecodePrimaryIndexValue(value);
  if (!std::get<0>(result).ok()) {
    return s;
  }
  uint32_t b = std::get<1>(result) + b_delta;
  uint32_t c = std::get<2>(result);
  Record record(a, b, c);
  std::string primary_index_value = record.EncodePrimaryIndexValue();
  ColumnFamilyHandle* cf = db_->DefaultColumnFamily();
  s = txn->Put(cf, pk_str, primary_index_value, /*assume_tracked=*/true);
  if (!s.ok()) {
    return s;
  }
  s = CommitTxn(txn);
  return s;
#endif  // !ROCKSDB_LITE
}

Status MultiOpsTxnsStressTest::PointLookupTxn(ThreadState* thread,
                                              ReadOptions ropts, uint32_t a) {
#ifdef ROCKSDB_LITE
  (void)thread;
  (void)ropts;
  (void)a;
  return Status::NotSupported();
#else
  std::string pk_str = Record::EncodePrimaryKey(a);
  // pk may or may not exist
  PinnableSlice value;

  Transaction* txn = nullptr;
  WriteOptions wopts;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);

  const Defer cleanup([&s, thread, txn, this]() {
    if (s.ok()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/1);
      return;
    } else if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    } else {
      thread->stats.AddErrors(1);
    }
    RollbackTxn(txn).PermitUncheckedError();
  });

  txn->SetSnapshot();
  ropts.snapshot = txn->GetSnapshot();

  if (FLAGS_delay_snapshot_read_one_in > 0 &&
      thread->rand.OneIn(FLAGS_delay_snapshot_read_one_in)) {
    uint64_t delay_ms = thread->rand.Uniform(100) + 1;
    db_->GetDBOptions().env->SleepForMicroseconds(
        static_cast<int>(delay_ms * 1000));
  }

  s = txn->Get(ropts, db_->DefaultColumnFamily(), pk_str, &value);
  if (s.ok()) {
    s = txn->Commit();
  }
  return s;
#endif  // !ROCKSDB_LITE
}

Status MultiOpsTxnsStressTest::RangeScanTxn(ThreadState* thread,
                                            ReadOptions ropts, uint32_t c) {
#ifdef ROCKSDB_LITE
  (void)thread;
  (void)ropts;
  (void)c;
  return Status::NotSupported();
#else
  std::string sk = Record::EncodeSecondaryKey(c);

  Transaction* txn = nullptr;
  WriteOptions wopts;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);

  const Defer cleanup([&s, thread, txn, this]() {
    if (s.ok()) {
      thread->stats.AddIterations(1);
      return;
    }
    thread->stats.AddErrors(1);
    RollbackTxn(txn).PermitUncheckedError();
  });

  txn->SetSnapshot();
  ropts.snapshot = txn->GetSnapshot();

  if (FLAGS_delay_snapshot_read_one_in > 0 &&
      thread->rand.OneIn(FLAGS_delay_snapshot_read_one_in)) {
    uint64_t delay_ms = thread->rand.Uniform(100) + 1;
    db_->GetDBOptions().env->SleepForMicroseconds(
        static_cast<int>(delay_ms * 1000));
  }

  std::unique_ptr<Iterator> iter(txn->GetIterator(ropts));

  constexpr size_t total_nexts = 10;
  size_t nexts = 0;
  for (iter->Seek(sk);
       iter->Valid() && nexts < total_nexts && iter->status().ok();
       iter->Next(), ++nexts) {
  }

  if (iter->status().ok()) {
    s = txn->Commit();
  } else {
    s = iter->status();
  }

  return s;
#endif  // !ROCKSDB_LITE
}

void MultiOpsTxnsStressTest::VerifyDb(ThreadState* thread) const {
  if (thread->shared->HasVerificationFailedYet()) {
    return;
  }
  const Snapshot* const snapshot = db_->GetSnapshot();
  assert(snapshot);
  ManagedSnapshot snapshot_guard(db_, snapshot);

  if (FLAGS_delay_snapshot_read_one_in > 0 &&
      thread->rand.OneIn(FLAGS_delay_snapshot_read_one_in)) {
    uint64_t delay_ms = thread->rand.Uniform(100) + 1;
    db_->GetDBOptions().env->SleepForMicroseconds(
        static_cast<int>(delay_ms * 1000));
  }

  // TODO (yanqin) with a probability, we can use either forward or backward
  // iterator in subsequent checks. We can also use more advanced features in
  // range scan. For now, let's just use simple forward iteration with
  // total_order_seek = true.

  // First, iterate primary index.
  size_t primary_index_entries_count = 0;
  {
    char buf[4];
    EncodeFixed32(buf, Record::kPrimaryIndexId + 1);
    std::reverse(buf, buf + sizeof(buf));
    std::string iter_ub_str(buf, sizeof(buf));
    Slice iter_ub = iter_ub_str;

    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions ropts;
    ropts.snapshot = snapshot;
    ropts.total_order_seek = true;
    ropts.iterate_upper_bound = &iter_ub;

    std::unique_ptr<Iterator> it(db_->NewIterator(ropts));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ++primary_index_entries_count;
    }
  }

  // Second, iterate secondary index.
  size_t secondary_index_entries_count = 0;
  {
    char buf[4];
    EncodeFixed32(buf, Record::kSecondaryIndexId);
    std::reverse(buf, buf + sizeof(buf));
    const std::string start_key(buf, sizeof(buf));

    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions ropts;
    ropts.snapshot = snapshot;
    ropts.total_order_seek = true;

    std::unique_ptr<Iterator> it(db_->NewIterator(ropts));
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      ++secondary_index_entries_count;
      Record record;
      Status s = record.DecodeSecondaryIndexEntry(it->key(), it->value());
      if (!s.ok()) {
        VerificationAbort(thread->shared, "Cannot decode secondary index entry",
                          s);
        return;
      }
      // After decoding secondary index entry, we know a and c. Crc is verified
      // in decoding phase.
      //
      // Form a primary key and search in the primary index.
      std::string pk = Record::EncodePrimaryKey(record.a_value());
      std::string value;
      s = db_->Get(ropts, pk, &value);
      if (!s.ok()) {
        std::ostringstream oss;
        oss << "Error searching pk " << Slice(pk).ToString(true) << ". "
            << s.ToString();
        VerificationAbort(thread->shared, oss.str(), s);
        return;
      }
      auto result = Record::DecodePrimaryIndexValue(value);
      s = std::get<0>(result);
      if (!s.ok()) {
        std::ostringstream oss;
        oss << "Error decoding primary index value "
            << Slice(value).ToString(true) << ". " << s.ToString();
        VerificationAbort(thread->shared, oss.str(), s);
      }
      uint32_t c_in_primary = std::get<2>(result);
      if (c_in_primary != record.c_value()) {
        std::ostringstream oss;
        oss << "Pk/sk mismatch. pk: (c=" << c_in_primary
            << "), sk: (c=" << record.c_value() << ")";
        VerificationAbort(thread->shared, oss.str(), s);
      }
    }
  }

  if (secondary_index_entries_count != primary_index_entries_count) {
    std::ostringstream oss;
    oss << "Pk/sk mismatch: primary index has " << primary_index_entries_count
        << " entries. Secondary index has " << secondary_index_entries_count
        << " entries.";
    VerificationAbort(thread->shared, oss.str(), Status::OK());
  }
}

std::pair<uint32_t, uint32_t> MultiOpsTxnsStressTest::ChooseExistingA(
    ThreadState* thread) {
  uint32_t tid = thread->tid;
  std::unordered_set<uint32_t>& existing_a_set = existing_a_sets_[tid];
  std::vector<uint32_t>& existing_a_vec = existing_a_vecs_[tid];
  const size_t N = existing_a_vec.size();
  assert(N > 0);
  assert(N == existing_a_set.size());
  uint32_t rnd = thread->rand.Uniform(N);
  assert(rnd < N);
  return std::make_pair(existing_a_vec[rnd], rnd);
}

uint32_t MultiOpsTxnsStressTest::GenerateNextA(ThreadState* thread) {
  uint32_t tid = thread->tid;
  std::unordered_set<uint32_t>& next_a_set = next_a_sets_[tid];
  auto it = next_a_set.begin();
  assert(it != next_a_set.end());
  uint32_t ret = *it;
  // Remove this element.
  // Will need to add it back if the calling transaction does not commit.
  next_a_set.erase(it);
  return ret;
}

std::pair<uint32_t, uint32_t> MultiOpsTxnsStressTest::ChooseExistingC(
    ThreadState* thread) {
  uint32_t tid = thread->tid;
  std::unordered_set<uint32_t>& existing_c_set = existing_c_sets_[tid];
  std::vector<uint32_t>& existing_c_vec = existing_c_vecs_[tid];
  const size_t N = existing_c_vec.size();
  assert(N > 0);
  assert(N == existing_c_set.size());
  uint32_t rnd = thread->rand.Uniform(N);
  assert(rnd < N);
  return std::make_pair(existing_c_vec[rnd], rnd);
}

uint32_t MultiOpsTxnsStressTest::ChooseRandomC(ThreadState* thread) {
  uint32_t tid = thread->tid;
  const uint32_t num_c = FLAGS_ub_c - 0;
  const uint32_t num_c_per_thread = (num_c % FLAGS_threads)
                                        ? (num_c / FLAGS_threads + 1)
                                        : (num_c / FLAGS_threads);
  const uint32_t c_lower_bound = tid * num_c_per_thread;
  const uint32_t rnd = thread->rand.Uniform(num_c_per_thread);
  return rnd + c_lower_bound;
}

void MultiOpsTxnsStressTest::PreloadDb(SharedState* shared, int threads,
                                       uint32_t lb_a, uint32_t ub_a,
                                       uint32_t lb_c, uint32_t ub_c) {
#ifdef ROCKSDB_LITE
  (void)shared;
  (void)threads;
  (void)lb_a;
  (void)ub_a;
  (void)lb_c;
  (void)ub_c;
#else
  existing_a_sets_.resize(threads);
  existing_a_vecs_.resize(threads);
  next_a_sets_.resize(threads);

  existing_c_sets_.resize(threads);
  existing_c_vecs_.resize(threads);

  assert(ub_a > lb_a && ub_a > lb_a + threads);
  assert(ub_c > lb_c && ub_c > lb_c + threads);

  const uint32_t num_c = ub_c - lb_c;
  const uint32_t num_c_per_thread =
      (num_c % threads) ? (num_c / threads + 1) : (num_c / threads);
  const uint32_t num_a = ub_a - lb_a;
  const uint32_t num_a_per_thread =
      (num_a % threads) ? (num_a / threads + 1) : (num_a / threads);

  WriteOptions wopts;
  wopts.disableWAL = true;
  wopts.sync = false;
  Random rnd(shared->GetSeed());
  assert(txn_db_);

  for (uint32_t a = lb_a; a < ub_a; ++a) {
    uint32_t tid = (a - lb_a) / num_a_per_thread;
    uint32_t a_base = lb_a + tid * num_a_per_thread;
    uint32_t a_delta = a - a_base;
    if (a - a_base + 1 >= num_a_per_thread) {
      auto& next_a_set = next_a_sets_[tid];
      next_a_set.insert(a);
      continue;
    }

    uint32_t c_base = lb_c + (tid * num_c_per_thread);
    uint32_t c_delta = a_delta % num_c_per_thread;
    uint32_t c = c_base + c_delta;

    uint32_t b = rnd.Next();
    Record record(a, b, c);
    WriteBatch wb;
    const auto primary_index_entry = record.EncodePrimaryIndexEntry();
    Status s = wb.Put(primary_index_entry.first, primary_index_entry.second);
    assert(s.ok());

    const auto secondary_index_entry = record.EncodeSecondaryIndexEntry();
    s = wb.Put(secondary_index_entry.first, secondary_index_entry.second);
    assert(s.ok());

    s = txn_db_->Write(wopts, &wb);
    assert(s.ok());

    // TODO (yanqin): make the following check optional, especially when data
    // size is large.
    Record tmp_rec;
    tmp_rec.SetB(record.b_value());
    s = tmp_rec.DecodeSecondaryIndexEntry(secondary_index_entry.first,
                                          secondary_index_entry.second);
    assert(s.ok());
    assert(tmp_rec == record);

    auto& existing_a_set = existing_a_sets_[tid];
    auto& existing_a_vec = existing_a_vecs_[tid];
    existing_a_set.insert(a);
    existing_a_vec.push_back(a);

    auto& existing_c_set = existing_c_sets_[tid];
    if (existing_c_set.count(c) == 0) {
      existing_c_set.insert(c);
    }
  }

  Status s = db_->Flush(FlushOptions());
  assert(s.ok());
  fprintf(stdout, "DB preloaded with %d entries\n", static_cast<int>(num_a));
#endif  // !ROCKSDB_LITE
}

void MultiOpsTxnsStressTest::ScanExistingDb(int threads, uint32_t lb_a,
                                            uint32_t ub_a, uint32_t lb_c,
                                            uint32_t ub_c) {
  existing_a_sets_.resize(threads);
  existing_a_vecs_.resize(threads);
  next_a_sets_.resize(threads);

  existing_c_sets_.resize(threads);
  existing_c_vecs_.resize(threads);

  assert(ub_a > lb_a && ub_a > lb_a + threads);
  assert(ub_c > lb_c && ub_c > lb_c + threads);

  const uint32_t num_c = ub_c - lb_c;
  const uint32_t num_c_per_thread =
      (num_c % threads) ? (num_c / threads + 1) : (num_c / threads);
  const uint32_t num_a = ub_a - lb_a;
  const uint32_t num_a_per_thread =
      (num_a % threads) ? (num_a / threads + 1) : (num_a / threads);

  assert(txn_db_);
  const Snapshot* snapshot = txn_db_->GetSnapshot();
  ReadOptions ropts;
  ropts.snapshot = snapshot;
  uint32_t count = 0;
  {
    std::string pk_lb_str = Record::EncodePrimaryKey(0);
    std::string pk_ub_str =
        Record::EncodePrimaryKey(std::numeric_limits<uint32_t>::max());
    Slice pk_lb = pk_lb_str;
    Slice pk_ub = pk_ub_str;
    ropts.iterate_lower_bound = &pk_lb;
    ropts.iterate_upper_bound = &pk_ub;
    std::unique_ptr<Iterator> it(txn_db_->NewIterator(ropts));

    for (it->SeekToFirst(); it->Valid(); it->Next(), ++count) {
      Record record;
      Status s = record.DecodePrimaryIndexEntry(it->key(), it->value());
      assert(s.ok());
      uint32_t a = record.a_value();
      uint32_t idx = a / num_a_per_thread;
      auto& existing_a_set = existing_a_sets_[idx];
      auto& existing_a_vec = existing_a_vecs_[idx];
      if (existing_a_set.count(a) == 0) {
        existing_a_set.insert(a);
        existing_a_vec.push_back(a);
      }

      uint32_t c = record.c_value();
      idx = c / num_c_per_thread;
      auto& existing_c_set = existing_c_sets_[idx];
      auto& existing_c_vec = existing_c_vecs_[idx];
      if (existing_c_set.count(c) == 0) {
        existing_c_set.insert(c);
        existing_c_vec.push_back(c);
      }
    }
  }
  txn_db_->ReleaseSnapshot(snapshot);

  for (uint32_t a = lb_a; a < ub_a; ++a) {
    uint32_t idx = a / num_a_per_thread;
    const auto& existing_a_set = existing_a_sets_[idx];
    auto& next_a_set = next_a_sets_[idx];
    auto it = existing_a_set.find(a);
    if (it == existing_a_set.end()) {
      next_a_set.insert(a);
    }
  }
  fprintf(stdout, "Processed %d records\n", static_cast<int>(num_a));
}

StressTest* CreateMultiOpsTxnsStressTest() {
  return new MultiOpsTxnsStressTest();
}

void CheckAndSetOptionsForMultiOpsTxnStressTest() {
#ifndef ROCKSDB_LITE
  if (FLAGS_test_batches_snapshots || FLAGS_test_cf_consistency) {
    fprintf(stderr,
            "-test_multi_ops_txns is not compatible with "
            "-test_bathces_snapshots and -test_cf_consistency\n");
    exit(1);
  }
  if (!FLAGS_use_txn) {
    fprintf(stderr, "-use_txn must be true if -test_multi_ops_txns\n");
    exit(1);
  } else if (FLAGS_test_secondary > 0) {
    fprintf(
        stderr,
        "secondary instance does not support replaying logs (MANIFEST + WAL) "
        "of TransactionDB with write-prepared/write-unprepared policy\n");
    exit(1);
  }
  if (FLAGS_clear_column_family_one_in > 0) {
    fprintf(stderr,
            "-test_multi_ops_txns is not compatible with clearing column "
            "families\n");
    exit(1);
  }
  if (FLAGS_column_families > 1) {
    // TODO (yanqin) support separating primary index and secondary index in
    // different column families.
    fprintf(stderr,
            "-test_multi_ops_txns currently does not use more than one column "
            "family\n");
    exit(1);
  }
  if (FLAGS_writepercent > 0 || FLAGS_delpercent > 0 ||
      FLAGS_delrangepercent > 0) {
    fprintf(stderr,
            "-test_multi_ops_txns requires that -writepercent, -delpercent and "
            "-delrangepercent be 0\n");
    exit(1);
  }
#else
  fprintf(stderr, "-test_multi_ops_txns not supported in ROCKSDB_LITE mode\n");
  exit(1);
#endif  // !ROCKSDB_LITE
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
