//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#include "db_stress_tool/db_stress_trace.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <limits>
#include <thread>

#ifndef OS_WIN
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#include "db_stress_tool/db_stress_common.h"
#include "port/lang.h"

namespace ROCKSDB_NAMESPACE {
namespace {

constexpr size_t kTraceBudgetBytes = 32ULL << 20;
constexpr size_t kMaxTraceThreads = 32;
constexpr size_t kTraceEntrySize = 256;
constexpr size_t kEntriesPerThread =
    kTraceBudgetBytes / kMaxTraceThreads / kTraceEntrySize;
constexpr uint32_t kTraceFileVersion = 1;
constexpr uint8_t kNoThreadSlot = 0xFF;
constexpr uint8_t kResultBoolUnset = 0xFF;
constexpr std::array<char, 8> kTraceFileMagic = {'D', 'B', 'S', 'P',
                                                 'I', 'T', 'R', '1'};

enum class IteratorTraceEventType : uint8_t {
  kCreate = 1,
  kSeek = 2,
  kSeekForPrev = 3,
  kSeekToFirst = 4,
  kSeekToLast = 5,
  kNext = 6,
  kPrev = 7,
  kPrepareValue = 8,
  kRefresh = 9,
};

enum IteratorTraceFlags : uint32_t {
  kHasSnapshot = 1u << 0,
  kHasLowerBound = 1u << 1,
  kHasUpperBound = 1u << 2,
  kAllowUnpreparedValue = 1u << 3,
  kTotalOrderSeek = 1u << 4,
  kPrefixSameAsStart = 1u << 5,
  kTailing = 1u << 6,
  kPinData = 1u << 7,
  kAutoRefreshIterator = 1u << 8,
};

struct KeySample {
  uint16_t full_len;
  uint8_t head_len;
  uint8_t tail_len;
  char head[32];
  char tail[16];
};

static_assert(sizeof(KeySample) == 52, "KeySample must remain compact");

struct TraceEntry {
  uint64_t timestamp_us;
  uint64_t sequence;
  uint64_t object_id;
  uint64_t aux0;
  uint64_t aux1;
  uint32_t os_thread_id_hash;
  uint32_t cf_id;
  uint32_t flags;
  uint8_t slot;
  uint8_t event_type;
  uint8_t status_code;
  uint8_t status_subcode;
  uint8_t valid_before;
  uint8_t valid_after;
  uint8_t result_bool;
  uint8_t reserved0;
  KeySample key0;
  KeySample key1;
  char reserved[92];
};

static_assert(sizeof(TraceEntry) == kTraceEntrySize,
              "TraceEntry must remain 256B");

struct ThreadTraceLog {
  std::atomic<uint64_t> head;
  std::atomic<uint64_t> thread_id_hash;
  TraceEntry entries[kEntriesPerThread];
};

struct TraceFileHeader {
  char magic[8];
  uint64_t trace_budget_bytes;
  uint64_t dropped_threads;
  uint64_t next_sequence;
  uint64_t next_iterator_id;
  uint64_t dump_timestamp_us;
  uint32_t version;
  uint32_t header_size;
  uint32_t slot_header_size;
  uint32_t entry_size;
  uint32_t max_threads;
  uint32_t entries_per_thread;
  uint32_t used_slots;
  uint32_t reserved0;
};

struct TraceFileSlotHeader {
  uint64_t thread_id_hash;
  uint64_t total_entries;
  uint32_t slot;
  uint32_t entry_count;
  uint32_t reserved0;
  uint32_t reserved1;
};

static_assert(sizeof(TraceFileHeader) == 80,
              "TraceFileHeader size must stay stable");
static_assert(sizeof(TraceFileSlotHeader) == 32,
              "TraceFileSlotHeader size must stay stable");
static_assert(kEntriesPerThread == 4096,
              "32 MiB / 32 threads / 256 B entries must be 4096 entries");

uint64_t NowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

void CopyVolatileBytes(const volatile char* src, char* dst, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    dst[i] = src[i];
  }
}

bool WriteAll(int fd, const char* data, size_t len) {
#ifndef OS_WIN
  while (len > 0) {
    ssize_t written = write(fd, data, len);
    if (written <= 0) {
      return false;
    }
    data += static_cast<size_t>(written);
    len -= static_cast<size_t>(written);
  }
  return true;
#else
  (void)fd;
  (void)data;
  (void)len;
  return false;
#endif
}

KeySample CaptureKeySample(const Slice& key) {
  KeySample sample{};
  sample.full_len = static_cast<uint16_t>(
      std::min<size_t>(key.size(), std::numeric_limits<uint16_t>::max()));
  sample.head_len =
      static_cast<uint8_t>(std::min<size_t>(key.size(), sizeof(sample.head)));
  if (sample.head_len > 0) {
    memcpy(sample.head, key.data(), sample.head_len);
  }
  if (key.size() > sample.head_len) {
    sample.tail_len = static_cast<uint8_t>(
        std::min<size_t>(key.size() - sample.head_len, sizeof(sample.tail)));
    if (sample.tail_len > 0) {
      memcpy(sample.tail, key.data() + key.size() - sample.tail_len,
             sample.tail_len);
    }
  }
  return sample;
}

uint32_t BuildCreateFlags(const ReadOptions& read_opts) {
  uint32_t flags = 0;
  if (read_opts.snapshot != nullptr) {
    flags |= kHasSnapshot;
  }
  if (read_opts.iterate_lower_bound != nullptr) {
    flags |= kHasLowerBound;
  }
  if (read_opts.iterate_upper_bound != nullptr) {
    flags |= kHasUpperBound;
  }
  if (read_opts.allow_unprepared_value) {
    flags |= kAllowUnpreparedValue;
  }
  if (read_opts.total_order_seek) {
    flags |= kTotalOrderSeek;
  }
  if (read_opts.prefix_same_as_start) {
    flags |= kPrefixSameAsStart;
  }
  if (read_opts.tailing) {
    flags |= kTailing;
  }
  if (read_opts.pin_data) {
    flags |= kPinData;
  }
  if (read_opts.auto_refresh_iterator_with_snapshot) {
    flags |= kAutoRefreshIterator;
  }
  return flags;
}

uint64_t HashCurrentThreadId() {
  return std::hash<std::thread::id>{}(std::this_thread::get_id());
}

class DbStressPublicIteratorTraceLog {
 public:
  DbStressPublicIteratorTraceLog()
      : next_sequence_(0),
        next_iterator_id_(0),
        next_slot_(0),
        dropped_threads_(0),
        dump_started_(0),
        log_fd_(-1),
        logs_{},
        log_path_{} {}

  DbStressPublicIteratorTraceLog(const DbStressPublicIteratorTraceLog&) =
      delete;
  DbStressPublicIteratorTraceLog& operator=(
      const DbStressPublicIteratorTraceLog&) = delete;
  DbStressPublicIteratorTraceLog(DbStressPublicIteratorTraceLog&&) = delete;
  DbStressPublicIteratorTraceLog& operator=(DbStressPublicIteratorTraceLog&&) =
      delete;

  ~DbStressPublicIteratorTraceLog() {
#ifndef OS_WIN
    if (log_fd_ >= 0) {
      close(log_fd_);
    }
    if (dump_started_.load(std::memory_order_relaxed) == 0 &&
        log_path_[0] != '\0') {
      unlink(log_path_);
    }
#endif
  }

  void SetLogFilePath(const std::string& path) {
#ifndef OS_WIN
    if (log_fd_ >= 0) {
      close(log_fd_);
      log_fd_ = -1;
    }
    if (dump_started_.load(std::memory_order_relaxed) == 0 &&
        log_path_[0] != '\0') {
      unlink(log_path_);
    }
#endif

    size_t len = std::min(path.size(), sizeof(log_path_) - 1);
    memcpy(log_path_, path.data(), len);
    log_path_[len] = '\0';

#ifndef OS_WIN
    if (log_path_[0] == '\0') {
      return;
    }
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    log_fd_ = open(log_path_, flags, 0644);
#endif
  }

  std::string GetLogFilePath() const {
    if (log_path_[0] == '\0') {
      return std::string();
    }
    return std::string(log_path_);
  }

  uint64_t NextIteratorId() {
    return next_iterator_id_.fetch_add(1, std::memory_order_relaxed) + 1;
  }

  TSAN_SUPPRESSION void RecordIteratorCreate(uint64_t iterator_id,
                                             uint32_t cf_id,
                                             const ReadOptions& read_opts) {
    KeySample lower{};
    KeySample upper{};
    if (read_opts.iterate_lower_bound != nullptr) {
      lower = CaptureKeySample(*read_opts.iterate_lower_bound);
    }
    if (read_opts.iterate_upper_bound != nullptr) {
      upper = CaptureKeySample(*read_opts.iterate_upper_bound);
    }
    Record(
        IteratorTraceEventType::kCreate, iterator_id, cf_id, lower, upper,
        false, false, Status::OK(), BuildCreateFlags(read_opts),
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(read_opts.snapshot)),
        0, kResultBoolUnset);
  }

  TSAN_SUPPRESSION void RecordIteratorOp(
      IteratorTraceEventType event_type, uint64_t iterator_id, uint32_t cf_id,
      const KeySample& key0, const KeySample& key1, bool valid_before,
      bool valid_after, const Status& status, uint32_t flags = 0,
      uint64_t aux0 = 0, uint64_t aux1 = 0,
      uint8_t result_bool = kResultBoolUnset) {
    Record(event_type, iterator_id, cf_id, key0, key1, valid_before,
           valid_after, status, flags, aux0, aux1, result_bool);
  }

  TSAN_SUPPRESSION void DumpRaw() {
#ifndef OS_WIN
    if (log_fd_ < 0) {
      return;
    }

    uint32_t expected = 0;
    if (!dump_started_.compare_exchange_strong(expected, 1,
                                               std::memory_order_relaxed)) {
      return;
    }

    uint32_t used_slots = 0;
    for (size_t slot = 0; slot < kMaxTraceThreads; ++slot) {
      if (logs_[slot].head.load(std::memory_order_relaxed) != 0) {
        ++used_slots;
      }
    }

    TraceFileHeader header{};
    for (size_t i = 0; i < kTraceFileMagic.size(); ++i) {
      header.magic[i] = kTraceFileMagic[i];
    }
    header.trace_budget_bytes = kTraceBudgetBytes;
    header.dropped_threads = dropped_threads_.load(std::memory_order_relaxed);
    header.next_sequence = next_sequence_.load(std::memory_order_relaxed);
    header.next_iterator_id = next_iterator_id_.load(std::memory_order_relaxed);
    header.dump_timestamp_us = 0;
    header.version = kTraceFileVersion;
    header.header_size = static_cast<uint32_t>(sizeof(header));
    header.slot_header_size =
        static_cast<uint32_t>(sizeof(TraceFileSlotHeader));
    header.entry_size = static_cast<uint32_t>(sizeof(TraceEntry));
    header.max_threads = static_cast<uint32_t>(kMaxTraceThreads);
    header.entries_per_thread = static_cast<uint32_t>(kEntriesPerThread);
    header.used_slots = used_slots;
    header.reserved0 = 0;

    if (!WriteAll(log_fd_, reinterpret_cast<const char*>(&header),
                  sizeof(header))) {
      return;
    }

    std::array<TraceEntry, 16> chunk{};
    for (size_t slot = 0; slot < kMaxTraceThreads; ++slot) {
      const uint64_t total = logs_[slot].head.load(std::memory_order_relaxed);
      if (total == 0) {
        continue;
      }

      const uint64_t count =
          std::min<uint64_t>(total, static_cast<uint64_t>(kEntriesPerThread));
      const uint64_t start =
          (total >= kEntriesPerThread) ? (total % kEntriesPerThread) : 0;

      TraceFileSlotHeader slot_header{};
      slot_header.thread_id_hash =
          logs_[slot].thread_id_hash.load(std::memory_order_relaxed);
      slot_header.total_entries = total;
      slot_header.slot = static_cast<uint32_t>(slot);
      slot_header.entry_count = static_cast<uint32_t>(count);
      slot_header.reserved0 = 0;
      slot_header.reserved1 = 0;

      if (!WriteAll(log_fd_, reinterpret_cast<const char*>(&slot_header),
                    sizeof(slot_header))) {
        return;
      }

      size_t chunk_count = 0;
      for (uint64_t i = 0; i < count; ++i) {
        const uint64_t idx = (start + i) % kEntriesPerThread;
        TraceEntry& dst_entry = chunk[chunk_count++];
        CopyVolatileBytes(
            reinterpret_cast<const volatile char*>(&logs_[slot].entries[idx]),
            reinterpret_cast<char*>(&dst_entry), sizeof(dst_entry));
        if (chunk_count == chunk.size() || i + 1 == count) {
          if (!WriteAll(log_fd_, reinterpret_cast<const char*>(chunk.data()),
                        chunk_count * sizeof(TraceEntry))) {
            return;
          }
          chunk_count = 0;
        }
      }
    }
#endif
  }

 private:
  void Record(IteratorTraceEventType event_type, uint64_t iterator_id,
              uint32_t cf_id, const KeySample& key0, const KeySample& key1,
              bool valid_before, bool valid_after, const Status& status,
              uint32_t flags, uint64_t aux0, uint64_t aux1,
              uint8_t result_bool) {
    const uint8_t slot = GetThreadSlot();
    if (slot == kNoThreadSlot) {
      return;
    }
    ThreadTraceLog& log = logs_[slot];
    const uint64_t seq =
        next_sequence_.fetch_add(1, std::memory_order_relaxed) + 1;
    const uint64_t pos = log.head.fetch_add(1, std::memory_order_relaxed);
    TraceEntry& entry = log.entries[pos % kEntriesPerThread];
    entry = TraceEntry{};
    entry.object_id = iterator_id;
    entry.aux0 = aux0;
    entry.aux1 = aux1;
    entry.os_thread_id_hash = static_cast<uint32_t>(
        log.thread_id_hash.load(std::memory_order_relaxed));
    entry.cf_id = cf_id;
    entry.flags = flags;
    entry.slot = slot;
    entry.event_type = static_cast<uint8_t>(event_type);
    entry.status_code = static_cast<uint8_t>(status.code());
    entry.status_subcode = static_cast<uint8_t>(status.subcode());
    entry.valid_before = valid_before ? 1 : 0;
    entry.valid_after = valid_after ? 1 : 0;
    entry.result_bool = result_bool;
    entry.key0 = key0;
    entry.key1 = key1;
    entry.timestamp_us = NowMicros();
    std::atomic_signal_fence(std::memory_order_release);
    entry.sequence = seq;
  }

  uint8_t GetThreadSlot() {
    static thread_local uint8_t thread_slot = kNoThreadSlot;
    static thread_local bool slot_initialized = false;
    if (slot_initialized) {
      return thread_slot;
    }
    slot_initialized = true;
    const uint32_t slot = next_slot_.fetch_add(1, std::memory_order_relaxed);
    if (slot >= kMaxTraceThreads) {
      dropped_threads_.fetch_add(1, std::memory_order_relaxed);
      return thread_slot;
    }
    thread_slot = static_cast<uint8_t>(slot);
    logs_[slot].thread_id_hash.store(HashCurrentThreadId(),
                                     std::memory_order_relaxed);
    return thread_slot;
  }

  std::atomic<uint64_t> next_sequence_;
  std::atomic<uint64_t> next_iterator_id_;
  std::atomic<uint32_t> next_slot_;
  std::atomic<uint64_t> dropped_threads_;
  std::atomic<uint32_t> dump_started_;
  int log_fd_;
  ThreadTraceLog logs_[kMaxTraceThreads];
  char log_path_[PATH_MAX];
};

class DbStressTraceIterator : public Iterator {
 public:
  DbStressTraceIterator(std::unique_ptr<Iterator>&& iter,
                        DbStressPublicIteratorTraceLog* trace_log,
                        uint32_t cf_id, const ReadOptions& read_opts)
      : iter_(std::move(iter)),
        trace_log_(trace_log),
        cf_id_(cf_id),
        iterator_id_(trace_log_->NextIteratorId()) {
    trace_log_->RecordIteratorCreate(iterator_id_, cf_id_, read_opts);
  }

  bool Valid() const override { return iter_->Valid(); }

  void SeekToFirst() override {
    KeySample before{};
    if (iter_->Valid()) {
      before = CaptureKeySample(iter_->key());
    }
    iter_->SeekToFirst();
    KeySample after{};
    if (iter_->Valid()) {
      after = CaptureKeySample(iter_->key());
    }
    trace_log_->RecordIteratorOp(
        IteratorTraceEventType::kSeekToFirst, iterator_id_, cf_id_, before,
        after, before.full_len != 0, iter_->Valid(), iter_->status());
  }

  void SeekToLast() override {
    KeySample before{};
    if (iter_->Valid()) {
      before = CaptureKeySample(iter_->key());
    }
    iter_->SeekToLast();
    KeySample after{};
    if (iter_->Valid()) {
      after = CaptureKeySample(iter_->key());
    }
    trace_log_->RecordIteratorOp(
        IteratorTraceEventType::kSeekToLast, iterator_id_, cf_id_, before,
        after, before.full_len != 0, iter_->Valid(), iter_->status());
  }

  void Seek(const Slice& target) override {
    KeySample target_key = CaptureKeySample(target);
    const bool valid_before = iter_->Valid();
    iter_->Seek(target);
    KeySample result{};
    if (iter_->Valid()) {
      result = CaptureKeySample(iter_->key());
    }
    trace_log_->RecordIteratorOp(IteratorTraceEventType::kSeek, iterator_id_,
                                 cf_id_, target_key, result, valid_before,
                                 iter_->Valid(), iter_->status());
  }

  void SeekForPrev(const Slice& target) override {
    KeySample target_key = CaptureKeySample(target);
    const bool valid_before = iter_->Valid();
    iter_->SeekForPrev(target);
    KeySample result{};
    if (iter_->Valid()) {
      result = CaptureKeySample(iter_->key());
    }
    trace_log_->RecordIteratorOp(IteratorTraceEventType::kSeekForPrev,
                                 iterator_id_, cf_id_, target_key, result,
                                 valid_before, iter_->Valid(), iter_->status());
  }

  void Next() override {
    KeySample before{};
    if (iter_->Valid()) {
      before = CaptureKeySample(iter_->key());
    }
    iter_->Next();
    KeySample after{};
    if (iter_->Valid()) {
      after = CaptureKeySample(iter_->key());
    }
    trace_log_->RecordIteratorOp(IteratorTraceEventType::kNext, iterator_id_,
                                 cf_id_, before, after, before.full_len != 0,
                                 iter_->Valid(), iter_->status());
  }

  void Prev() override {
    KeySample before{};
    if (iter_->Valid()) {
      before = CaptureKeySample(iter_->key());
    }
    iter_->Prev();
    KeySample after{};
    if (iter_->Valid()) {
      after = CaptureKeySample(iter_->key());
    }
    trace_log_->RecordIteratorOp(IteratorTraceEventType::kPrev, iterator_id_,
                                 cf_id_, before, after, before.full_len != 0,
                                 iter_->Valid(), iter_->status());
  }

  Status Refresh() override { return Refresh(nullptr); }

  Status Refresh(const Snapshot* snapshot) override {
    KeySample before{};
    if (iter_->Valid()) {
      before = CaptureKeySample(iter_->key());
    }
    Status status = iter_->Refresh(snapshot);
    KeySample after{};
    if (iter_->Valid()) {
      after = CaptureKeySample(iter_->key());
    }
    trace_log_->RecordIteratorOp(
        IteratorTraceEventType::kRefresh, iterator_id_, cf_id_, before, after,
        before.full_len != 0, iter_->Valid(), status,
        snapshot != nullptr ? static_cast<uint32_t>(kHasSnapshot) : 0u,
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(snapshot)));
    return status;
  }

  bool PrepareValue() override {
    KeySample before{};
    if (iter_->Valid()) {
      before = CaptureKeySample(iter_->key());
    }
    bool ok = iter_->PrepareValue();
    KeySample after{};
    if (iter_->Valid()) {
      after = CaptureKeySample(iter_->key());
    }
    trace_log_->RecordIteratorOp(IteratorTraceEventType::kPrepareValue,
                                 iterator_id_, cf_id_, before, after,
                                 before.full_len != 0, iter_->Valid(),
                                 iter_->status(), 0, 0, 0, ok ? 1 : 0);
    return ok;
  }

  Slice key() const override { return iter_->key(); }

  Status status() const override { return iter_->status(); }

  Slice value() const override { return iter_->value(); }

  const WideColumns& columns() const override { return iter_->columns(); }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return iter_->GetProperty(std::move(prop_name), prop);
  }

  Slice timestamp() const override { return iter_->timestamp(); }

  void Prepare(const MultiScanArgs& scan_opts) override {
    iter_->Prepare(scan_opts);
  }

 private:
  std::unique_ptr<Iterator> iter_;
  DbStressPublicIteratorTraceLog* trace_log_;
  uint32_t cf_id_;
  uint64_t iterator_id_;
};

std::unique_ptr<DbStressPublicIteratorTraceLog>&
DbStressPublicIteratorTraceLogSingleton() {
  static std::unique_ptr<DbStressPublicIteratorTraceLog> trace_log;
  return trace_log;
}

}  // namespace

bool IsDbStressPublicIteratorTraceEnabled() {
  return FLAGS_trace_public_iterator_api &&
         DbStressPublicIteratorTraceLogSingleton();
}

void InitDbStressPublicIteratorTrace(const std::string& path) {
  if (!FLAGS_trace_public_iterator_api) {
    return;
  }
  auto& trace_log = DbStressPublicIteratorTraceLogSingleton();
  if (!trace_log) {
    trace_log.reset(new DbStressPublicIteratorTraceLog());
  }
  trace_log->SetLogFilePath(path);
}

void DumpDbStressPublicIteratorTrace() {
  auto& trace_log = DbStressPublicIteratorTraceLogSingleton();
  if (!trace_log) {
    return;
  }
  trace_log->DumpRaw();
}

std::string GetDbStressPublicIteratorTracePath() {
  auto& trace_log = DbStressPublicIteratorTraceLogSingleton();
  if (!trace_log) {
    return std::string();
  }
  return trace_log->GetLogFilePath();
}

std::unique_ptr<Iterator> MaybeWrapDbStressTraceIterator(
    std::unique_ptr<Iterator> iter, const ReadOptions& read_opts,
    ColumnFamilyHandle* column_family) {
  auto& trace_log = DbStressPublicIteratorTraceLogSingleton();
  if (!trace_log || !iter) {
    return iter;
  }
  const uint32_t cf_id = column_family != nullptr ? column_family->GetID() : 0;
  return std::unique_ptr<Iterator>(new DbStressTraceIterator(
      std::move(iter), trace_log.get(), cf_id, read_opts));
}

std::unique_ptr<Iterator> NewDbStressTraceIterator(
    DB* db, const ReadOptions& read_opts, ColumnFamilyHandle* column_family) {
  if (db == nullptr) {
    return nullptr;
  }
  if (column_family != nullptr) {
    return MaybeWrapDbStressTraceIterator(
        std::unique_ptr<Iterator>(db->NewIterator(read_opts, column_family)),
        read_opts, column_family);
  }
  return MaybeWrapDbStressTraceIterator(
      std::unique_ptr<Iterator>(db->NewIterator(read_opts)), read_opts,
      nullptr);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
