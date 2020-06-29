//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <array>
#include <deque>

#include "rocksdb/filter_policy.h"

#include "rocksdb/slice.h"
#include "table/block_based/block_based_filter_block.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "third-party/folly/folly/ConstexprMath.h"
#include "util/bloom_impl.h"
#include "util/coding.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// See description in FastLocalBloomImpl
class FastLocalBloomBitsBuilder : public BuiltinFilterBitsBuilder {
 public:
  // Non-null aggregate_rounding_balance implies optimize_filters_for_memory
  explicit FastLocalBloomBitsBuilder(
      const int millibits_per_key,
      std::atomic<int64_t>* aggregate_rounding_balance)
      : millibits_per_key_(millibits_per_key),
        aggregate_rounding_balance_(aggregate_rounding_balance) {
    assert(millibits_per_key >= 1000);
  }

  // No Copy allowed
  FastLocalBloomBitsBuilder(const FastLocalBloomBitsBuilder&) = delete;
  void operator=(const FastLocalBloomBitsBuilder&) = delete;

  ~FastLocalBloomBitsBuilder() override {}

  virtual void AddKey(const Slice& key) override {
    uint64_t hash = GetSliceHash64(key);
    if (hash_entries_.empty() || hash != hash_entries_.back()) {
      hash_entries_.push_back(hash);
    }
  }

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
    size_t num_entry = hash_entries_.size();
    std::unique_ptr<char[]> mutable_buf;
    uint32_t len_with_metadata =
        CalculateAndAllocate(num_entry, &mutable_buf, /*update_balance*/ true);

    assert(mutable_buf);
    assert(len_with_metadata >= 5);

    // Compute num_probes after any rounding / adjustments
    int num_probes = GetNumProbes(num_entry, len_with_metadata);

    uint32_t len = len_with_metadata - 5;
    if (len > 0) {
      AddAllEntries(mutable_buf.get(), len, num_probes);
    }

    assert(hash_entries_.empty());

    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -1 = Marker for newer Bloom implementations
    mutable_buf[len] = static_cast<char>(-1);
    // 0 = Marker for this sub-implementation
    mutable_buf[len + 1] = static_cast<char>(0);
    // num_probes (and 0 in upper bits for 64-byte block size)
    mutable_buf[len + 2] = static_cast<char>(num_probes);
    // rest of metadata stays zero

    Slice rv(mutable_buf.get(), len_with_metadata);
    *buf = std::move(mutable_buf);
    return rv;
  }

  int CalculateNumEntry(const uint32_t bytes) override {
    uint32_t bytes_no_meta = bytes >= 5u ? bytes - 5u : 0;
    return static_cast<int>(uint64_t{8000} * bytes_no_meta /
                            millibits_per_key_);
  }

  uint32_t CalculateSpace(const int num_entry) override {
    // NB: the BuiltinFilterBitsBuilder API presumes len fits in uint32_t.
    return static_cast<uint32_t>(
        CalculateAndAllocate(static_cast<size_t>(num_entry),
                             /* buf */ nullptr,
                             /*update_balance*/ false));
  }

  // To choose size using malloc_usable_size, we have to actually allocate.
  uint32_t CalculateAndAllocate(size_t num_entry, std::unique_ptr<char[]>* buf,
                                bool update_balance) {
    std::unique_ptr<char[]> tmpbuf;

    // If not for cache line blocks in the filter, what would the target
    // length in bytes be?
    size_t raw_target_len = static_cast<size_t>(
        (uint64_t{num_entry} * millibits_per_key_ + 7999) / 8000);

    if (raw_target_len >= size_t{0xffffffc0}) {
      // Max supported for this data structure implementation
      raw_target_len = size_t{0xffffffc0};
    }

    // Round up to nearest multiple of 64 (block size). This adjustment is
    // used for target FP rate only so that we don't receive complaints about
    // lower FP rate vs. historic Bloom filter behavior.
    uint32_t target_len =
        static_cast<uint32_t>(raw_target_len + 63) & ~uint32_t{63};

    // Return value set to a default; overwritten in some cases
    uint32_t rv = target_len + /* metadata */ 5;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    if (aggregate_rounding_balance_ != nullptr) {
      // Do optimize_filters_for_memory, using malloc_usable_size.
      // Approach: try to keep FP rate balance better than or on
      // target (negative aggregate_rounding_balance_). We can then select a
      // lower bound filter size (within reasonable limits) that gets us as
      // close to on target as possible. We request allocation for that filter
      // size and use malloc_usable_size to "round up" to the actual
      // allocation size.

      // Although it can be considered bad practice to use malloc_usable_size
      // to access an object beyond its original size, this approach should
      // quite general: working for all allocators that properly support
      // malloc_usable_size.

      // Race condition on balance is OK because it can only cause temporary
      // skew in rounding up vs. rounding down, as long as updates are atomic
      // and relative.
      int64_t balance = aggregate_rounding_balance_->load();

      double target_fp_rate = EstimatedFpRate(num_entry, target_len + 5);
      double rv_fp_rate = target_fp_rate;

      if (balance < 0) {
        // See formula for BloomFilterPolicy::aggregate_rounding_balance_
        double for_balance_fp_rate =
            -balance / double{0x100000000} + target_fp_rate;

        // To simplify, we just try a few modified smaller sizes. This also
        // caps how much we vary filter size vs. target, to avoid outlier
        // behavior from excessive variance.
        for (uint64_t maybe_len64 :
             {uint64_t{3} * target_len / 4, uint64_t{13} * target_len / 16,
              uint64_t{7} * target_len / 8, uint64_t{15} * target_len / 16}) {
          uint32_t maybe_len =
              static_cast<uint32_t>(maybe_len64) & ~uint32_t{63};
          double maybe_fp_rate = EstimatedFpRate(num_entry, maybe_len + 5);
          if (maybe_fp_rate <= for_balance_fp_rate) {
            rv = maybe_len + /* metadata */ 5;
            rv_fp_rate = maybe_fp_rate;
            break;
          }
        }
      }

      // Filter blocks are loaded into block cache with their block trailer.
      // We need to make sure that's accounted for in choosing a
      // fragmentation-friendly size.
      const uint32_t kExtraPadding = kBlockTrailerSize;
      size_t requested = rv + kExtraPadding;

      // Allocate and get usable size
      tmpbuf.reset(new char[requested]);
      size_t usable = malloc_usable_size(tmpbuf.get());

      if (usable - usable / 4 > requested) {
        // Ratio greater than 4/3 is too much for utilizing, if it's
        // not a buggy or mislinked malloc_usable_size implementation.
        // Non-linearity of FP rates with bits/key means rapidly
        // diminishing returns in overall accuracy for additional
        // storage on disk.
        // Nothing to do, except assert that the result is accurate about
        // the usable size. (Assignment never used.)
        assert((tmpbuf[usable - 1] = 'x'));
      } else if (usable > requested) {
        // Adjust for reasonably larger usable size
        size_t usable_len = (usable - kExtraPadding - /* metadata */ 5);
        if (usable_len >= size_t{0xffffffc0}) {
          // Max supported for this data structure implementation
          usable_len = size_t{0xffffffc0};
        }

        rv = (static_cast<uint32_t>(usable_len) & ~uint32_t{63}) +
             /* metadata */ 5;
        rv_fp_rate = EstimatedFpRate(num_entry, rv);
      } else {
        // Too small means bad malloc_usable_size
        assert(usable == requested);
      }
      memset(tmpbuf.get(), 0, rv);

      if (update_balance) {
        int64_t diff = static_cast<int64_t>((rv_fp_rate - target_fp_rate) *
                                            double{0x100000000});
        *aggregate_rounding_balance_ += diff;
      }
    }
#else
    (void)update_balance;
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    if (buf) {
      if (tmpbuf) {
        *buf = std::move(tmpbuf);
      } else {
        buf->reset(new char[rv]());
      }
    }
    return rv;
  }

  double EstimatedFpRate(size_t keys, size_t len_with_metadata) override {
    int num_probes = GetNumProbes(keys, len_with_metadata);
    return FastLocalBloomImpl::EstimatedFpRate(
        keys, len_with_metadata - /*metadata*/ 5, num_probes, /*hash bits*/ 64);
  }

 private:
  // Compute num_probes after any rounding / adjustments
  int GetNumProbes(size_t keys, size_t len_with_metadata) {
    uint64_t millibits = uint64_t{len_with_metadata - 5} * 8000;
    int actual_millibits_per_key =
        static_cast<int>(millibits / std::max(keys, size_t{1}));
    // BEGIN XXX/TODO(peterd): preserving old/default behavior for now to
    // minimize unit test churn. Remove this some time.
    if (!aggregate_rounding_balance_) {
      actual_millibits_per_key = millibits_per_key_;
    }
    // END XXX/TODO
    return FastLocalBloomImpl::ChooseNumProbes(actual_millibits_per_key);
  }

  void AddAllEntries(char* data, uint32_t len, int num_probes) {
    // Simple version without prefetching:
    //
    // for (auto h : hash_entries_) {
    //   FastLocalBloomImpl::AddHash(Lower32of64(h), Upper32of64(h), len,
    //                               num_probes, data);
    // }

    const size_t num_entries = hash_entries_.size();
    constexpr size_t kBufferMask = 7;
    static_assert(((kBufferMask + 1) & kBufferMask) == 0,
                  "Must be power of 2 minus 1");

    std::array<uint32_t, kBufferMask + 1> hashes;
    std::array<uint32_t, kBufferMask + 1> byte_offsets;

    // Prime the buffer
    size_t i = 0;
    for (; i <= kBufferMask && i < num_entries; ++i) {
      uint64_t h = hash_entries_.front();
      hash_entries_.pop_front();
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len, data,
                                      /*out*/ &byte_offsets[i]);
      hashes[i] = Upper32of64(h);
    }

    // Process and buffer
    for (; i < num_entries; ++i) {
      uint32_t& hash_ref = hashes[i & kBufferMask];
      uint32_t& byte_offset_ref = byte_offsets[i & kBufferMask];
      // Process (add)
      FastLocalBloomImpl::AddHashPrepared(hash_ref, num_probes,
                                          data + byte_offset_ref);
      // And buffer
      uint64_t h = hash_entries_.front();
      hash_entries_.pop_front();
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len, data,
                                      /*out*/ &byte_offset_ref);
      hash_ref = Upper32of64(h);
    }

    // Finish processing
    for (i = 0; i <= kBufferMask && i < num_entries; ++i) {
      FastLocalBloomImpl::AddHashPrepared(hashes[i], num_probes,
                                          data + byte_offsets[i]);
    }
  }

  // Target allocation per added key, in thousandths of a bit.
  int millibits_per_key_;
  // See BloomFilterPolicy::aggregate_rounding_balance_. If nullptr,
  // always "round up" like historic behavior.
  std::atomic<int64_t>* aggregate_rounding_balance_;
  // A deque avoids unnecessary copying of already-saved values
  // and has near-minimal peak memory use.
  std::deque<uint64_t> hash_entries_;
};

// See description in FastLocalBloomImpl
class FastLocalBloomBitsReader : public FilterBitsReader {
 public:
  FastLocalBloomBitsReader(const char* data, int num_probes, uint32_t len_bytes)
      : data_(data), num_probes_(num_probes), len_bytes_(len_bytes) {}

  // No Copy allowed
  FastLocalBloomBitsReader(const FastLocalBloomBitsReader&) = delete;
  void operator=(const FastLocalBloomBitsReader&) = delete;

  ~FastLocalBloomBitsReader() override {}

  bool MayMatch(const Slice& key) override {
    uint64_t h = GetSliceHash64(key);
    uint32_t byte_offset;
    FastLocalBloomImpl::PrepareHash(Lower32of64(h), len_bytes_, data_,
                                    /*out*/ &byte_offset);
    return FastLocalBloomImpl::HashMayMatchPrepared(Upper32of64(h), num_probes_,
                                                    data_ + byte_offset);
  }

  virtual void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> hashes;
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> byte_offsets;
    for (int i = 0; i < num_keys; ++i) {
      uint64_t h = GetSliceHash64(*keys[i]);
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len_bytes_, data_,
                                      /*out*/ &byte_offsets[i]);
      hashes[i] = Upper32of64(h);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = FastLocalBloomImpl::HashMayMatchPrepared(
          hashes[i], num_probes_, data_ + byte_offsets[i]);
    }
  }

 private:
  const char* data_;
  const int num_probes_;
  const uint32_t len_bytes_;
};

using LegacyBloomImpl = LegacyLocalityBloomImpl</*ExtraRotates*/ false>;

class LegacyBloomBitsBuilder : public BuiltinFilterBitsBuilder {
 public:
  explicit LegacyBloomBitsBuilder(const int bits_per_key, Logger* info_log);

  // No Copy allowed
  LegacyBloomBitsBuilder(const LegacyBloomBitsBuilder&) = delete;
  void operator=(const LegacyBloomBitsBuilder&) = delete;

  ~LegacyBloomBitsBuilder() override;

  void AddKey(const Slice& key) override;

  Slice Finish(std::unique_ptr<const char[]>* buf) override;

  int CalculateNumEntry(const uint32_t bytes) override;

  uint32_t CalculateSpace(const int num_entry) override {
    uint32_t dont_care1;
    uint32_t dont_care2;
    return CalculateSpace(num_entry, &dont_care1, &dont_care2);
  }

  double EstimatedFpRate(size_t keys, size_t bytes) override {
    return LegacyBloomImpl::EstimatedFpRate(keys, bytes - /*metadata*/ 5,
                                            num_probes_);
  }

 private:
  int bits_per_key_;
  int num_probes_;
  std::vector<uint32_t> hash_entries_;
  Logger* info_log_;

  // Get totalbits that optimized for cpu cache line
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);

  // Reserve space for new filter
  char* ReserveSpace(const int num_entry, uint32_t* total_bits,
                     uint32_t* num_lines);

  // Implementation-specific variant of public CalculateSpace
  uint32_t CalculateSpace(const int num_entry, uint32_t* total_bits,
                          uint32_t* num_lines);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);
};

LegacyBloomBitsBuilder::LegacyBloomBitsBuilder(const int bits_per_key,
                                               Logger* info_log)
    : bits_per_key_(bits_per_key),
      num_probes_(LegacyNoLocalityBloomImpl::ChooseNumProbes(bits_per_key_)),
      info_log_(info_log) {
  assert(bits_per_key_);
}

LegacyBloomBitsBuilder::~LegacyBloomBitsBuilder() {}

void LegacyBloomBitsBuilder::AddKey(const Slice& key) {
  uint32_t hash = BloomHash(key);
  if (hash_entries_.size() == 0 || hash != hash_entries_.back()) {
    hash_entries_.push_back(hash);
  }
}

Slice LegacyBloomBitsBuilder::Finish(std::unique_ptr<const char[]>* buf) {
  uint32_t total_bits, num_lines;
  size_t num_entries = hash_entries_.size();
  char* data =
      ReserveSpace(static_cast<int>(num_entries), &total_bits, &num_lines);
  assert(data);

  if (total_bits != 0 && num_lines != 0) {
    for (auto h : hash_entries_) {
      AddHash(h, data, num_lines, total_bits);
    }

    // Check for excessive entries for 32-bit hash function
    if (num_entries >= /* minimum of 3 million */ 3000000U) {
      // More specifically, we can detect that the 32-bit hash function
      // is causing significant increase in FP rate by comparing current
      // estimated FP rate to what we would get with a normal number of
      // keys at same memory ratio.
      double est_fp_rate = LegacyBloomImpl::EstimatedFpRate(
          num_entries, total_bits / 8, num_probes_);
      double vs_fp_rate = LegacyBloomImpl::EstimatedFpRate(
          1U << 16, (1U << 16) * bits_per_key_ / 8, num_probes_);

      if (est_fp_rate >= 1.50 * vs_fp_rate) {
        // For more details, see
        // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
        ROCKS_LOG_WARN(
            info_log_,
            "Using legacy SST/BBT Bloom filter with excessive key count "
            "(%.1fM @ %dbpk), causing estimated %.1fx higher filter FP rate. "
            "Consider using new Bloom with format_version>=5, smaller SST "
            "file size, or partitioned filters.",
            num_entries / 1000000.0, bits_per_key_, est_fp_rate / vs_fp_rate);
      }
    }
  }
  // See BloomFilterPolicy::GetFilterBitsReader for metadata
  data[total_bits / 8] = static_cast<char>(num_probes_);
  EncodeFixed32(data + total_bits / 8 + 1, static_cast<uint32_t>(num_lines));

  const char* const_data = data;
  buf->reset(const_data);
  hash_entries_.clear();

  return Slice(data, total_bits / 8 + 5);
}

uint32_t LegacyBloomBitsBuilder::GetTotalBitsForLocality(uint32_t total_bits) {
  uint32_t num_lines =
      (total_bits + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);

  // Make num_lines an odd number to make sure more bits are involved
  // when determining which block.
  if (num_lines % 2 == 0) {
    num_lines++;
  }
  return num_lines * (CACHE_LINE_SIZE * 8);
}

uint32_t LegacyBloomBitsBuilder::CalculateSpace(const int num_entry,
                                                uint32_t* total_bits,
                                                uint32_t* num_lines) {
  assert(bits_per_key_);
  if (num_entry != 0) {
    uint32_t total_bits_tmp = static_cast<uint32_t>(num_entry * bits_per_key_);

    *total_bits = GetTotalBitsForLocality(total_bits_tmp);
    *num_lines = *total_bits / (CACHE_LINE_SIZE * 8);
    assert(*total_bits > 0 && *total_bits % 8 == 0);
  } else {
    // filter is empty, just leave space for metadata
    *total_bits = 0;
    *num_lines = 0;
  }

  // Reserve space for Filter
  uint32_t sz = *total_bits / 8;
  sz += 5;  // 4 bytes for num_lines, 1 byte for num_probes
  return sz;
}

char* LegacyBloomBitsBuilder::ReserveSpace(const int num_entry,
                                           uint32_t* total_bits,
                                           uint32_t* num_lines) {
  uint32_t sz = CalculateSpace(num_entry, total_bits, num_lines);
  char* data = new char[sz];
  memset(data, 0, sz);
  return data;
}

int LegacyBloomBitsBuilder::CalculateNumEntry(const uint32_t bytes) {
  assert(bits_per_key_);
  assert(bytes > 0);
  int high = static_cast<int>(bytes * 8 / bits_per_key_ + 1);
  int low = 1;
  int n = high;
  for (; n >= low; n--) {
    if (CalculateSpace(n) <= bytes) {
      break;
    }
  }
  assert(n < high);  // High should be an overestimation
  return n;
}

inline void LegacyBloomBitsBuilder::AddHash(uint32_t h, char* data,
                                            uint32_t num_lines,
                                            uint32_t total_bits) {
#ifdef NDEBUG
  static_cast<void>(total_bits);
#endif
  assert(num_lines > 0 && total_bits > 0);

  LegacyBloomImpl::AddHash(h, num_lines, num_probes_, data,
                           folly::constexpr_log2(CACHE_LINE_SIZE));
}

class LegacyBloomBitsReader : public FilterBitsReader {
 public:
  LegacyBloomBitsReader(const char* data, int num_probes, uint32_t num_lines,
                        uint32_t log2_cache_line_size)
      : data_(data),
        num_probes_(num_probes),
        num_lines_(num_lines),
        log2_cache_line_size_(log2_cache_line_size) {}

  // No Copy allowed
  LegacyBloomBitsReader(const LegacyBloomBitsReader&) = delete;
  void operator=(const LegacyBloomBitsReader&) = delete;

  ~LegacyBloomBitsReader() override {}

  // "contents" contains the data built by a preceding call to
  // FilterBitsBuilder::Finish. MayMatch must return true if the key was
  // passed to FilterBitsBuilder::AddKey. This method may return true or false
  // if the key was not on the list, but it should aim to return false with a
  // high probability.
  bool MayMatch(const Slice& key) override {
    uint32_t hash = BloomHash(key);
    uint32_t byte_offset;
    LegacyBloomImpl::PrepareHashMayMatch(
        hash, num_lines_, data_, /*out*/ &byte_offset, log2_cache_line_size_);
    return LegacyBloomImpl::HashMayMatchPrepared(
        hash, num_probes_, data_ + byte_offset, log2_cache_line_size_);
  }

  virtual void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> hashes;
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> byte_offsets;
    for (int i = 0; i < num_keys; ++i) {
      hashes[i] = BloomHash(*keys[i]);
      LegacyBloomImpl::PrepareHashMayMatch(hashes[i], num_lines_, data_,
                                           /*out*/ &byte_offsets[i],
                                           log2_cache_line_size_);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = LegacyBloomImpl::HashMayMatchPrepared(
          hashes[i], num_probes_, data_ + byte_offsets[i],
          log2_cache_line_size_);
    }
  }

 private:
  const char* data_;
  const int num_probes_;
  const uint32_t num_lines_;
  const uint32_t log2_cache_line_size_;
};

class AlwaysTrueFilter : public FilterBitsReader {
 public:
  bool MayMatch(const Slice&) override { return true; }
  using FilterBitsReader::MayMatch;  // inherit overload
};

class AlwaysFalseFilter : public FilterBitsReader {
 public:
  bool MayMatch(const Slice&) override { return false; }
  using FilterBitsReader::MayMatch;  // inherit overload
};

}  // namespace

const std::vector<BloomFilterPolicy::Mode> BloomFilterPolicy::kAllFixedImpls = {
    kLegacyBloom,
    kDeprecatedBlock,
    kFastLocalBloom,
};

const std::vector<BloomFilterPolicy::Mode> BloomFilterPolicy::kAllUserModes = {
    kDeprecatedBlock,
    kAuto,
};

BloomFilterPolicy::BloomFilterPolicy(double bits_per_key, Mode mode)
    : mode_(mode), warned_(false), aggregate_rounding_balance_(0) {
  // Sanitize bits_per_key
  if (bits_per_key < 1.0) {
    bits_per_key = 1.0;
  } else if (!(bits_per_key < 100.0)) {  // including NaN
    bits_per_key = 100.0;
  }

  // Includes a nudge toward rounding up, to ensure on all platforms
  // that doubles specified with three decimal digits after the decimal
  // point are interpreted accurately.
  millibits_per_key_ = static_cast<int>(bits_per_key * 1000.0 + 0.500001);

  // For better or worse, this is a rounding up of a nudged rounding up,
  // e.g. 7.4999999999999 will round up to 8, but that provides more
  // predictability against small arithmetic errors in floating point.
  whole_bits_per_key_ = (millibits_per_key_ + 500) / 1000;
}

BloomFilterPolicy::~BloomFilterPolicy() {}

const char* BloomFilterPolicy::Name() const {
  return "rocksdb.BuiltinBloomFilter";
}

void BloomFilterPolicy::CreateFilter(const Slice* keys, int n,
                                     std::string* dst) const {
  // We should ideally only be using this deprecated interface for
  // appropriately constructed BloomFilterPolicy
  assert(mode_ == kDeprecatedBlock);

  // Compute bloom filter size (in both bits and bytes)
  uint32_t bits = static_cast<uint32_t>(n * whole_bits_per_key_);

  // For small n, we can see a very high false positive rate.  Fix it
  // by enforcing a minimum bloom filter length.
  if (bits < 64) bits = 64;

  uint32_t bytes = (bits + 7) / 8;
  bits = bytes * 8;

  int num_probes =
      LegacyNoLocalityBloomImpl::ChooseNumProbes(whole_bits_per_key_);

  const size_t init_size = dst->size();
  dst->resize(init_size + bytes, 0);
  dst->push_back(static_cast<char>(num_probes));  // Remember # of probes
  char* array = &(*dst)[init_size];
  for (int i = 0; i < n; i++) {
    LegacyNoLocalityBloomImpl::AddHash(BloomHash(keys[i]), bits, num_probes,
                                       array);
  }
}

bool BloomFilterPolicy::KeyMayMatch(const Slice& key,
                                    const Slice& bloom_filter) const {
  const size_t len = bloom_filter.size();
  if (len < 2 || len > 0xffffffffU) {
    return false;
  }

  const char* array = bloom_filter.data();
  const uint32_t bits = static_cast<uint32_t>(len - 1) * 8;

  // Use the encoded k so that we can read filters generated by
  // bloom filters created using different parameters.
  const int k = static_cast<uint8_t>(array[len - 1]);
  if (k > 30) {
    // Reserved for potentially new encodings for short bloom filters.
    // Consider it a match.
    return true;
  }
  // NB: using stored k not num_probes for whole_bits_per_key_
  return LegacyNoLocalityBloomImpl::HashMayMatch(BloomHash(key), bits, k,
                                                 array);
}

FilterBitsBuilder* BloomFilterPolicy::GetFilterBitsBuilder() const {
  // This code path should no longer be used, for the built-in
  // BloomFilterPolicy. Internal to RocksDB and outside
  // BloomFilterPolicy, only get a FilterBitsBuilder with
  // BloomFilterPolicy::GetBuilderFromContext(), which will call
  // BloomFilterPolicy::GetBuilderWithContext(). RocksDB users have
  // been warned (HISTORY.md) that they can no longer call this on
  // the built-in BloomFilterPolicy (unlikely).
  assert(false);
  return GetBuilderWithContext(FilterBuildingContext(BlockBasedTableOptions()));
}

FilterBitsBuilder* BloomFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  Mode cur = mode_;
  bool offm = context.table_options.optimize_filters_for_memory;
  // Unusual code construction so that we can have just
  // one exhaustive switch without (risky) recursion
  for (int i = 0; i < 2; ++i) {
    switch (cur) {
      case kAuto:
        if (context.table_options.format_version < 5) {
          cur = kLegacyBloom;
        } else {
          cur = kFastLocalBloom;
        }
        break;
      case kDeprecatedBlock:
        return nullptr;
      case kFastLocalBloom:
        return new FastLocalBloomBitsBuilder(
            millibits_per_key_, offm ? &aggregate_rounding_balance_ : nullptr);
      case kLegacyBloom:
        if (whole_bits_per_key_ >= 14 && context.info_log &&
            !warned_.load(std::memory_order_relaxed)) {
          warned_ = true;
          const char* adjective;
          if (whole_bits_per_key_ >= 20) {
            adjective = "Dramatic";
          } else {
            adjective = "Significant";
          }
          // For more details, see
          // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
          ROCKS_LOG_WARN(
              context.info_log,
              "Using legacy Bloom filter with high (%d) bits/key. "
              "%s filter space and/or accuracy improvement is available "
              "with format_version>=5.",
              whole_bits_per_key_, adjective);
        }
        return new LegacyBloomBitsBuilder(whole_bits_per_key_,
                                          context.info_log);
    }
  }
  assert(false);
  return nullptr;  // something legal
}

FilterBitsBuilder* BloomFilterPolicy::GetBuilderFromContext(
    const FilterBuildingContext& context) {
  if (context.table_options.filter_policy) {
    return context.table_options.filter_policy->GetBuilderWithContext(context);
  } else {
    return nullptr;
  }
}

// Read metadata to determine what kind of FilterBitsReader is needed
// and return a new one.
FilterBitsReader* BloomFilterPolicy::GetFilterBitsReader(
    const Slice& contents) const {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  if (len_with_meta <= 5) {
    // filter is empty or broken. Treat like zero keys added.
    return new AlwaysFalseFilter();
  }

  // Legacy Bloom filter data:
  //             0 +-----------------------------------+
  //               | Raw Bloom filter data             |
  //               | ...                               |
  //           len +-----------------------------------+
  //               | byte for num_probes or            |
  //               |   marker for new implementations  |
  //         len+1 +-----------------------------------+
  //               | four bytes for number of cache    |
  //               |   lines                           |
  // len_with_meta +-----------------------------------+

  int8_t raw_num_probes =
      static_cast<int8_t>(contents.data()[len_with_meta - 5]);
  // NB: *num_probes > 30 and < 128 probably have not been used, because of
  // BloomFilterPolicy::initialize, unless directly calling
  // LegacyBloomBitsBuilder as an API, but we are leaving those cases in
  // limbo with LegacyBloomBitsReader for now.

  if (raw_num_probes < 1) {
    // Note: < 0 (or unsigned > 127) indicate special new implementations
    // (or reserved for future use)
    if (raw_num_probes == -1) {
      // Marker for newer Bloom implementations
      return GetBloomBitsReader(contents);
    }
    // otherwise
    // Treat as zero probes (always FP) for now.
    return new AlwaysTrueFilter();
  }
  // else attempt decode for LegacyBloomBitsReader

  int num_probes = raw_num_probes;
  assert(num_probes >= 1);
  assert(num_probes <= 127);

  uint32_t len = len_with_meta - 5;
  assert(len > 0);

  uint32_t num_lines = DecodeFixed32(contents.data() + len_with_meta - 4);
  uint32_t log2_cache_line_size;

  if (num_lines * CACHE_LINE_SIZE == len) {
    // Common case
    log2_cache_line_size = folly::constexpr_log2(CACHE_LINE_SIZE);
  } else if (num_lines == 0 || len % num_lines != 0) {
    // Invalid (no solution to num_lines * x == len)
    // Treat as zero probes (always FP) for now.
    return new AlwaysTrueFilter();
  } else {
    // Determine the non-native cache line size (from another system)
    log2_cache_line_size = 0;
    while ((num_lines << log2_cache_line_size) < len) {
      ++log2_cache_line_size;
    }
    if ((num_lines << log2_cache_line_size) != len) {
      // Invalid (block size not a power of two)
      // Treat as zero probes (always FP) for now.
      return new AlwaysTrueFilter();
    }
  }
  // if not early return
  return new LegacyBloomBitsReader(contents.data(), num_probes, num_lines,
                                   log2_cache_line_size);
}

// For newer Bloom filter implementations
FilterBitsReader* BloomFilterPolicy::GetBloomBitsReader(
    const Slice& contents) const {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  uint32_t len = len_with_meta - 5;

  assert(len > 0);  // precondition

  // New Bloom filter data:
  //             0 +-----------------------------------+
  //               | Raw Bloom filter data             |
  //               | ...                               |
  //           len +-----------------------------------+
  //               | char{-1} byte -> new Bloom filter |
  //         len+1 +-----------------------------------+
  //               | byte for subimplementation        |
  //               |   0: FastLocalBloom               |
  //               |   other: reserved                 |
  //         len+2 +-----------------------------------+
  //               | byte for block_and_probes         |
  //               |   0 in top 3 bits -> 6 -> 64-byte |
  //               |   reserved:                       |
  //               |   1 in top 3 bits -> 7 -> 128-byte|
  //               |   2 in top 3 bits -> 8 -> 256-byte|
  //               |   ...                             |
  //               |   num_probes in bottom 5 bits,    |
  //               |     except 0 and 31 reserved      |
  //         len+3 +-----------------------------------+
  //               | two bytes reserved                |
  //               |   possibly for hash seed          |
  // len_with_meta +-----------------------------------+

  // Read more metadata (see above)
  char sub_impl_val = contents.data()[len_with_meta - 4];
  char block_and_probes = contents.data()[len_with_meta - 3];
  int log2_block_bytes = ((block_and_probes >> 5) & 7) + 6;

  int num_probes = (block_and_probes & 31);
  if (num_probes < 1 || num_probes > 30) {
    // Reserved / future safe
    return new AlwaysTrueFilter();
  }

  uint16_t rest = DecodeFixed16(contents.data() + len_with_meta - 2);
  if (rest != 0) {
    // Reserved, possibly for hash seed
    // Future safe
    return new AlwaysTrueFilter();
  }

  if (sub_impl_val == 0) {        // FastLocalBloom
    if (log2_block_bytes == 6) {  // Only block size supported for now
      return new FastLocalBloomBitsReader(contents.data(), num_probes, len);
    }
  }
  // otherwise
  // Reserved / future safe
  return new AlwaysTrueFilter();
}

const FilterPolicy* NewBloomFilterPolicy(double bits_per_key,
                                         bool use_block_based_builder) {
  BloomFilterPolicy::Mode m;
  if (use_block_based_builder) {
    m = BloomFilterPolicy::kDeprecatedBlock;
  } else {
    m = BloomFilterPolicy::kAuto;
  }
  assert(std::find(BloomFilterPolicy::kAllUserModes.begin(),
                   BloomFilterPolicy::kAllUserModes.end(),
                   m) != BloomFilterPolicy::kAllUserModes.end());
  return new BloomFilterPolicy(bits_per_key, m);
}

FilterBuildingContext::FilterBuildingContext(
    const BlockBasedTableOptions& _table_options)
    : table_options(_table_options) {}

FilterPolicy::~FilterPolicy() { }

Status FilterPolicy::CreateFromString(
    const ConfigOptions& /*options*/, const std::string& value,
    std::shared_ptr<const FilterPolicy>* policy) {
  const std::string kBloomName = "bloomfilter:";
  if (value == kNullptrString || value == "rocksdb.BuiltinBloomFilter") {
    policy->reset();
#ifndef ROCKSDB_LITE
  } else if (value.compare(0, kBloomName.size(), kBloomName) == 0) {
    size_t pos = value.find(':', kBloomName.size());
    if (pos == std::string::npos) {
      return Status::InvalidArgument(
          "Invalid filter policy config, missing bits_per_key");
    } else {
      double bits_per_key = ParseDouble(
          trim(value.substr(kBloomName.size(), pos - kBloomName.size())));
      bool use_block_based_builder =
          ParseBoolean("use_block_based_builder", trim(value.substr(pos + 1)));
      policy->reset(
          NewBloomFilterPolicy(bits_per_key, use_block_based_builder));
    }
  } else {
    return Status::InvalidArgument("Invalid filter policy name ", value);
#else
  } else {
    return Status::NotSupported("Cannot load filter policy in LITE mode ",
                                value);
#endif  // ROCKSDB_LITE
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
