//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/filter_policy.h"

#include <array>
#include <deque>
#include <limits>

#include "rocksdb/slice.h"
#include "table/block_based/block_based_filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "third-party/folly/folly/ConstexprMath.h"
#include "util/bloom_impl.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/ribbon_config.h"
#include "util/ribbon_impl.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Metadata trailer size for built-in filters. (This is separate from
// block-based table block trailer.)
//
// Originally this was 1 byte for num_probes and 4 bytes for number of
// cache lines in the Bloom filter, but now the first trailer byte is
// usually an implementation marker and remaining 4 bytes have various
// meanings.
static constexpr uint32_t kMetadataLen = 5;

Slice FinishAlwaysFalse(std::unique_ptr<const char[]>* /*buf*/) {
  // Missing metadata, treated as zero entries
  return Slice(nullptr, 0);
}

// Base class for filter builders using the XXH3 preview hash,
// also known as Hash64 or GetSliceHash64.
class XXH3pFilterBitsBuilder : public BuiltinFilterBitsBuilder {
 public:
  explicit XXH3pFilterBitsBuilder(
      std::atomic<int64_t>* aggregate_rounding_balance)
      : aggregate_rounding_balance_(aggregate_rounding_balance) {}

  ~XXH3pFilterBitsBuilder() override {}

  virtual void AddKey(const Slice& key) override {
    uint64_t hash = GetSliceHash64(key);
    // Especially with prefixes, it is common to have repetition,
    // though only adjacent repetition, which we want to immediately
    // recognize and collapse for estimating true filter space
    // requirements.
    if (hash_entries_.empty() || hash != hash_entries_.back()) {
      hash_entries_.push_back(hash);
    }
  }

 protected:
  static constexpr uint32_t kMetadataLen = 5;

  // For delegating between XXH3pFilterBitsBuilders
  void SwapEntriesWith(XXH3pFilterBitsBuilder* other) {
    std::swap(hash_entries_, other->hash_entries_);
  }

  virtual size_t RoundDownUsableSpace(size_t available_size) = 0;

  // To choose size using malloc_usable_size, we have to actually allocate.
  size_t AllocateMaybeRounding(size_t target_len_with_metadata,
                               size_t num_entries,
                               std::unique_ptr<char[]>* buf) {
    // Return value set to a default; overwritten in some cases
    size_t rv = target_len_with_metadata;
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
      // to access an object beyond its original size, this approach should be
      // quite general: working for all allocators that properly support
      // malloc_usable_size.

      // Race condition on balance is OK because it can only cause temporary
      // skew in rounding up vs. rounding down, as long as updates are atomic
      // and relative.
      int64_t balance = aggregate_rounding_balance_->load();

      double target_fp_rate =
          EstimatedFpRate(num_entries, target_len_with_metadata);
      double rv_fp_rate = target_fp_rate;

      if (balance < 0) {
        // See formula for BloomFilterPolicy::aggregate_rounding_balance_
        double for_balance_fp_rate =
            -balance / double{0x100000000} + target_fp_rate;

        // To simplify, we just try a few modified smaller sizes. This also
        // caps how much we vary filter size vs. target, to avoid outlier
        // behavior from excessive variance.
        size_t target_len = target_len_with_metadata - kMetadataLen;
        assert(target_len < target_len_with_metadata);  // check underflow
        for (uint64_t maybe_len_rough :
             {uint64_t{3} * target_len / 4, uint64_t{13} * target_len / 16,
              uint64_t{7} * target_len / 8, uint64_t{15} * target_len / 16}) {
          size_t maybe_len_with_metadata =
              RoundDownUsableSpace(maybe_len_rough + kMetadataLen);
          double maybe_fp_rate =
              EstimatedFpRate(num_entries, maybe_len_with_metadata);
          if (maybe_fp_rate <= for_balance_fp_rate) {
            rv = maybe_len_with_metadata;
            rv_fp_rate = maybe_fp_rate;
            break;
          }
        }
      }

      // Filter blocks are loaded into block cache with their block trailer.
      // We need to make sure that's accounted for in choosing a
      // fragmentation-friendly size.
      const size_t kExtraPadding = kBlockTrailerSize;
      size_t requested = rv + kExtraPadding;

      // Allocate and get usable size
      buf->reset(new char[requested]);
      size_t usable = malloc_usable_size(buf->get());

      if (usable - usable / 4 > requested) {
        // Ratio greater than 4/3 is too much for utilizing, if it's
        // not a buggy or mislinked malloc_usable_size implementation.
        // Non-linearity of FP rates with bits/key means rapidly
        // diminishing returns in overall accuracy for additional
        // storage on disk.
        // Nothing to do, except assert that the result is accurate about
        // the usable size. (Assignment never used.)
        assert(((*buf)[usable - 1] = 'x'));
      } else if (usable > requested) {
        rv = RoundDownUsableSpace(usable - kExtraPadding);
        assert(rv <= usable - kExtraPadding);
        rv_fp_rate = EstimatedFpRate(num_entries, rv);
      } else {
        // Too small means bad malloc_usable_size
        assert(usable == requested);
      }
      memset(buf->get(), 0, rv);

      // Update balance
      int64_t diff = static_cast<int64_t>((rv_fp_rate - target_fp_rate) *
                                          double{0x100000000});
      *aggregate_rounding_balance_ += diff;
    } else {
      buf->reset(new char[rv]());
    }
#else
    (void)num_entries;
    buf->reset(new char[rv]());
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return rv;
  }

  // A deque avoids unnecessary copying of already-saved values
  // and has near-minimal peak memory use.
  std::deque<uint64_t> hash_entries_;

  // See BloomFilterPolicy::aggregate_rounding_balance_. If nullptr,
  // always "round up" like historic behavior.
  std::atomic<int64_t>* aggregate_rounding_balance_;
};

// #################### FastLocalBloom implementation ################## //
// ############## also known as format_version=5 Bloom filter ########## //

// See description in FastLocalBloomImpl
class FastLocalBloomBitsBuilder : public XXH3pFilterBitsBuilder {
 public:
  // Non-null aggregate_rounding_balance implies optimize_filters_for_memory
  explicit FastLocalBloomBitsBuilder(
      const int millibits_per_key,
      std::atomic<int64_t>* aggregate_rounding_balance)
      : XXH3pFilterBitsBuilder(aggregate_rounding_balance),
        millibits_per_key_(millibits_per_key) {
    assert(millibits_per_key >= 1000);
  }

  // No Copy allowed
  FastLocalBloomBitsBuilder(const FastLocalBloomBitsBuilder&) = delete;
  void operator=(const FastLocalBloomBitsBuilder&) = delete;

  ~FastLocalBloomBitsBuilder() override {}

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
    size_t num_entries = hash_entries_.size();
    size_t len_with_metadata = CalculateSpace(num_entries);

    std::unique_ptr<char[]> mutable_buf;
    len_with_metadata =
        AllocateMaybeRounding(len_with_metadata, num_entries, &mutable_buf);

    assert(mutable_buf);
    assert(len_with_metadata >= kMetadataLen);

    // Max size supported by implementation
    assert(len_with_metadata <= 0xffffffffU);

    // Compute num_probes after any rounding / adjustments
    int num_probes = GetNumProbes(num_entries, len_with_metadata);

    uint32_t len = static_cast<uint32_t>(len_with_metadata - kMetadataLen);
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

  size_t ApproximateNumEntries(size_t bytes) override {
    size_t bytes_no_meta =
        bytes >= kMetadataLen ? RoundDownUsableSpace(bytes) - kMetadataLen : 0;
    return static_cast<size_t>(uint64_t{8000} * bytes_no_meta /
                               millibits_per_key_);
  }

  size_t CalculateSpace(size_t num_entries) override {
    // If not for cache line blocks in the filter, what would the target
    // length in bytes be?
    size_t raw_target_len = static_cast<size_t>(
        (uint64_t{num_entries} * millibits_per_key_ + 7999) / 8000);

    if (raw_target_len >= size_t{0xffffffc0}) {
      // Max supported for this data structure implementation
      raw_target_len = size_t{0xffffffc0};
    }

    // Round up to nearest multiple of 64 (block size). This adjustment is
    // used for target FP rate only so that we don't receive complaints about
    // lower FP rate vs. historic Bloom filter behavior.
    return ((raw_target_len + 63) & ~size_t{63}) + kMetadataLen;
  }

  double EstimatedFpRate(size_t keys, size_t len_with_metadata) override {
    int num_probes = GetNumProbes(keys, len_with_metadata);
    return FastLocalBloomImpl::EstimatedFpRate(
        keys, len_with_metadata - kMetadataLen, num_probes, /*hash bits*/ 64);
  }

 protected:
  size_t RoundDownUsableSpace(size_t available_size) override {
    size_t rv = available_size - kMetadataLen;

    if (rv >= size_t{0xffffffc0}) {
      // Max supported for this data structure implementation
      rv = size_t{0xffffffc0};
    }

    // round down to multiple of 64 (block size)
    rv &= ~size_t{63};

    return rv + kMetadataLen;
  }

 private:
  // Compute num_probes after any rounding / adjustments
  int GetNumProbes(size_t keys, size_t len_with_metadata) {
    uint64_t millibits = uint64_t{len_with_metadata - kMetadataLen} * 8000;
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

// ##################### Ribbon filter implementation ################### //

// Implements concept RehasherTypesAndSettings in ribbon_impl.h
struct Standard128RibbonRehasherTypesAndSettings {
  // These are schema-critical. Any change almost certainly changes
  // underlying data.
  static constexpr bool kIsFilter = true;
  static constexpr bool kHomogeneous = false;
  static constexpr bool kFirstCoeffAlwaysOne = true;
  static constexpr bool kUseSmash = false;
  using CoeffRow = ROCKSDB_NAMESPACE::Unsigned128;
  using Hash = uint64_t;
  using Seed = uint32_t;
  // Changing these doesn't necessarily change underlying data,
  // but might affect supported scalability of those dimensions.
  using Index = uint32_t;
  using ResultRow = uint32_t;
  // Save a conditional in Ribbon queries
  static constexpr bool kAllowZeroStarts = false;
};

using Standard128RibbonTypesAndSettings =
    ribbon::StandardRehasherAdapter<Standard128RibbonRehasherTypesAndSettings>;

class Standard128RibbonBitsBuilder : public XXH3pFilterBitsBuilder {
 public:
  explicit Standard128RibbonBitsBuilder(
      double desired_one_in_fp_rate, int bloom_millibits_per_key,
      std::atomic<int64_t>* aggregate_rounding_balance, Logger* info_log)
      : XXH3pFilterBitsBuilder(aggregate_rounding_balance),
        desired_one_in_fp_rate_(desired_one_in_fp_rate),
        info_log_(info_log),
        bloom_fallback_(bloom_millibits_per_key, aggregate_rounding_balance) {
    assert(desired_one_in_fp_rate >= 1.0);
  }

  // No Copy allowed
  Standard128RibbonBitsBuilder(const Standard128RibbonBitsBuilder&) = delete;
  void operator=(const Standard128RibbonBitsBuilder&) = delete;

  ~Standard128RibbonBitsBuilder() override {}

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
    if (hash_entries_.size() > kMaxRibbonEntries) {
      ROCKS_LOG_WARN(info_log_, "Too many keys for Ribbon filter: %llu",
                     static_cast<unsigned long long>(hash_entries_.size()));
      SwapEntriesWith(&bloom_fallback_);
      assert(hash_entries_.empty());
      return bloom_fallback_.Finish(buf);
    }
    if (hash_entries_.size() == 0) {
      // Save a conditional in Ribbon queries by using alternate reader
      // for zero entries added.
      return FinishAlwaysFalse(buf);
    }
    uint32_t num_entries = static_cast<uint32_t>(hash_entries_.size());
    uint32_t num_slots;
    size_t len_with_metadata;

    CalculateSpaceAndSlots(num_entries, &len_with_metadata, &num_slots);

    // Bloom fall-back indicator
    if (num_slots == 0) {
      SwapEntriesWith(&bloom_fallback_);
      assert(hash_entries_.empty());
      return bloom_fallback_.Finish(buf);
    }

    uint32_t entropy = 0;
    if (!hash_entries_.empty()) {
      entropy = Lower32of64(hash_entries_.front());
    }

    BandingType banding;
    bool success = banding.ResetAndFindSeedToSolve(
        num_slots, hash_entries_.begin(), hash_entries_.end(),
        /*starting seed*/ entropy & 255, /*seed mask*/ 255);
    if (!success) {
      ROCKS_LOG_WARN(info_log_,
                     "Too many re-seeds (256) for Ribbon filter, %llu / %llu",
                     static_cast<unsigned long long>(hash_entries_.size()),
                     static_cast<unsigned long long>(num_slots));
      SwapEntriesWith(&bloom_fallback_);
      assert(hash_entries_.empty());
      return bloom_fallback_.Finish(buf);
    }
    hash_entries_.clear();

    uint32_t seed = banding.GetOrdinalSeed();
    assert(seed < 256);

    std::unique_ptr<char[]> mutable_buf;
    len_with_metadata =
        AllocateMaybeRounding(len_with_metadata, num_entries, &mutable_buf);

    SolnType soln(mutable_buf.get(), len_with_metadata);
    soln.BackSubstFrom(banding);
    uint32_t num_blocks = soln.GetNumBlocks();
    // This should be guaranteed:
    // num_entries < 2^30
    // => (overhead_factor < 2.0)
    // num_entries * overhead_factor == num_slots < 2^31
    // => (num_blocks = num_slots / 128)
    // num_blocks < 2^24
    assert(num_blocks < 0x1000000U);

    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -2 = Marker for Standard128 Ribbon
    mutable_buf[len_with_metadata - 5] = static_cast<char>(-2);
    // Hash seed
    mutable_buf[len_with_metadata - 4] = static_cast<char>(seed);
    // Number of blocks, in 24 bits
    // (Along with bytes, we can derive other settings)
    mutable_buf[len_with_metadata - 3] = static_cast<char>(num_blocks & 255);
    mutable_buf[len_with_metadata - 2] =
        static_cast<char>((num_blocks >> 8) & 255);
    mutable_buf[len_with_metadata - 1] =
        static_cast<char>((num_blocks >> 16) & 255);

    Slice rv(mutable_buf.get(), len_with_metadata);
    *buf = std::move(mutable_buf);
    return rv;
  }

  // Setting num_slots to 0 means "fall back on Bloom filter."
  // And note this implementation does not support num_entries or num_slots
  // beyond uint32_t; see kMaxRibbonEntries.
  void CalculateSpaceAndSlots(size_t num_entries,
                              size_t* target_len_with_metadata,
                              uint32_t* num_slots) {
    if (num_entries > kMaxRibbonEntries) {
      // More entries than supported by this Ribbon
      *num_slots = 0;  // use Bloom
      *target_len_with_metadata = bloom_fallback_.CalculateSpace(num_entries);
      return;
    }
    uint32_t entropy = 0;
    if (!hash_entries_.empty()) {
      entropy = Upper32of64(hash_entries_.front());
    }

    *num_slots = NumEntriesToNumSlots(static_cast<uint32_t>(num_entries));
    *target_len_with_metadata =
        SolnType::GetBytesForOneInFpRate(*num_slots, desired_one_in_fp_rate_,
                                         /*rounding*/ entropy) +
        kMetadataLen;

    // Consider possible Bloom fallback for small filters
    if (*num_slots < 1024) {
      size_t bloom = bloom_fallback_.CalculateSpace(num_entries);
      if (bloom < *target_len_with_metadata) {
        *num_slots = 0;  // use Bloom
        *target_len_with_metadata = bloom;
        return;
      }
    }
  }

  size_t CalculateSpace(size_t num_entries) override {
    if (num_entries == 0) {
      // See FinishAlwaysFalse
      return 0;
    }
    size_t target_len_with_metadata;
    uint32_t num_slots;
    CalculateSpaceAndSlots(num_entries, &target_len_with_metadata, &num_slots);
    (void)num_slots;
    return target_len_with_metadata;
  }

  // This is a somewhat ugly but reasonably fast and reasonably accurate
  // reversal of CalculateSpace.
  size_t ApproximateNumEntries(size_t bytes) override {
    size_t len_no_metadata =
        RoundDownUsableSpace(std::max(bytes, size_t{kMetadataLen})) -
        kMetadataLen;

    if (!(desired_one_in_fp_rate_ > 1.0)) {
      // Effectively asking for 100% FP rate, or NaN etc.
      // Note that NaN is neither < 1.0 nor > 1.0
      return kMaxRibbonEntries;
    }

    // Find a slight under-estimate for actual average bits per slot
    double min_real_bits_per_slot;
    if (desired_one_in_fp_rate_ >= 1.0 + std::numeric_limits<uint32_t>::max()) {
      // Max of 32 solution columns (result bits)
      min_real_bits_per_slot = 32.0;
    } else {
      // Account for mix of b and b+1 solution columns being slightly
      // suboptimal vs. ideal log2(1/fp_rate) bits.
      uint32_t rounded = static_cast<uint32_t>(desired_one_in_fp_rate_);
      int upper_bits_per_key = 1 + FloorLog2(rounded);
      double fp_rate_for_upper = std::pow(2.0, -upper_bits_per_key);
      double portion_lower =
          (1.0 / desired_one_in_fp_rate_ - fp_rate_for_upper) /
          fp_rate_for_upper;
      min_real_bits_per_slot = upper_bits_per_key - portion_lower;
      assert(min_real_bits_per_slot > 0.0);
      assert(min_real_bits_per_slot <= 32.0);
    }

    // An overestimate, but this should only be O(1) slots away from truth.
    double max_slots = len_no_metadata * 8.0 / min_real_bits_per_slot;

    // Let's not bother accounting for overflow to Bloom filter
    // (Includes NaN case)
    if (!(max_slots < ConfigHelper::GetNumSlots(kMaxRibbonEntries))) {
      return kMaxRibbonEntries;
    }

    // Set up for short iteration
    uint32_t slots = static_cast<uint32_t>(max_slots);
    slots = SolnType::RoundUpNumSlots(slots);

    // Assert that we have a valid upper bound on slots
    assert(SolnType::GetBytesForOneInFpRate(
               SolnType::RoundUpNumSlots(slots + 1), desired_one_in_fp_rate_,
               /*rounding*/ 0) > len_no_metadata);

    // Iterate up to a few times to rather precisely account for small effects
    for (int i = 0; slots > 0; ++i) {
      size_t reqd_bytes =
          SolnType::GetBytesForOneInFpRate(slots, desired_one_in_fp_rate_,
                                           /*rounding*/ 0);
      if (reqd_bytes <= len_no_metadata) {
        break;  // done
      }
      if (i >= 2) {
        // should have been enough iterations
        assert(false);
        break;
      }
      slots = SolnType::RoundDownNumSlots(slots - 1);
    }

    uint32_t num_entries = ConfigHelper::GetNumToAdd(slots);

    // Consider possible Bloom fallback for small filters
    if (slots < 1024) {
      size_t bloom = bloom_fallback_.ApproximateNumEntries(bytes);
      if (bloom > num_entries) {
        return bloom;
      } else {
        return num_entries;
      }
    } else {
      return std::min(num_entries, kMaxRibbonEntries);
    }
  }

  double EstimatedFpRate(size_t num_entries,
                         size_t len_with_metadata) override {
    if (num_entries > kMaxRibbonEntries) {
      // More entries than supported by this Ribbon
      return bloom_fallback_.EstimatedFpRate(num_entries, len_with_metadata);
    }
    uint32_t num_slots =
        NumEntriesToNumSlots(static_cast<uint32_t>(num_entries));
    SolnType fake_soln(nullptr, len_with_metadata);
    fake_soln.ConfigureForNumSlots(num_slots);
    return fake_soln.ExpectedFpRate();
  }

 protected:
  size_t RoundDownUsableSpace(size_t available_size) override {
    size_t rv = available_size - kMetadataLen;

    // round down to multiple of 16 (segment size)
    rv &= ~size_t{15};

    return rv + kMetadataLen;
  }

 private:
  using TS = Standard128RibbonTypesAndSettings;
  using SolnType = ribbon::SerializableInterleavedSolution<TS>;
  using BandingType = ribbon::StandardBanding<TS>;
  using ConfigHelper = ribbon::BandingConfigHelper1TS<ribbon::kOneIn20, TS>;

  static uint32_t NumEntriesToNumSlots(uint32_t num_entries) {
    uint32_t num_slots1 = ConfigHelper::GetNumSlots(num_entries);
    return SolnType::RoundUpNumSlots(num_slots1);
  }

  // Approximate num_entries to ensure number of bytes fits in 32 bits, which
  // is not an inherent limitation but does ensure somewhat graceful Bloom
  // fallback for crazy high number of entries, since the Bloom implementation
  // does not support number of bytes bigger than fits in 32 bits. This is
  // within an order of magnitude of implementation limit on num_slots
  // fitting in 32 bits, and even closer for num_blocks fitting in 24 bits
  // (for filter metadata).
  static constexpr uint32_t kMaxRibbonEntries = 950000000;  // ~ 1 billion

  // A desired value for 1/fp_rate. For example, 100 -> 1% fp rate.
  double desired_one_in_fp_rate_;

  // For warnings, or can be nullptr
  Logger* info_log_;

  // For falling back on Bloom filter in some exceptional cases and
  // very small filter cases
  FastLocalBloomBitsBuilder bloom_fallback_;
};

// for the linker, at least with DEBUG_LEVEL=2
constexpr uint32_t Standard128RibbonBitsBuilder::kMaxRibbonEntries;

class Standard128RibbonBitsReader : public FilterBitsReader {
 public:
  Standard128RibbonBitsReader(const char* data, size_t len_bytes,
                              uint32_t num_blocks, uint32_t seed)
      : soln_(const_cast<char*>(data), len_bytes) {
    soln_.ConfigureForNumBlocks(num_blocks);
    hasher_.SetOrdinalSeed(seed);
  }

  // No Copy allowed
  Standard128RibbonBitsReader(const Standard128RibbonBitsReader&) = delete;
  void operator=(const Standard128RibbonBitsReader&) = delete;

  ~Standard128RibbonBitsReader() override {}

  bool MayMatch(const Slice& key) override {
    uint64_t h = GetSliceHash64(key);
    return soln_.FilterQuery(h, hasher_);
  }

  virtual void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    struct SavedData {
      uint64_t seeded_hash;
      uint32_t segment_num;
      uint32_t num_columns;
      uint32_t start_bits;
    };
    std::array<SavedData, MultiGetContext::MAX_BATCH_SIZE> saved;
    for (int i = 0; i < num_keys; ++i) {
      ribbon::InterleavedPrepareQuery(
          GetSliceHash64(*keys[i]), hasher_, soln_, &saved[i].seeded_hash,
          &saved[i].segment_num, &saved[i].num_columns, &saved[i].start_bits);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = ribbon::InterleavedFilterQuery(
          saved[i].seeded_hash, saved[i].segment_num, saved[i].num_columns,
          saved[i].start_bits, hasher_, soln_);
    }
  }

 private:
  using TS = Standard128RibbonTypesAndSettings;
  ribbon::SerializableInterleavedSolution<TS> soln_;
  ribbon::StandardHasher<TS> hasher_;
};

// ##################### Legacy Bloom implementation ################### //

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

  size_t CalculateSpace(size_t num_entries) override {
    uint32_t dont_care1;
    uint32_t dont_care2;
    return CalculateSpace(num_entries, &dont_care1, &dont_care2);
  }

  double EstimatedFpRate(size_t keys, size_t bytes) override {
    return LegacyBloomImpl::EstimatedFpRate(keys, bytes - kMetadataLen,
                                            num_probes_);
  }

  size_t ApproximateNumEntries(size_t bytes) override;

 private:
  int bits_per_key_;
  int num_probes_;
  std::vector<uint32_t> hash_entries_;
  Logger* info_log_;

  // Get totalbits that optimized for cpu cache line
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);

  // Reserve space for new filter
  char* ReserveSpace(size_t num_entries, uint32_t* total_bits,
                     uint32_t* num_lines);

  // Implementation-specific variant of public CalculateSpace
  uint32_t CalculateSpace(size_t num_entries, uint32_t* total_bits,
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

  return Slice(data, total_bits / 8 + kMetadataLen);
}

size_t LegacyBloomBitsBuilder::ApproximateNumEntries(size_t bytes) {
  assert(bits_per_key_);
  assert(bytes > 0);

  uint64_t total_bits_tmp = bytes * 8;
  // total bits, including temporary computations, cannot exceed 2^32
  // for compatibility
  total_bits_tmp = std::min(total_bits_tmp, uint64_t{0xffff0000});

  uint32_t high = static_cast<uint32_t>(total_bits_tmp) /
                      static_cast<uint32_t>(bits_per_key_) +
                  1;
  uint32_t low = 1;
  uint32_t n = high;
  for (; n >= low; n--) {
    if (CalculateSpace(n) <= bytes) {
      break;
    }
  }
  return n;
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

uint32_t LegacyBloomBitsBuilder::CalculateSpace(size_t num_entries,
                                                uint32_t* total_bits,
                                                uint32_t* num_lines) {
  assert(bits_per_key_);
  if (num_entries != 0) {
    size_t total_bits_tmp = num_entries * bits_per_key_;
    // total bits, including temporary computations, cannot exceed 2^32
    // for compatibility
    total_bits_tmp = std::min(total_bits_tmp, size_t{0xffff0000});

    *total_bits =
        GetTotalBitsForLocality(static_cast<uint32_t>(total_bits_tmp));
    *num_lines = *total_bits / (CACHE_LINE_SIZE * 8);
    assert(*total_bits > 0 && *total_bits % 8 == 0);
  } else {
    // filter is empty, just leave space for metadata
    *total_bits = 0;
    *num_lines = 0;
  }

  // Reserve space for Filter
  uint32_t sz = *total_bits / 8;
  sz += kMetadataLen;  // 4 bytes for num_lines, 1 byte for num_probes
  return sz;
}

char* LegacyBloomBitsBuilder::ReserveSpace(size_t num_entries,
                                           uint32_t* total_bits,
                                           uint32_t* num_lines) {
  uint32_t sz = CalculateSpace(num_entries, total_bits, num_lines);
  char* data = new char[sz];
  memset(data, 0, sz);
  return data;
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
    kStandard128Ribbon,
};

const std::vector<BloomFilterPolicy::Mode> BloomFilterPolicy::kAllUserModes = {
    kDeprecatedBlock,
    kAutoBloom,
    kStandard128Ribbon,
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

  // For now configure Ribbon filter to match Bloom FP rate and save
  // memory. (Ribbon bits per key will be ~30% less than Bloom bits per key
  // for same FP rate.)
  desired_one_in_fp_rate_ =
      1.0 / BloomMath::CacheLocalFpRate(
                bits_per_key,
                FastLocalBloomImpl::ChooseNumProbes(millibits_per_key_),
                /*cache_line_bits*/ 512);

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
      case kAutoBloom:
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
      case kStandard128Ribbon:
        return new Standard128RibbonBitsBuilder(
            desired_one_in_fp_rate_, millibits_per_key_,
            offm ? &aggregate_rounding_balance_ : nullptr, context.info_log);
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
  if (len_with_meta <= kMetadataLen) {
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
      static_cast<int8_t>(contents.data()[len_with_meta - kMetadataLen]);
  // NB: *num_probes > 30 and < 128 probably have not been used, because of
  // BloomFilterPolicy::initialize, unless directly calling
  // LegacyBloomBitsBuilder as an API, but we are leaving those cases in
  // limbo with LegacyBloomBitsReader for now.

  if (raw_num_probes < 1) {
    // Note: < 0 (or unsigned > 127) indicate special new implementations
    // (or reserved for future use)
    switch (raw_num_probes) {
      case 0:
        // Treat as zero probes (always FP)
        return new AlwaysTrueFilter();
      case -1:
        // Marker for newer Bloom implementations
        return GetBloomBitsReader(contents);
      case -2:
        // Marker for Ribbon implementations
        return GetRibbonBitsReader(contents);
      default:
        // Reserved (treat as zero probes, always FP, for now)
        return new AlwaysTrueFilter();
    }
  }
  // else attempt decode for LegacyBloomBitsReader

  int num_probes = raw_num_probes;
  assert(num_probes >= 1);
  assert(num_probes <= 127);

  uint32_t len = len_with_meta - kMetadataLen;
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

FilterBitsReader* BloomFilterPolicy::GetRibbonBitsReader(
    const Slice& contents) const {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  uint32_t len = len_with_meta - kMetadataLen;

  assert(len > 0);  // precondition

  uint32_t seed = static_cast<uint8_t>(contents.data()[len + 1]);
  uint32_t num_blocks = static_cast<uint8_t>(contents.data()[len + 2]);
  num_blocks |= static_cast<uint8_t>(contents.data()[len + 3]) << 8;
  num_blocks |= static_cast<uint8_t>(contents.data()[len + 4]) << 16;
  if (num_blocks < 2) {
    // Not supported
    // num_blocks == 1 is not used because num_starts == 1 is problematic
    // for the hashing scheme. num_blocks == 0 is unused because there's
    // already a concise encoding of an "always false" filter.
    // Return something safe:
    return new AlwaysTrueFilter();
  }
  return new Standard128RibbonBitsReader(contents.data(), len, num_blocks,
                                         seed);
}

// For newer Bloom filter implementations
FilterBitsReader* BloomFilterPolicy::GetBloomBitsReader(
    const Slice& contents) const {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  uint32_t len = len_with_meta - kMetadataLen;

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
    m = BloomFilterPolicy::kAutoBloom;
  }
  assert(std::find(BloomFilterPolicy::kAllUserModes.begin(),
                   BloomFilterPolicy::kAllUserModes.end(),
                   m) != BloomFilterPolicy::kAllUserModes.end());
  return new BloomFilterPolicy(bits_per_key, m);
}

extern const FilterPolicy* NewExperimentalRibbonFilterPolicy(
    double bloom_equivalent_bits_per_key) {
  return new BloomFilterPolicy(bloom_equivalent_bits_per_key,
                               BloomFilterPolicy::kStandard128Ribbon);
}

FilterBuildingContext::FilterBuildingContext(
    const BlockBasedTableOptions& _table_options)
    : table_options(_table_options) {}

FilterPolicy::~FilterPolicy() { }

Status FilterPolicy::CreateFromString(
    const ConfigOptions& /*options*/, const std::string& value,
    std::shared_ptr<const FilterPolicy>* policy) {
  const std::string kBloomName = "bloomfilter:";
  const std::string kExpRibbonName = "experimental_ribbon:";
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
  } else if (value.compare(0, kExpRibbonName.size(), kExpRibbonName) == 0) {
    double bloom_equivalent_bits_per_key =
        ParseDouble(trim(value.substr(kExpRibbonName.size())));
    policy->reset(
        NewExperimentalRibbonFilterPolicy(bloom_equivalent_bits_per_key));
  } else {
    return Status::NotFound("Invalid filter policy name ", value);
#else
  } else {
    return Status::NotSupported("Cannot load filter policy in LITE mode ",
                                value);
#endif  // ROCKSDB_LITE
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
