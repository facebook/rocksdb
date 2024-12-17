#ifndef FAST_SNARF_BITSET_HPP
#define FAST_SNARF_BITSET_HPP

#include <cassert>
#include <cstdint>
#include <cstring>
#include <vector>

#include "rocksdb/range_delete_filter/fast_snarf/fast_snarf_util.hpp"

namespace fast_snarf {

class BitSet {
 public:
  BitSet(const std::vector<uint32_t> &keys, uint32_t max_range);
  BitSet(size_t nkeys, uint32_t max_range, uint8_t *data);

  ~BitSet();

  auto query(uint32_t query) -> bool;
  /* [left, right] */
  auto query(uint32_t left, uint32_t right) -> bool;

  auto size() const -> size_t;

  auto serialize() const -> std::pair<uint8_t *, size_t>;
  static auto deserialzie(uint8_t *ser, uint32_t max_range) -> BitSet *;

 private:
  inline static auto get_lower_bit_length(uint32_t nkeys, uint32_t max_range)
      -> uint32_t;

  inline static auto bit_array_bsize(uint32_t nkeys, uint32_t max_range,
                                     uint32_t lower_bit_len) -> size_t;

  inline auto get_lower_bit_length(uint32_t max_range) -> uint32_t;

  inline auto bit_array_bsize(uint32_t max_range) -> size_t;

  inline auto find_next_set_bit(size_t idx) -> size_t;

 private:
  uint32_t lower_bit_len_;
  size_t b_size_;

  // need to be serialized
  size_t nkeys_;
  uint8_t *data_;
};

BitSet::BitSet(size_t nkeys, uint32_t max_range, uint8_t *data)
    : nkeys_(nkeys) {
  lower_bit_len_ = get_lower_bit_length(max_range);
  b_size_ = bit_array_bsize(max_range);
  size_t data_size = align_bit_to_byte(b_size_);
  data_ = new uint8_t[data_size];
  memcpy(data_, data, data_size * sizeof(uint8_t));
}

BitSet::BitSet(const std::vector<uint32_t> &keys, uint32_t max_range)
    : nkeys_(keys.size()) {
  assert(nkeys_ > 0);

  lower_bit_len_ = get_lower_bit_length(max_range);
  b_size_ = bit_array_bsize(max_range);

  size_t data_size = align_bit_to_byte(b_size_);
  data_ = new uint8_t[data_size];
  memset(data_, 0, data_size * sizeof(uint8_t));

  size_t low_idx = 0;
  size_t up_idx = lower_bit_len_ * nkeys_;

  uint32_t pre_upper = 0;
  for (size_t i = 0; i < nkeys_; ++i) {
    uint32_t lower_key = keys[i] & ((1 << lower_bit_len_) - 1);
    uint32_t bit_cnt = 0;
    while (bit_cnt < lower_bit_len_) {
      uint32_t byte_bias = low_idx & 0x7ULL;
      uint32_t delta = min((8 - byte_bias), lower_bit_len_ - bit_cnt);
      uint32_t tmp_mask = (1 << delta) - 1;
      data_[low_idx >> 3] |= (lower_key & tmp_mask) << byte_bias;
      low_idx += delta;
      bit_cnt += delta;
      lower_key >>= delta;
    }

    while (pre_upper < (keys[i] >> lower_bit_len_)) {
      data_[up_idx >> 3] |= 1 << (up_idx & 0x7ULL);
      ++pre_upper;
      ++up_idx;
    }
    ++up_idx;
  }
  data_[up_idx >> 3] |= 1 << (up_idx & 0x7ULL);
}

BitSet::~BitSet() { delete[] data_; }

auto BitSet::query(uint32_t query) -> bool {
  size_t up_idx = lower_bit_len_ * nkeys_;
  uint32_t up_query = query >> lower_bit_len_;

  // upper bit search
  size_t zero_cnt = 0;
  size_t range = 0;
  uint32_t upper = 0;
  while (upper <= up_query && up_idx < b_size_) {
    if (data_[up_idx >> 3] & (1U << (up_idx & 0x7ULL))) {
      ++up_idx;
      ++upper;
    } else {
      size_t next_idx = find_next_set_bit(up_idx);
      size_t delta = next_idx - up_idx;
      if (upper == up_query) {
        range = delta;
        break;
      }
      zero_cnt += delta;
      up_idx = next_idx;
    }
  }

  if (range == 0) {
    return false;
  } else if (lower_bit_len_ == 0) {
    return true;
  }

  // lower bit search
  uint32_t low_query = query & ((1 << lower_bit_len_) - 1);
  size_t low_idx = zero_cnt * lower_bit_len_;
  while (low_idx < (zero_cnt + range) * lower_bit_len_) {
    uint32_t low_key = 0;
    uint32_t bit_cnt = 0;
    while (bit_cnt < lower_bit_len_) {
      uint32_t byte_bias = low_idx & 0x7ULL;
      uint32_t delta = min((8 - byte_bias), lower_bit_len_ - bit_cnt);
      uint32_t tmp_mask = (1 << delta) - 1;
      uint32_t cur_data = (data_[low_idx >> 3] >> byte_bias) & tmp_mask;
      low_key |= cur_data << bit_cnt;
      bit_cnt += delta;
      low_idx += delta;
    }
    if (low_key >= low_query) {
      return low_key == low_query;
    }
  }
  return false;
}

auto BitSet::query(uint32_t left, uint32_t right) -> bool {
  size_t up_idx = lower_bit_len_ * nkeys_;
  uint32_t up_left = left >> lower_bit_len_;
  uint32_t up_right = right >> lower_bit_len_;

  // upper bit search
  size_t zero_cnt = 0;
  size_t range = 0;
  uint32_t upper = 0;
  std::vector<std::pair<uint32_t, size_t>> upper_keys;
  while (upper <= up_right && up_idx < b_size_) {
    if (data_[up_idx >> 3] & (1U << (up_idx & 0x7ULL))) {
      ++up_idx;
      ++upper;
    } else {
      size_t next_idx = find_next_set_bit(up_idx);
      size_t delta = next_idx - up_idx;
      if (upper >= up_left && upper <= up_right) {
        if (range > 0 && upper < up_right) {
          return true;
        }

        upper_keys.emplace_back(upper << lower_bit_len_, delta);
        range += delta;
      }
      zero_cnt += range > 0 ? 0 : delta;
      up_idx = next_idx;
    }
  }

  if (range == 0) {
    return false;
  } else if (lower_bit_len_ == 0) {
    return true;
  }

  size_t key_begin_idx = zero_cnt;
  size_t low_idx = key_begin_idx * lower_bit_len_;
  for (const auto &[up_key, n] : upper_keys) {
    key_begin_idx += n;
    while (low_idx < key_begin_idx * lower_bit_len_) {
      uint32_t low_key = 0;
      uint32_t bit_cnt = 0;
      while (bit_cnt < lower_bit_len_) {
        uint32_t byte_bias = low_idx & 0x7ULL;
        uint32_t delta = min((8 - byte_bias), lower_bit_len_ - bit_cnt);
        uint32_t tmp_mask = (1 << delta) - 1;
        uint32_t cur_data = (data_[low_idx >> 3] >> byte_bias) & tmp_mask;
        low_key |= cur_data << bit_cnt;
        bit_cnt += delta;
        low_idx += delta;
      }
      uint32_t key = low_key | up_key;
      if (key >= left && key <= right) {
        return true;
      }
      if (key > right) {
        return false;
      }
    }
  }
  return false;
}

auto BitSet::size() const -> size_t {
  return sizeof(size_t)                 // nkeys_
         + align_bit_to_byte(b_size_);  // data_ size
}

auto BitSet::serialize() const -> std::pair<uint8_t *, size_t> {
  size_t data_size = align_bit_to_byte(b_size_);
  size_t size = sizeof(size_t)  // nkeys_
                + data_size;    // data_ size

  uint8_t *ser = new uint8_t[size];
  uint8_t *pos = ser;

  memcpy(pos, &nkeys_, sizeof(size_t));
  pos += sizeof(size_t);

  memcpy(pos, data_, data_size);

  return {ser, size};
}

auto BitSet::deserialzie(uint8_t *ser, uint32_t max_range) -> BitSet * {
  size_t nkeys;
  memcpy(&nkeys, ser, sizeof(size_t));
  ser += sizeof(size_t);

  return (new BitSet(nkeys, max_range, ser));
}

/** Helping Method */
auto BitSet::get_lower_bit_length(uint32_t nkeys, uint32_t max_range)
    -> uint32_t {
  return __builtin_clzl(nkeys) > __builtin_clzl(max_range)
             ? __builtin_clzl(nkeys) - __builtin_clzl(max_range)
             : 0;
}

auto BitSet::get_lower_bit_length(uint32_t max_range) -> uint32_t {
  return get_lower_bit_length(nkeys_, max_range);
}

auto BitSet::bit_array_bsize(uint32_t nkeys, uint32_t max_range,
                             uint32_t lower_bit_len) -> size_t {
  uint32_t total_quotient = max_range >> lower_bit_len;
  return nkeys * (1U + lower_bit_len) + total_quotient + 1;
}

auto BitSet::bit_array_bsize(uint32_t max_range) -> size_t {
  return bit_array_bsize(nkeys_, max_range, lower_bit_len_);
}

auto BitSet::find_next_set_bit(size_t idx) -> size_t {
  if (!(data_[idx >> 3] >> (idx & 0x7U))) {
    idx = (idx & (~static_cast<size_t>(0x7U))) + 8U;
  }

  while (data_[idx >> 3] == 0) {
    idx += 8U;
  }

  idx += __builtin_ctz(data_[idx >> 3] >> (idx & 0x7U));
  return idx;
}

}  // namespace fast_snarf

#endif