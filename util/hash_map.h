//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <algorithm>
#include <array>
#include <utility>

#include "port/likely.h"
#include "util/autovector.h"

#if defined(__GNUC__) || defined(__clang__)
#define ROCKSDB_FORCE_INLINE inline __attribute__((__always_inline__))
#elif _WIN32
#define ROCKSDB_FORCE_INLINE __forceinline
#endif

namespace ROCKSDB_NAMESPACE {

// This is similar to std::unordered_map, except that it tries to avoid
// allocating or deallocating memory as much as possible. With
// std::unordered_map, an allocation/deallocation is made for every insertion
// or deletion because of the requirement that iterators remain valid even
// with insertions or deletions. This means that the hash chains will be
// implemented as linked lists.
//
// This implementation uses autovector as hash chains insteads.
//
template <typename K, typename V, size_t size = 128>
class HashMap {
  std::array<autovector<std::pair<K, V>, 1>, size> table_;

 public:
  bool Contains(K key) {
    auto& bucket = table_[key % size];
    auto it = std::find_if(
        bucket.begin(), bucket.end(),
        [key](const std::pair<K, V>& p) { return p.first == key; });
    return it != bucket.end();
  }

  void Insert(K key, V value) {
    auto& bucket = table_[key % size];
    bucket.push_back({key, value});
  }

  void Delete(K key) {
    auto& bucket = table_[key % size];
    auto it = std::find_if(
        bucket.begin(), bucket.end(),
        [key](const std::pair<K, V>& p) { return p.first == key; });
    if (it != bucket.end()) {
      auto last = bucket.end() - 1;
      if (it != last) {
        *it = *last;
      }
      bucket.pop_back();
    }
  }

  V& Get(K key) {
    auto& bucket = table_[key % size];
    auto it = std::find_if(
        bucket.begin(), bucket.end(),
        [key](const std::pair<K, V>& p) { return p.first == key; });
    return it->second;
  }
};

// This is a hash map implementation heavily inspired by
// https://github.com/martinus/robin-hood-hashing
//
// Robinhood hashing is used, where metadata about the distance between the
// current slot and the desired slot is kept. On collisions during inserts, if
// the occupying item's distance is smaller than the inserted item's distance,
// then the inserted item takes over the slot, and the occupying item is
// reinserted.
//
// The scheme implies that all keys in the same bucket are always adjacent.
// eg. If max_size is 10, and the hash function is just k % 10:
//
// slot: 0  1  2  3  4  5  6  7  8  9
// key: 19 11 21 31 41 33    17     9
// dist: 1  0  1  2  3  2     0     0
//
// There is 1 byte of metadata for every slot in hash table. The top bit
// records whether the slot is empty or not, the next 4 bits hold distance,
// and the bottom 3 bits hold the top 3 bits of the hashed key to optimize for
// fewer key comparisons.
//
// If distance exceeds 4 bits, then rehash occurs.
//
// There are no tombstones for deletions, as elements are shifted back filling
// the 'hole'.
//
template <typename K, typename V, class Hash = std::hash<K>>
class HashMapRB {
 public:
  using key_type = K;
  using mapped_type = V;
  using value_type = std::pair<K, V>;
  using size_type = size_t;
  using difference_type = std::ptrdiff_t;

 private:
  static constexpr uint8_t get_dist(uint8_t x) { return ((x >> 3) & 15); }

  static constexpr uint8_t get_hashbits(uint8_t x) { return (x & 7); }

  static constexpr uint8_t set_info(uint8_t offset, uint8_t hashbits) {
    return ((1 << 7) | (offset << 3) | hashbits);
  }

  static constexpr uint8_t inc_dist(uint8_t x) { return x + (1 << 3); }

  static constexpr uint8_t dec_dist(uint8_t x) { return x - (1 << 3); }

  uint8_t* info_;
  value_type* values_;
  size_t max_size_;
  size_t mask_;
  size_t size_;
  size_t rehash_size_;
  Hash hash_fn;

 public:
  template <class THashMap, bool IsConst>
  class iterator_impl {
   public:
    using self_type = iterator_impl<THashMap, IsConst>;
    // -- iterator traits
    using difference_type = typename THashMap::difference_type;
    using value_type = typename THashMap::value_type;
    using pointer = typename std::conditional<IsConst, const value_type*,
                                              value_type*>::type;
    using reference = typename std::conditional<IsConst, const value_type&,
                                                value_type&>::type;
    using iterator_category = std::bidirectional_iterator_tag;

    iterator_impl(THashMap* map, size_t idx) : map_(map), index_(idx) {
      for (; index_ < map_->max_size_ && !map_->info_[index_]; index_++)
        ;
    }

    iterator_impl(const iterator_impl&) = default;
    ~iterator_impl() {}
    iterator_impl& operator=(const iterator_impl&) = default;

    // -- Advancement
    // ++iterator
    self_type& operator++() {
      index_++;
      for (; index_ < map_->max_size_ && !map_->info_[index_]; index_++)
        ;
      return *this;
    }

    // iterator++
    self_type operator++(int) {
      auto old = *this;
      ++*this;
      return old;
    }

    // --iterator
    self_type& operator--() {
      index_--;
      for (; index_ >= 0 && !map_->info_[index_]; index_--)
        ;
      return *this;
    }

    // iterator--
    self_type operator--(int) {
      auto old = *this;
      --*this;
      return old;
    }

    // -- Reference
    value_type& operator*() { return map_->values_[index_]; }

    const value_type& operator*() const { return map_->values_[index_]; }

    value_type* operator->() { return std::addressof(map_->values_[index_]); }

    const value_type* operator->() const {
      return std::addressof(map_->values_[index_]);
    }

    // -- Logical Operators
    bool operator==(const self_type& other) const {
      assert(map_ == other.map_);
      return index_ == other.index_;
    }

    bool operator!=(const self_type& other) const { return !(*this == other); }

    bool operator>(const self_type& other) const {
      assert(map_ == other.map_);
      return index_ > other.index_;
    }

    bool operator<(const self_type& other) const {
      assert(map_ == other.map_);
      return index_ < other.index_;
    }

    bool operator>=(const self_type& other) const {
      assert(map_ == other.map_);
      return !(other > *this);
    }

    bool operator<=(const self_type& other) const {
      assert(map_ == other.map_);
      return !(other < *this);
    }

    friend class HashMapRB;

   private:
    THashMap* map_ = nullptr;
    size_t index_ = 0;
  };

  typedef iterator_impl<HashMapRB, false> iterator;
  typedef iterator_impl<const HashMapRB, true> const_iterator;

  // -- Iterator Operations
  iterator begin() { return iterator(this, 0); }

  const_iterator begin() const { return const_iterator(this, 0); }

  iterator end() { return iterator(this, this->max_size_); }

  const_iterator end() const { return const_iterator(this, this->max_size_); }

  HashMapRB() {
    // Start with capacity 16.
    init(1 << 4);
  }

  ~HashMapRB() { destroy(); }

  HashMapRB(const HashMapRB& other) {
    init(other.max_size_);
    for (auto& it : other) {
      auto tmp = it;
      insert(std::move(tmp));
    }
  }

  HashMapRB& operator=(const HashMapRB& other) {
    if (&other != this) {
      this->~HashMapRB();
      new (this) HashMapRB(other);
    }
    return *this;
  }

  HashMapRB& operator=(HashMapRB&& other) {
    if (this != &other) {
      destroy();

      memcpy(this, &other, sizeof(*this));
      other.values_ = nullptr;
      other.info_ = nullptr;
    }

    return *this;
  }

  HashMapRB(HashMapRB&& other) {
    memcpy(this, &other, sizeof(*this));

    other.values_ = nullptr;
    other.info_ = nullptr;
  }

  size_t size() const { return size_; }

  void clear() {
    if (values_) {
      for (size_t pos = 0; pos < max_size_; pos++) {
        if (info_[pos] != 0) {
          values_[pos].~value_type();
        }
      }
    }

    free(values_);
    init(max_size_);
  }

  template <typename... Args>
  ROCKSDB_FORCE_INLINE std::pair<iterator, bool> emplace(Args&&... args) {
    return insert(std::make_pair(std::forward<Args>(args)...));
  }

  ROCKSDB_FORCE_INLINE std::pair<iterator, bool> insert(value_type&& value) {
    auto it = find(value.first);
    if (it != end()) {
      return std::make_pair(it, false);
    }

    if (size_ >= rehash_size_) {
      rehash(max_size_ << 1);
    }

    size_t h = hash_fn(value.first);
    size_t insert_pos = (size_t)-1;
    size_t pos = h & mask_;

    // Rehash until we get a short distances. This could loop infinitely if we
    // have a bad hash function.
    while (true) {
      pos = h & mask_;

      bool rehashed = false;
      // Check if we need to rehash due to distance exceeding 4 bits.
      for (pos = h & mask_;; pos = (pos + 1) & mask_) {
        if (!info_[pos]) {
          break;
        }
        if (get_dist(info_[pos]) == 15) {
          rehash(max_size_ << 1);
          rehashed = true;
          break;
        }
      }

      if (!rehashed) {
        break;
      }
    }

    pos = h & mask_;
    uint8_t info = set_info(0, h >> 61);
    // Displace elements
    while (true) {
      // If slot is empty, just insert value there.
      if (!info_[pos]) {
        if (insert_pos == (size_t)-1) insert_pos = pos;
        ::new ((void*)&values_[pos]) value_type(std::move(value));
        info_[pos] = info;
        break;
      }

      // If the occupying element has a smaller distance than the inserted
      // element, then swap the two elements, and continue with inserting the
      // displaced element.
      if (get_dist(info_[pos]) < get_dist(info)) {
        if (insert_pos == (size_t)-1) insert_pos = pos;

        std::swap(value, values_[pos]);
        std::swap(info, info_[pos]);
      }

      // Assert that distance will not overflow the 4 bits. We should have
      // checked in the first loop whether we need to rehash or not due to
      // distance exceeding 4 bits already.
      assert(get_dist(info) < 15);

      info = inc_dist(info);
      pos = (pos + 1) & mask_;
    }

    size_++;
    assert(insert_pos != (size_t)-1);
    check();
    return std::make_pair(iterator(this, insert_pos), true);
  }

  ROCKSDB_FORCE_INLINE mapped_type& operator[](key_type const& key) {
    auto it = find(key);
    if (it == end()) {
      return insert({key, mapped_type()}).first->second;
    }
    return it->second;
  }

  ROCKSDB_FORCE_INLINE size_type count(key_type const& key) const {
    return find(key) == end() ? 0 : 1;
  }

  ROCKSDB_FORCE_INLINE const_iterator find(const K& key) const {
    size_t h = hash_fn(key);
    size_t pos = h & mask_;
    uint8_t info = set_info(0, h >> 61);
    while (true) {
      if (info_[pos]) {
        if (get_dist(info_[pos]) < get_dist(info)) {
          break;
        }
        if (info_[pos] == info) {
          if (LIKELY(values_[pos].first == key)) {
            return const_iterator(this, pos);
          }
        }
      } else {
        break;
      }
      pos = (pos + 1) & mask_;
      assert(get_dist(info) < 15);
      info = inc_dist(info);
    }

    return end();
  }

  ROCKSDB_FORCE_INLINE iterator find(const K& key) {
    const_iterator it = const_cast<typename std::add_const<
        typename std::remove_pointer<decltype(this)>::type>::type&>(*this)
                            .find(key);
    return iterator(this, it.index_);
  }

  // TODO(mung): erase is supposed to return an iterator to the element after
  // the deleted element.
  ROCKSDB_FORCE_INLINE void erase(iterator it) {
    size_t pos = it.index_;
    assert(info_[pos] != 0);

    // We rely on the property that keys in the same bucket must be adjacent.
    // Keys are in the same bucket if their distances form an ascending
    // sequence.  To erase, find the last element of the current group, and
    // swap it into the erased spot. Do this for every group that can be
    // shifted down.
    //
    // eg. To erase '21' from:
    // slot: 0  1  2  3  4  5  6  7  8  9
    // key: 19 11 21 31 41 33    17     9
    // dist: 1  0  1  2  3  2     0     0
    //
    // (swap with 41) =>
    // slot: 0  1  2  3  4  5  6  7  8  9
    // key: 19 11 41 31 21 33    17     9
    // dist: 1  0  1  2     2     0     0
    //
    // (swap with 33) =>
    // slot: 0  1  2  3  4  5  6  7  8  9
    // key: 19 11 41 31 33 21    17     9
    // dist: 1  0  1  2  1        0     0
    //
    while (true) {
      size_t next_pos = (pos + 1) & mask_;
      size_t last_pos = pos;
      size_t dist = get_dist(info_[pos]) + 1;

      // Find the last element of the current group.
      for (; get_dist(info_[next_pos]) == dist;
           last_pos = next_pos, next_pos = (next_pos + 1) & mask_, dist++)
        ;

      assert(get_dist(info_[next_pos]) < dist);
      assert(((pos + (get_dist(info_[last_pos]) - get_dist(info_[pos]))) &
              mask_) == last_pos);

      // Swap the two values if the positions are different.
      if (last_pos != pos) {
        std::swap(values_[last_pos], values_[pos]);
        info_[pos] =
            set_info(get_dist(info_[pos]), get_hashbits(info_[last_pos]));
      }

      // If the next position is either empty, or has distance zero, then
      // we're done, so just destroy the erased element and break.
      if (get_dist(info_[next_pos]) == 0) {
        info_[last_pos] = 0;
        values_[last_pos].~value_type();
        break;
      }

      // Update info_ for the replaced element.
      info_[last_pos] = set_info(get_dist(info_[next_pos]) - 1, 0);
      pos = last_pos;
    }
    size_--;

    check();
  }

 private:
  void init(size_t num_buckets) {
    max_size_ = num_buckets;
    values_ = reinterpret_cast<decltype(values_)>(
        calloc(1, (sizeof(value_type) + 1) * max_size_));
    info_ = reinterpret_cast<uint8_t*>(values_ + max_size_);
    mask_ = max_size_ - 1;
    size_ = 0;
    rehash_size_ = static_cast<size_t>(0.9 * max_size_);
  }

  void rehash(size_t num_buckets) {
    size_t old_max_size_ = max_size_;
    uint8_t* old_info = info_;
    value_type* old_values_ = values_;

    init(num_buckets);

    for (size_t pos = 0; pos < old_max_size_; pos++) {
      if (old_info[pos]) {
        insert(std::move(old_values_[pos]));
        old_values_[pos].~value_type();
      }
    }

    free(old_values_);
    check();
  }

  void destroy() {
    if (values_) {
      for (size_t pos = 0; pos < max_size_; pos++) {
        if (info_[pos] != 0) {
          values_[pos].~value_type();
        }
      }
    }

    free(values_);
    values_ = nullptr;
    info_ = nullptr;
  }

  void check() const {
#ifndef NDEBUG
    // Check that info_ is consistent with size.
    size_t total = 0;
    for (size_t i = 0; i < max_size_; i++) {
      if (info_[i] != 0) {
        total++;
      }
    }
    assert(size_ == total);

    // Check that every element is searchable, and has correct distance.
    for (size_t i = 0; i < max_size_; i++) {
      if (info_[i] != 0) {
        auto it = find(values_[i].first);
        assert(it != end());
        assert(it.index_ == i);

        size_t pos = hash_fn(values_[i].first) & mask_;
        assert(((pos + get_dist(info_[i])) & mask_) == i);
      }
    }

    // Redo the checks with iterators
    total = 0;
    for (auto it = begin(); it != end(); it++) {
      total++;
    }
    assert(size_ == total);

    for (auto it = begin(); it != end(); it++) {
      size_t pos = hash_fn(it->first) & mask_;
      assert(((pos + get_dist(info_[it.index_])) & mask_) == it.index_);

      auto find_it = find(it->first);
      assert(find_it != end());
    }
#endif
  }
};

}  // namespace ROCKSDB_NAMESPACE
