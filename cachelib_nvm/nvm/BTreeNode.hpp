#pragma once

#include <cassert>
#include <cstdlib>
#include <cstring>

#include <algorithm>
#include <memory>
#include <utility>

#include "cachelib_nvm/common/CompilerUtils.hpp"

namespace facebook {
namespace cachelib {
namespace details {
template <typename T>
void insertAt(T* buf, size_t size, size_t pos, T what) {
  std::copy_backward(buf + pos, buf + size, buf + size + 1);
  buf[pos] = what;
}

template <typename T>
void removeAt(T* buf, size_t size, size_t pos) {
  std::copy(buf + pos + 1, buf + size, buf + pos);
}
}

enum class BTreeNodeType : uint32_t {
  Leaf = 0,
  Node = 1,
};

template <BTreeNodeType Type, typename Key, typename Value>
class CACHELIB_PACKED_ATTR BTreeNode {
 public:
  static constexpr auto kType{Type};

  static size_t allocSize(uint32_t capacity) {
    return sizeof(BTreeNode) + sizeof(Key) * capacity +
           sizeof(Value) * (capacity + extra());
  }

  static BTreeNode* create(uint32_t capacity) {
    return new (std::malloc(allocSize(capacity))) BTreeNode{capacity};
  }

  void destroy() {
    this->~BTreeNode();
    std::free(this);
  }

  uint32_t size() const { return size_; }
  uint32_t capacity() const { return capacity_; }

  Key keyAt(uint32_t i) const { return keys_[i]; }
  Key lastKey() const { return keys_[size_ - 1]; }

  uint32_t find(Key key) const {
    // constexpr uint32_t kStep{64 / sizeof(Key)};
    // uint32_t i = 0;
    // while (i + kStep < size_ && key >= keys_[i + kStep]) {
    //   i += kStep;
    // }
    // if (LIKELY(i + kStep < size_)) {
    //   while (keys_[i] < key) {
    //     i++;
    //   }
    // } else {
    //   while (i < size_ && keys_[i] < key) {
    //     i++;
    //   }
    // }
    // assert(i <= size_);
    // return i;
    assert(isSorted());
    return std::lower_bound(keys_, keys_ + size_, key,
                            [](auto k, auto e) { return k < e; }) -
           keys_;
  }

  void insert(uint32_t where, Key key, Value value) {
    assert(isSorted());
    details::insertAt(keys_, size_, where, key);
    details::insertAt(data_, size_ + extra(), where, value);
    size_++;
    assert(isSorted());
  }

  void remove(uint32_t where) {
    assert(isSorted());
    details::removeAt(keys_, size_, where);
    details::removeAt(data_, size_ + extra(), where);
    size_--;
    assert(isSorted());
  }

  void split(BTreeNode& out1, BTreeNode& out2, uint32_t where) {
    assert(out1.size_ == 0);
    assert(out2.size_ == 0);
    std::copy(keys_, keys_ + where, out1.keys_);
    std::copy(data_, data_ + where, out1.data_);
    out1.size_ = where;
    if (Type == BTreeNodeType::Node) {
      out1.data_[out1.size_] = {};
    }
    std::copy(keys_ + where, keys_ + size_, out2.keys_);
    std::copy(data_ + where, data_ + size_ + extra(), out2.data_);
    out2.size_ = size_ - where;
    assert(out1.size_ > 0);
    assert(out2.size_ > 0);
    assert(out1.isSorted());
    assert(out2.isSorted());
  }

  void merge(const BTreeNode& in1, const BTreeNode& in2) {
    std::copy(in1.keys_, in1.keys_ + in1.size_, keys_);
    std::copy(in1.data_, in1.data_ + in1.size_, data_);
    std::copy(in2.keys_, in2.keys_ + in2.size_, keys_ + in1.size_);
    std::copy(in2.data_,
              in2.data_ + in2.size_ + extra(),
              data_ + in1.size_);
    size_ = in1.size_ + in2.size_;
    assert(isSorted());
  }

  void copyFrom(const BTreeNode& node) {
    std::copy(node.keys_, node.keys_ + node.size_, keys_);
    std::copy(node.data_, node.data_ + node.size_ + extra(), data_);
    size_ = node.size_;
  }

  static void rebalance(const BTreeNode& in1,
                        const BTreeNode& in2,
                        uint32_t firstPart,
                        BTreeNode& out1,
                        BTreeNode& out2) {
    if (in1.size_ > firstPart) {
      std::copy(in1.keys_, in1.keys_ + firstPart, out1.keys_);
      std::copy(in1.data_, in1.data_ + firstPart, out1.data_);
      auto k = std::copy(in1.keys_ + firstPart,
                         in1.keys_ + in1.size_,
                         out2.keys_);
      auto v = std::copy(in1.data_ + firstPart,
                         in1.data_ + in1.size_,
                         out2.data_);
      std::copy(in2.keys_, in2.keys_ + in2.size_, k);
      std::copy(in2.data_, in2.data_ + in2.size_ + extra(), v);
    } else {
      auto k = std::copy(in1.keys_, in1.keys_ + in1.size_, out1.keys_);
      auto v = std::copy(in1.data_, in1.data_ + in1.size_, out1.data_);
      auto j = firstPart - in1.size_;
      std::copy(in2.keys_, in2.keys_ + j, k);
      std::copy(in2.data_, in2.data_ + j, v);
      std::copy(in2.keys_ + j, in2.keys_ + in2.size_, out2.keys_);
      std::copy(in2.data_ + j, in2.data_ + in2.size_ + extra(), out2.data_);
    }
    out1.size_ = firstPart;
    if (Type == BTreeNodeType::Node) {
      out1.data_[out1.size_] = {};
    }
    out2.size_ = in1.size_ + in2.size_ - firstPart;
  }

 private:
  template <typename K, typename V>
  friend class BTree;

  static uint32_t extra() {
    return static_cast<uint32_t>(Type);
  }

  explicit BTreeNode(uint32_t capacity)
      : capacity_{capacity},
        data_{reinterpret_cast<Value*>(keys_ + capacity)} {
    if (Type == BTreeNodeType::Node) {
      data_[0] = {};
    }
    assert(capacity_ >= 2);
  }
  BTreeNode(const BTreeNode&) = delete;
  BTreeNode& operator=(const BTreeNode&) = delete;
  ~BTreeNode() = default;

  bool isSorted() const {
    return std::is_sorted(keys_, keys_ + size_);
  }

  uint32_t size_{};
  uint32_t capacity_{};
  Value* const data_{};
  Key keys_[];
  // Followed by children pointers or values
};
}
}
