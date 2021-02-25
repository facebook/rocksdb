//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//  @author Adam Retter

#pragma once
#include <cstdint>
#include <iterator>
#include <stack>
#include <vector>

#include "collection_merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

/**
 * An iterator which iterates over a operand list,
 * as it goes if it finds a CollectionOperation::_kMulti
 * it will transparently expand it on the fly, so its operands
 * appear as standard operands during the iteration.
 * 
 * NOTE this iterator is a "Stashing Iterator" and
 * so cannot be used with std::reverse_iterator.
 * 
 * If you wish to iterate in reverse the following pattern
 * can be used:
 * 
 *     auto it = multi_operand_list.end();
 *     while (it != multi_operand_list.begin()) {
 *         --it;
 * 
 *         // process *it here
 *     }
 */
template <typename CollectionType>
class MultiOperandListIterator
    : public std::iterator<std::bidirectional_iterator_tag, const Slice> {
 
 private:
  typename CollectionType::const_iterator it_operand_list_;
  const typename CollectionType::const_iterator it_operand_list_begin_;
  const typename CollectionType::const_iterator it_operand_list_end_;
  // the current operand of the iterator
  const Slice* current_operand_ = nullptr;
  // state for _kMulti Collection Operation operands
  bool in_multi_ = false;
  const Slice* multi_operand_;
  std::vector<const char*> multi_operand_index_; 
  size_t multi_operand_index_offset_ = 0;

 public:
  using iterator = MultiOperandListIterator<CollectionType>;

  MultiOperandListIterator() = default;

  explicit MultiOperandListIterator(
      typename CollectionType::const_iterator it_operand_list,
      const typename CollectionType::const_iterator it_operand_list_begin,
      const typename CollectionType::const_iterator it_operand_list_end)
        : it_operand_list_(it_operand_list),
        it_operand_list_begin_(it_operand_list_begin),
        it_operand_list_end_(it_operand_list_end) {
    if (it_operand_list < it_operand_list_end) {
      current_operand_ = begin_operand();
    }
  }

  MultiOperandListIterator(const iterator& other)
      : it_operand_list_(other.it_operand_list_),
      it_operand_list_begin_(other.it_operand_list_begin_),
      it_operand_list_end_(other.it_operand_list_end_),
      in_multi_(other.in_multi_),
      multi_operand_(other.multi_operand_),
      multi_operand_index_(other.multi_operand_index_),
      multi_operand_index_offset_(other.multi_operand_index_offset_)  {
    if (other.in_multi_) {
      current_operand_ = new Slice(other.current_operand_->data(), other.current_operand_->size());
    } else {
      current_operand_ = other.current_operand_;
    }
  }

  ~MultiOperandListIterator() {
    if (in_multi_ && current_operand_ != nullptr) {
      delete current_operand_;
    }
  }

  iterator operator=(const iterator& rhs) const {
    return MultiOperandListIterator(rhs);
  }

  reference operator*() const {
    return *current_operand_;
  }

  bool operator==(const iterator& rhs) const {
    if(it_operand_list_ == rhs.it_operand_list_) {
      if(!in_multi_) {
        return true;
      } else {
        // we are in a _kMulti, check rhs _kMulti is equal
        return rhs.in_multi
            && multi_operand_index_offset_ == rhs.multi_operand_index_offset_;
      }
    }

    return false;
  }

  bool operator!=(const iterator& rhs) const {
    if(it_operand_list_ != rhs.it_operand_list_) {
      return true;
    }

    if(in_multi_) {
      if (!rhs.in_multi_) {
        // we are in a _kMulti, but rhs is not, so not equal
        return true;    
      } else {
        return multi_operand_index_offset_ != rhs.multi_operand_index_offset_;
      }
    }

    return false;
  }

  iterator& operator++() {
    current_operand_ = next_operand();
    return *this;
  }

  iterator operator++(int) {
    MultiOperandListIterator tmp(*this);
    operator++();
    return tmp;
  }

  iterator& operator--() {
    current_operand_ = prev_operand();
    return *this;
  }

  iterator operator--(int) {
    MultiOperandListIterator tmp(*this);
    operator--();
    return tmp;
  }

 private:
  // const Slice* begin_operand() {
  //     // read from underlying operand_list
  //     const Slice* operand = &(*it_operand_list_);

  //     // is multi Collection Operation?
  //     if ((*operand)[0] == CollectionOperation::_kMulti) {
  //       in_multi_ = true;
  //       multi_operand_ = operand;
  //       build_multi_operand_index();

  //       operand = get_multi_operand_operand(multi_operand_index_offset_);
  //     }

  //     return operand;
  // }
  // TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
  // handles the case where previous PartialMergeMulti returns a no-op
  const Slice* begin_operand() {
      while (it_operand_list_ != it_operand_list_end_) {
        // read from underlying operand_list
        const Slice* operand = &(*it_operand_list_);

        // handles the case where previous PartialMergeMulti returns a no-op
        if (operand->size() == 0) {
            ++it_operand_list_;   // this is empty, attempt next
            continue;
        }

        // is multi Collection Operation?
        if ((*operand)[0] == CollectionOperation::_kMulti) {
          in_multi_ = true;
          multi_operand_ = operand;
          build_multi_operand_index();

          operand = get_multi_operand_operand(multi_operand_index_offset_);
        }

        return operand;
      }

      return nullptr;
  }

  // const Slice* next_operand() {
  //   const Slice* operand = nullptr;

  //   if (in_multi_) {
  //     if (has_next_multi_operand()) {

  //       // cleanup memory from previous operand of multi
  //       delete current_operand_;

  //       // return next from multi
  //       operand = get_multi_operand_operand(++multi_operand_index_offset_);

  //     } else {
  //       // end of previous multi
  //       in_multi_ = false;
  //       multi_operand_ = nullptr;
  //       delete current_operand_;  // free the memory allocated for the last multi
  //     }
  //   }

  //   if (!in_multi_
  //       && it_operand_list_ != it_operand_list_end_) {
  //     // read next from underlying operand_list

  //     if (++it_operand_list_ == it_operand_list_end_) {
  //       operand = nullptr;
  //     } else {
  //       operand = &(*it_operand_list_);

  //       // is multi Collection Operation?
  //       if ((*operand)[0] == CollectionOperation::_kMulti) {
  //         in_multi_ = true;
  //         multi_operand_ = operand;
  //         build_multi_operand_index();
  //         multi_operand_index_offset_ = 0;

  //         operand = get_multi_operand_operand(multi_operand_index_offset_);
  //       }
  //     }
  //   }

  //   return operand;
  // }
  // TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
  // handles the case where previous PartialMergeMulti returns a no-op
  const Slice* next_operand() {
    const Slice* operand = nullptr;

    if (in_multi_) {
      if (has_next_multi_operand()) {

        // cleanup memory from previous operand of multi
        delete current_operand_;

        // return next from multi
        operand = get_multi_operand_operand(++multi_operand_index_offset_);

      } else {
        // end of previous multi
        in_multi_ = false;
        multi_operand_ = nullptr;
        delete current_operand_;  // free the memory allocated for the last multi
      }
    }
    
    if (!in_multi_
        && it_operand_list_ != it_operand_list_end_) {
      // read next from underlying operand_list
      
      while (++it_operand_list_ != it_operand_list_end_) {
        operand = &(*it_operand_list_);

        // handles the case where previous PartialMergeMulti returns a no-op
        if (operand->size() == 0) {
            continue; // this is empty, attempt next
        }

        // is multi Collection Operation?
        if ((*operand)[0] == CollectionOperation::_kMulti) {
          in_multi_ = true;
          multi_operand_ = operand;
          build_multi_operand_index();
          multi_operand_index_offset_ = 0;
          
          operand = get_multi_operand_operand(multi_operand_index_offset_);
        }

        break;
      }

      if (it_operand_list_ == it_operand_list_end_) {
        operand = nullptr;
      }
    }

    return operand;
  }

  bool has_next_multi_operand() const {
    return in_multi_
        && multi_operand_index_offset_ + 1 < multi_operand_index_.size() ;
  }

  const Slice* get_multi_operand_operand(const size_t multi_operand_index_offset) const {
    const char* multi_operand_ptr = multi_operand_index_.at(multi_operand_index_offset);
    const uint32_t records_len = DecodeFixed32(multi_operand_ptr);
    multi_operand_ptr += sizeof(uint32_t);

    const Slice* operand = new Slice(multi_operand_ptr, sizeof(CollectionOperation) + records_len);
    return operand;
  }

  // const Slice* prev_operand() {
  //   const Slice* operand = nullptr;

  //   if (in_multi_) {
  //     if (has_prev_multi_operand()) {

  //       // cleanup memory from previous operand of multi
  //       delete current_operand_;

  //       // return prev from multi
  //       operand = get_multi_operand_operand(--multi_operand_index_offset_);

  //       } else {
  //       // end of previous multi
  //       in_multi_ = false;
  //       multi_operand_ = nullptr;
  //       delete current_operand_;  // free the memory allocated for the last multi
  //     }
  //   }

  //   if (!in_multi_
  //       && it_operand_list_ > it_operand_list_begin_) {
  //     // read prev from underlying operand_list
  //     operand = &(*--it_operand_list_);

  //     // is multi Collection Operation?
  //     if ((*operand)[0] == CollectionOperation::_kMulti) {
  //       in_multi_ = true;
  //       multi_operand_ = operand;
  //       build_multi_operand_index();
  //       multi_operand_index_offset_ = multi_operand_index_.size();

  //       operand = get_multi_operand_operand(--multi_operand_index_offset_);
  //     }
  //   }

  //   return operand;
  // }
  // TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
  // handles the case where previous PartialMergeMulti returns a no-op
  const Slice* prev_operand() {
    const Slice* operand = nullptr;

    if (in_multi_) {
      if (has_prev_multi_operand()) {

        // cleanup memory from previous operand of multi
        delete current_operand_;

        // return prev from multi
        operand = get_multi_operand_operand(--multi_operand_index_offset_);

        } else {
        // end of previous multi
        in_multi_ = false;
        multi_operand_ = nullptr;
        delete current_operand_;  // free the memory allocated for the last multi
      }
    }

    if (!in_multi_) {
      while (it_operand_list_ > it_operand_list_begin_) {
        // read prev from underlying operand_list
        operand = &(*--it_operand_list_);

        // handles the case where previous PartialMergeMulti returns a no-op
        if (operand->size() == 0) {
            continue; // this is empty, attempt prev
        }

        // is multi Collection Operation?
        if ((*operand)[0] == CollectionOperation::_kMulti) {
          in_multi_ = true;
          multi_operand_ = operand;
          build_multi_operand_index();
          multi_operand_index_offset_ = multi_operand_index_.size();

          operand = get_multi_operand_operand(--multi_operand_index_offset_);
        }

        break;
      }
    }

    return operand;
  }

  bool has_prev_multi_operand() const {
    return in_multi_
        && multi_operand_index_offset_ > 0;
  }

  void build_multi_operand_index() {
    assert(multi_operand_->data()[0] == CollectionOperation::_kMulti);

    multi_operand_index_.clear();  // clear the current index

    const char * const end_ptr = multi_operand_->data() + multi_operand_->size();
    const char* data_ptr = multi_operand_->data();
    data_ptr += sizeof(CollectionOperation);  // skip over _kMulti

    while (data_ptr < end_ptr) {
      multi_operand_index_.push_back(data_ptr);

      const size_t records_len = DecodeFixed32(data_ptr);
      data_ptr += sizeof(uint32_t) + sizeof(CollectionOperation) + records_len;
    }
  }
};

/**
 * Wrapper for an operand list
 * which provides iterators which
 * are aware of CollectionOperation::_kMulti
 * operands.
 */
template <class CollectionType>
class MultiOperandList {
 private:
  const CollectionType& operand_list_;

 public:
  using iterator = MultiOperandListIterator<CollectionType>;
  
  MultiOperandList(CollectionType& operand_list)
      : operand_list_(operand_list) {}
  
  iterator begin() const {
    return MultiOperandListIterator<CollectionType>(
        operand_list_.begin(), operand_list_.begin(), operand_list_.end());
  }

  iterator end() const {
    return MultiOperandListIterator<CollectionType>(
        operand_list_.end(), operand_list_.begin(), operand_list_.end());
  }
};

}  // end ROCKSDB_NAMESPACE namespace
