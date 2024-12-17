#pragma once

#include <boost/dynamic_bitset.hpp>
#include "rocksdb/range_delete_filter/bucket/bucket_model.hpp"

namespace bucket_filter {

class Bucket {
 public:
  Bucket(size_t bit_per_key, uint64_t num_keys, uint64_t num_blocks,
            uint64_t min_key, uint64_t max_key);
  
  Bucket(uint64_t block_bit_size, uint64_t num_blocks,
            BucketModel *bucket_model, std::vector<boost::dynamic_bitset<> *> &block_list)
      : block_bit_size_(block_bit_size),
        num_blocks_(num_blocks),
        bucket_model_(bucket_model),
        block_list_(std::move(block_list)) {}

  ~Bucket() {
    delete bucket_model_;
    for (size_t i = 0; i < block_list_.size(); ++i) {
      delete block_list_[i];
    }
  }

  bool InsertRange(const uint64_t &left, const uint64_t &right);

  bool Query(const uint64_t &query_key);
  /* [left, right] */
  bool Query(const uint64_t &left, const uint64_t &right);

  // auto serialize() const -> std::pair<uint8_t *, size_t>;
  // static auto deserialize(uint8_t *ser) -> Bucket *;

  // auto size() const -> size_t;

 private:
  inline void Init(uint64_t &min_key, uint64_t &max_key);
  // auto cal_block_idx (const uint64_t &pos, uint64_t &block_idx, uint64_t &block_pos) -> bool;

 private:
  uint64_t block_bit_size_ = 0;
  uint64_t num_blocks_ = 0;

  BucketModel *bucket_model_ = nullptr;
  std::vector<boost::dynamic_bitset<> *> block_list_;
};

Bucket::Bucket(size_t bit_per_key, uint64_t num_keys, uint64_t num_blocks,
            uint64_t min_key, uint64_t max_key)
    : num_blocks_(num_blocks) {
  block_bit_size_ = std::min(num_keys / num_blocks_ * bit_per_key , std::numeric_limits<uint64_t>::max());
  Init(min_key, max_key);
}

void Bucket::Init(uint64_t &min_key, uint64_t &max_key) {
  assert(min_key < max_key);
  assert(num_blocks_ != 0);
  assert(block_bit_size_ != 0);

  // 1. build the bucket_model
  bucket_model_ = new BucketModel(min_key, max_key, num_blocks_ * block_bit_size_);

  // 2. build the bitsets
  block_list_.reserve(num_blocks_);
  
  for (uint64_t i = 0; i < num_blocks_; ++i) {
    block_list_.emplace_back(new boost::dynamic_bitset<>());
    block_list_[i]->resize(block_bit_size_);
    block_list_[i]->reset();
  }

  assert(block_list_.size() == num_blocks_);
}

auto Bucket::InsertRange(const uint64_t &left, const uint64_t &right) -> bool {
  std::pair<uint64_t, uint64_t> pos;
  BucketModel::QueryPosStatus status = bucket_model_->query(left, right, pos);
  if (status == BucketModel::OUT_OF_SCOPE) {
      return false;
  }

  uint64_t l_block_idx = pos.first / block_bit_size_;
  uint64_t r_block_idx = pos.second / block_bit_size_;

  uint64_t l_pos = pos.first % block_bit_size_;
  uint64_t r_pos = pos.second % block_bit_size_;

  // set corresponded bits to 1
  if (l_block_idx == r_block_idx){
    uint64_t len = r_pos - l_pos + 1;
    block_list_[l_block_idx]->set(l_pos, len, true);

  } else if (r_block_idx > l_block_idx){
    uint64_t len = block_list_[l_block_idx]->size() - l_pos;
    block_list_[l_block_idx]->set(l_pos, len, true);

    for (uint64_t idx = l_block_idx + 1; idx < r_block_idx; idx++)
    {
      block_list_[idx]->set();
    }
    
    block_list_[r_block_idx]->set(0, r_pos + 1, true);
  }

  return true;

}

auto Bucket::Query(const uint64_t &query_key) -> bool {
  uint64_t pos;
  BucketModel::QueryPosStatus status = bucket_model_->query(query_key, pos);
  if (status == BucketModel::OUT_OF_SCOPE) {
      return false;
  }
  uint64_t block_idx = pos / block_bit_size_;
  uint64_t block_pos = pos % block_bit_size_;
  return block_list_[block_idx]->test(block_pos);
}

auto Bucket::Query(const uint64_t &left, const uint64_t &right) -> bool {
  std::pair<uint64_t, uint64_t> pos;
  BucketModel::QueryPosStatus status = bucket_model_->query(left, right, pos);
  if (status == BucketModel::OUT_OF_SCOPE) {
      return false;
  }

  uint64_t l_block_idx = pos.first / block_bit_size_;
  uint64_t r_block_idx = pos.second / block_bit_size_;

  uint64_t l_pos = pos.first % block_bit_size_;
  uint64_t r_pos = pos.second % block_bit_size_;

  // check corresponded blocks
  if (l_block_idx == r_block_idx){
    if (block_list_[l_block_idx]->test(l_pos)){
      return true;
    } else if (block_list_[l_block_idx]->find_next(l_pos) != boost::dynamic_bitset<>::npos){
      return block_list_[l_block_idx]->find_next(l_pos) <= r_pos;
    }
    return false;
  } else if (r_block_idx > l_block_idx){
    if (block_list_[l_block_idx]->test(l_pos)){
      return true;
    } else if (block_list_[l_block_idx]->find_next(l_pos) != boost::dynamic_bitset<>::npos){
      return true;
    }

    for (uint64_t idx = l_block_idx + 1; idx < r_block_idx; idx++)
    {
      if (block_list_[idx]->any()){
        return true;
      }
    }

    return block_list_[r_block_idx]->find_first() != boost::dynamic_bitset<>::npos ? block_list_[r_block_idx]->find_first() <= r_pos : false;    
  }

  return true;
}

// auto Bucket::serialize() const -> std::pair<uint8_t *, size_t> {
//   std::pair<uint8_t *, size_t> cdf_ser = bucket_model_->serialize();

//   size_t bit_map_sz = align_bit2byte(nbatches_);
//   std::vector<uint8_t> bitmap(bit_map_sz, 0);

//   size_t bitsets_size = 0;
//   std::vector<std::pair<uint8_t *, size_t>> bitsets_ser;
//   for (size_t i = 0; i < nbatches_; ++i) {
//     BitSet *p_bit_array = block_list_[i];
//     if (p_bit_array == nullptr) {
//       continue;
//     }
//     bitmap[i >> 3] |= 1U << (i & 0x7ULL);

//     auto data = p_bit_array->serialize();
//     bitsets_size += data.second;
//     bitsets_ser.push_back(data);
//   }

//   size_t size = sizeof(uint64_t) /* K_ */
//                 + sizeof(size_t) /* nbatches_ */
//                 + sizeof(size_t) /* elements_per_block_ */
//                 + cdf_ser.second /* cdf model */
//                 + bit_map_sz     /* bitmap */
//                 + bitsets_size;  /* block_list_ */

//   uint8_t *ser = new uint8_t[size];
//   uint8_t *pos = ser;

//   memcpy(pos, &K_, sizeof(uint64_t));
//   pos += sizeof(uint64_t);

//   memcpy(pos, &nbatches_, sizeof(size_t));
//   pos += sizeof(size_t);

//   memcpy(pos, &elements_per_block_, sizeof(size_t));
//   pos += sizeof(size_t);

//   memcpy(pos, cdf_ser.first, cdf_ser.second);
//   pos += cdf_ser.second;
//   delete cdf_ser.first;

//   memcpy(pos, bitmap.data(), bit_map_sz);
//   pos += bit_map_sz;

//   for (auto &[ptr, sz] : bitsets_ser) {
//     memcpy(pos, ptr, sz);
//     pos += sz;
//     delete ptr;
//   }

//   return {ser, size};
// }

// auto Bucket::deserialize(uint8_t *ser) -> Bucket * {
//   uint64_t K;
//   memcpy(&K, ser, sizeof(uint64_t));
//   ser += sizeof(uint64_t);

//   size_t nbatches;
//   memcpy(&nbatches, ser, sizeof(size_t));
//   ser += sizeof(size_t);

//   size_t elements_per_block;
//   memcpy(&elements_per_block, ser, sizeof(size_t));
//   ser += sizeof(size_t);

//   BucketModel *model = BucketModel::deserialize(ser, K);
//   model->init_K(K);
//   ser += model->size();

//   size_t bias = align_bit_to_byte(nbatches);
//   ser += bias;

//   std::vector<BitSet *> block_list;
//   uint64_t block_range = K * elements_per_block;
//   for (size_t i = 0; i < nbatches; ++i) {
//     bool exist = ser[(i >> 3) - bias] & (1 << (i & 0x7ULL));
//     if (exist) {
//       BitSet *bitarray_ptr = BitSet::deserialzie(ser, block_range);
//       ser += bitarray_ptr->size();
//       block_list.emplace_back(bitarray_ptr);
//     } else {
//       block_list.emplace_back(nullptr);
//     }
//   }

//   return {new Bucket(K, nbatches, elements_per_block, model, block_list)};
// }

// auto Bucket::size() const -> size_t {
//   size_t size = sizeof(uint64_t) /* K_ */
//                 + sizeof(size_t) /* nbatches_ */
//                 + sizeof(size_t) /* elements_per_block_ */
//                 + bucket_model_->size();

//   size_t bit_map_sz = align_bit2byte(nbatches_);
//   size_t bitsets_sz = 0;
//   for (const auto p : block_list_) {
//     bitsets_sz += p == nullptr ? 0 : p->size();
//   }
//   return size + bit_map_sz + bitsets_sz;
// }

}  // namespace bucket
