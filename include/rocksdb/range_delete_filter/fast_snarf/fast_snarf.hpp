#pragma once

#include "rocksdb/range_delete_filter/fast_snarf/bitset.hpp"
#include "rocksdb/range_delete_filter/fast_snarf/cdf_model.hpp"

namespace fast_snarf {

class FastSNARF {
 public:
  FastSNARF(size_t bit_per_key, size_t elements_per_cdf,
            size_t elements_per_block, const std::vector<uint64_t> &keys);

  FastSNARF(size_t K, size_t nbatches, size_t elements_per_block,
            CDFModel *cdf_model, std::vector<BitSet *> &block_list)
      : block_range_(K * elements_per_block),
        K_(K),
        nbatches_(nbatches),
        elements_per_block_(elements_per_block),
        cdf_model_(cdf_model),
        block_list_(std::move(block_list)) {}

  ~FastSNARF() {
    delete cdf_model_;
    for (BitSet *bit_array : block_list_) {
      delete bit_array;
    }
  }

  auto insertrange(const uint64_t left, const uint64_t right) -> bool;

  auto query(uint64_t query_key) -> bool;
  /* [left, right] */
  auto query(uint64_t left, uint64_t right) -> bool;

  auto serialize() const -> std::pair<uint8_t *, size_t>;
  static auto deserialize(uint8_t *ser) -> FastSNARF *;

  auto size() const -> size_t;

 private:
  inline void init(size_t elements_per_cdf, const std::vector<uint64_t> &keys);

 private:
  uint64_t block_range_ = 0;

  // need to be serialize;
  uint64_t K_ = 0;
  size_t nbatches_ = 0;
  size_t elements_per_block_ = 0;

  CDFModel *cdf_model_ = nullptr;
  std::vector<BitSet *> block_list_;
};

FastSNARF::FastSNARF(size_t bit_per_key, size_t elements_per_cdf,
                     size_t elements_per_block,
                     const std::vector<uint64_t> &keys)
    : K_(1 << (bit_per_key - 3)), elements_per_block_(elements_per_block) {
  block_range_ = K_ * elements_per_block_;
  init(elements_per_cdf, keys);
}

void FastSNARF::init(size_t elements_per_cdf,
                     const std::vector<uint64_t> &keys) {
  assert(elements_per_block_ != 0);
  assert(K_ != 0);

  // 1. build the cdf_model
  cdf_model_ = new CDFModel(K_, elements_per_cdf, keys);
  std::vector<uint64_t> keys_pos = cdf_model_->modeling(keys);

  nbatches_ = keys.size() / elements_per_block_;
  nbatches_ += keys.size() % elements_per_block_ ? 1 : 0;
  size_t key_pos_idx = 0;
  uint64_t lower = 0;
  uint64_t upper = block_range_;
  for (size_t i = 0; i < nbatches_; ++i) {
    std::vector<uint32_t> cur_batch;

    while (key_pos_idx < keys_pos.size() && lower <= keys_pos[key_pos_idx] &&
           upper > keys_pos[key_pos_idx]) {
      cur_batch.emplace_back(keys_pos[key_pos_idx++] - lower);
    }
    while (i == nbatches_ - 1 && key_pos_idx < keys_pos.size()) {
      cur_batch.emplace_back(keys_pos[key_pos_idx++] - lower);
    }

    if (cur_batch.size() == 0) {
      block_list_.push_back(nullptr);
    } else {
      block_list_.emplace_back(new BitSet(cur_batch, block_range_));
    }

    lower = upper;
    upper += block_range_;
  }
  assert(key_pos_idx == keys_pos.size());
  assert(block_list_.size() == nbatches_);
}

auto FastSNARF::insertrange(const uint64_t left, const uint64_t right) -> bool {

}

auto FastSNARF::query(uint64_t query_key) -> bool {
  size_t pos;
  CDFModel::QueryPosStatus status = cdf_model_->query(query_key, pos);
  switch (status) {
    case CDFModel::EXIST:
      return true;
    case CDFModel::OUT_OF_SCOPE:
      return false;
  }

  size_t block_idx = pos / (elements_per_block_ * K_);
  assert(block_idx < nbatches_);
  if (block_list_[block_idx] == nullptr) {
    return false;
  }
  return block_list_[block_idx]->query(
      static_cast<uint32_t>(pos - block_idx * block_range_));
}

auto FastSNARF::query(uint64_t left, uint64_t right) -> bool {
  std::pair<size_t, size_t> pos;
  CDFModel::QueryPosStatus status = cdf_model_->query(left, right, pos);
  switch (status) {
    case CDFModel::EXIST:
      return true;
    case CDFModel::OUT_OF_SCOPE:
      return false;
  }

  size_t l_block_idx = pos.first / block_range_;
  size_t r_block_idx = pos.second / block_range_;

  size_t cnt = 0;
  for (size_t idx = l_block_idx; idx <= r_block_idx && cnt < 3; ++idx) {
    cnt += block_list_[idx] == nullptr ? 0 : 1;
  }

  if (cnt > 2) {
    return true;
  }
  if (cnt == 0) {
    return false;
  }
  uint64_t low_pos = l_block_idx * block_range_;
  if (r_block_idx == l_block_idx || block_list_[r_block_idx] == nullptr) {
    return block_list_[l_block_idx]->query(pos.first - low_pos,
                                           pos.second - low_pos);
  } else if (block_list_[l_block_idx] == nullptr) {
    return block_list_[r_block_idx]->query(0, pos.second - low_pos);
  }
  uint64_t r_low_pos = r_block_idx * block_range_;
  return block_list_[l_block_idx]->query(pos.first - low_pos,
                                         r_low_pos - low_pos) ||
         block_list_[r_block_idx]->query(0, pos.second - r_low_pos);
}

auto FastSNARF::serialize() const -> std::pair<uint8_t *, size_t> {
  std::pair<uint8_t *, size_t> cdf_ser = cdf_model_->serialize();

  size_t bit_map_sz = align_bit2byte(nbatches_);
  std::vector<uint8_t> bitmap(bit_map_sz, 0);

  size_t bitsets_size = 0;
  std::vector<std::pair<uint8_t *, size_t>> bitsets_ser;
  for (size_t i = 0; i < nbatches_; ++i) {
    BitSet *p_bit_array = block_list_[i];
    if (p_bit_array == nullptr) {
      continue;
    }
    bitmap[i >> 3] |= 1U << (i & 0x7ULL);

    auto data = p_bit_array->serialize();
    bitsets_size += data.second;
    bitsets_ser.push_back(data);
  }

  size_t size = sizeof(uint64_t) /* K_ */
                + sizeof(size_t) /* nbatches_ */
                + sizeof(size_t) /* elements_per_block_ */
                + cdf_ser.second /* cdf model */
                + bit_map_sz     /* bitmap */
                + bitsets_size;  /* block_list_ */

  uint8_t *ser = new uint8_t[size];
  uint8_t *pos = ser;

  memcpy(pos, &K_, sizeof(uint64_t));
  pos += sizeof(uint64_t);

  memcpy(pos, &nbatches_, sizeof(size_t));
  pos += sizeof(size_t);

  memcpy(pos, &elements_per_block_, sizeof(size_t));
  pos += sizeof(size_t);

  memcpy(pos, cdf_ser.first, cdf_ser.second);
  pos += cdf_ser.second;
  delete cdf_ser.first;

  memcpy(pos, bitmap.data(), bit_map_sz);
  pos += bit_map_sz;

  for (auto &[ptr, sz] : bitsets_ser) {
    memcpy(pos, ptr, sz);
    pos += sz;
    delete ptr;
  }

  return {ser, size};
}

auto FastSNARF::deserialize(uint8_t *ser) -> FastSNARF * {
  uint64_t K;
  memcpy(&K, ser, sizeof(uint64_t));
  ser += sizeof(uint64_t);

  size_t nbatches;
  memcpy(&nbatches, ser, sizeof(size_t));
  ser += sizeof(size_t);

  size_t elements_per_block;
  memcpy(&elements_per_block, ser, sizeof(size_t));
  ser += sizeof(size_t);

  CDFModel *model = CDFModel::deserialize(ser, K);
  model->init_K(K);
  ser += model->size();

  size_t bias = align_bit_to_byte(nbatches);
  ser += bias;

  std::vector<BitSet *> block_list;
  uint64_t block_range = K * elements_per_block;
  for (size_t i = 0; i < nbatches; ++i) {
    bool exist = ser[(i >> 3) - bias] & (1 << (i & 0x7ULL));
    if (exist) {
      BitSet *bitarray_ptr = BitSet::deserialzie(ser, block_range);
      ser += bitarray_ptr->size();
      block_list.emplace_back(bitarray_ptr);
    } else {
      block_list.emplace_back(nullptr);
    }
  }

  return {new FastSNARF(K, nbatches, elements_per_block, model, block_list)};
}

auto FastSNARF::size() const -> size_t {
  size_t size = sizeof(uint64_t) /* K_ */
                + sizeof(size_t) /* nbatches_ */
                + sizeof(size_t) /* elements_per_block_ */
                + cdf_model_->size();

  size_t bit_map_sz = align_bit2byte(nbatches_);
  size_t bitsets_sz = 0;
  for (const auto p : block_list_) {
    bitsets_sz += p == nullptr ? 0 : p->size();
  }
  return size + bit_map_sz + bitsets_sz;
}

}  // namespace fast_snarf
