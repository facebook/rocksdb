#ifndef BUCKET_MODEL_HPP
#define BUCKET_MODEL_HPP

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>

namespace bucket_filter {

class BucketModel {
 public:
  enum QueryPosStatus { OUT_OF_SCOPE, EXIST, NO_IDEA };

 public:
  BucketModel(uint64_t min_key, uint64_t max_key, uint64_t total_bit_size)
      : min_key_(min_key),
        max_key_(max_key),
        total_bit_size_(total_bit_size) {}

  /**
   * For given query (point/range), return the (slope, bias) of the model
   * return (-1, 0): query crosses the models
   * return (0, 2): query out of scop;
   */
  QueryPosStatus query(const uint64_t &key, size_t &result);
  /* [l_key, r_key], and return [l_pos, r_pos] */
  QueryPosStatus query(const uint64_t &l_key, const uint64_t &r_key,
             std::pair<uint64_t, uint64_t> &result);

  // auto size() const -> size_t;

  // auto serialize() const -> std::pair<uint8_t *, size_t>;

  // static auto deserialize(uint8_t *ser, uint32_t K) -> BucketModel *;

 private:
 inline uint64_t get_location(const uint64_t &key);
//   /* idx range in [1, index_.size() - 1] */
//   // inline auto get_params(size_t idx) -> std::pair<uint64_t, uint64_t>;
//   /* idx range in [0, index_.size() - 2] */
//   inline auto get_location(const uint64_t &key, size_t idx,
//                            const std::pair<uint64_t, uint64_t> &params)
//       -> uint64_t;

 private:
  uint64_t min_key_;
  uint64_t max_key_;
  uint64_t total_bit_size_;
};


uint64_t BucketModel::get_location(const uint64_t &key){
  uint64_t location = 0;
  if (key > max_key_){
    location = std::numeric_limits<uint64_t>::max();
  } else if (key > min_key_) {
    location = static_cast<uint64_t>(static_cast<long double>(key - min_key_)/(max_key_ - min_key_) * total_bit_size_);
  }
  assert(location <= total_bit_size_ && location >= 0);
  return location;
}

BucketModel::QueryPosStatus 
BucketModel::query(const uint64_t &key, size_t &result){
  if (key < min_key_ || key > max_key_) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }
  result = get_location(key);
  return QueryPosStatus::NO_IDEA;
}

BucketModel::QueryPosStatus 
BucketModel::query(const uint64_t &l_key, const uint64_t &r_key,
                     std::pair<uint64_t, uint64_t> &result){
  assert(l_key < r_key);

  if (l_key > max_key_ || r_key < min_key_) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }

  result.first = get_location(l_key);
  result.second = get_location(r_key);

  return QueryPosStatus::NO_IDEA;
}

// auto BucketModel::size() const -> size_t {
//   return sizeof(size_t)                      /* nkeys_ */
//          + sizeof(size_t)                    /* elements_per_cdf_ */
//          + sizeof(size_t)                    /* # elements in index_ */
//          + sizeof(uint64_t) * index_.size(); /* index_ */
// }

// auto BucketModel::serialize() const -> std::pair<uint8_t *, size_t> {
//   size_t size = this->size();
//   uint8_t *out = new uint8_t[size];
//   uint8_t *pos = out;

//   memcpy(pos, &nkeys_, sizeof(size_t));
//   pos += sizeof(size_t);

//   memcpy(pos, &elements_per_cdf_, sizeof(size_t));
//   pos += sizeof(size_t);

//   size_t idx_size = index_.size();
//   memcpy(pos, &idx_size, sizeof(size_t));
//   pos += sizeof(size_t);

//   memcpy(pos, index_.data(), sizeof(uint64_t) * index_.size());
//   return {out, size};
// }

// auto BucketModel::deserialize(uint8_t *ser, uint32_t K) -> BucketModel * {
//   assert(ser != nullptr);

//   size_t nkeys;
//   memcpy(&nkeys, ser, sizeof(size_t));
//   ser += sizeof(size_t);

//   size_t elements_per_cdf;
//   memcpy(&elements_per_cdf, ser, sizeof(size_t));
//   ser += sizeof(size_t);

//   size_t idx_size;
//   memcpy(&idx_size, ser, sizeof(size_t));
//   ser += sizeof(size_t);

//   uint64_t *raw_index = reinterpret_cast<uint64_t *>(ser);
//   std::vector<uint64_t> index(raw_index, raw_index + idx_size);
//   return {new BucketModel(K, nkeys, elements_per_cdf, index)};
// }

}  // namespace bucket_filter

#endif