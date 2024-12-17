#ifndef FAST_SNARF_CDF_MODEL_HPP
#define FAST_SNARF_CDF_MODEL_HPP

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>

namespace fast_snarf {

class CDFModel {
 public:
  enum QueryPosStatus { OUT_OF_SCOPE, EXIST, NO_IDEA };

 public:
  CDFModel(size_t K, size_t elements_per_cdf,
           const std::vector<uint64_t> &keys);
  CDFModel(size_t K, size_t nkeys, size_t elements_per_cdf,
           std::vector<uint64_t> &index)
      : K_(K),
        nkeys_(nkeys),
        elements_per_cdf_(elements_per_cdf),
        index_(std::move(index)) {
    m_ = K_ * elements_per_cdf_;
  }

  void init_K(size_t K) { K_ = K; }

  /* return the estimate distribution of all the key */
  auto modeling(const std::vector<uint64_t> &keys) -> std::vector<size_t>;

  /**
   * For given query (point/range), return the (slope, bias) of the model
   * return (-1, 0): query crosses the models
   * return (0, 2): query out of scop;
   */
  auto query(const uint64_t &key, size_t &result) -> QueryPosStatus;
  /* [l_key, r_key], and return [l_pos, r_pos] */
  auto query(const uint64_t &l_key, const uint64_t &r_key,
             std::pair<size_t, size_t> &result) -> QueryPosStatus;

  auto size() const -> size_t;

  auto serialize() const -> std::pair<uint8_t *, size_t>;

  static auto deserialize(uint8_t *ser, uint32_t K) -> CDFModel *;

 private:
  /* idx range in [1, index_.size() - 1] */
  inline auto get_params(size_t idx) -> std::pair<uint64_t, uint64_t>;
  /* idx range in [0, index_.size() - 2] */
  inline auto get_location(const uint64_t &key, size_t idx,
                           const std::pair<uint64_t, uint64_t> &params)
      -> uint64_t;

 private:
  size_t K_;
  uint64_t m_;

  /* serializable number */
  size_t nkeys_;
  size_t elements_per_cdf_;
  std::vector<uint64_t> index_;
};

CDFModel::CDFModel(size_t K, size_t elements_per_cdf,
                   const std::vector<uint64_t> &keys)
    : K_(K), elements_per_cdf_(elements_per_cdf) {
  assert(elements_per_cdf > 0);

  nkeys_ = keys.size();
  m_ = K_ * elements_per_cdf_;
  size_t size = nkeys_ / elements_per_cdf_ + 1;
  if (nkeys_ % elements_per_cdf_ != 0) {
    ++size;
  }

  index_.resize(size);
  index_[size - 1] = keys.back();
  index_[0] = keys[0];
  for (size_t i = 1; i < size - 1; ++i) {
    index_[i] = keys[i * elements_per_cdf_ - 1];
  }
}

auto CDFModel::modeling(const std::vector<uint64_t> &keys)
    -> std::vector<size_t> {
  assert(keys.size() == nkeys_);
  std::vector<size_t> positions(nkeys_ - index_.size());

  std::pair<uint64_t, uint64_t> params;
  size_t pos_iter = 0;
  size_t idx_iter = 0;
  for (size_t i = 0; i < nkeys_ - 1; ++i) {
    if (keys[i] == index_[idx_iter]) {
      params = get_params(++idx_iter);
      continue;
    }

    positions[pos_iter++] = get_location(keys[i], idx_iter - 1, params);
  }
  return positions;
}

auto CDFModel::query(const uint64_t &key, size_t &result)
    -> CDFModel::QueryPosStatus {
  if (key < index_[0] || key > index_.back()) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }

  auto up = std::upper_bound(index_.begin(), index_.end(), key);
  if (key == index_.back() || *(up - 1) == key) {
    return QueryPosStatus::EXIST;
  }

  size_t idx = up - index_.begin();
  auto params = get_params(idx);
  result = get_location(key, idx - 1, params);
  return QueryPosStatus::NO_IDEA;
}

auto CDFModel::query(const uint64_t &l_key, const uint64_t &r_key,
                     std::pair<size_t, size_t> &result)
    -> CDFModel::QueryPosStatus {
  assert(l_key < r_key);

  if (l_key > index_.back() || r_key < index_[0]) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }

  auto up = std::upper_bound(index_.begin(), index_.end(), l_key);
  if (r_key >= (*up)) {
    return QueryPosStatus::EXIST;
  }

  size_t idx = up - index_.begin();
  auto params = get_params(idx);
  result.first = get_location(l_key, idx - 1, params);
  result.second = get_location(r_key, idx - 1, params);

  return QueryPosStatus::NO_IDEA;
}

auto CDFModel::size() const -> size_t {
  return sizeof(size_t)                      /* nkeys_ */
         + sizeof(size_t)                    /* elements_per_cdf_ */
         + sizeof(size_t)                    /* # elements in index_ */
         + sizeof(uint64_t) * index_.size(); /* index_ */
}

auto CDFModel::serialize() const -> std::pair<uint8_t *, size_t> {
  size_t size = this->size();
  uint8_t *out = new uint8_t[size];
  uint8_t *pos = out;

  memcpy(pos, &nkeys_, sizeof(size_t));
  pos += sizeof(size_t);

  memcpy(pos, &elements_per_cdf_, sizeof(size_t));
  pos += sizeof(size_t);

  size_t idx_size = index_.size();
  memcpy(pos, &idx_size, sizeof(size_t));
  pos += sizeof(size_t);

  memcpy(pos, index_.data(), sizeof(uint64_t) * index_.size());
  return {out, size};
}

auto CDFModel::deserialize(uint8_t *ser, uint32_t K) -> CDFModel * {
  assert(ser != nullptr);

  size_t nkeys;
  memcpy(&nkeys, ser, sizeof(size_t));
  ser += sizeof(size_t);

  size_t elements_per_cdf;
  memcpy(&elements_per_cdf, ser, sizeof(size_t));
  ser += sizeof(size_t);

  size_t idx_size;
  memcpy(&idx_size, ser, sizeof(size_t));
  ser += sizeof(size_t);

  uint64_t *raw_index = reinterpret_cast<uint64_t *>(ser);
  std::vector<uint64_t> index(raw_index, raw_index + idx_size);
  return {new CDFModel(K, nkeys, elements_per_cdf, index)};
}

/** Helping Function */
auto CDFModel::get_params(size_t idx) -> std::pair<uint64_t, uint64_t> {
  uint64_t delta = index_[idx] - index_[idx - 1];
  if (idx == index_.size() - 1) {
    double tmp = static_cast<double>(K_) * nkeys_;
    tmp -= static_cast<double>(idx - 1) * m_;
    return {static_cast<uint64_t>(tmp), delta};
  }
  return {m_, delta};
}

auto CDFModel::get_location(const uint64_t &key, size_t idx,
                            const std::pair<uint64_t, uint64_t> &params)
    -> uint64_t {
  return static_cast<uint64_t>(static_cast<long double>(key - index_[idx]) *
                               params.first / params.second) +
         m_ * idx;
}

}  // namespace fast_snarf

#endif