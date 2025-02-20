//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <optional>
#include <stdexcept>
#include <utility>

#include "faiss/IndexIVF.h"
#include "faiss/invlists/InvertedLists.h"
#include "rocksdb/utilities/secondary_index_faiss.h"
#include "util/autovector.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

namespace {

std::string SerializeLabel(faiss::idx_t label) {
  std::string label_str;
  PutVarsignedint64(&label_str, label);

  return label_str;
}

faiss::idx_t DeserializeLabel(Slice label_slice) {
  faiss::idx_t label = -1;
  [[maybe_unused]] const bool ok = GetVarsignedint64(&label_slice, &label);
  assert(ok);

  return label;
}

}  // namespace

struct FaissIVFIndex::KNNContext {
  SecondaryIndexIterator* it;
  autovector<std::string> keys;
};

class FaissIVFIndex::Adapter : public faiss::InvertedLists {
 public:
  Adapter(size_t num_lists, size_t code_size)
      : faiss::InvertedLists(num_lists, code_size) {
    use_iterator = true;
  }

  // Non-iterator-based read interface; not implemented/used since use_iterator
  // is true
  size_t list_size(size_t /* list_no */) const override {
    assert(false);
    return 0;
  }

  const uint8_t* get_codes(size_t /* list_no */) const override {
    assert(false);
    return nullptr;
  }

  const faiss::idx_t* get_ids(size_t /* list_no */) const override {
    assert(false);
    return nullptr;
  }

  // Iterator-based read interface
  faiss::InvertedListsIterator* get_iterator(
      size_t list_no, void* inverted_list_context = nullptr) const override {
    KNNContext* const knn_context =
        static_cast<KNNContext*>(inverted_list_context);
    assert(knn_context);

    return new IteratorAdapter(knn_context, list_no, code_size);
  }

  // Write interface; only add_entry is implemented/required for now
  size_t add_entry(size_t /* list_no */, faiss::idx_t /* id */,
                   const uint8_t* code,
                   void* inverted_list_context = nullptr) override {
    std::string* const code_str =
        static_cast<std::string*>(inverted_list_context);
    assert(code_str);

    code_str->assign(reinterpret_cast<const char*>(code), code_size);

    return 0;
  }

  size_t add_entries(size_t /* list_no */, size_t /* num_entries */,
                     const faiss::idx_t* /* ids */,
                     const uint8_t* /* code */) override {
    assert(false);
    return 0;
  }

  void update_entry(size_t /* list_no */, size_t /* offset */,
                    faiss::idx_t /* id */, const uint8_t* /* code */) override {
    assert(false);
  }

  void update_entries(size_t /* list_no */, size_t /* offset */,
                      size_t /* num_entries */, const faiss::idx_t* /* ids */,
                      const uint8_t* /* code */) override {
    assert(false);
  }

  void resize(size_t /* list_no */, size_t /* new_size */) override {
    assert(false);
  }

 private:
  class IteratorAdapter : public faiss::InvertedListsIterator {
   public:
    IteratorAdapter(KNNContext* knn_context, size_t list_no, size_t code_size)
        : knn_context_(knn_context),
          it_(knn_context_->it),
          code_size_(code_size) {
      assert(knn_context_);
      assert(it_);

      const std::string label = SerializeLabel(list_no);
      it_->Seek(label);
      Update();
    }

    bool is_available() const override { return id_and_codes_.has_value(); }

    void next() override {
      it_->Next();
      Update();
    }

    std::pair<faiss::idx_t, const uint8_t*> get_id_and_codes() override {
      assert(is_available());

      return *id_and_codes_;
    }

   private:
    void Update() {
      id_and_codes_.reset();

      const Status status = it_->status();
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }

      if (!it_->Valid()) {
        return;
      }

      if (!it_->PrepareValue()) {
        throw std::runtime_error(
            "Failed to prepare value during iteration in FaissIVFIndex");
      }

      const Slice value = it_->value();
      if (value.size() != code_size_) {
        throw std::runtime_error(
            "Code with unexpected size encountered during iteration in "
            "FaissIVFIndex");
      }

      const faiss::idx_t id = knn_context_->keys.size();
      knn_context_->keys.emplace_back(it_->key().ToString());

      id_and_codes_.emplace(id, reinterpret_cast<const uint8_t*>(value.data()));
    }

    KNNContext* knn_context_;
    SecondaryIndexIterator* it_;
    size_t code_size_;
    std::optional<std::pair<faiss::idx_t, const uint8_t*>> id_and_codes_;
  };
};

FaissIVFIndex::FaissIVFIndex(std::unique_ptr<faiss::IndexIVF>&& index,
                             std::string primary_column_name)
    : adapter_(std::make_unique<Adapter>(index->nlist, index->code_size)),
      index_(std::move(index)),
      primary_column_name_(std::move(primary_column_name)) {
  assert(index_);
  assert(index_->quantizer);

  index_->parallel_mode = 0;
  index_->replace_invlists(adapter_.get());
}

FaissIVFIndex::~FaissIVFIndex() = default;

void FaissIVFIndex::SetPrimaryColumnFamily(ColumnFamilyHandle* column_family) {
  assert(column_family);
  primary_column_family_ = column_family;
}

void FaissIVFIndex::SetSecondaryColumnFamily(
    ColumnFamilyHandle* column_family) {
  assert(column_family);
  secondary_column_family_ = column_family;
}

ColumnFamilyHandle* FaissIVFIndex::GetPrimaryColumnFamily() const {
  return primary_column_family_;
}

ColumnFamilyHandle* FaissIVFIndex::GetSecondaryColumnFamily() const {
  return secondary_column_family_;
}

Slice FaissIVFIndex::GetPrimaryColumnName() const {
  return primary_column_name_;
}

Status FaissIVFIndex::UpdatePrimaryColumnValue(
    const Slice& /* primary_key */, const Slice& primary_column_value,
    std::optional<std::variant<Slice, std::string>>* updated_column_value)
    const {
  assert(updated_column_value);

  const float* const embedding =
      ConvertSliceToFloats(primary_column_value, index_->d);
  if (!embedding) {
    return Status::InvalidArgument(
        "Incorrectly sized vector passed to FaissIVFIndex");
  }

  constexpr faiss::idx_t n = 1;
  faiss::idx_t label = -1;

  try {
    index_->quantizer->assign(n, embedding, &label);
  } catch (const std::exception& e) {
    return Status::InvalidArgument(e.what());
  }

  if (label < 0 || label >= index_->nlist) {
    return Status::InvalidArgument(
        "Unexpected label returned by coarse quantizer");
  }

  updated_column_value->emplace(SerializeLabel(label));

  return Status::OK();
}

Status FaissIVFIndex::GetSecondaryKeyPrefix(
    const Slice& /* primary_key */, const Slice& primary_column_value,
    std::variant<Slice, std::string>* secondary_key_prefix) const {
  assert(secondary_key_prefix);

  [[maybe_unused]] const faiss::idx_t label =
      DeserializeLabel(primary_column_value);
  assert(label >= 0);
  assert(label < index_->nlist);

  *secondary_key_prefix = primary_column_value;

  return Status::OK();
}

Status FaissIVFIndex::FinalizeSecondaryKeyPrefix(
    std::variant<Slice, std::string>* /* secondary_key_prefix */) const {
  return Status::OK();
}

Status FaissIVFIndex::GetSecondaryValue(
    const Slice& /* primary_key */, const Slice& primary_column_value,
    const Slice& original_column_value,
    std::optional<std::variant<Slice, std::string>>* secondary_value) const {
  assert(secondary_value);

  const faiss::idx_t label = DeserializeLabel(primary_column_value);
  assert(label >= 0);
  assert(label < index_->nlist);

  constexpr faiss::idx_t n = 1;

  const float* const embedding =
      ConvertSliceToFloats(original_column_value, index_->d);
  assert(embedding);

  constexpr faiss::idx_t* xids = nullptr;
  std::string code_str;

  try {
    index_->add_core(n, embedding, xids, &label, &code_str);
  } catch (const std::exception& e) {
    return Status::Corruption(e.what());
  }

  if (code_str.size() != index_->code_size) {
    return Status::Corruption(
        "Code with unexpected size returned by fine quantizer");
  }

  secondary_value->emplace(std::move(code_str));

  return Status::OK();
}

Status FaissIVFIndex::FindKNearestNeighbors(
    SecondaryIndexIterator* it, const Slice& target, size_t neighbors,
    size_t probes, std::vector<std::pair<std::string, float>>* result) const {
  if (!it) {
    return Status::InvalidArgument("Secondary index iterator must be provided");
  }

  const float* const embedding = ConvertSliceToFloats(target, index_->d);
  if (!embedding) {
    return Status::InvalidArgument(
        "Incorrectly sized vector passed to FaissIVFIndex");
  }

  if (!neighbors) {
    return Status::InvalidArgument("Invalid number of neighbors");
  }

  if (!probes) {
    return Status::InvalidArgument("Invalid number of probes");
  }

  if (!result) {
    return Status::InvalidArgument("Result parameter must be provided");
  }

  result->clear();

  std::vector<float> distances(neighbors, 0.0f);
  std::vector<faiss::idx_t> ids(neighbors, -1);

  KNNContext knn_context{it, {}};

  faiss::SearchParametersIVF params;
  params.nprobe = probes;
  params.inverted_list_context = &knn_context;

  constexpr faiss::idx_t n = 1;

  try {
    index_->search(n, embedding, neighbors, distances.data(), ids.data(),
                   &params);
  } catch (const std::exception& e) {
    return Status::Corruption(e.what());
  }

  result->reserve(neighbors);

  for (size_t i = 0; i < neighbors; ++i) {
    if (ids[i] < 0) {
      break;
    }

    if (ids[i] >= knn_context.keys.size()) {
      result->clear();
      return Status::Corruption("Unexpected id returned by FAISS");
    }

    result->emplace_back(knn_context.keys[ids[i]], distances[i]);
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
