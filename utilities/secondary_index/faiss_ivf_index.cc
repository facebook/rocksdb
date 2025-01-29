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

class FaissIVFIndex::KNNIterator : public Iterator {
 public:
  KNNIterator(faiss::IndexIVF* index,
              std::unique_ptr<Iterator>&& secondary_index_it, size_t k,
              size_t probes)
      : index_(index),
        secondary_index_it_(std::move(secondary_index_it)),
        k_(k),
        probes_(probes),
        distances_(k_, 0.0f),
        labels_(k_, -1),
        pos_(0) {
    assert(index_);
    assert(secondary_index_it_);
    assert(k_ > 0);
    assert(probes_ > 0);
  }

  Iterator* GetSecondaryIndexIterator() const {
    return secondary_index_it_.get();
  }

  faiss::idx_t AddKey(std::string&& key) {
    keys_.emplace_back(std::move(key));

    return static_cast<faiss::idx_t>(keys_.size()) - 1;
  }

  bool Valid() const override {
    assert(!labels_.empty());
    assert(labels_.size() == k_);

    return status_.ok() && pos_ >= 0 && pos_ < labels_.size() &&
           labels_[pos_] >= 0;
  }

  void SeekToFirst() override {
    status_ =
        Status::NotSupported("SeekToFirst not supported for FaissIVFIndex");
  }

  void SeekToLast() override {
    status_ =
        Status::NotSupported("SeekToLast not supported for FaissIVFIndex");
  }

  void Seek(const Slice& target) override {
    distances_.assign(k_, 0.0f);
    labels_.assign(k_, -1);
    status_ = Status::OK();
    pos_ = 0;
    keys_.clear();

    const float* const embedding = ConvertSliceToFloats(target, index_->d);
    if (!embedding) {
      status_ = Status::InvalidArgument(
          "Incorrectly sized vector passed to FaissIVFIndex");
      return;
    }

    faiss::SearchParametersIVF params;
    params.nprobe = probes_;
    params.inverted_list_context = this;

    constexpr faiss::idx_t n = 1;

    try {
      index_->search(n, embedding, k_, distances_.data(), labels_.data(),
                     &params);
    } catch (const std::exception& e) {
      status_ = Status::InvalidArgument(e.what());
    }
  }

  void SeekForPrev(const Slice& /* target */) override {
    status_ =
        Status::NotSupported("SeekForPrev not supported for FaissIVFIndex");
  }

  void Next() override {
    assert(Valid());

    ++pos_;
  }

  void Prev() override {
    assert(Valid());

    --pos_;
  }

  Status status() const override { return status_; }

  Slice key() const override {
    assert(Valid());
    assert(labels_[pos_] >= 0 && labels_[pos_] < keys_.size());

    return keys_[labels_[pos_]];
  }

  Slice value() const override {
    assert(Valid());

    return Slice();
  }

  const WideColumns& columns() const override {
    assert(Valid());

    return kNoWideColumns;
  }

  Slice timestamp() const override {
    assert(Valid());

    return Slice();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    if (!prop) {
      return Status::InvalidArgument("No property pointer provided");
    }

    if (!Valid()) {
      return Status::InvalidArgument("Iterator is not valid");
    }

    if (prop_name == kPropertyName_) {
      assert(!distances_.empty());
      assert(distances_.size() == k_);
      assert(pos_ >= 0 && pos_ < distances_.size());

      *prop = std::to_string(distances_[pos_]);
      return Status::OK();
    }

    return Iterator::GetProperty(std::move(prop_name), prop);
  }

 private:
  faiss::IndexIVF* index_;
  std::unique_ptr<Iterator> secondary_index_it_;
  size_t k_;
  size_t probes_;
  std::vector<float> distances_;
  std::vector<faiss::idx_t> labels_;
  Status status_;
  faiss::idx_t pos_;
  autovector<std::string> keys_;

  static const std::string kPropertyName_;
};

const std::string FaissIVFIndex::KNNIterator::kPropertyName_ =
    "rocksdb.faiss.ivf.index.distance";

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
    KNNIterator* const it = static_cast<KNNIterator*>(inverted_list_context);
    assert(it);

    return new IteratorAdapter(it, list_no, code_size);
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
    IteratorAdapter(KNNIterator* it, size_t list_no, size_t code_size)
        : it_(it),
          secondary_index_it_(it->GetSecondaryIndexIterator()),
          code_size_(code_size) {
      assert(it_);
      assert(secondary_index_it_);

      const std::string label = SerializeLabel(list_no);
      secondary_index_it_->Seek(label);
      Update();
    }

    bool is_available() const override { return id_and_codes_.has_value(); }

    void next() override {
      secondary_index_it_->Next();
      Update();
    }

    std::pair<faiss::idx_t, const uint8_t*> get_id_and_codes() override {
      assert(is_available());

      return *id_and_codes_;
    }

   private:
    void Update() {
      id_and_codes_.reset();

      const Status status = secondary_index_it_->status();
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }

      if (!secondary_index_it_->Valid()) {
        return;
      }

      if (!secondary_index_it_->PrepareValue()) {
        throw std::runtime_error(
            "Failed to prepare value during iteration in FaissIVFIndex");
      }

      const Slice value = secondary_index_it_->value();
      if (value.size() != code_size_) {
        throw std::runtime_error(
            "Code with unexpected size encountered during iteration in "
            "FaissIVFIndex");
      }

      const Slice key = secondary_index_it_->key();
      const faiss::idx_t id = it_->AddKey(key.ToString());

      id_and_codes_.emplace(id, reinterpret_cast<const uint8_t*>(value.data()));
    }

    KNNIterator* it_;
    Iterator* secondary_index_it_;
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
    return Status::InvalidArgument(e.what());
  }

  if (code_str.size() != index_->code_size) {
    return Status::InvalidArgument(
        "Code with unexpected size returned by fine quantizer");
  }

  secondary_value->emplace(std::move(code_str));

  return Status::OK();
}

std::unique_ptr<Iterator> FaissIVFIndex::NewIterator(
    const SecondaryIndexReadOptions& read_options,
    std::unique_ptr<Iterator>&& underlying_it) const {
  if (!read_options.similarity_search_neighbors.has_value() ||
      *read_options.similarity_search_neighbors == 0) {
    return std::unique_ptr<Iterator>(NewErrorIterator(
        Status::InvalidArgument("Invalid number of neighbors")));
  }

  if (!read_options.similarity_search_probes.has_value() ||
      *read_options.similarity_search_probes == 0) {
    return std::unique_ptr<Iterator>(
        NewErrorIterator(Status::InvalidArgument("Invalid number of probes")));
  }

  return std::make_unique<KNNIterator>(
      index_.get(), NewSecondaryIndexIterator(this, std::move(underlying_it)),
      *read_options.similarity_search_neighbors,
      *read_options.similarity_search_probes);
}

std::unique_ptr<FaissIVFIndex> NewFaissIVFIndex(
    std::unique_ptr<faiss::IndexIVF>&& index, std::string primary_column_name) {
  return std::make_unique<FaissIVFIndex>(std::move(index),
                                         std::move(primary_column_name));
}

}  // namespace ROCKSDB_NAMESPACE
