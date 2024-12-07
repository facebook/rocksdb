//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/secondary_index/faiss_ivf_index.h"

#include <cassert>

#include "faiss/invlists/InvertedLists.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

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

  // Iterator-based read interface; not yet implemented
  faiss::InvertedListsIterator* get_iterator(
      size_t /* list_no */,
      void* /* inverted_list_context */ = nullptr) const override {
    // TODO: implement this

    assert(false);
    return nullptr;
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
};

std::string FaissIVFIndex::SerializeLabel(faiss::idx_t label) {
  std::string label_str;
  PutVarsignedint64(&label_str, label);

  return label_str;
}

faiss::idx_t FaissIVFIndex::DeserializeLabel(Slice label_slice) {
  faiss::idx_t label = -1;
  [[maybe_unused]] const bool ok = GetVarsignedint64(&label_slice, &label);
  assert(ok);

  return label;
}

FaissIVFIndex::FaissIVFIndex(std::unique_ptr<faiss::IndexIVF>&& index,
                             std::string primary_column_name)
    : adapter_(std::make_unique<Adapter>(index->nlist, index->code_size)),
      index_(std::move(index)),
      primary_column_name_(std::move(primary_column_name)) {
  assert(index_);
  assert(index_->quantizer);

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

  if (primary_column_value.size() != index_->d * sizeof(float)) {
    return Status::InvalidArgument(
        "Incorrectly sized vector passed to FaissIVFIndex");
  }

  constexpr faiss::idx_t n = 1;
  faiss::idx_t label = -1;

  try {
    index_->quantizer->assign(
        n, reinterpret_cast<const float*>(primary_column_value.data()), &label);
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

Status FaissIVFIndex::GetSecondaryValue(
    const Slice& /* primary_key */, const Slice& primary_column_value,
    const Slice& original_column_value,
    std::optional<std::variant<Slice, std::string>>* secondary_value) const {
  assert(secondary_value);

  const faiss::idx_t label = DeserializeLabel(primary_column_value);
  assert(label >= 0);
  assert(label < index_->nlist);

  constexpr faiss::idx_t n = 1;
  constexpr faiss::idx_t* xids = nullptr;
  std::string code_str;

  try {
    index_->add_core(
        n, reinterpret_cast<const float*>(original_column_value.data()), xids,
        &label, &code_str);
  } catch (const std::exception& e) {
    return Status::InvalidArgument(e.what());
  }

  if (code_str.size() != index_->code_size) {
    return Status::InvalidArgument(
        "Unexpected code returned by fine quantizer");
  }

  secondary_value->emplace(std::move(code_str));

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
