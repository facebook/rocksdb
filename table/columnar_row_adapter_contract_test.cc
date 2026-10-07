//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <folly/io/IOBufQueue.h>
#include <folly/testing/TestUtil.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "rocks/nimblev2/NimbleKeyValueCodec.h"
#include "rocks/nimblev2/NimbleTableFactory.h"
#include "rocks/nimblev2/NimbleTableOptions.h"
#include "rocksdb/columnar_row_adapter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/types_util.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#ifndef ROCKSDB_USE_VELOX
#error "Columnar row adapter contract tests require ROCKSDB_USE_VELOX"
#endif

namespace facebook::rocks {
namespace {

constexpr rocksdb::ColumnarSchemaId kSchemaOne{
    .fingerprint = 0xC011'0001,
    .adapter_version = 1,
    .format_version = 1,
};

constexpr rocksdb::ColumnarSchemaId kSchemaTwo{
    .fingerprint = 0xC011'0002,
    .adapter_version = 1,
    .format_version = 1,
};

constexpr std::array<uint32_t, 3> kCompatibleVersions{{7, 8, 9}};

std::string toString(std::string_view value) {
  return std::string{value.data(), value.size()};
}

std::string toString(const rocksdb::Slice& value) {
  return std::string{value.data(), value.size()};
}

bool startsWith(std::string_view value, std::string_view prefix) {
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

std::optional<rocksdb::ColumnarSchemaId> schemaIdForKey(std::string_view key) {
  if (startsWith(key, "s1:")) {
    return kSchemaOne;
  }
  if (startsWith(key, "s2:")) {
    return kSchemaTwo;
  }
  return std::nullopt;
}

bool isCompatibleRowVersion(uint32_t rowVersion) {
  return std::find(kCompatibleVersions.begin(), kCompatibleVersions.end(),
                   rowVersion) != kCompatibleVersions.end();
}

std::vector<std::string> split(std::string_view value) {
  std::vector<std::string> parts;
  size_t start = 0;
  while (start <= value.size()) {
    const auto end = value.find('|', start);
    if (end == std::string_view::npos) {
      parts.push_back(toString(value.substr(start)));
      break;
    }
    parts.push_back(toString(value.substr(start, end - start)));
    start = end + 1;
  }
  return parts;
}

struct ParsedToyRow {
  uint32_t rowVersion = 0;
  std::string a;
  std::optional<std::string> b;
  std::string c;
  std::optional<std::string> d;
  bool dIsNull = false;
};

rocksdb::Status parseToyRow(const rocksdb::Slice& value, ParsedToyRow* output) {
  const auto parts = split(toString(value));
  if (parts.empty() || parts.front().size() < 2 ||
      parts.front().front() != 'v') {
    return rocksdb::Status::Corruption("missing row version");
  }

  uint32_t rowVersion = 0;
  try {
    rowVersion = static_cast<uint32_t>(std::stoul(parts.front().substr(1)));
  } catch (const std::exception&) {
    return rocksdb::Status::Corruption("invalid row version");
  }

  if (rowVersion == 7) {
    if (parts.size() != 4) {
      return rocksdb::Status::Corruption("bad version 7 row");
    }
    *output = ParsedToyRow{
        .rowVersion = rowVersion,
        .a = parts.at(1),
        .b = parts.at(2),
        .c = parts.at(3),
        .d = std::nullopt,
        .dIsNull = false,
    };
    return rocksdb::Status::OK();
  }

  if (rowVersion == 8) {
    if (parts.size() != 5) {
      return rocksdb::Status::Corruption("bad version 8 row");
    }
    *output = ParsedToyRow{
        .rowVersion = rowVersion,
        .a = parts.at(1),
        .b = parts.at(2),
        .c = parts.at(3),
        .d = parts.at(4) == "#NULL" ? std::nullopt
                                    : std::make_optional(parts.at(4)),
        .dIsNull = parts.at(4) == "#NULL",
    };
    return rocksdb::Status::OK();
  }

  if (rowVersion == 9) {
    if (parts.size() != 4) {
      return rocksdb::Status::Corruption("bad version 9 row");
    }
    *output = ParsedToyRow{
        .rowVersion = rowVersion,
        .a = parts.at(1),
        .b = std::nullopt,
        .c = parts.at(2),
        .d = parts.at(3),
        .dIsNull = false,
    };
    return rocksdb::Status::OK();
  }

  output->rowVersion = rowVersion;
  return rocksdb::Status::OK();
}

velox::RowTypePtr customerRowType() {
  return velox::ROW({"user_key", "a", "b", "c", "d", "row_version"},
                    {velox::VARBINARY(), velox::VARCHAR(), velox::VARCHAR(),
                     velox::VARCHAR(), velox::VARCHAR(), velox::INTEGER()});
}

velox::RowTypePtr userKeyRowType() {
  return velox::ROW({"user_key"}, {velox::VARBINARY()});
}

rocksdb::ColumnarFileLayout makeLayout(rocksdb::ColumnarSchemaId schemaId) {
  rocksdb::ColumnarFileSchema schema;
  schema.schema_id = schemaId;
  schema.data_row_type = customerRowType();
  schema.user_key_row_type = userKeyRowType();
  schema.compatible_row_versions.assign(kCompatibleVersions.begin(),
                                        kCompatibleVersions.end());
  schema.comparator_name = rocksdb::BytewiseComparator()->Name();
  schema.preserves_comparator_order = true;
  schema.supports_prefix_bounds = true;
  schema.columns = {
      rocksdb::ColumnarColumnDescriptor{
          .column_id = 1,
          .name = "user_key",
          .type = velox::VARBINARY(),
          .role = rocksdb::ColumnarColumnRole::kUserKey,
          .presence_encoding =
              rocksdb::ColumnarColumnPresenceEncoding::kAlwaysPresent,
      },
      rocksdb::ColumnarColumnDescriptor{
          .column_id = 2,
          .name = "a",
          .type = velox::VARCHAR(),
          .role = rocksdb::ColumnarColumnRole::kValue,
          .presence_encoding =
              rocksdb::ColumnarColumnPresenceEncoding::kAlwaysPresent,
      },
      rocksdb::ColumnarColumnDescriptor{
          .column_id = 3,
          .name = "b",
          .type = velox::VARCHAR(),
          .role = rocksdb::ColumnarColumnRole::kValue,
          .presence_encoding =
              rocksdb::ColumnarColumnPresenceEncoding::kDerivedFromRowVersion,
      },
      rocksdb::ColumnarColumnDescriptor{
          .column_id = 4,
          .name = "c",
          .type = velox::VARCHAR(),
          .role = rocksdb::ColumnarColumnRole::kValue,
          .presence_encoding =
              rocksdb::ColumnarColumnPresenceEncoding::kAlwaysPresent,
      },
      rocksdb::ColumnarColumnDescriptor{
          .column_id = 5,
          .name = "d",
          .type = velox::VARCHAR(),
          .role = rocksdb::ColumnarColumnRole::kValue,
          .presence_encoding =
              rocksdb::ColumnarColumnPresenceEncoding::kDerivedFromRowVersion,
      },
      rocksdb::ColumnarColumnDescriptor{
          .column_id = 6,
          .name = "row_version",
          .type = velox::INTEGER(),
          .role = rocksdb::ColumnarColumnRole::kRowVersion,
          .presence_encoding =
              rocksdb::ColumnarColumnPresenceEncoding::kAlwaysPresent,
      },
  };
  rocksdb::ColumnarFileLayout layout;
  layout.schemas.push_back(rocksdb::ColumnarSchemaInFile{
      .schema_ordinal = 0,
      .schema = std::move(schema),
  });
  return layout;
}

std::string nextPrefix(std::string_view prefix) {
  auto end = toString(prefix);
  for (auto iter = end.rbegin(); iter != end.rend(); ++iter) {
    if (static_cast<unsigned char>(*iter) != 0xff) {
      ++(*iter);
      end.resize(static_cast<size_t>(end.rend() - iter));
      return end;
    }
  }
  return {};
}

velox::RowVectorPtr makeKeyBoundsVector(const std::vector<std::string>& keys,
                                        velox::memory::MemoryPool* pool) {
  const auto size = static_cast<velox::vector_size_t>(keys.size());
  auto userKeys =
      velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
          velox::VARBINARY(), size, pool);
  for (velox::vector_size_t row = 0; row < size; ++row) {
    userKeys->set(row, velox::StringView{keys.at(row)});
  }
  return std::make_shared<velox::RowVector>(
      pool, userKeyRowType(), nullptr, size,
      std::vector<velox::VectorPtr>{std::move(userKeys)});
}

std::string readStringColumn(const velox::RowVectorPtr& rows,
                             velox::column_index_t column,
                             velox::vector_size_t row) {
  const auto* vector =
      rows->childAt(column)->as<velox::SimpleVector<velox::StringView>>();
  if (vector->isNullAt(row)) {
    return "";
  }
  const auto value = vector->valueAt(row);
  return std::string{value.data(), value.size()};
}

int32_t readIntColumn(const velox::RowVectorPtr& rows,
                      velox::column_index_t column, velox::vector_size_t row) {
  return rows->childAt(column)->as<velox::SimpleVector<int32_t>>()->valueAt(
      row);
}

void copySlicesToQueue(const rocksdb::Slice* slices, size_t numRows,
                       folly::IOBufQueue& storage,
                       std::vector<std::string_view>& output) {
  size_t numBytes = 0;
  for (size_t row = 0; row < numRows; ++row) {
    numBytes += slices[row].size();
  }

  const auto allocationSize = std::max<size_t>(numBytes, 1);
  auto [buffer, capacity] =
      storage.preallocate(allocationSize, allocationSize, allocationSize);
  if (capacity < allocationSize) {
    throw std::runtime_error("IOBufQueue preallocate returned short capacity");
  }
  auto* data = static_cast<char*>(buffer);
  size_t offset = 0;
  output.clear();
  output.reserve(numRows);
  for (size_t row = 0; row < numRows; ++row) {
    const auto& slice = slices[row];
    if (!slice.empty()) {
      std::memcpy(data + offset, slice.data(), slice.size());
    }
    output.emplace_back(data + offset, slice.size());
    offset += slice.size();
  }
  storage.postallocate(numBytes);
}

void copyKeysToQueue(const rocksdb::ColumnarReconstructedKey* reconstructedKeys,
                     size_t numRows, folly::IOBufQueue& storage,
                     std::vector<std::string_view>& output) {
  std::vector<rocksdb::Slice> slices;
  slices.reserve(numRows);
  for (size_t row = 0; row < numRows; ++row) {
    slices.push_back(reconstructedKeys[row].user_key);
  }
  copySlicesToQueue(slices.data(), slices.size(), storage, output);
}

class ToyColumnarFileWriterAdapter final
    : public rocksdb::ColumnarFileWriterAdapter {
 public:
  ToyColumnarFileWriterAdapter(rocksdb::ColumnarSchemaId schemaId,
                               velox::memory::MemoryPool* pool)
      : pool_(pool), layout_(makeLayout(schemaId)) {}

  void ResetBatch() override {
    rows_.reset();
    stringStorage_.clear();
  }

  rocksdb::Status ReleaseUnusedMemory() override {
    ResetBatch();
    stringStorage_.shrink_to_fit();
    rowVersionsSeen_.shrink_to_fit();
    return rocksdb::Status::OK();
  }

  uint64_t RetainedBytes() const override {
    uint64_t bytes = sizeof(*this);
    for (const auto& value : stringStorage_) {
      bytes += value.capacity();
    }
    return bytes;
  }

  const rocksdb::ColumnarFileLayout& GetFileLayout() const override {
    return layout_;
  }

  rocksdb::Status CheckCompatibility(
      const rocksdb::ColumnarRowSchema& rowSchema,
      rocksdb::ColumnarPlanCompatibility* compatibility) const override {
    if (rowSchema.schema_id != layout_.schemas.front().schema.schema_id) {
      *compatibility = rocksdb::ColumnarPlanCompatibility::kDifferentSchemaId;
      return rocksdb::Status::OK();
    }
    if (!isCompatibleRowVersion(rowSchema.row_version)) {
      *compatibility =
          rocksdb::ColumnarPlanCompatibility::kIncompatibleRowVersion;
      return rocksdb::Status::OK();
    }
    *compatibility = rocksdb::ColumnarPlanCompatibility::kCompatible;
    return rocksdb::Status::OK();
  }

  rocksdb::Status DecodeRows(const rocksdb::ColumnarKeyValueInputBatch& input,
                             rocksdb::ColumnarVectorBatch* output) override {
    if (pool_ == nullptr) {
      return rocksdb::Status::InvalidArgument("missing Velox memory pool");
    }
    if (input.entries == nullptr || input.values == nullptr) {
      return rocksdb::Status::InvalidArgument("missing input rows");
    }

    const auto size = static_cast<velox::vector_size_t>(input.num_rows);
    std::vector<ParsedToyRow> parsed;
    parsed.reserve(input.num_rows);
    stringStorage_.clear();
    rowVersionsSeen_.clear();
    rowVersionsSeen_.reserve(input.num_rows);

    for (size_t row = 0; row < input.num_rows; ++row) {
      rocksdb::ColumnarPlanCompatibility compatibility;
      if (input.row_schemas != nullptr) {
        auto status =
            CheckCompatibility(input.row_schemas[row], &compatibility);
        if (!status.ok()) {
          return status;
        }
        if (compatibility != rocksdb::ColumnarPlanCompatibility::kCompatible) {
          return rocksdb::Status::InvalidArgument(
              "row is incompatible with writer plan");
        }
      }

      ParsedToyRow parsedRow;
      auto status = parseToyRow(input.values[row], &parsedRow);
      if (!status.ok()) {
        return status;
      }
      if (!isCompatibleRowVersion(parsedRow.rowVersion)) {
        return rocksdb::Status::InvalidArgument("incompatible row version");
      }
      rowVersionsSeen_.push_back(parsedRow.rowVersion);
      parsed.push_back(std::move(parsedRow));
    }

    auto userKeys =
        velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
            velox::VARBINARY(), size, pool_);
    auto a = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARCHAR(), size, pool_);
    auto b = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARCHAR(), size, pool_);
    auto c = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARCHAR(), size, pool_);
    auto d = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARCHAR(), size, pool_);
    auto rowVersions = velox::BaseVector::create<velox::FlatVector<int32_t>>(
        velox::INTEGER(), size, pool_);

    const auto copyString = [&](std::string value) {
      stringStorage_.push_back(std::move(value));
      return velox::StringView{stringStorage_.back()};
    };

    for (velox::vector_size_t row = 0; row < size; ++row) {
      const auto index = static_cast<size_t>(row);
      userKeys->set(row, copyString(toString(input.entries[index].user_key)));
      a->set(row, copyString(parsed[index].a));
      if (parsed[index].b.has_value()) {
        b->set(row, copyString(*parsed[index].b));
      } else {
        b->setNull(row, true);
      }
      c->set(row, copyString(parsed[index].c));
      if (parsed[index].d.has_value()) {
        d->set(row, copyString(*parsed[index].d));
      } else {
        d->setNull(row, true);
      }
      rowVersions->set(row, static_cast<int32_t>(parsed[index].rowVersion));
    }

    rows_ = std::make_shared<velox::RowVector>(
        pool_, customerRowType(), nullptr, size,
        std::vector<velox::VectorPtr>{std::move(userKeys), std::move(a),
                                      std::move(b), std::move(c), std::move(d),
                                      std::move(rowVersions)});
    *output = rocksdb::ColumnarVectorBatch{
        .schema_ordinal = 0,
        .rows = rows_,
    };
    return rocksdb::Status::OK();
  }

  rocksdb::Status FinishFile(rocksdb::ColumnarFileMetadata* metadata) override {
    metadata->file_layout = layout_;
    metadata->row_versions_seen = rowVersionsSeen_;
    metadata->serialized_adapter_metadata = "toy-adapter-v1";
    return rocksdb::Status::OK();
  }

 private:
  velox::memory::MemoryPool* pool_;
  rocksdb::ColumnarFileLayout layout_;
  velox::RowVectorPtr rows_;
  std::deque<std::string> stringStorage_;
  std::vector<uint32_t> rowVersionsSeen_;
};

class ToyColumnarFileReaderAdapter final
    : public rocksdb::ColumnarFileReaderAdapter {
 public:
  ToyColumnarFileReaderAdapter(rocksdb::ColumnarFileLayout layout,
                               velox::memory::MemoryPool* pool,
                               bool reserveInternalKeyTrailerSpace)
      : layout_(std::move(layout)),
        pool_(pool),
        reserveInternalKeyTrailerSpace_(reserveInternalKeyTrailerSpace) {}

  void ResetBatch() override {
    lowerBoundStorage_.clear();
    upperBoundStorage_.clear();
    keyStorage_.clear();
    valueStorage_.clear();
    keys_.clear();
    values_.clear();
  }

  rocksdb::Status ReleaseUnusedMemory() override {
    ResetBatch();
    lowerBoundStorage_.shrink_to_fit();
    upperBoundStorage_.shrink_to_fit();
    keyStorage_.shrink_to_fit();
    valueStorage_.shrink_to_fit();
    keys_.shrink_to_fit();
    values_.shrink_to_fit();
    return rocksdb::Status::OK();
  }

  uint64_t RetainedBytes() const override {
    uint64_t bytes = sizeof(*this);
    for (const auto& key : keyStorage_) {
      bytes += key.capacity();
    }
    for (const auto& value : valueStorage_) {
      bytes += value.capacity();
    }
    return bytes;
  }

  const rocksdb::ColumnarFileLayout& GetFileLayout() const override {
    return layout_;
  }

  rocksdb::Status ProjectKeyBounds(
      const rocksdb::ColumnarKeyProjectionRequest& request,
      rocksdb::ColumnarProjectedKeyBounds* output) override {
    if (pool_ == nullptr) {
      return rocksdb::Status::InvalidArgument("missing Velox memory pool");
    }
    if (request.user_keys == nullptr && request.num_keys != 0) {
      return rocksdb::Status::InvalidArgument("missing user keys");
    }

    lowerBoundStorage_.clear();
    upperBoundStorage_.clear();
    lowerBoundStorage_.reserve(request.num_keys);
    upperBoundStorage_.reserve(request.num_keys);
    std::vector<rocksdb::ColumnarKeyBoundKind> kinds;
    kinds.reserve(request.num_keys);
    for (size_t row = 0; row < request.num_keys; ++row) {
      const auto userKey = toString(request.user_keys[row]);
      lowerBoundStorage_.push_back(userKey);
      switch (request.purpose) {
        case rocksdb::ColumnarKeyProjectionPurpose::kPointLookup:
          upperBoundStorage_.push_back(userKey);
          kinds.push_back(rocksdb::ColumnarKeyBoundKind::kPoint);
          break;
        case rocksdb::ColumnarKeyProjectionPurpose::kSeek:
          upperBoundStorage_.emplace_back();
          kinds.push_back(rocksdb::ColumnarKeyBoundKind::kLowerBound);
          break;
        case rocksdb::ColumnarKeyProjectionPurpose::kSeekForPrev:
          upperBoundStorage_.push_back(userKey);
          kinds.push_back(rocksdb::ColumnarKeyBoundKind::kUpperBound);
          break;
        case rocksdb::ColumnarKeyProjectionPurpose::kPrefixScan:
          upperBoundStorage_.push_back(nextPrefix(userKey));
          kinds.push_back(rocksdb::ColumnarKeyBoundKind::kHalfOpenRange);
          break;
      }
    }

    rocksdb::ColumnarProjectedKeyBoundsForSchema bounds;
    bounds.schema_ordinal = 0;
    bounds.lower_bound_rows = makeKeyBoundsVector(lowerBoundStorage_, pool_);
    bounds.upper_bound_rows = makeKeyBoundsVector(upperBoundStorage_, pool_);
    bounds.bound_kinds = std::move(kinds);

    output->bounds_by_schema.clear();
    output->bounds_by_schema.push_back(std::move(bounds));
    return rocksdb::Status::OK();
  }

  rocksdb::Status EncodeRows(
      const rocksdb::ColumnarVectorBatch& input,
      const rocksdb::ColumnarInternalKeyFieldsBatch& systemFields,
      const rocksdb::ColumnarInternalKeyFinisher* internalKeyFinisher,
      rocksdb::ColumnarKeyValueOutputBatch* output) override {
    if (input.schema_ordinal != 0) {
      return rocksdb::Status::InvalidArgument("unknown schema ordinal");
    }
    if (input.rows == nullptr) {
      return rocksdb::Status::InvalidArgument("missing row vector");
    }
    if (systemFields.fields != nullptr &&
        systemFields.num_rows != static_cast<size_t>(input.rows->size())) {
      return rocksdb::Status::InvalidArgument("bad system field count");
    }

    keyStorage_.assign(input.rows->size(), std::string{});
    valueStorage_.assign(input.rows->size(), std::string{});
    keys_.assign(input.rows->size(), rocksdb::ColumnarReconstructedKey{});
    values_.assign(input.rows->size(), rocksdb::Slice{});

    for (velox::vector_size_t row = 0; row < input.rows->size(); ++row) {
      const auto index = static_cast<size_t>(row);
      const auto userKey = readStringColumn(input.rows, 0, row);
      const auto rowVersion =
          static_cast<uint32_t>(readIntColumn(input.rows, 5, row));
      keyStorage_[index] = userKey;
      if (reserveInternalKeyTrailerSpace_ || internalKeyFinisher != nullptr) {
        keyStorage_[index].resize(
            userKey.size() + rocksdb::kColumnarInternalKeyTrailerSize, '\0');
      }

      keys_[index].user_key =
          rocksdb::Slice{keyStorage_[index].data(), userKey.size()};
      if (keyStorage_[index].size() >=
          userKey.size() + rocksdb::kColumnarInternalKeyTrailerSize) {
        keys_[index].internal_key_trailer =
            keyStorage_[index].data() + userKey.size();
      }

      valueStorage_[index] = encodeValue(input.rows, rowVersion, row);
      values_[index] = rocksdb::Slice{valueStorage_[index]};

      if (internalKeyFinisher != nullptr) {
        rocksdb::ColumnarInternalKeyFields fields{
            .sequence = 0,
            .type = rocksdb::kEntryPut,
        };
        if (systemFields.fields != nullptr) {
          fields = systemFields.fields[index];
        }
        auto status = internalKeyFinisher->FinishKey(fields, &keys_[index]);
        if (!status.ok()) {
          return status;
        }
      }
    }

    *output = rocksdb::ColumnarKeyValueOutputBatch{
        .keys = keys_.data(),
        .values = values_.data(),
        .num_rows = static_cast<size_t>(input.rows->size()),
    };
    return rocksdb::Status::OK();
  }

 private:
  static std::string encodeValue(const velox::RowVectorPtr& rows,
                                 uint32_t rowVersion,
                                 velox::vector_size_t row) {
    const auto a = readStringColumn(rows, 1, row);
    const auto bIsNull = rows->childAt(2)->isNullAt(row);
    const auto c = readStringColumn(rows, 3, row);
    const auto dIsNull = rows->childAt(4)->isNullAt(row);

    if (rowVersion == 7) {
      return "v7|" + a + "|" + (bIsNull ? "" : readStringColumn(rows, 2, row)) +
             "|" + c;
    }
    if (rowVersion == 8) {
      return "v8|" + a + "|" + (bIsNull ? "" : readStringColumn(rows, 2, row)) +
             "|" + c + "|" +
             (dIsNull ? "#NULL" : readStringColumn(rows, 4, row));
    }
    if (rowVersion == 9) {
      return "v9|" + a + "|" + c + "|" +
             (dIsNull ? "" : readStringColumn(rows, 4, row));
    }
    throw std::runtime_error("unexpected row version");
  }

  rocksdb::ColumnarFileLayout layout_;
  velox::memory::MemoryPool* pool_;
  bool reserveInternalKeyTrailerSpace_;
  std::vector<std::string> lowerBoundStorage_;
  std::vector<std::string> upperBoundStorage_;
  std::vector<std::string> keyStorage_;
  std::vector<std::string> valueStorage_;
  std::vector<rocksdb::ColumnarReconstructedKey> keys_;
  std::vector<rocksdb::Slice> values_;
};

class ToyColumnarRowAdapterFactory final
    : public rocksdb::ColumnarRowAdapterFactory {
 public:
  rocksdb::Status ClassifyRow(
      const rocksdb::ParsedEntryInfo& entry, const rocksdb::Slice& value,
      rocksdb::ColumnarRowClassification* classification) const override {
    const auto schemaId = schemaIdForKey(toString(entry.user_key));
    if (!schemaId.has_value()) {
      *classification = rocksdb::ColumnarRowClassification{
          .eligibility = rocksdb::ColumnarRowEligibility::kFallbackToBlockBased,
      };
      return rocksdb::Status::OK();
    }

    ParsedToyRow parsed;
    auto status = parseToyRow(value, &parsed);
    if (!status.ok()) {
      return status;
    }

    *classification = rocksdb::ColumnarRowClassification{
        .eligibility = rocksdb::ColumnarRowEligibility::kEligible,
        .row_schema =
            rocksdb::ColumnarRowSchema{
                .schema_id = *schemaId,
                .row_version = parsed.rowVersion,
            },
    };
    return rocksdb::Status::OK();
  }

  rocksdb::Status NewWriter(const rocksdb::ColumnarRowSchema& firstRowSchema,
                            const rocksdb::ColumnarWriteOpenContext& context,
                            std::unique_ptr<rocksdb::ColumnarFileWriterAdapter>*
                                writer) const override {
    if (!isCompatibleRowVersion(firstRowSchema.row_version)) {
      return rocksdb::Status::InvalidArgument("incompatible first row");
    }
    writer->reset(new ToyColumnarFileWriterAdapter(firstRowSchema.schema_id,
                                                   context.memory.pool));
    return rocksdb::Status::OK();
  }

  rocksdb::Status NewReader(const rocksdb::ColumnarFileMetadata& metadata,
                            const rocksdb::ColumnarReadOpenContext& context,
                            std::unique_ptr<rocksdb::ColumnarFileReaderAdapter>*
                                reader) const override {
    if (metadata.file_layout.schemas.size() != 1) {
      return rocksdb::Status::InvalidArgument("expected one schema");
    }
    const auto& schema = metadata.file_layout.schemas.front().schema;
    if (!schema.preserves_comparator_order) {
      return rocksdb::Status::InvalidArgument("schema is not order preserving");
    }
    if (context.user_comparator != nullptr &&
        schema.comparator_name != context.user_comparator->Name()) {
      return rocksdb::Status::InvalidArgument("comparator mismatch");
    }
    reader->reset(new ToyColumnarFileReaderAdapter(
        metadata.file_layout, context.memory.pool,
        context.reserve_internal_key_trailer_space));
    return rocksdb::Status::OK();
  }
};

rocksdb::ParsedEntryInfo parsedPutEntry(std::string_view key) {
  return rocksdb::ParsedEntryInfo{
      .user_key = rocksdb::Slice{key.data(), key.size()},
      .timestamp = rocksdb::Slice{},
      .sequence = 0,
      .type = rocksdb::kEntryPut,
  };
}

velox::RowVectorPtr addRocksDbKeyColumn(
    const std::vector<std::string_view>& keys,
    const velox::RowVectorPtr& customerRows, velox::memory::MemoryPool& pool) {
  const auto size = static_cast<velox::vector_size_t>(keys.size());
  auto rocksKeys =
      velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
          velox::VARBINARY(), size, &pool);
  for (velox::vector_size_t row = 0; row < size; ++row) {
    rocksKeys->set(row, velox::StringView{keys.at(row)});
  }

  auto customerType =
      std::dynamic_pointer_cast<const velox::RowType>(customerRows->type());
  std::vector<std::string> names{std::string{kRocksDbKeyColumnName}};
  std::vector<velox::TypePtr> types{velox::VARBINARY()};
  std::vector<velox::VectorPtr> children{std::move(rocksKeys)};
  for (velox::column_index_t column = 0; column < customerRows->childrenSize();
       ++column) {
    names.push_back(customerType->nameOf(column));
    types.push_back(customerType->childAt(column));
    children.push_back(customerRows->childAt(column));
  }

  return std::make_shared<velox::RowVector>(
      &pool, velox::ROW(std::move(names), std::move(types)), nullptr, size,
      std::move(children));
}

velox::RowVectorPtr customerRowsFromNimbleRows(
    const velox::RowVectorPtr& rows) {
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(rows->type());
  if (rowType == nullptr || rowType->size() == 0) {
    throw std::runtime_error("Nimble row type is missing columns");
  }
  if (rowType->nameOf(0) == "user_key") {
    return rows;
  }
  if (rowType->nameOf(0) != kRocksDbKeyColumnName) {
    throw std::runtime_error(
        "Nimble row type is missing RocksDB and user key columns");
  }

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  std::vector<velox::VectorPtr> children;
  names.reserve(rowType->size() - 1);
  types.reserve(rowType->size() - 1);
  children.reserve(rowType->size() - 1);
  for (velox::column_index_t column = 1; column < rowType->size(); ++column) {
    names.push_back(rowType->nameOf(column));
    types.push_back(rowType->childAt(column));
    children.push_back(rows->childAt(column));
  }
  return std::make_shared<velox::RowVector>(
      rows->pool(), velox::ROW(std::move(names), std::move(types)), nullptr,
      rows->size(), std::move(children));
}

class AdapterBackedNimbleEncoder final : public NimbleKeyValueEncoder {
 public:
  explicit AdapterBackedNimbleEncoder(
      std::shared_ptr<const ToyColumnarRowAdapterFactory> adapter)
      : adapter_(std::move(adapter)) {}

  velox::RowVectorPtr encode(const std::vector<std::string_view>& keys,
                             const std::vector<std::string_view>& values,
                             velox::memory::MemoryPool& pool) const override {
    if (keys.size() != values.size()) {
      throw std::runtime_error("key/value batch size mismatch");
    }

    std::vector<rocksdb::ParsedEntryInfo> entries;
    std::vector<rocksdb::Slice> valueSlices;
    std::vector<rocksdb::ColumnarRowSchema> rowSchemas;
    entries.reserve(keys.size());
    valueSlices.reserve(values.size());
    rowSchemas.reserve(keys.size());

    std::unique_ptr<rocksdb::ColumnarFileWriterAdapter> writer;
    for (size_t row = 0; row < keys.size(); ++row) {
      entries.push_back(parsedPutEntry(keys[row]));
      valueSlices.emplace_back(values[row].data(), values[row].size());

      rocksdb::ColumnarRowClassification classification;
      auto status = adapter_->ClassifyRow(entries.back(), valueSlices.back(),
                                          &classification);
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }
      if (classification.eligibility !=
          rocksdb::ColumnarRowEligibility::kEligible) {
        throw std::runtime_error("row is not eligible for Nimble encoding");
      }

      if (writer == nullptr) {
        rocksdb::ColumnarWriteOpenContext context{
            .user_comparator = rocksdb::BytewiseComparator(),
            .prefix_extractor = nullptr,
            .memory =
                rocksdb::ColumnarAdapterMemoryContext{
                    .pool = &pool,
                    .memory_budget_bytes = 1 << 20,
                },
        };
        status =
            adapter_->NewWriter(classification.row_schema, context, &writer);
        if (!status.ok()) {
          throw std::runtime_error(status.ToString());
        }
      } else {
        rocksdb::ColumnarPlanCompatibility compatibility;
        status = writer->CheckCompatibility(classification.row_schema,
                                            &compatibility);
        if (!status.ok()) {
          throw std::runtime_error(status.ToString());
        }
        if (compatibility != rocksdb::ColumnarPlanCompatibility::kCompatible) {
          throw std::runtime_error("row requires a separate columnar file");
        }
      }
      rowSchemas.push_back(classification.row_schema);
    }

    rocksdb::ColumnarVectorBatch customerRows;
    const rocksdb::ColumnarKeyValueInputBatch input{
        .entries = entries.data(),
        .values = valueSlices.data(),
        .row_schemas = rowSchemas.data(),
        .num_rows = keys.size(),
    };
    auto status = writer->DecodeRows(input, &customerRows);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }
    return addRocksDbKeyColumn(keys, customerRows.rows, pool);
  }

 private:
  std::shared_ptr<const ToyColumnarRowAdapterFactory> adapter_;
};

class AdapterBackedNimbleDecoder final : public NimbleKeyValueDecoder {
 public:
  explicit AdapterBackedNimbleDecoder(
      std::shared_ptr<const ToyColumnarRowAdapterFactory> adapter)
      : adapter_(std::move(adapter)) {}

  bool decodeRequiresClusterIndexKeys() const override { return false; }

  void decode(const velox::RowVectorPtr& rows,
              const std::vector<std::string_view>&,
              folly::IOBufQueue& keyStorage, folly::IOBufQueue& valueStorage,
              std::vector<std::string_view>& keys,
              std::vector<std::string_view>& values) const override {
    rocksdb::ColumnarFileMetadata metadata;
    metadata.file_layout = makeLayout(kSchemaOne);
    rocksdb::ColumnarReadOpenContext context{
        .user_comparator = rocksdb::BytewiseComparator(),
        .prefix_extractor = nullptr,
        .memory =
            rocksdb::ColumnarAdapterMemoryContext{
                .pool = rows->pool(),
                .memory_budget_bytes = 1 << 20,
            },
        .reserve_internal_key_trailer_space = false,
        .prefer_flat_output_buffers = true,
    };
    std::unique_ptr<rocksdb::ColumnarFileReaderAdapter> reader;
    auto status = adapter_->NewReader(metadata, context, &reader);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }

    std::vector<rocksdb::ColumnarInternalKeyFields> systemFields(
        rows->size(), rocksdb::ColumnarInternalKeyFields{
                          .sequence = 0,
                          .type = rocksdb::kEntryPut,
                      });
    auto customerRowVector = customerRowsFromNimbleRows(rows);
    const rocksdb::ColumnarVectorBatch customerRows{
        .schema_ordinal = 0,
        .rows = std::move(customerRowVector),
    };
    const rocksdb::ColumnarInternalKeyFieldsBatch fields{
        .fields = systemFields.data(),
        .num_rows = systemFields.size(),
    };
    rocksdb::ColumnarKeyValueOutputBatch output;
    status = reader->EncodeRows(customerRows, fields, nullptr, &output);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }

    copyKeysToQueue(output.keys, output.num_rows, keyStorage, keys);
    copySlicesToQueue(output.values, output.num_rows, valueStorage, values);
  }

 private:
  std::shared_ptr<const ToyColumnarRowAdapterFactory> adapter_;
};

class AdapterBackedNimbleCodecFactory final
    : public NimbleKeyValueCodecFactory {
 public:
  explicit AdapterBackedNimbleCodecFactory(
      std::shared_ptr<const ToyColumnarRowAdapterFactory> adapter)
      : adapter_(std::move(adapter)) {}

  rocksdb::Status createEncoder(
      std::unique_ptr<NimbleKeyValueEncoder>* output) const override {
    *output = std::make_unique<AdapterBackedNimbleEncoder>(adapter_);
    return rocksdb::Status::OK();
  }

  rocksdb::Status createDecoder(
      const velox::RowTypePtr&, const std::map<std::string, std::string>&,
      std::unique_ptr<NimbleKeyValueDecoder>* output) const override {
    *output = std::make_unique<AdapterBackedNimbleDecoder>(adapter_);
    return rocksdb::Status::OK();
  }

 private:
  std::shared_ptr<const ToyColumnarRowAdapterFactory> adapter_;
};

uint8_t valueTypeForEntryType(rocksdb::EntryType type) {
  switch (type) {
    case rocksdb::kEntryPut:
      return 0x1;
    case rocksdb::kEntryDelete:
      return 0x0;
    case rocksdb::kEntrySingleDelete:
      return 0x7;
    case rocksdb::kEntryMerge:
      return 0x2;
    case rocksdb::kEntryBlobIndex:
      return 0x11;
    case rocksdb::kEntryDeleteWithTimestamp:
      return 0x14;
    case rocksdb::kEntryWideColumnEntity:
      return 0x16;
    case rocksdb::kEntryRangeDeletion:
    case rocksdb::kEntryTimedPut:
    case rocksdb::kEntryOther:
      return 0x19;
  }
  return 0x19;
}

void encodeFixed64(char* output, uint64_t value) {
  for (size_t index = 0; index < sizeof(value); ++index) {
    output[index] = static_cast<char>(value & 0xff);
    value >>= 8;
  }
}

class TestInternalKeyFinisher final
    : public rocksdb::ColumnarInternalKeyFinisher {
 public:
  rocksdb::Status FinishKey(
      const rocksdb::ColumnarInternalKeyFields& fields,
      rocksdb::ColumnarReconstructedKey* key) const override {
    if (key->internal_key_trailer == nullptr) {
      return rocksdb::Status::InvalidArgument("missing trailer storage");
    }
    encodeFixed64(key->internal_key_trailer,
                  (fields.sequence << 8) | valueTypeForEntryType(fields.type));
    key->internal_key = rocksdb::Slice{
        key->user_key.data(),
        key->user_key.size() + rocksdb::kColumnarInternalKeyTrailerSize};
    return rocksdb::Status::OK();
  }
};

class ColumnarRowAdapterContractTest : public testing::Test,
                                       public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  rocksdb::Options makeOptions(
      std::shared_ptr<const ToyColumnarRowAdapterFactory> adapter) {
    NimbleTableOptions nimbleOptions;
    nimbleOptions.maxBytesPerBatch = 1 << 20;
    nimbleOptions.valueCodecFactory =
        std::make_shared<const AdapterBackedNimbleCodecFactory>(
            std::move(adapter));

    rocksdb::Options options;
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.max_open_files = -1;
    options.table_factory = NewNimbleTableFactory(std::move(nimbleOptions));
    return options;
  }

  void writeTable(
      const std::string& path, const rocksdb::Options& options,
      const std::vector<std::pair<std::string, std::string>>& rows) {
    rocksdb::SstFileWriter writer{rocksdb::EnvOptions{options}, options};
    auto status = writer.Open(path);
    ASSERT_TRUE(status.ok()) << status.ToString();
    for (const auto& [key, value] : rows) {
      status = writer.Put(key, value);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
    status = writer.Finish();
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  std::unique_ptr<rocksdb::DB> openDb(const rocksdb::Options& options,
                                      const std::string& path) {
    std::unique_ptr<rocksdb::DB> db;
    auto status = rocksdb::DB::Open(options, path, &db);
    EXPECT_TRUE(status.ok()) << status.ToString();
    return db;
  }

  void ingestFiles(rocksdb::DB& db, const std::vector<std::string>& paths) {
    rocksdb::IngestExternalFileOptions ingestOptions;
    ingestOptions.allow_db_generated_files = true;
    auto status = db.IngestExternalFile(paths, ingestOptions);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  void expectGetRows(
      rocksdb::DB& db,
      const std::vector<std::pair<std::string, std::string>>& rows) {
    for (const auto& [key, expectedValue] : rows) {
      std::string value;
      const auto status = db.Get({}, key, &value);
      ASSERT_TRUE(status.ok()) << status.ToString();
      EXPECT_EQ(value, expectedValue);
    }
  }

  void expectMultiGetRows(
      rocksdb::DB& db,
      const std::vector<std::pair<std::string, std::string>>& rows) {
    std::vector<rocksdb::Slice> keySlices;
    std::vector<std::string> values(rows.size());
    keySlices.reserve(rows.size());
    for (const auto& [key, _] : rows) {
      keySlices.emplace_back(key);
    }
    const auto statuses = db.MultiGet({}, keySlices, &values);
    std::vector<std::string> expectedValues;
    expectedValues.reserve(rows.size());
    for (const auto& [_, value] : rows) {
      expectedValues.push_back(value);
    }
    ASSERT_EQ(statuses.size(), rows.size());
    for (const auto& status : statuses) {
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
    EXPECT_EQ(values, expectedValues);
  }

  std::vector<std::pair<std::string, std::string>> scanRows(
      rocksdb::DB& db, const rocksdb::ReadOptions& readOptions,
      std::string_view startKey) {
    std::vector<std::pair<std::string, std::string>> rows;
    std::unique_ptr<rocksdb::Iterator> iterator(db.NewIterator(readOptions));
    iterator->Seek(rocksdb::Slice{startKey.data(), startKey.size()});
    while (iterator->Valid()) {
      rows.emplace_back(iterator->key().ToString(),
                        iterator->value().ToString());
      iterator->Next();
    }
    EXPECT_TRUE(iterator->status().ok()) << iterator->status().ToString();
    return rows;
  }

  folly::test::TemporaryDirectory tempDirectory_;
};

TEST_F(ColumnarRowAdapterContractTest,
       RoundTripsAddDropAndNullColumnsThroughNimbleExternalTable) {
  const std::vector<std::pair<std::string, std::string>> rows{
      {"s1:a:001", "v7|A1|B1|C1"},
      {"s1:a:002", "v8|A2|B2|C2|D2"},
      {"s1:a:003", "v8|A3|B3|C3|#NULL"},
      {"s1:b:001", "v9|A4|C4|D4"},
  };
  auto adapter = std::make_shared<const ToyColumnarRowAdapterFactory>();
  auto options = makeOptions(adapter);
  options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(5));

  const auto tablePath =
      (tempDirectory_.path() / "schema_evolution.sst").string();
  writeTable(tablePath, options, rows);

  rocksdb::SstFileReader fileReader{options};
  auto status = fileReader.Open(tablePath);
  ASSERT_TRUE(status.ok()) << status.ToString();
  for (const auto& [key, expectedValue] : rows) {
    std::string value;
    status = fileReader.Get({}, key, &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(value, expectedValue);
  }

  const auto dbPath = (tempDirectory_.path() / "db").string();
  auto db = openDb(options, dbPath);
  ASSERT_NE(db, nullptr);
  ingestFiles(*db, {tablePath});

  expectGetRows(*db, rows);
  expectMultiGetRows(*db, rows);

  EXPECT_EQ(scanRows(*db, rocksdb::ReadOptions{}, rows.front().first), rows);

  rocksdb::ReadOptions prefixReadOptions;
  prefixReadOptions.prefix_same_as_start = true;
  const std::vector<std::pair<std::string, std::string>> expectedPrefixRows{
      rows.begin(), rows.begin() + 3};
  EXPECT_EQ(scanRows(*db, prefixReadOptions, "s1:a:"), expectedPrefixRows);

  ASSERT_TRUE(db->Close().ok());
}

TEST_F(ColumnarRowAdapterContractTest,
       DifferentCompatibleSchemasAreStoredInSeparateNimbleFiles) {
  const std::vector<std::pair<std::string, std::string>> schemaOneRows{
      {"s1:a:001", "v7|A1|B1|C1"},
      {"s1:a:002", "v8|A2|B2|C2|D2"},
  };
  const std::vector<std::pair<std::string, std::string>> schemaTwoRows{
      {"s2:a:001", "v7|E1|F1|G1"},
      {"s2:a:002", "v9|E2|G2|H2"},
  };
  auto adapter = std::make_shared<const ToyColumnarRowAdapterFactory>();
  const auto options = makeOptions(adapter);

  const std::vector<std::string> tablePaths{
      (tempDirectory_.path() / "schema_one.sst").string(),
      (tempDirectory_.path() / "schema_two.sst").string(),
  };
  writeTable(tablePaths.at(0), options, schemaOneRows);
  writeTable(tablePaths.at(1), options, schemaTwoRows);

  const auto dbPath = (tempDirectory_.path() / "multi_schema_db").string();
  auto db = openDb(options, dbPath);
  ASSERT_NE(db, nullptr);
  ingestFiles(*db, tablePaths);

  auto expectedRows = schemaOneRows;
  expectedRows.insert(expectedRows.end(), schemaTwoRows.begin(),
                      schemaTwoRows.end());
  expectGetRows(*db, expectedRows);
  expectMultiGetRows(*db, expectedRows);
  EXPECT_EQ(scanRows(*db, rocksdb::ReadOptions{}, expectedRows.front().first),
            expectedRows);
  ASSERT_TRUE(db->Close().ok());
}

TEST_F(ColumnarRowAdapterContractTest,
       ClassifiesFallbackDifferentSchemaAndIncompatibleVersions) {
  ToyColumnarRowAdapterFactory adapter;
  auto pool = pool_.get();
  const std::string s1Key{"s1:a:001"};
  const std::string s2Key{"s2:a:001"};
  const std::string rawKey{"raw:a:001"};
  const std::string v7{"v7|A|B|C"};
  const std::string v8{"v8|A|B|C|D"};
  const std::string v10{"v10|A|B|C|D"};

  rocksdb::ColumnarRowClassification firstClassification;
  auto status = adapter.ClassifyRow(parsedPutEntry(s1Key), rocksdb::Slice{v7},
                                    &firstClassification);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(firstClassification.eligibility,
            rocksdb::ColumnarRowEligibility::kEligible);
  EXPECT_EQ(firstClassification.row_schema.schema_id, kSchemaOne);
  EXPECT_EQ(firstClassification.row_schema.row_version, 7);

  rocksdb::ColumnarWriteOpenContext writeContext{
      .user_comparator = rocksdb::BytewiseComparator(),
      .prefix_extractor = nullptr,
      .memory =
          rocksdb::ColumnarAdapterMemoryContext{
              .pool = pool,
              .memory_budget_bytes = 1 << 20,
          },
  };
  std::unique_ptr<rocksdb::ColumnarFileWriterAdapter> writer;
  status =
      adapter.NewWriter(firstClassification.row_schema, writeContext, &writer);
  ASSERT_TRUE(status.ok()) << status.ToString();

  rocksdb::ColumnarRowClassification compatibleClassification;
  status = adapter.ClassifyRow(parsedPutEntry(s1Key), rocksdb::Slice{v8},
                               &compatibleClassification);
  ASSERT_TRUE(status.ok()) << status.ToString();
  rocksdb::ColumnarPlanCompatibility compatibility;
  status = writer->CheckCompatibility(compatibleClassification.row_schema,
                                      &compatibility);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(compatibility, rocksdb::ColumnarPlanCompatibility::kCompatible);

  rocksdb::ColumnarRowClassification differentSchemaClassification;
  status = adapter.ClassifyRow(parsedPutEntry(s2Key), rocksdb::Slice{v8},
                               &differentSchemaClassification);
  ASSERT_TRUE(status.ok()) << status.ToString();
  status = writer->CheckCompatibility(differentSchemaClassification.row_schema,
                                      &compatibility);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(compatibility,
            rocksdb::ColumnarPlanCompatibility::kDifferentSchemaId);

  rocksdb::ColumnarRowClassification incompatibleClassification;
  status = adapter.ClassifyRow(parsedPutEntry(s1Key), rocksdb::Slice{v10},
                               &incompatibleClassification);
  ASSERT_TRUE(status.ok()) << status.ToString();
  status = writer->CheckCompatibility(incompatibleClassification.row_schema,
                                      &compatibility);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(compatibility,
            rocksdb::ColumnarPlanCompatibility::kIncompatibleRowVersion);

  rocksdb::ColumnarRowClassification fallbackClassification;
  status = adapter.ClassifyRow(parsedPutEntry(rawKey), rocksdb::Slice{"opaque"},
                               &fallbackClassification);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(fallbackClassification.eligibility,
            rocksdb::ColumnarRowEligibility::kFallbackToBlockBased);
}

TEST_F(ColumnarRowAdapterContractTest,
       ProjectsPointSeekAndPrefixBoundsForNimbleClusterIndex) {
  ToyColumnarRowAdapterFactory adapter;
  rocksdb::ColumnarFileMetadata metadata;
  metadata.file_layout = makeLayout(kSchemaOne);
  std::unique_ptr<const rocksdb::SliceTransform> prefixExtractor{
      rocksdb::NewFixedPrefixTransform(5)};
  rocksdb::ColumnarReadOpenContext readContext{
      .user_comparator = rocksdb::BytewiseComparator(),
      .prefix_extractor = prefixExtractor.get(),
      .memory =
          rocksdb::ColumnarAdapterMemoryContext{
              .pool = pool_.get(),
              .memory_budget_bytes = 1 << 20,
          },
      .reserve_internal_key_trailer_space = true,
      .prefer_flat_output_buffers = true,
  };
  std::unique_ptr<rocksdb::ColumnarFileReaderAdapter> reader;
  auto status = adapter.NewReader(metadata, readContext, &reader);
  ASSERT_TRUE(status.ok()) << status.ToString();

  const std::string pointKey{"s1:a:002"};
  const rocksdb::Slice pointKeySlice{pointKey};
  rocksdb::ColumnarProjectedKeyBounds projected;
  status = reader->ProjectKeyBounds(
      rocksdb::ColumnarKeyProjectionRequest{
          .purpose = rocksdb::ColumnarKeyProjectionPurpose::kPointLookup,
          .user_keys = &pointKeySlice,
          .num_keys = 1,
      },
      &projected);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(projected.bounds_by_schema.size(), 1);
  EXPECT_EQ(projected.bounds_by_schema.front().bound_kinds,
            std::vector<rocksdb::ColumnarKeyBoundKind>{
                rocksdb::ColumnarKeyBoundKind::kPoint});
  EXPECT_EQ(readStringColumn(
                projected.bounds_by_schema.front().lower_bound_rows, 0, 0),
            pointKey);

  const std::string seekKey{"s1:a:002\x80"};
  const rocksdb::Slice seekKeySlice{seekKey};
  status = reader->ProjectKeyBounds(
      rocksdb::ColumnarKeyProjectionRequest{
          .purpose = rocksdb::ColumnarKeyProjectionPurpose::kSeek,
          .user_keys = &seekKeySlice,
          .num_keys = 1,
      },
      &projected);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(projected.bounds_by_schema.front().bound_kinds,
            std::vector<rocksdb::ColumnarKeyBoundKind>{
                rocksdb::ColumnarKeyBoundKind::kLowerBound});
  EXPECT_EQ(readStringColumn(
                projected.bounds_by_schema.front().lower_bound_rows, 0, 0),
            seekKey);

  const std::string prefix{"s1:a:"};
  const rocksdb::Slice prefixSlice{prefix};
  status = reader->ProjectKeyBounds(
      rocksdb::ColumnarKeyProjectionRequest{
          .purpose = rocksdb::ColumnarKeyProjectionPurpose::kPrefixScan,
          .user_keys = &prefixSlice,
          .num_keys = 1,
      },
      &projected);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(projected.bounds_by_schema.front().bound_kinds,
            std::vector<rocksdb::ColumnarKeyBoundKind>{
                rocksdb::ColumnarKeyBoundKind::kHalfOpenRange});
  EXPECT_EQ(readStringColumn(
                projected.bounds_by_schema.front().lower_bound_rows, 0, 0),
            prefix);
  EXPECT_EQ(readStringColumn(
                projected.bounds_by_schema.front().upper_bound_rows, 0, 0),
            "s1:a;");
}

TEST_F(ColumnarRowAdapterContractTest,
       FinishesInternalKeysInOnePassWithSequenceNumbers) {
  ToyColumnarRowAdapterFactory adapter;
  const std::vector<std::string> keys{"s1:a:001", "s1:a:002", "s1:a:003"};
  const std::vector<std::string> values{"v7|A1|B1|C1", "v8|A2|B2|C2|D2",
                                        "v9|A3|C3|D3"};
  std::vector<rocksdb::ParsedEntryInfo> entries;
  std::vector<rocksdb::Slice> valueSlices;
  std::vector<rocksdb::ColumnarRowSchema> rowSchemas;
  entries.reserve(keys.size());
  valueSlices.reserve(values.size());
  rowSchemas.reserve(keys.size());

  for (size_t row = 0; row < keys.size(); ++row) {
    entries.push_back(parsedPutEntry(keys[row]));
    valueSlices.emplace_back(values[row]);
    rocksdb::ColumnarRowClassification classification;
    auto status = adapter.ClassifyRow(entries.back(), valueSlices.back(),
                                      &classification);
    ASSERT_TRUE(status.ok()) << status.ToString();
    rowSchemas.push_back(classification.row_schema);
  }

  rocksdb::ColumnarWriteOpenContext writeContext{
      .user_comparator = rocksdb::BytewiseComparator(),
      .prefix_extractor = nullptr,
      .memory =
          rocksdb::ColumnarAdapterMemoryContext{
              .pool = pool_.get(),
              .memory_budget_bytes = 1 << 20,
          },
  };
  std::unique_ptr<rocksdb::ColumnarFileWriterAdapter> writer;
  auto status = adapter.NewWriter(rowSchemas.front(), writeContext, &writer);
  ASSERT_TRUE(status.ok()) << status.ToString();

  rocksdb::ColumnarVectorBatch decoded;
  status = writer->DecodeRows(
      rocksdb::ColumnarKeyValueInputBatch{
          .entries = entries.data(),
          .values = valueSlices.data(),
          .row_schemas = rowSchemas.data(),
          .num_rows = keys.size(),
      },
      &decoded);
  ASSERT_TRUE(status.ok()) << status.ToString();

  rocksdb::ColumnarFileMetadata metadata;
  status = writer->FinishFile(&metadata);
  ASSERT_TRUE(status.ok()) << status.ToString();

  rocksdb::ColumnarReadOpenContext readContext{
      .user_comparator = rocksdb::BytewiseComparator(),
      .prefix_extractor = nullptr,
      .memory =
          rocksdb::ColumnarAdapterMemoryContext{
              .pool = pool_.get(),
              .memory_budget_bytes = 1 << 20,
          },
      .reserve_internal_key_trailer_space = true,
      .prefer_flat_output_buffers = true,
  };
  std::unique_ptr<rocksdb::ColumnarFileReaderAdapter> reader;
  status = adapter.NewReader(metadata, readContext, &reader);
  ASSERT_TRUE(status.ok()) << status.ToString();

  const std::vector<rocksdb::ColumnarInternalKeyFields> systemFields{
      rocksdb::ColumnarInternalKeyFields{
          .sequence = 111,
          .type = rocksdb::kEntryPut,
      },
      rocksdb::ColumnarInternalKeyFields{
          .sequence = 109,
          .type = rocksdb::kEntryDelete,
      },
      rocksdb::ColumnarInternalKeyFields{
          .sequence = 108,
          .type = rocksdb::kEntryMerge,
      },
  };
  const TestInternalKeyFinisher finisher;
  rocksdb::ColumnarKeyValueOutputBatch encoded;
  status = reader->EncodeRows(decoded,
                              rocksdb::ColumnarInternalKeyFieldsBatch{
                                  .fields = systemFields.data(),
                                  .num_rows = systemFields.size(),
                              },
                              &finisher, &encoded);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(encoded.num_rows, keys.size());

  for (size_t row = 0; row < keys.size(); ++row) {
    EXPECT_EQ(toString(encoded.keys[row].user_key), keys[row]);
    EXPECT_EQ(toString(encoded.values[row]), values[row]);
    ASSERT_NE(encoded.keys[row].internal_key_trailer, nullptr);
    ASSERT_EQ(encoded.keys[row].internal_key.size(),
              keys[row].size() + rocksdb::kColumnarInternalKeyTrailerSize);

    rocksdb::ParsedEntryInfo parsed;
    status = rocksdb::ParseEntry(encoded.keys[row].internal_key,
                                 rocksdb::BytewiseComparator(), &parsed);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(toString(parsed.user_key), keys[row]);
    EXPECT_EQ(parsed.sequence, systemFields[row].sequence);
    EXPECT_EQ(parsed.type, systemFields[row].type);
  }
}

TEST_F(ColumnarRowAdapterContractTest,
       ExternalTableRejectsRowsThatNeedAnotherFileOrFallback) {
  auto adapter = std::make_shared<const ToyColumnarRowAdapterFactory>();
  const auto options = makeOptions(adapter);

  {
    rocksdb::SstFileWriter writer{rocksdb::EnvOptions{options}, options};
    const auto path =
        (tempDirectory_.path() / "incompatible_version.sst").string();
    auto status = writer.Open(path);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = writer.Put("s1:a:001", "v7|A|B|C");
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = writer.Put("s1:a:002", "v10|A|B|C|D");
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = writer.Finish();
    EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  }

  {
    rocksdb::SstFileWriter writer{rocksdb::EnvOptions{options}, options};
    const auto path = (tempDirectory_.path() / "fallback.sst").string();
    auto status = writer.Open(path);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = writer.Put("raw:a:001", "opaque");
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = writer.Finish();
    EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  }
}

}  // namespace
}  // namespace facebook::rocks
