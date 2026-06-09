// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/builtin_index_factory.h"

#include <cassert>
#include <memory>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/index_builder.h"

namespace ROCKSDB_NAMESPACE {

// Wrapper around the internal IndexBuilder::PreparedIndexEntry, adapting it
// to the public IndexFactoryBuilder::PreparedAddEntry interface for parallel
// compression support.
struct BuiltinPreparedAddEntry : public IndexFactoryBuilder::PreparedAddEntry {
  std::unique_ptr<IndexBuilder::PreparedIndexEntry> internal_entry;
  explicit BuiltinPreparedAddEntry(
      std::unique_ptr<IndexBuilder::PreparedIndexEntry> e)
      : internal_entry(std::move(e)) {}
};

BuiltinIndexFactoryBuilder::BuiltinIndexFactoryBuilder(
    std::unique_ptr<InternalKeyComparator> icmp,
    const BlockBasedTableOptions* table_opts)
    : icmp_(std::move(icmp)), table_opts_(table_opts) {}

BuiltinIndexFactoryBuilder::~BuiltinIndexFactoryBuilder() = default;

void BuiltinIndexFactoryBuilder::SetInternalBuilder(
    std::unique_ptr<IndexBuilder> builder) {
  internal_builder_ = std::move(builder);
}

const InternalKeyComparator* BuiltinIndexFactoryBuilder::GetComparator() const {
  return icmp_.get();
}

const BlockBasedTableOptions& BuiltinIndexFactoryBuilder::GetTableOptions()
    const {
  return *table_opts_;
}

void BuiltinIndexFactoryBuilder::ReconstructInternalKeys(
    const Slice& last_user_key, const Slice* next_user_key,
    const IndexEntryContext& ctx) {
  last_internal_key_.clear();
  last_internal_key_.append(last_user_key.data(), last_user_key.size());
  PutFixed64(&last_internal_key_, ctx.last_key_tag);

  if (next_user_key != nullptr) {
    next_internal_key_.clear();
    next_internal_key_.append(next_user_key->data(), next_user_key->size());
    PutFixed64(&next_internal_key_, ctx.first_key_tag);
  }
}

Slice BuiltinIndexFactoryBuilder::AddIndexEntry(
    const Slice& last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle,
    std::string* separator_scratch, const IndexEntryContext& context) {
  // Reconstruct internal keys from user keys + packed tags.
  // The internal IndexBuilder expects full internal keys:
  //   [user_key | packed_seq_and_type (8 bytes)]
  ReconstructInternalKeys(last_key_in_current_block, first_key_in_next_block,
                          context);
  Slice last_ik(last_internal_key_);

  Slice next_ik;
  const Slice* next_ik_ptr = nullptr;
  if (first_key_in_next_block != nullptr) {
    next_ik = Slice(next_internal_key_);
    next_ik_ptr = &next_ik;
  }

  // Convert the public BlockHandle to the internal BlockHandle.
  ROCKSDB_NAMESPACE::BlockHandle internal_handle(block_handle.offset,
                                                 block_handle.size);

  bool skip = skip_delta_encoding_;
  skip_delta_encoding_ = false;  // Reset after use
  return internal_builder_->AddIndexEntry(last_ik, next_ik_ptr, internal_handle,
                                          separator_scratch, skip);
}

void BuiltinIndexFactoryBuilder::OnKeyAdded(const Slice& /*key*/,
                                            ValueType /*type*/,
                                            const Slice& /*value*/) {
  // No-op: the internal builder needs the full internal key for
  // kBinarySearchWithFirstKey, which the table builder supplies via
  // OnKeyAddedInternal().
}

Status BuiltinIndexFactoryBuilder::Finish(Slice* index_contents) {
  IndexBuilder::IndexBlocks index_blocks;
  Status s = internal_builder_->Finish(&index_blocks);
  if (!s.ok()) {
    return s;
  }
  // Store the contents -- the internal builder's memory backs this Slice.
  *index_contents = index_blocks.index_block_contents;
  return Status::OK();
}

uint64_t BuiltinIndexFactoryBuilder::EstimatedSize() const {
  return internal_builder_->CurrentIndexSizeEstimate();
}

Status BuiltinIndexFactoryBuilder::FinishAndWrite(IndexBlockWriter* writer,
                                                  BlockHandle* final_handle,
                                                  bool compress) {
  IndexBuilder::IndexBlocks index_blocks;
  Status s = internal_builder_->Finish(&index_blocks);
  if (!s.ok() && !s.IsIncomplete()) {
    return s;
  }

  // Write any auxiliary meta blocks (e.g., hash index prefix blocks).
  // The writer callback registers them with the meta index builder.
  for (const auto& item : index_blocks.meta_blocks) {
    BlockHandle meta_bh{0, 0};
    Status ws = writer->WriteBlock(item.second.second, &meta_bh, compress);
    if (!ws.ok()) {
      return ws;
    }
    writer->AddMetaBlock(item.first, meta_bh);
  }

  // Write the first (or only) index block.
  BlockHandle handle{0, 0};
  Status ws =
      writer->WriteBlock(index_blocks.index_block_contents, &handle, compress);
  if (!ws.ok()) {
    return ws;
  }

  // For partitioned indexes, the internal builder returns
  // Status::Incomplete() to signal more partitions remain. Each
  // subsequent Finish() call receives the handle of the previously
  // written partition so it can build the top-level index.
  while (s.IsIncomplete()) {
    // Convert public BlockHandle to internal BlockHandle for Finish.
    ROCKSDB_NAMESPACE::BlockHandle internal_handle(handle.offset, handle.size);
    s = internal_builder_->Finish(&index_blocks, internal_handle);
    if (!s.ok() && !s.IsIncomplete()) {
      return s;
    }
    ws = writer->WriteBlock(index_blocks.index_block_contents, &handle,
                            compress);
    if (!ws.ok()) {
      return ws;
    }
  }

  *final_handle = {handle.offset, handle.size};
  return Status::OK();
}

bool BuiltinIndexFactoryBuilder::SupportsParallelAddEntry() const {
  return true;
}

std::unique_ptr<IndexFactoryBuilder::PreparedAddEntry>
BuiltinIndexFactoryBuilder::CreatePreparedAddEntry() {
  return std::make_unique<BuiltinPreparedAddEntry>(
      internal_builder_->CreatePreparedIndexEntry());
}

void BuiltinIndexFactoryBuilder::PrepareAddEntry(const Slice& last_key,
                                                 const Slice* next_key,
                                                 const IndexEntryContext& ctx,
                                                 PreparedAddEntry* out) {
  auto* entry = static_cast<BuiltinPreparedAddEntry*>(out);

  // Reconstruct internal keys from user keys + packed tags.
  ReconstructInternalKeys(last_key, next_key, ctx);

  Slice next_ik;
  const Slice* next_ik_ptr = nullptr;
  if (next_key) {
    next_ik = Slice(next_internal_key_);
    next_ik_ptr = &next_ik;
  }

  internal_builder_->PrepareIndexEntry(Slice(last_internal_key_), next_ik_ptr,
                                       entry->internal_entry.get());
}

void BuiltinIndexFactoryBuilder::FinishAddEntry(
    const BlockHandle& handle, PreparedAddEntry* entry,
    std::string* /*separator_scratch*/, bool skip_delta_encoding) {
  auto* builtin_entry = static_cast<BuiltinPreparedAddEntry*>(entry);
  ROCKSDB_NAMESPACE::BlockHandle internal_handle(handle.offset, handle.size);
  internal_builder_->FinishIndexEntry(internal_handle,
                                      builtin_entry->internal_entry.get(),
                                      skip_delta_encoding);
}

bool BuiltinIndexFactoryBuilder::separator_is_key_plus_seq() const {
  // The internal IndexBuilder::separator_is_key_plus_seq() is non-const
  // but the underlying implementations use RelaxedAtomic loads, which
  // are safe to call without mutation. const_cast is appropriate here.
  return const_cast<IndexBuilder*>(internal_builder_.get())
      ->separator_is_key_plus_seq();
}

uint64_t BuiltinIndexFactoryBuilder::NumUniformIndexBlocks() const {
  return internal_builder_->NumUniformIndexBlocks();
}

size_t BuiltinIndexFactoryBuilder::IndexSize() const {
  return internal_builder_->IndexSize();
}

uint64_t BuiltinIndexFactoryBuilder::NumPartitions() const {
  if (!IsPartitioned()) {
    return 0;
  }
  return static_cast<PartitionedIndexBuilder*>(internal_builder_.get())
      ->NumPartitions();
}

uint64_t BuiltinIndexFactoryBuilder::TopLevelIndexSize(uint64_t offset) const {
  if (!IsPartitioned()) {
    return 0;
  }
  return static_cast<PartitionedIndexBuilder*>(internal_builder_.get())
      ->TopLevelIndexSize(offset);
}

PartitionCoordinator* BuiltinIndexFactoryBuilder::GetPartitionCoordinator() {
  if (!IsPartitioned()) {
    return nullptr;
  }
  // PartitionedIndexBuilder implements PartitionCoordinator via
  // multiple inheritance. static_cast is safe because IsPartitioned()
  // checks the table_options index_type.
  return static_cast<PartitionedIndexBuilder*>(internal_builder_.get());
}

bool BuiltinIndexFactoryBuilder::IsPartitioned() const {
  return table_opts_ && table_opts_->index_type ==
                            BlockBasedTableOptions::kTwoLevelIndexSearch;
}

IndexBuilder* BuiltinIndexFactoryBuilder::GetInternalBuilder() {
  return internal_builder_.get();
}

Slice BuiltinIndexFactoryBuilder::AddIndexEntryDirect(
    const Slice& last_internal_key, const Slice* first_internal_key_next,
    const ::ROCKSDB_NAMESPACE::BlockHandle& handle,
    std::string* separator_scratch, bool skip_delta_encoding) {
  bool skip = skip_delta_encoding || skip_delta_encoding_;
  skip_delta_encoding_ = false;  // Reset after use
  return internal_builder_->AddIndexEntry(last_internal_key,
                                          first_internal_key_next, handle,
                                          separator_scratch, skip);
}

void BuiltinIndexFactoryBuilder::PrepareAddEntryDirect(
    const Slice& last_internal_key, const Slice* first_internal_key_next,
    PreparedAddEntry* out) {
  auto* entry = static_cast<BuiltinPreparedAddEntry*>(out);
  // Skip the user-key reconstruction path entirely; pass the caller's
  // internal keys straight through to the internal builder.
  internal_builder_->PrepareIndexEntry(
      last_internal_key, first_internal_key_next, entry->internal_entry.get());
}

// ============================================================================
// Factory implementations
// ============================================================================

// --- BinarySearchIndexFactory ---

static const char* const kBinarySearchName =
    "rocksdb.builtin.BinarySearchIndex";
static const char* const kBinarySearchWithFirstKeyName =
    "rocksdb.builtin.BinarySearchWithFirstKeyIndex";

BinarySearchIndexFactory::BinarySearchIndexFactory(bool with_first_key)
    : with_first_key_(with_first_key) {}

BinarySearchIndexFactory::BinarySearchIndexFactory(
    bool with_first_key, const BuiltinIndexFactoryConfig& config)
    : with_first_key_(with_first_key), has_config_(true), config_(config) {}

const char* BinarySearchIndexFactory::Name() const {
  return with_first_key_ ? kBinarySearchWithFirstKeyName : kBinarySearchName;
}

const char* BinarySearchIndexFactory::kClassName() { return kBinarySearchName; }

const char* BinarySearchIndexFactory::kClassNameWithFirstKey() {
  return kBinarySearchWithFirstKeyName;
}

Status BinarySearchIndexFactory::NewBuilder(
    const IndexFactoryOptions& options,
    std::unique_ptr<IndexFactoryBuilder>& builder) const {
  if (options.comparator == nullptr) {
    return Status::InvalidArgument(
        "BinarySearchIndexFactory::NewBuilder requires a comparator");
  }

  if (has_config_) {
    // Full construction path used by the table builder. config_ holds
    // the per-SST configuration; the table_options pointer remains
    // valid for the builder's lifetime (Rep owns it).
    auto icmp = std::make_unique<InternalKeyComparator>(
        config_.internal_comparator->user_comparator());
    auto index_type = with_first_key_
                          ? BlockBasedTableOptions::kBinarySearchWithFirstKey
                          : BlockBasedTableOptions::kBinarySearch;
    auto wrapper = std::make_unique<BuiltinIndexFactoryBuilder>(
        std::move(icmp), config_.table_options);
    std::unique_ptr<IndexBuilder> internal(IndexBuilder::CreateIndexBuilder(
        index_type, wrapper->GetComparator(), config_.internal_prefix_transform,
        config_.use_delta_encoding_for_index_values, wrapper->GetTableOptions(),
        config_.ts_sz, config_.persist_user_defined_timestamps, config_.stats));
    wrapper->SetInternalBuilder(std::move(internal));
    builder = std::move(wrapper);
    return Status::OK();
  }

  // Standalone / test path. Uses a static default BlockBasedTableOptions
  // since there is no Rep to borrow from.
  auto icmp = std::make_unique<InternalKeyComparator>(options.comparator);
  auto index_type = with_first_key_
                        ? BlockBasedTableOptions::kBinarySearchWithFirstKey
                        : BlockBasedTableOptions::kBinarySearch;
  static const BlockBasedTableOptions kDefaultBinarySearchOpts;
  auto wrapper = std::make_unique<BuiltinIndexFactoryBuilder>(
      std::move(icmp), &kDefaultBinarySearchOpts);
  std::unique_ptr<IndexBuilder> internal(IndexBuilder::CreateIndexBuilder(
      index_type, wrapper->GetComparator(),
      /*int_key_slice_transform=*/nullptr,
      /*use_value_delta_encoding=*/true, wrapper->GetTableOptions(),
      /*ts_sz=*/0, /*persist_user_defined_timestamps=*/true));
  wrapper->SetInternalBuilder(std::move(internal));
  builder = std::move(wrapper);
  return Status::OK();
}

Status BinarySearchIndexFactory::NewReader(
    const IndexFactoryOptions& /*options*/, Slice& /*index_contents*/,
    std::unique_ptr<IndexFactoryReader>& /*reader*/) const {
  // Built-in reads go through BlockBasedTable::CreateIndexReader directly
  // (see the asymmetric-API note in include/rocksdb/index_factory.h).
  return Status::NotSupported(
      "BinarySearchIndexFactory::NewReader is not used directly. "
      "The built-in reader is created through "
      "BlockBasedTable::CreateIndexReader.");
}

// --- HashIndexFactory ---

static const char* const kHashIndexName = "rocksdb.builtin.HashIndex";

HashIndexFactory::HashIndexFactory(const BuiltinIndexFactoryConfig& config)
    : has_config_(true), config_(config) {}

const char* HashIndexFactory::Name() const { return kHashIndexName; }
const char* HashIndexFactory::kClassName() { return kHashIndexName; }

// NOTE: OnKeyAdded is not forwarded to the internal HashIndexBuilder, so
// hash prefix metadata is not built through the public OnKeyAdded path.
// However, the FinishAndWrite protocol correctly writes and registers
// hash prefix meta blocks (prefix block and prefix metadata block) via
// the IndexBlockWriter callback.
Status HashIndexFactory::NewBuilder(
    const IndexFactoryOptions& options,
    std::unique_ptr<IndexFactoryBuilder>& builder) const {
  if (options.comparator == nullptr) {
    return Status::InvalidArgument(
        "HashIndexFactory::NewBuilder requires a comparator");
  }

  if (has_config_) {
    auto icmp = std::make_unique<InternalKeyComparator>(
        config_.internal_comparator->user_comparator());
    auto wrapper = std::make_unique<BuiltinIndexFactoryBuilder>(
        std::move(icmp), config_.table_options);
    std::unique_ptr<IndexBuilder> internal(IndexBuilder::CreateIndexBuilder(
        BlockBasedTableOptions::kHashSearch, wrapper->GetComparator(),
        config_.internal_prefix_transform,
        config_.use_delta_encoding_for_index_values, wrapper->GetTableOptions(),
        config_.ts_sz, config_.persist_user_defined_timestamps, config_.stats));
    wrapper->SetInternalBuilder(std::move(internal));
    builder = std::move(wrapper);
    return Status::OK();
  }

  // Standalone / test path with a static default BlockBasedTableOptions.
  auto icmp = std::make_unique<InternalKeyComparator>(options.comparator);
  static const BlockBasedTableOptions kDefaultHashOpts;
  auto wrapper = std::make_unique<BuiltinIndexFactoryBuilder>(
      std::move(icmp), &kDefaultHashOpts);
  std::unique_ptr<IndexBuilder> internal(IndexBuilder::CreateIndexBuilder(
      BlockBasedTableOptions::kHashSearch, wrapper->GetComparator(),
      /*int_key_slice_transform=*/nullptr,
      /*use_value_delta_encoding=*/true, wrapper->GetTableOptions(),
      /*ts_sz=*/0, /*persist_user_defined_timestamps=*/true));
  wrapper->SetInternalBuilder(std::move(internal));
  builder = std::move(wrapper);
  return Status::OK();
}

Status HashIndexFactory::NewReader(
    const IndexFactoryOptions& /*options*/, Slice& /*index_contents*/,
    std::unique_ptr<IndexFactoryReader>& /*reader*/) const {
  return Status::NotSupported(
      "HashIndexFactory::NewReader is not used directly.");
}

// --- PartitionedIndexFactory ---

static const char* const kPartitionedIndexName =
    "rocksdb.builtin.PartitionedIndex";

PartitionedIndexFactory::PartitionedIndexFactory(
    const BuiltinIndexFactoryConfig& config)
    : has_config_(true), config_(config) {}

const char* PartitionedIndexFactory::Name() const {
  return kPartitionedIndexName;
}
const char* PartitionedIndexFactory::kClassName() {
  return kPartitionedIndexName;
}

// The partitioned index uses a multi-call Finish protocol internally
// (returning Status::Incomplete() for each partition). The single-call
// Finish(Slice*) only returns the first partition block. For full
// partitioned index construction, use FinishAndWrite() which drives
// the multi-call protocol through the IndexBlockWriter callback.
Status PartitionedIndexFactory::NewBuilder(
    const IndexFactoryOptions& options,
    std::unique_ptr<IndexFactoryBuilder>& builder) const {
  if (options.comparator == nullptr) {
    return Status::InvalidArgument(
        "PartitionedIndexFactory::NewBuilder requires a comparator");
  }

  if (has_config_) {
    auto icmp = std::make_unique<InternalKeyComparator>(
        config_.internal_comparator->user_comparator());
    auto wrapper = std::make_unique<BuiltinIndexFactoryBuilder>(
        std::move(icmp), config_.table_options);
    std::unique_ptr<IndexBuilder> internal(
        PartitionedIndexBuilder::CreateIndexBuilder(
            wrapper->GetComparator(),
            config_.use_delta_encoding_for_index_values,
            wrapper->GetTableOptions(), config_.ts_sz,
            config_.persist_user_defined_timestamps, config_.stats));
    wrapper->SetInternalBuilder(std::move(internal));
    builder = std::move(wrapper);
    return Status::OK();
  }

  // Standalone / test path. PartitionedIndexBuilder holds a const ref to
  // table_opts_, so the wrapper must be constructed before the builder.
  auto icmp = std::make_unique<InternalKeyComparator>(options.comparator);
  static const BlockBasedTableOptions kDefaultPartitionedOpts;
  auto wrapper = std::make_unique<BuiltinIndexFactoryBuilder>(
      std::move(icmp), &kDefaultPartitionedOpts);
  std::unique_ptr<IndexBuilder> internal(
      PartitionedIndexBuilder::CreateIndexBuilder(
          wrapper->GetComparator(), /*use_value_delta_encoding=*/true,
          wrapper->GetTableOptions(),
          /*ts_sz=*/0, /*persist_user_defined_timestamps=*/true));
  wrapper->SetInternalBuilder(std::move(internal));
  builder = std::move(wrapper);
  return Status::OK();
}

Status PartitionedIndexFactory::NewReader(
    const IndexFactoryOptions& /*options*/, Slice& /*index_contents*/,
    std::unique_ptr<IndexFactoryReader>& /*reader*/) const {
  return Status::NotSupported(
      "PartitionedIndexFactory::NewReader is not used directly.");
}

Status NewBuiltinIndexFactoryBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const BuiltinIndexFactoryConfig& config, const IndexFactoryOptions& options,
    std::unique_ptr<IndexFactoryBuilder>& out) {
  // Stack-local factory objects avoid shared_ptr heap allocation. The factory
  // is only needed for the duration of NewBuilder(); the builder it produces
  // is independent.
  switch (index_type) {
    case BlockBasedTableOptions::kBinarySearch: {
      BinarySearchIndexFactory factory(/*with_first_key=*/false, config);
      return factory.NewBuilder(options, out);
    }
    case BlockBasedTableOptions::kBinarySearchWithFirstKey: {
      BinarySearchIndexFactory factory(/*with_first_key=*/true, config);
      return factory.NewBuilder(options, out);
    }
    case BlockBasedTableOptions::kHashSearch: {
      HashIndexFactory factory(config);
      return factory.NewBuilder(options, out);
    }
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      PartitionedIndexFactory factory(config);
      return factory.NewBuilder(options, out);
    }
  }
  // Unreachable for known IndexType values; keep the compiler happy.
  return Status::InvalidArgument("Unknown BlockBasedTableOptions::IndexType");
}

}  // namespace ROCKSDB_NAMESPACE
