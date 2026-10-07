//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

#ifdef ROCKSDB_USE_VELOX
#include "velox/common/memory/MemoryPool.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#endif

// EXPERIMENTAL
// The interfaces defined in this file are subject to change at any time without
// warning.
//
// This API is available only when RocksDB is built with Velox support. Builds
// that do not define ROCKSDB_USE_VELOX can include this header without pulling
// in Velox dependencies, but the adapter types below are not declared.
//
// ColumnarRowAdapterFactory is the customer boundary for schema-aware columnar
// storage. It lets RocksDB store selected key/value rows in a Nimble/Velox
// columnar representation while preserving the ordinary RocksDB user-facing
// key/value API.
//
// Ownership split:
// - RocksDB owns LSM semantics: stored internal-key parsing and construction,
//   sequence numbers, value types, snapshots, merges, deletes, range bounds,
//   table properties, memory budgets, and file cutting.
// - Nimble owns physical columnar storage, cluster-index lookup, and projected
//   RowVector reads.
// - The adapter owns customer row knowledge: whether a key/value belongs to a
//   columnar-capable schema, how to decode it into Velox customer columns, how
//   to encode projected Velox rows back into byte-identical user keys and
//   values, and how to validate schema/version compatibility.
//
// Existing API references:
// - rocksdb/types.h defines ParsedEntryInfo, SequenceNumber, and EntryType.
//   RocksDB passes ParsedEntryInfo to the adapter instead of exposing internal
//   dbformat.h details.
// - rocksdb/external_table.h defines full-mode external table semantics:
//   builders receive stored internal keys, readers return stored internal keys,
//   and point lookups provide same-user-key versions in internal-key order.
// - rocksdb/comparator.h and rocksdb/slice_transform.h define the user-key
//   ordering and prefix extraction contracts that cluster-index projection must
//   respect.
// - velox/type/Type.h and velox/vector/ComplexVector.h provide the RowType and
//   RowVector structures exchanged between Nimble and the adapter.
//
// Schema identity model:
// - ColumnarSchemaId identifies a compatible physical schema family. For
//   MyRocks this normally means one MySQL/MyRocks index plus a compatible
//   history of row encodings for that index.
// - row_version identifies the concrete customer row encoding version carried
//   by one key/value row, for example a version field in a MyRocks value
//   header.
// - A file writer chooses one immutable ColumnarFileLayout. V1 writers should
//   put exactly one ColumnarFileSchema in that layout. Future multi-schema
//   files can add more schemas and route homogeneous batches by schema_ordinal.
// - A writer must not change its layout after the file starts. If a later row
//   has a different schema id or an incompatible row version, RocksDB cuts the
//   current file and opens another writer.
//
// Example write path:
// 1. RocksDB receives a stored internal key and value from flush/compaction.
// 2. RocksDB decodes the internal key into ParsedEntryInfo {user_key, sequence,
//    type}. The adapter never parses or emits RocksDB internal-key trailers.
// 3. RocksDB calls ClassifyRow(entry, value). A MyRocks adapter might decode
//    the index id and value header, returning
//    {schema_id = primary-index-family, row_version = 8}.
//    Non-columnar rows return kFallbackToBlockBased.
// 4. The first eligible row opens one ColumnarFileWriterAdapter. The writer
//    chooses an immutable file layout from adapter metadata. For example,
//    schema id X may cover MyRocks row versions 7, 8, and 9 by using a superset
//    physical RowType and deriving column existence from the projected
//    row-version column.
// 5. For later rows, RocksDB asks the writer whether {schema_id, row_version}
//    is compatible. Different schema ids or incompatible row versions cause
//    RocksDB to finish the current file and start another file; the writer does
//    not change the layout in place.
// 6. DecodeRows() produces only customer columns. RocksDB/Nimble append system
//    columns for sequence/type and use customer key columns plus those system
//    columns as the cluster index.
//
// Example read path:
// 1. RocksDB opens a ColumnarFileReaderAdapter from persisted
//    ColumnarFileMetadata. The reader validates schema ids, adapter versions,
//    format versions, row versions, physical types, and comparator ordering.
// 2. For Get, Seek, SeekForPrev, or prefix scan, RocksDB asks
//    ProjectKeyBounds() to translate arbitrary user keys into Velox user-key
//    bound rows. RocksDB appends internal-key endpoint columns before using
//    Nimble's cluster index.
// 3. Nimble returns customer RowVectors plus RocksDB system columns. RocksDB
//    decodes the system columns into ColumnarInternalKeyFields.
// 4. EncodeRows() reconstructs byte-identical user keys and values. For a fast
//    row-major implementation, the adapter can lay out keys in a flat buffer as
//    [user-key][8-byte trailer space], call ColumnarInternalKeyFinisher for
//    each row, and return complete internal-key slices without a second pass.
//
// Example schema evolution:
// Suppose one MyRocks index has three compatible row versions:
//   version 7: columns A, B, C
//   version 8: columns A, B, C, D   (D is nullable)
//   version 9: columns A, C, D      (B was dropped)
//
// The adapter can assign all three versions to one ColumnarSchemaId and expose
// one physical RowType:
//   (A, B, C, D, row_version)
//
// Presence and nulls are different:
// - For a version 7 row, D did not exist in the original row. That is an absent
//   column, not a NULL D value.
// - For a version 8 row, D exists. If MySQL stored NULL for D, the adapter
//   represents that with Velox nulls while the existence state remains true.
// - For a version 9 row, B did not exist in the original row. The adapter must
//   not reconstruct bytes as if B existed with NULL or a default value.
//
// If column existence is a deterministic function of row_version, the adapter
// should mark B and D kDerivedFromRowVersion. RocksDB/Nimble then do not need
// to store per-row exists bitmaps for those columns. During reconstruction, the
// adapter looks at row_version to decide whether to consume/emit B and D bytes.
//
// If two rows with the same row_version can independently include or omit a
// column, row_version is insufficient. The adapter should mark that column
// kMaterializedBitmap so the file stores a per-row existence bitmap. Velox null
// bits still describe SQL NULL for rows where the column exists.
//
// If a schema change alters the byte-exact interpretation of a retained column,
// for example changing a fixed-width field in a way the old physical column
// cannot represent, the adapter should either model it as a different physical
// column id or return kIncompatibleRowVersion so RocksDB cuts the file and asks
// for a new writer/layout.
//
// Example comparator contract:
// The adapter may write Nimble files only when its projected user-key columns
// are order-preserving for the active RocksDB comparator. A MyRocks adapter may
// project memcomparable index components or order-adjusted numeric values. A
// generic custom comparator, such as locale-aware or case-insensitive ordering,
// must fall back unless the adapter can certify that the Velox/Nimble cluster
// key order matches RocksDB comparator order.

namespace ROCKSDB_NAMESPACE {

#ifdef ROCKSDB_USE_VELOX

inline constexpr size_t kColumnarInternalKeyTrailerSize = 8;

// Identifies a customer schema family that can share one physical columnar
// write plan. For MyRocks, fingerprint is expected to identify a MySQL/MyRocks
// index and schema-compatible history. adapter_version and format_version let
// readers reject files written by adapter or RocksDB/Nimble encodings they do
// not understand.
struct ColumnarSchemaId {
  uint64_t fingerprint = 0;
  uint32_t adapter_version = 0;
  uint32_t format_version = 0;
};

inline bool operator==(const ColumnarSchemaId& lhs,
                       const ColumnarSchemaId& rhs) {
  return lhs.fingerprint == rhs.fingerprint &&
         lhs.adapter_version == rhs.adapter_version &&
         lhs.format_version == rhs.format_version;
}

inline bool operator!=(const ColumnarSchemaId& lhs,
                       const ColumnarSchemaId& rhs) {
  return !(lhs == rhs);
}

// Identifies the concrete row encoding version used by one key-value pair.
// schema_id is the compatible family; row_version is the exact customer row
// version found in that row.
struct ColumnarRowSchema {
  ColumnarSchemaId schema_id;
  uint32_t row_version = 0;
};

// RocksDB-owned internal-key fields. RocksDB decodes these fields from stored
// internal keys on the write path, stores them as Nimble system columns, and
// uses them to reconstruct stored internal keys on the read path. The customer
// adapter must not encode sequence numbers or entry types as customer columns.
struct ColumnarInternalKeyFields {
  SequenceNumber sequence = 0;
  EntryType type = kEntryOther;
};

enum class ColumnarRowEligibility : uint8_t {
  // The row can be encoded by the columnar path if it is compatible with the
  // active per-file write plan.
  kEligible,
  // The row is valid, but should be written to a block-based table.
  kFallbackToBlockBased,
};

struct ColumnarRowClassification {
  ColumnarRowEligibility eligibility =
      ColumnarRowEligibility::kFallbackToBlockBased;
  ColumnarRowSchema row_schema;
};

enum class ColumnarPlanCompatibility : uint8_t {
  // The row can be appended to the active file.
  kCompatible,
  // The row belongs to another schema_id. RocksDB should finish the active
  // file and open a new writer for the new schema_id.
  kDifferentSchemaId,
  // The schema_id matches, but the row_version cannot be represented by the
  // active immutable write plan. RocksDB should cut the file and ask the
  // factory for a new plan.
  kIncompatibleRowVersion,
  // The row is valid, but should be written to a block-based table.
  kFallbackToBlockBased,
};

// Describes how a physical column records whether the column existed in the
// original row. Existence is distinct from SQL NULL: a column can exist and
// contain NULL, or be absent because the original row version predated or
// postdated that column.
enum class ColumnarColumnPresenceEncoding : uint8_t {
  // The column exists in every row covered by the file's write plan.
  kAlwaysPresent,
  // The column exists in no row covered by the file's write plan.
  kAlwaysAbsent,
  // The adapter derives existence from another projected column, typically the
  // customer row version column.
  kDerivedFromRowVersion,
  // The physical schema stores a per-row existence bitmap for this column.
  kMaterializedBitmap,
};

enum class ColumnarColumnRole : uint8_t {
  // A projected user-key component. The ordered tuple of these columns must be
  // order-preserving for the active RocksDB user comparator.
  kUserKey,
  // A projected value component.
  kValue,
  // Customer row version encoded as a physical column for reconstruction and
  // presence derivation.
  kRowVersion,
  // A materialized existence bitmap column.
  kExistsBitmap,
};

struct ColumnarColumnDescriptor {
  // Stable customer-visible or adapter-visible column id. The id should remain
  // stable across compatible schema versions even if the physical name changes.
  uint32_t column_id = 0;
  std::string name;
  facebook::velox::TypePtr type;
  ColumnarColumnRole role = ColumnarColumnRole::kValue;
  ColumnarColumnPresenceEncoding presence_encoding =
      ColumnarColumnPresenceEncoding::kAlwaysPresent;
};

// Immutable per-file plan. RocksDB/Nimble append their own system columns for
// internal key sequence/type ordering; this schema describes only customer
// columns produced and consumed by the adapter.
struct ColumnarFileSchema {
  ColumnarSchemaId schema_id;

  // Row type for all customer columns emitted to Nimble. Its children must
  // correspond to columns.
  facebook::velox::RowTypePtr data_row_type;
  std::vector<ColumnarColumnDescriptor> columns;

  // Row type returned by ProjectKeyBounds(). This is the ordered user-key
  // prefix of the cluster index. RocksDB/Nimble append internal-key fields
  // after these columns.
  facebook::velox::RowTypePtr user_key_row_type;

  // Concrete row versions the adapter says this immutable plan can represent.
  std::vector<uint32_t> compatible_row_versions;

  // Name of the RocksDB comparator this plan was validated against. Empty means
  // the adapter does not need a comparator-specific name check.
  std::string comparator_name;

  // Must be true before RocksDB may write a Nimble file. If false, RocksDB must
  // fall back because Nimble cluster-index order would not be guaranteed to
  // match RocksDB user-key comparator order.
  bool preserves_comparator_order = false;

  // True when ProjectKeyBounds() can construct exact prefix-scan bounds for the
  // configured prefix extractor. When false, RocksDB may still perform
  // conservative total-order seeks and validate prefixes after reconstruction.
  bool supports_prefix_bounds = false;
};

// One physical customer schema stored in a columnar file. schema_ordinal is
// file-local and is used to route homogeneous vector batches to the matching
// schema. V1 files are expected to contain exactly one schema with ordinal 0.
struct ColumnarSchemaInFile {
  uint32_t schema_ordinal = 0;
  ColumnarFileSchema schema;
};

// Describes all customer physical schemas stored in a file. Multiple schemas in
// one file are reserved for future use; v1 writers should return one entry.
// RocksDB/Nimble still append system columns for internal-key ordering to every
// stored physical schema.
struct ColumnarFileLayout {
  std::vector<ColumnarSchemaInFile> schemas;

  // Future multi-schema files may need a file-local schema ordinal column to
  // dispatch projected rows to the correct adapter schema. V1 single-schema
  // files should leave this false.
  bool requires_schema_ordinal_column = false;
};

// Adapter-private file metadata persisted in the table properties or Nimble
// metadata. RocksDB stores and returns the bytes opaquely; the adapter owns its
// encoding and compatibility.
struct ColumnarFileMetadata {
  ColumnarFileLayout file_layout;
  std::vector<uint32_t> row_versions_seen;
  std::string serialized_adapter_metadata;
};

struct ColumnarAdapterMemoryContext {
  // All Velox vectors and buffers returned by the adapter must allocate from
  // this pool. RocksDB owns the pool and its lifetime.
  facebook::velox::memory::MemoryPool* pool = nullptr;

  // A zero budget means RocksDB has not imposed an adapter-specific limit.
  uint64_t memory_budget_bytes = 0;
};

struct ColumnarWriteOpenContext {
  const Comparator* user_comparator = nullptr;
  const SliceTransform* prefix_extractor = nullptr;
  ColumnarAdapterMemoryContext memory;
};

struct ColumnarReadOpenContext {
  const Comparator* user_comparator = nullptr;
  const SliceTransform* prefix_extractor = nullptr;
  ColumnarAdapterMemoryContext memory;

  // When true, the reader asks the adapter to back reconstructed user keys with
  // writable storage that has kColumnarInternalKeyTrailerSize bytes immediately
  // after each user key. RocksDB owns the trailer encoding and may write those
  // bytes in place to form stored internal keys without copying the user key.
  bool reserve_internal_key_trailer_space = true;

  // Hint that reconstructed keys and values should be laid out in compact
  // batch-scoped buffers when practical. Slices returned by the adapter still
  // define the authoritative boundaries.
  bool prefer_flat_output_buffers = true;
};

// A batch of original RocksDB entries. entries are decoded by RocksDB from
// stored internal keys before calling the adapter. row_schemas is optional when
// every row uses the writer's initial row schema; otherwise it must contain one
// entry per row.
struct ColumnarKeyValueInputBatch {
  const ParsedEntryInfo* entries = nullptr;
  const Slice* values = nullptr;
  const ColumnarRowSchema* row_schemas = nullptr;
  size_t num_rows = 0;
};

// Customer columns exchanged between the adapter and Nimble. rows must use the
// data_row_type for schema_ordinal and must not include RocksDB system columns.
// The RowVector is batch-scoped: it remains valid until the next adapter batch
// call, ResetBatch(), ReleaseUnusedMemory(), or adapter destruction.
struct ColumnarVectorBatch {
  uint32_t schema_ordinal = 0;
  facebook::velox::RowVectorPtr rows;
};

// RocksDB system columns decoded from Nimble rows. The batch order matches the
// corresponding ColumnarVectorBatch.
struct ColumnarInternalKeyFieldsBatch {
  const ColumnarInternalKeyFields* fields = nullptr;
  size_t num_rows = 0;
};

struct ColumnarReconstructedKey {
  // Byte-identical RocksDB user key.
  Slice user_key;

  // Optional full stored internal key. When set, this must be the same bytes as
  // user_key followed by the RocksDB internal-key trailer for the corresponding
  // ColumnarInternalKeyFields.
  Slice internal_key;

  // Optional writable trailer storage. When non-null, this must point to
  // exactly kColumnarInternalKeyTrailerSize bytes immediately following
  // user_key.data() + user_key.size(). RocksDB may encode the internal-key
  // trailer there and then treat [user_key.data(), user_key.size() + 8) as the
  // stored internal key. When null, RocksDB will copy the user key into its own
  // internal-key buffer before appending the trailer.
  char* internal_key_trailer = nullptr;
};

// RocksDB-owned helper passed to EncodeRows(). It lets row-major adapters
// finish internal keys while reconstructing each row without a second pass over
// the output keys. Implementations must not retain this object beyond
// EncodeRows().
class ColumnarInternalKeyFinisher {
 public:
  virtual ~ColumnarInternalKeyFinisher() = default;

  // Encodes fields into key->internal_key_trailer and sets key->internal_key to
  // user_key plus the encoded trailer. The key must provide writable trailer
  // storage immediately following user_key.
  virtual Status FinishKey(const ColumnarInternalKeyFields& fields,
                           ColumnarReconstructedKey* key) const = 0;
};

// Reconstructed byte-identical RocksDB user keys and values. RocksDB combines
// keys with the corresponding ColumnarInternalKeyFields to reconstruct full
// stored internal keys when internal_key is unset. Slices are backed by
// adapter-owned reusable memory and are valid until the next adapter batch
// call, ResetBatch(), ReleaseUnusedMemory(), or adapter destruction.
struct ColumnarKeyValueOutputBatch {
  const ColumnarReconstructedKey* keys = nullptr;
  const Slice* values = nullptr;
  size_t num_rows = 0;
};

enum class ColumnarKeyBoundKind : uint8_t {
  // lower_bound_rows and upper_bound_rows both contain the exact key. RocksDB
  // and Nimble encode it as a point lookup after appending internal-key fields.
  kPoint,
  // lower_bound_rows contains the starting user-key bound. There is no
  // adapter-projected upper bound.
  kLowerBound,
  // upper_bound_rows contains the ending user-key bound. There is no
  // adapter-projected lower bound.
  kUpperBound,
  // Both lower_bound_rows and upper_bound_rows are present and define a
  // half-open user-key range before RocksDB appends internal-key fields.
  kHalfOpenRange,
  // The adapter cannot project this input key into cluster-index bounds.
  kCannotProject,
};

enum class ColumnarKeyProjectionPurpose : uint8_t {
  kPointLookup,
  kSeek,
  kSeekForPrev,
  kPrefixScan,
};

struct ColumnarKeyProjectionRequest {
  ColumnarKeyProjectionPurpose purpose = ColumnarKeyProjectionPurpose::kSeek;
  const Slice* user_keys = nullptr;
  size_t num_keys = 0;
};

// The vectors have the user_key_row_type for schema_ordinal and row cardinality
// num_keys when present. Per-input bound_kinds has num_keys entries and
// explains which endpoints are meaningful for each row. RocksDB/Nimble use the
// same Velox key encoding for these projected columns and for the file's
// cluster index.
struct ColumnarProjectedKeyBoundsForSchema {
  uint32_t schema_ordinal = 0;
  facebook::velox::RowVectorPtr lower_bound_rows;
  facebook::velox::RowVectorPtr upper_bound_rows;
  std::vector<ColumnarKeyBoundKind> bound_kinds;
};

// Projected lookup bounds grouped by physical schema. V1 adapters should return
// one entry with schema_ordinal 0.
struct ColumnarProjectedKeyBounds {
  std::vector<ColumnarProjectedKeyBoundsForSchema> bounds_by_schema;
};

class ColumnarFileAdapter {
 public:
  virtual ~ColumnarFileAdapter() = default;

  // Releases batch-scoped output while keeping reusable capacity.
  virtual void ResetBatch() = 0;

  // Attempts to release retained reusable memory. Implementations may keep
  // state required for correctness, such as the immutable file schema.
  virtual Status ReleaseUnusedMemory() = 0;

  // Approximate bytes retained by this per-file adapter outside memory owned by
  // live Velox vectors returned to the caller.
  virtual uint64_t RetainedBytes() const = 0;
};

class ColumnarFileWriterAdapter : public ColumnarFileAdapter {
 public:
  ~ColumnarFileWriterAdapter() override = default;

  virtual const ColumnarFileLayout& GetFileLayout() const = 0;

  virtual Status CheckCompatibility(
      const ColumnarRowSchema& row_schema,
      ColumnarPlanCompatibility* compatibility) const = 0;

  // Decodes byte-identical original user keys and values into customer columns.
  // Output rows must have the same cardinality and order as input rows.
  virtual Status DecodeRows(const ColumnarKeyValueInputBatch& input,
                            ColumnarVectorBatch* output) = 0;

  // Returns metadata to persist with the completed file. This should be called
  // after the last successful DecodeRows().
  virtual Status FinishFile(ColumnarFileMetadata* metadata) = 0;
};

class ColumnarFileReaderAdapter : public ColumnarFileAdapter {
 public:
  ~ColumnarFileReaderAdapter() override = default;

  virtual const ColumnarFileLayout& GetFileLayout() const = 0;

  // Projects arbitrary RocksDB user keys into user-key cluster-index bounds.
  // RocksDB/Nimble append internal-key sequence/type endpoint fields before
  // encoding the final cluster-index lookup ranges.
  virtual Status ProjectKeyBounds(const ColumnarKeyProjectionRequest& request,
                                  ColumnarProjectedKeyBounds* output) = 0;

  // Encodes projected customer columns into byte-identical RocksDB keys and
  // values. RocksDB provides the separately decoded system fields. Adapters may
  // call internal_key_finisher while reconstructing each row to publish
  // complete stored internal keys in one pass, or leave internal_key unset and
  // let RocksDB finish them after this call returns.
  virtual Status EncodeRows(
      const ColumnarVectorBatch& input,
      const ColumnarInternalKeyFieldsBatch& system_fields,
      const ColumnarInternalKeyFinisher* internal_key_finisher,
      ColumnarKeyValueOutputBatch* output) = 0;
};

class ColumnarRowAdapterFactory {
 public:
  virtual ~ColumnarRowAdapterFactory() = default;

  // Classifies one original user key and value. Invalid/corrupt rows should
  // return a non-OK status; valid rows that should not use Nimble should return
  // OK with kFallbackToBlockBased.
  virtual Status ClassifyRow(
      const ParsedEntryInfo& entry, const Slice& value,
      ColumnarRowClassification* classification) const = 0;

  // Opens an immutable per-file writer plan based on the first eligible row's
  // schema. The writer may use adapter-owned metadata to choose a physical
  // schema that covers all compatible row versions known to that schema_id.
  virtual Status NewWriter(
      const ColumnarRowSchema& first_row_schema,
      const ColumnarWriteOpenContext& context,
      std::unique_ptr<ColumnarFileWriterAdapter>* writer) const = 0;

  // Opens a per-file reader. Implementations must validate schema ids, adapter
  // versions, format versions, row versions, physical types, and comparator
  // compatibility before returning OK.
  virtual Status NewReader(
      const ColumnarFileMetadata& metadata,
      const ColumnarReadOpenContext& context,
      std::unique_ptr<ColumnarFileReaderAdapter>* reader) const = 0;
};

#endif  // ROCKSDB_USE_VELOX

}  // namespace ROCKSDB_NAMESPACE
