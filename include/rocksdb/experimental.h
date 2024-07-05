// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <variant>

#include "rocksdb/data_structure.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace experimental {

// Supported only for Leveled compaction
Status SuggestCompactRange(DB* db, ColumnFamilyHandle* column_family,
                           const Slice* begin, const Slice* end);
Status SuggestCompactRange(DB* db, const Slice* begin, const Slice* end);

// Move all L0 files to target_level skipping compaction.
// This operation succeeds only if the files in L0 have disjoint ranges; this
// is guaranteed to happen, for instance, if keys are inserted in sorted
// order. Furthermore, all levels between 1 and target_level must be empty.
// If any of the above condition is violated, InvalidArgument will be
// returned.
Status PromoteL0(DB* db, ColumnFamilyHandle* column_family,
                 int target_level = 1);

struct UpdateManifestForFilesStateOptions {
  // When true, read current file temperatures from FileSystem and update in
  // DB manifest when a temperature other than Unknown is reported and
  // inconsistent with manifest.
  bool update_temperatures = true;

  // TODO: new_checksums: to update files to latest file checksum algorithm
};

// Utility for updating manifest of DB directory (not open) for current state
// of files on filesystem. See UpdateManifestForFilesStateOptions.
//
// To minimize interference with ongoing DB operations, only the following
// guarantee is provided, assuming no IO error encountered:
// * Only files live in DB at start AND end of call to
// UpdateManifestForFilesState() are guaranteed to be updated (as needed) in
// manifest.
//   * For example, new files after start of call to
//   UpdateManifestForFilesState() might not be updated, but that is not
//   typically required to achieve goal of manifest consistency/completeness
//   (because current DB configuration would ensure new files get the desired
//   consistent metadata).
Status UpdateManifestForFilesState(
    const DBOptions& db_opts, const std::string& db_name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const UpdateManifestForFilesStateOptions& opts = {});

// ****************************************************************************
// EXPERIMENTAL new filtering features
// ****************************************************************************

// A class for splitting a key into meaningful pieces, or "segments" for
// filtering purposes. Keys can also be put in "categories" to simplify
// some configuration and handling. To simplify satisfying some filtering
// requirements, the segments must encompass a complete key prefix (or the whole
// key) and segments cannot overlap.
//
// Once in production, the behavior associated with a particular Name()
// cannot change. Introduce a new Name() when introducing new behaviors.
// See also SstQueryFilterConfigsManager below.
//
// OTHER CURRENT LIMITATIONS (maybe relaxed in the future for segments only
// needing point query or WHERE filtering):
// * Assumes the (default) byte-wise comparator is used.
// * Assumes the category contiguousness property: that each category is
// contiguous in comparator order. In other words, any key between two keys of
// category c must also be in category c.
// * Assumes the (weak) segment ordering property (described below) always
// holds. (For byte-wise comparator, this is implied by the segment prefix
// property, also described below.)
// * Not yet compatible with user timestamp feature
//
// SEGMENT ORDERING PROPERTY: For maximum use in filters, especially for
// filtering key range queries, we must have a correspondence between
// the lexicographic ordering of key segments and the ordering of keys
// they are extracted from. In other words, if we took the segmented keys
// and ordered them primarily by (byte-wise) order on segment 0, then
// on segment 1, etc., then key order of the original keys would not be
// violated. This is the WEAK form of the property, where multiple keys
// might generate the same segments, but such keys must be contiguous in
// key order. (The STRONG form of the property is potentially more useful,
// but for bytewise comparator, it can be inferred from segments satisfying
// the weak property by assuming another segment that extends to the end of
// the key, which would be empty if the segments already extend to the end
// of the key.)
//
// The segment ordering property is hard to think about directly, but for
// bytewise comparator, it is implied by a simpler property to reason about:
// the segment prefix property (see below). (NOTE: an example way to satisfy
// the segment ordering property while breaking the segment prefix property
// is to have a segment delimited by any byte smaller than a certain value,
// and not include the delimiter with the segment leading up to the delimiter.
// For example, the space character is ordered before other printable
// characters, so breaking "foo bar" into "foo", " ", and "bar" would be
// legal, but not recommended.)
//
// SEGMENT PREFIX PROPERTY: If a key generates segments s0, ..., sn (possibly
// more beyond sn) and sn does not extend to the end of the key, then all keys
// starting with bytes s0+...+sn (concatenated) also generate the same segments
// (possibly more). For example, if a key has segment s0 which is less than the
// whole key and another key starts with the bytes of s0--or only has the bytes
// of s0--then the other key must have the same segment s0. In other words, any
// prefix of segments that might not extend to the end of the key must form an
// unambiguous prefix code. See
// https://en.wikipedia.org/wiki/Prefix_code  In other other words, parsing
// a key into segments cannot use even a single byte of look-ahead. Upon
// processing each byte, the extractor decides whether to cut a segment that
// ends with that byte, but not one that ends before that byte. The only
// exception is that upon reaching the end of the key, the extractor can choose
// whether to make a segment that ends at the end of the key.
//
// Example types of key segments that can be freely mixed in any order:
// * Some fixed number of bytes or codewords.
// * Ends in a delimiter byte or codeword. (Not including the delimiter as
// part of the segment leading up to it would very likely violate the segment
// prefix property.)
// * Length-encoded sequence of bytes or codewords. The length could even
// come from a preceding segment.
// * Any/all remaining bytes to the end of the key, though this implies all
// subsequent segments will be empty.
// For each kind of segment, it should be determined before parsing the segment
// whether an incomplete/short parse will be treated as a segment extending to
// the end of the key or as an empty segment.
//
// For example, keys might consist of
// * Segment 0: Any sequence of bytes up to and including the first ':'
// character, or the whole key if no ':' is present.
// * Segment 1: The next four bytes, all or nothing (in case of short key).
// * Segment 2: An unsigned byte indicating the number of additional bytes in
// the segment, and then that many bytes (or less up to the end of the key).
// * Segment 3: Any/all remaining bytes in the key
//
// For an example of what can go wrong, consider using '4' as a delimiter
// but not including it with the segment leading up to it. Suppose we have
// these keys and corresponding first segments:
// "123456" -> "123"
// "124536" -> "12"
// "125436" -> "125"
// Notice how byte-wise comparator ordering of the segments does not follow
// the ordering of the keys. This means we cannot safely use a filter with
// a range of segment values for filtering key range queries.
//
// Also note that it is legal for all keys in a category (or many categories)
// to return an empty sequence of segments.
//
// To eliminate a confusing distinction between a segment that is empty vs.
// "not present" for a particular key, each key is logically assiciated with
// an infinite sequence of segments, including some infinite tail of 0-length
// segments. In practice, we only represent a finite sequence that (at least)
// covers the non-trivial segments.
//
class KeySegmentsExtractor {
 public:
  // The extractor assigns keys to categories so that it is easier to
  // combine distinct (though disjoint) key representations within a single
  // column family while applying different or overlapping filtering
  // configurations to the categories.
  // To enable fast set representation, the user is allowed up to 64
  // categories for assigning to keys with the extractor. The user will
  // likely cast to their own enum type or scalars.
  enum KeyCategory : uint_fast8_t {
    kDefaultCategory = 0,
    kMinCategory = kDefaultCategory,
    // ... (user categories)
    // Can be used for keys ordered before typical keys. Not necessarily an
    // error.
    kReservedLowCategory = 62,
    // Can be used for keys ordered after typical keys. Not necessarily an
    // error.
    kReservedHighCategory = 63,
    kMaxUsableCategory = kReservedHighCategory,

    // Signals to the caller that an unexpected input or condition has
    // been reached and filter construction should be aborted.
    kErrorCategoryFilterScope = UINT8_MAX - 2,
    kMinErrorCategory = kErrorCategoryFilterScope,
    // Signals to the caller that an unexpected input or condition has
    // been reached and SST construction (and compaction or flush)
    // should be aborted.
    kErrorCategoryFileScope = UINT8_MAX - 1,
    // Signals to the caller that an unexpected input or condition has
    // been reached and the DB should be considered to have reached an
    // invalid state, at least in memory.
    kErrorCategoryDbScope = UINT8_MAX,
  };
  using KeyCategorySet = SmallEnumSet<KeyCategory, kMaxUsableCategory>;

  // The extractor can process three kinds of key-like inputs
  enum KeyKind {
    // User key, not including user timestamp
    kFullUserKey,
    // An iterator lower bound (inclusive). This should generally be handled
    // the same as a full user key but the distinction might be useful for
    // diagnostics or assertions.
    kInclusiveLowerBound,
    // An iterator upper bound (exclusive). Upper bounds are frequently
    // constructed by incrementing the last byte of a key prefix, and this can
    // affect what should be considered as a segment delimiter.
    kExclusiveUpperBound,
  };

  // The extractor result
  struct Result {
    // Positions in the key (or bound) that represent boundaries
    // between segments, or the exclusive end of each segment. For example, if
    // the key is "abc|123|xyz" then following the guidance of including
    // delimiters with the preceding segment, segment_ends would be {4, 8, 11},
    // representing segments "abc|" "123|" and "xyz". Empty segments are
    // naturally represented with repeated values, as in {4, 8, 8} for
    // "abc|123|", though {4, 8} would be logically equivalent because an
    // infinite sequence of 0-length segments is assumed after what is
    // explicitly represented here. However, segments might not reach the end
    // the key (no automatic last segment to the end of the key) and that is
    // OK for the WEAK ordering property.
    //
    // The first segment automatically starts at key position 0. The only way
    // to put gaps between segments of interest is to assign those gaps to
    // numbered segments, which can be left unused.
    std::vector<uint32_t> segment_ends;

    // A category to assign to the key or bound. This default may be kept,
    // such as to put all keys into a single category.
    // IMPORTANT CURRENT LIMITATION from above: each category must be
    // contiguous in key comparator order, so any key between two keys in
    // category c must also be in category c. (Typically the category will be
    // determined by segment 0 in some way, often the first byte.) The enum
    // scalar values do not need to be related to key order.
    KeyCategory category = kDefaultCategory;
  };

  virtual ~KeySegmentsExtractor() {}

  // A class name for this extractor. See also expectations in GetId().
  virtual const char* Name() const = 0;

  // An identifying string that is permanently associated with the behavior
  // of this extractor. If a behavior change is made or set in the constructor,
  // the id must change to avoid incorrect filtering behavior on DBs using a
  // previous version of the extractor.
  virtual std::string GetId() const {
    // The default implementation assumes no configuration variance in the
    // constructor and just returns the class name.
    return Name();
  }

  // Populates the extraction result and returns OK. Error can be signaled
  // with `kError` pseudo-categories. This function is expected to generate
  // non-error results (though possibly empty) on all keys or bounds expected
  // to be encountered by the DB. RocksDB will always call the function with
  // a (pointer to a) default-initialized result object.
  virtual void Extract(const Slice& key_or_bound, KeyKind kind,
                       Result* result) const = 0;
};

// Alternatives for filtering inputs

// An individual key segment.
struct SelectKeySegment {
  // Segments are numbered starting from 0.
  explicit SelectKeySegment(uint16_t _segment_index)
      : segment_index(_segment_index) {}
  uint16_t segment_index;
};

// A range of key segments concatenated together. No concatenation operations
// are needed, however, because with no gaps between segments, a range of
// segments is a simple substring of the key.
struct SelectKeySegmentRange {
  // Segments are numbered starting from 0. Range is inclusive.
  explicit SelectKeySegmentRange(uint8_t _from_segment_index,
                                 uint8_t _to_segment_index)
      : from_segment_index(_from_segment_index),
        to_segment_index(_to_segment_index) {}
  // Inclusive
  uint8_t from_segment_index;
  // Inclusive
  uint8_t to_segment_index;
};

// User key without timestamp
struct SelectWholeKey {};

// TODO: The remaining Select* are not yet supported
// As generated by prefix_extractor
struct SelectLegacyKeyPrefix {};

struct SelectUserTimestamp {};

struct SelectColumnName {};

struct SelectValue {};

// Note: more variants might be added in the future.
using FilterInput =
    std::variant<SelectWholeKey, SelectKeySegment, SelectKeySegmentRange,
                 SelectLegacyKeyPrefix, SelectUserTimestamp, SelectColumnName,
                 SelectValue>;

// Base class for individual filtering schemes in terms of chosen
// FilterInputs, but not tied to a particular KeySegmentsExtractor.
//
// Not user extensible, name sometimes shortened to SQFC
class SstQueryFilterConfig {
 public:
  virtual ~SstQueryFilterConfig() {}
};

// A filtering scheme that stores minimum and maximum values (according
// to bytewise ordering) of the specified filter input. Because the
// empty string is often a special case, the filter excludes that from the
// min/max computation and keeps a separate boolean for whether empty is
// present.
//
// The filter is also limited to the specified categories, ignoring entries
// outside the given set of categories. If not All, ranges can only be
// filtered if upper and lower bounds are in the same category (and that
// category is in the set relevant to the filter).
std::shared_ptr<SstQueryFilterConfig> MakeSharedBytewiseMinMaxSQFC(
    FilterInput select, KeySegmentsExtractor::KeyCategorySet categories =
                            KeySegmentsExtractor::KeyCategorySet::All());

// TODO: more kinds of filters, eventually including Bloom/ribbon filters
// and replacing the old filter configuration APIs

// Represents a complete strategy for representing filters in SST files
// and applying them to optimize range and point queries by excluding
// irrelevant SST files (as best we can). This is a set of filtering
// schemes and a KeySegmentsExtractor. For performance, a single extractor
// should be implemented to meet all the filtering needs of any given
// column family. KeySegmentsExtractor and FilterInput should be flexible
// enough that there is no loss of generality, e.g. with leaving segments
// blank and using segment ranges.
struct SstQueryFilterConfigs {
  std::vector<std::shared_ptr<SstQueryFilterConfig>> filters;
  std::shared_ptr<const KeySegmentsExtractor> extractor;

  // Whether this object represent an empty set of configs because no
  // applicable configurations were found. (This case is represented by
  // an internal singleton instance.)
  bool IsEmptyNotFound() const;
};

// SstQueryFilterConfigsManager provides facilities for safe and effective
// filtering version management, with simple dynamic upgrade/downgrade
// support. It is designed to encourage a development pattern that
// minimizes the risk of filter and extractor versioning bugs.
//
// SstQueryFilterConfigsManager is essentially an immutable mapping
// from {config_name_string, version_number} -> SstQueryFilterConfigs
// for some contiguous range of version numbers. It is also a starting
// point for specifying which configuration should be used, with awareness
// of other configurations that might already be persisted in SST files
// or switched to dynamically.
//
// Background: a single codebase might have many use cases and for
// each use case, a sequence of past, current, and future filtering
// configurations. It is common for future configurations to be
// implemented before automatically deployed in order to ensure that
// a DB can be effectively opened and operated on by a recent older code
// version. And it is common to maintain a reasonable history of past
// configurations to ensure smooth upgrade path and proper handling of
// older SST files that haven't been compacted recently.
//
// It would be possible to make SstQueryFilterConfigs dynamically
// configurable through strings, but that would encourage deployment
// of ad-hoc, under-tested configurations.
//
// Solution: the {config_name_string, version_number} -> SstQueryFilterConfigs
// mapping in SstQueryFilterConfigsManager formalizes the history (past and
// future) of approved/tested configurations for a given use case. Filter
// configurations are kept paired with extractors that they make sense with.
//
// The version numbers are "global" to the SstQueryFilterConfigsManager so
// that it is simple to determine whether a particular code version supports
// a particular filtering version, regardless of which use case. Numbering
// always starts with 1, as 0 is reserved for selecting a "no filters"
// configuration
//
// Consider an example initialized with this Data:
//
// SstQueryFilterConfigsManager::Data data = {
//      {1, {{"foo", foo_configs}}},
//      {2, {{"bar", bar_configs},
//           {"baz", baz_configs}}},
//      {3, {{"bar", bar_configs_v2}}},
//   };
//
// For example, MakeSharedFactory(..., "baz", 1) will use a default empty
// config, while both MakeSharedFactory(..., "baz", 2) and
// MakeSharedFactory(..., "baz", 3) select `baz_configs`. Selecting version
// >= 4 is rejected because those configurations are not known to this
// version of the code.
//
// For correct operation, existing versions should be treated as immutable
// (once committed to where they could enter production). For example, an
// update to "baz" should be done in a new version (4), not by amending to
// version 3. Note also (from before) that the behavior of named extractors
// must not change, so changes to key segment extraction should introduce
// new named extractors while keeping the old in the older configs.
//
// It is possible to eventually remove the oldest versions, as long as
// * You won't be rolling back to that version.
// * You don't have any SST files using an extractor that is only available
// in that version (and prior).
// * For each use case and version you might roll back to, you aren't removing
// the configuration in effect for that version. In our example above, we
// cannot simply remove version 1 because that would change the configuration
// of "foo" at version 2. Moving {"foo", foo_configs} to version 2 would be
// an acceptable work-around for retiring version 1.
// * There are no gaps in the version numbers specified. Even if you completely
// retire a use case and want to remove relevant code, you still need to keep
// an explicit mapping, even if it's empty, as in `{3, {}}` if retiring "bar".
//
// Internally, the SstQueryFilterConfigsManager greatly simplifies lifetime
// management for relevant objects such as KeySegmentExtractors, and provides
// a lighter weight (and less troublesome) mechanism for relevant named object
// look-up vs. ObjectRegistry. If following the guidelines above, any extractor
// referenced in a read SST file should also be referenced by the
// SstQueryFilterConfigsManager.
//
// Not user extensible
class SstQueryFilterConfigsManager
    : public std::enable_shared_from_this<SstQueryFilterConfigsManager> {
 public:
  using FilteringVersion = uint32_t;
  using NamedConfigs = std::pair<std::string, SstQueryFilterConfigs>;
  using Data =
      std::vector<std::pair<FilteringVersion, std::vector<NamedConfigs>>>;

  static Status MakeShared(const Data& data,
                           std::shared_ptr<SstQueryFilterConfigsManager>* out);

  virtual ~SstQueryFilterConfigsManager() {}

  // EXPERIMENTAL/TEMPORARY: hook into table properties for persisting
  // filters and table_filter for applying to range queries.

  class Factory : public TablePropertiesCollectorFactory {
   public:
    // Modify the target filtering version for new filters. Returns
    // non-OK if the version is not supported. Thread-safe.
    virtual Status SetFilteringVersion(FilteringVersion ver) = 0;
    virtual FilteringVersion GetFilteringVersion() const = 0;

    // The configs_name used to create this Factory. Immutable.
    virtual const std::string& GetConfigsName() const = 0;

    // The relevant configs from the SstQueryFilterConfigsManager for
    // the ConfigsName and FilteringVersion.
    virtual const SstQueryFilterConfigs& GetConfigs() const = 0;

    // The buffers pointed to by the Slices must live as long as any read
    // operations using this table filter function.
    // Can read and process any filters created under this
    // SstQueryFilterConfigsManager but is most efficient when using the
    // same KeySegmentExtractor as this Factory's configs.
    // (That performance optimization is the only reason this function is here
    // rather than in SstQueryFilterConfigsManager.)
    virtual std::function<bool(const TableProperties&)>
    GetTableFilterForRangeQuery(Slice lower_bound_incl,
                                Slice upper_bound_excl) const = 0;
  };

  // Returns OK and creates a Factory as long as `ver` is in the
  // supported range or 0 (always empty/not found). If the particular
  // config_name is not found under that version, then
  // factory->GetConfigs().IsEmptyNotFound() will be true. Such a factory can
  // read filters but will not write any filters.
  virtual Status MakeSharedFactory(const std::string& configs_name,
                                   FilteringVersion ver,
                                   std::shared_ptr<Factory>* out) const = 0;
};

}  // namespace experimental
}  // namespace ROCKSDB_NAMESPACE
