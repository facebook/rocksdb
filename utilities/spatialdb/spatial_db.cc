//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/spatial_db.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string>
#include <vector>
#include <algorithm>
#include <set>
#include <unordered_set>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"
#include "util/coding.h"
#include "utilities/spatialdb/utils.h"

namespace rocksdb {
namespace spatial {

// Column families are used to store element's data and spatial indexes. We use
// [default] column family to store the element data. This is the format of
// [default] column family:
// * id (fixed 64 big endian) -> blob (length prefixed slice) feature_set
// (serialized)
// We have one additional column family for each spatial index. The name of the
// column family is [spatial$<spatial_index_name>]. The format is:
// * quad_key (fixed 64 bit big endian) id (fixed 64 bit big endian) -> ""
// We store information about indexes in [metadata] column family. Format is:
// * spatial$<spatial_index_name> -> bbox (4 double encodings) tile_bits
// (varint32)

namespace {
const std::string kMetadataColumnFamilyName("metadata");
inline std::string GetSpatialIndexColumnFamilyName(
    const std::string& spatial_index_name) {
  return "spatial$" + spatial_index_name;
}
inline bool GetSpatialIndexName(const std::string& column_family_name,
                                Slice* dst) {
  *dst = Slice(column_family_name);
  if (dst->starts_with("spatial$")) {
    dst->remove_prefix(8);  // strlen("spatial$")
    return true;
  }
  return false;
}

}  // namespace

Variant::Variant(const Variant& v) : type_(v.type_) {
  switch (v.type_) {
    case kNull:
      break;
    case kBool:
      data_.b = v.data_.b;
      break;
    case kInt:
      data_.i = v.data_.i;
      break;
    case kDouble:
      data_.d = v.data_.d;
      break;
    case kString:
      new (&data_.s) std::string(v.data_.s);
      break;
    default:
      assert(false);
  }
}

bool Variant::operator==(const Variant& rhs) {
  if (type_ != rhs.type_) {
    return false;
  }

  switch (type_) {
    case kNull:
      return true;
    case kBool:
      return data_.b == rhs.data_.b;
    case kInt:
      return data_.i == rhs.data_.i;
    case kDouble:
      return data_.d == rhs.data_.d;
    case kString:
      return data_.s == rhs.data_.s;
    default:
      assert(false);
  }
  // it will never reach here, but otherwise the compiler complains
  return false;
}

bool Variant::operator!=(const Variant& rhs) { return !(*this == rhs); }

FeatureSet* FeatureSet::Set(const std::string& key, const Variant& value) {
  map_.insert({key, value});
  return this;
}

bool FeatureSet::Contains(const std::string& key) const {
  return map_.find(key) != map_.end();
}

const Variant& FeatureSet::Get(const std::string& key) const {
  auto itr = map_.find(key);
  assert(itr != map_.end());
  return itr->second;
}

FeatureSet::iterator FeatureSet::Find(const std::string& key) const {
  return iterator(map_.find(key));
}

void FeatureSet::Clear() { map_.clear(); }

void FeatureSet::Serialize(std::string* output) const {
  for (const auto& iter : map_) {
    PutLengthPrefixedSlice(output, iter.first);
    output->push_back(static_cast<char>(iter.second.type()));
    switch (iter.second.type()) {
      case Variant::kNull:
        break;
      case Variant::kBool:
        output->push_back(static_cast<char>(iter.second.get_bool()));
        break;
      case Variant::kInt:
        PutVarint64(output, iter.second.get_int());
        break;
      case Variant::kDouble: {
        PutDouble(output, iter.second.get_double());
        break;
      }
      case Variant::kString:
        PutLengthPrefixedSlice(output, iter.second.get_string());
        break;
      default:
        assert(false);
    }
  }
}

bool FeatureSet::Deserialize(const Slice& input) {
  assert(map_.empty());
  Slice s(input);
  while (s.size()) {
    Slice key;
    if (!GetLengthPrefixedSlice(&s, &key) || s.size() == 0) {
      return false;
    }
    char type = s[0];
    s.remove_prefix(1);
    switch (type) {
      case Variant::kNull: {
        map_.insert({key.ToString(), Variant()});
        break;
      }
      case Variant::kBool: {
        if (s.size() == 0) {
          return false;
        }
        map_.insert({key.ToString(), Variant(static_cast<bool>(s[0]))});
        s.remove_prefix(1);
        break;
      }
      case Variant::kInt: {
        uint64_t v;
        if (!GetVarint64(&s, &v)) {
          return false;
        }
        map_.insert({key.ToString(), Variant(v)});
        break;
      }
      case Variant::kDouble: {
        double d;
        if (!GetDouble(&s, &d)) {
          return false;
        }
        map_.insert({key.ToString(), Variant(d)});
        break;
      }
      case Variant::kString: {
        Slice str;
        if (!GetLengthPrefixedSlice(&s, &str)) {
          return false;
        }
        map_.insert({key.ToString(), str.ToString()});
        break;
      }
      default:
        return false;
    }
  }
  return true;
}

std::string FeatureSet::DebugString() const {
  std::string out = "{";
  bool comma = false;
  for (const auto& iter : map_) {
    if (comma) {
      out.append(", ");
    } else {
      comma = true;
    }
    out.append("\"" + iter.first + "\": ");
    switch (iter.second.type()) {
      case Variant::kNull:
        out.append("null");
      case Variant::kBool:
        if (iter.second.get_bool()) {
          out.append("true");
        } else {
          out.append("false");
        }
        break;
      case Variant::kInt: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%" PRIu64, iter.second.get_int());
        out.append(buf);
        break;
      }
      case Variant::kDouble: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%lf", iter.second.get_double());
        out.append(buf);
        break;
      }
      case Variant::kString:
        out.append("\"" + iter.second.get_string() + "\"");
        break;
      default:
        assert(false);
    }
  }
  return out + "}";
}

class SpatialIndexCursor : public Cursor {
 public:
  // tile_box is inclusive
  SpatialIndexCursor(Iterator* spatial_iterator, Iterator* data_iterator,
                     const BoundingBox<uint64_t>& tile_bbox, uint32_t tile_bits)
      : data_iterator_(data_iterator),
        valid_(true) {
    // calculate quad keys we'll need to query
    std::vector<uint64_t> quad_keys;
    quad_keys.reserve((tile_bbox.max_x - tile_bbox.min_x + 1) *
                      (tile_bbox.max_y - tile_bbox.min_y + 1));
    for (uint64_t x = tile_bbox.min_x; x <= tile_bbox.max_x; ++x) {
      for (uint64_t y = tile_bbox.min_y; y <= tile_bbox.max_y; ++y) {
        quad_keys.push_back(GetQuadKeyFromTile(x, y, tile_bits));
      }
    }
    std::sort(quad_keys.begin(), quad_keys.end());

    // load primary key ids for all quad keys
    for (auto quad_key : quad_keys) {
      std::string encoded_quad_key;
      PutFixed64BigEndian(&encoded_quad_key, quad_key);
      Slice slice_quad_key(encoded_quad_key);

      // If CheckQuadKey is true, there is no need to reseek, since
      // spatial_iterator is already pointing at the correct quad key. This is
      // an optimization.
      if (!CheckQuadKey(spatial_iterator, slice_quad_key)) {
        spatial_iterator->Seek(slice_quad_key);
      }

      while (CheckQuadKey(spatial_iterator, slice_quad_key)) {
        // extract ID from spatial_iterator
        uint64_t id;
        bool ok = GetFixed64BigEndian(
            Slice(spatial_iterator->key().data() + sizeof(uint64_t),
                  sizeof(uint64_t)),
            &id);
        if (!ok) {
          valid_ = false;
          status_ = Status::Corruption("Spatial index corruption");
          break;
        }
        primary_key_ids_.insert(id);
        spatial_iterator->Next();
      }
    }

    if (!spatial_iterator->status().ok()) {
      status_ = spatial_iterator->status();
      valid_ = false;
    }
    delete spatial_iterator;

    valid_ = valid_ && primary_key_ids_.size() > 0;

    if (valid_) {
      primary_keys_iterator_ = primary_key_ids_.begin();
      ExtractData();
    }
  }

  virtual bool Valid() const override { return valid_; }

  virtual void Next() override {
    assert(valid_);

    ++primary_keys_iterator_;
    if (primary_keys_iterator_ == primary_key_ids_.end()) {
      valid_ = false;
      return;
    }

    ExtractData();
  }

  virtual const Slice blob() override { return current_blob_; }
  virtual const FeatureSet& feature_set() override {
    return current_feature_set_;
  }

  virtual Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    return data_iterator_->status();
  }

 private:
  // * returns true if spatial iterator is on the current quad key and all is
  // well
  // * returns false if spatial iterator is not on current, or iterator is
  // invalid or corruption
  bool CheckQuadKey(Iterator* spatial_iterator, const Slice& quad_key) {
    if (!spatial_iterator->Valid()) {
      return false;
    }
    if (spatial_iterator->key().size() != 2 * sizeof(uint64_t)) {
      status_ = Status::Corruption("Invalid spatial index key");
      valid_ = false;
      return false;
    }
    Slice spatial_iterator_quad_key(spatial_iterator->key().data(),
                                    sizeof(uint64_t));
    if (spatial_iterator_quad_key != quad_key) {
      // caller needs to reseek
      return false;
    }
    // if we come to here, we have found the quad key
    return true;
  }

  // doesn't return anything, but sets valid_ and status_ on corruption
  void ExtractData() {
    assert(valid_);
    std::string encoded_id;
    PutFixed64BigEndian(&encoded_id, *primary_keys_iterator_);

    data_iterator_->Seek(encoded_id);

    if (!data_iterator_->Valid() ||
        data_iterator_->key() != Slice(encoded_id)) {
      status_ = Status::Corruption("Index inconsistency");
      valid_ = false;
      return;
    }

    Slice data = data_iterator_->value();
    current_feature_set_.Clear();
    if (!GetLengthPrefixedSlice(&data, &current_blob_) ||
        !current_feature_set_.Deserialize(data)) {
      status_ = Status::Corruption("Primary key column family corruption");
      valid_ = false;
      return;
    }
  }

  unique_ptr<Iterator> data_iterator_;
  bool valid_;
  Status status_;

  FeatureSet current_feature_set_;
  Slice current_blob_;

  // This is loaded from spatial iterator.
  std::unordered_set<uint64_t> primary_key_ids_;
  std::unordered_set<uint64_t>::iterator primary_keys_iterator_;
};

class ErrorCursor : public Cursor {
 public:
  explicit ErrorCursor(Status s) : s_(s) { assert(!s.ok()); }
  virtual Status status() const override { return s_; }
  virtual bool Valid() const override { return false; }
  virtual void Next() override { assert(false); }

  virtual const Slice blob() override {
    assert(false);
    return Slice();
  }
  virtual const FeatureSet& feature_set() override {
    assert(false);
    // compiler complains otherwise
    return trash_;
  }

 private:
  Status s_;
  FeatureSet trash_;
};

class SpatialDBImpl : public SpatialDB {
 public:
  // * db -- base DB that needs to be forwarded to StackableDB
  // * data_column_family -- column family used to store the data
  // * spatial_indexes -- a list of spatial indexes together with column
  // families that correspond to those spatial indexes
  // * next_id -- next ID in auto-incrementing ID. This is usually
  // `max_id_currenty_in_db + 1`
  SpatialDBImpl(
      DB* db, ColumnFamilyHandle* data_column_family,
      const std::vector<std::pair<SpatialIndexOptions, ColumnFamilyHandle*>>&
          spatial_indexes,
      uint64_t next_id)
      : SpatialDB(db),
        data_column_family_(data_column_family),
        next_id_(next_id) {
    for (const auto& index : spatial_indexes) {
      name_to_index_.insert(
          {index.first.name, IndexColumnFamily(index.first, index.second)});
    }
  }

  ~SpatialDBImpl() {
    for (auto& iter : name_to_index_) {
      delete iter.second.column_family;
    }
    delete data_column_family_;
  }

  virtual Status Insert(
      const WriteOptions& write_options, const BoundingBox<double>& bbox,
      const Slice& blob, const FeatureSet& feature_set,
      const std::vector<std::string>& spatial_indexes) override {
    WriteBatch batch;

    if (spatial_indexes.size() == 0) {
      return Status::InvalidArgument("Spatial indexes can't be empty");
    }

    uint64_t id = next_id_.fetch_add(1);

    for (const auto& si : spatial_indexes) {
      auto itr = name_to_index_.find(si);
      if (itr == name_to_index_.end()) {
        return Status::InvalidArgument("Can't find index " + si);
      }
      const auto& spatial_index = itr->second.index;
      if (!spatial_index.bbox.Intersects(bbox)) {
        continue;
      }
      BoundingBox<uint64_t> tile_bbox = GetTileBoundingBox(spatial_index, bbox);

      for (uint64_t x = tile_bbox.min_x; x <= tile_bbox.max_x; ++x) {
        for (uint64_t y = tile_bbox.min_y; y <= tile_bbox.max_y; ++y) {
          // see above for format
          std::string key;
          PutFixed64BigEndian(
              &key, GetQuadKeyFromTile(x, y, spatial_index.tile_bits));
          PutFixed64BigEndian(&key, id);
          batch.Put(itr->second.column_family, key, Slice());
        }
      }
    }

    // see above for format
    std::string data_key;
    PutFixed64BigEndian(&data_key, id);
    std::string data_value;
    PutLengthPrefixedSlice(&data_value, blob);
    feature_set.Serialize(&data_value);
    batch.Put(data_column_family_, data_key, data_value);

    return Write(write_options, &batch);
  }

  virtual Status Compact() override {
    Status s, t;
    for (auto& iter : name_to_index_) {
      t = Flush(FlushOptions(), iter.second.column_family);
      if (!t.ok()) {
        s = t;
      }
      t = CompactRange(iter.second.column_family, nullptr, nullptr);
      if (!t.ok()) {
        s = t;
      }
    }
    t = Flush(FlushOptions(), data_column_family_);
    if (!t.ok()) {
      s = t;
    }
    t = CompactRange(data_column_family_, nullptr, nullptr);
    if (!t.ok()) {
      s = t;
    }
    return s;
  }

  virtual Cursor* Query(const ReadOptions& read_options,
                        const BoundingBox<double>& bbox,
                        const std::string& spatial_index) override {
    auto itr = name_to_index_.find(spatial_index);
    if (itr == name_to_index_.end()) {
      return new ErrorCursor(Status::InvalidArgument(
          "Spatial index " + spatial_index + " not found"));
    }

    std::vector<Iterator*> iterators;
    Status s = NewIterators(read_options,
                            {data_column_family_, itr->second.column_family},
                            &iterators);
    if (!s.ok()) {
      return new ErrorCursor(s);
    }

    const auto& si = itr->second.index;
    return new SpatialIndexCursor(iterators[1], iterators[0],
                                  GetTileBoundingBox(si, bbox), si.tile_bits);
  }

 private:
  ColumnFamilyHandle* data_column_family_;
  struct IndexColumnFamily {
    SpatialIndexOptions index;
    ColumnFamilyHandle* column_family;
    IndexColumnFamily(const SpatialIndexOptions& _index,
                      ColumnFamilyHandle* _cf)
        : index(_index), column_family(_cf) {}
  };
  // constant after construction!
  std::unordered_map<std::string, IndexColumnFamily> name_to_index_;

  std::atomic<uint64_t> next_id_;
};

namespace {
Options GetRocksDBOptionsFromOptions(const SpatialDBOptions& options) {
  Options rocksdb_options;
  rocksdb_options.OptimizeLevelStyleCompaction();
  rocksdb_options.IncreaseParallelism(options.num_threads);
  rocksdb_options.block_cache = NewLRUCache(options.cache_size);
  if (options.bulk_load) {
    rocksdb_options.PrepareForBulkLoad();
  }
  return rocksdb_options;
}
}  // namespace

class MetadataStorage {
 public:
  MetadataStorage(DB* db, ColumnFamilyHandle* cf) : db_(db), cf_(cf) {}
  ~MetadataStorage() {}

  // format: <min_x double> <min_y double> <max_x double> <max_y double>
  // <tile_bits varint32>
  Status AddIndex(const SpatialIndexOptions& index) {
    std::string encoded_index;
    PutDouble(&encoded_index, index.bbox.min_x);
    PutDouble(&encoded_index, index.bbox.min_y);
    PutDouble(&encoded_index, index.bbox.max_x);
    PutDouble(&encoded_index, index.bbox.max_y);
    PutVarint32(&encoded_index, index.tile_bits);
    return db_->Put(WriteOptions(), cf_,
                    GetSpatialIndexColumnFamilyName(index.name), encoded_index);
  }

  Status GetIndex(const std::string& name, SpatialIndexOptions* dst) {
    std::string value;
    Status s = db_->Get(ReadOptions(), cf_,
                        GetSpatialIndexColumnFamilyName(name), &value);
    if (!s.ok()) {
      return s;
    }
    dst->name = name;
    Slice encoded_index(value);
    bool ok = GetDouble(&encoded_index, &(dst->bbox.min_x));
    ok = ok && GetDouble(&encoded_index, &(dst->bbox.min_y));
    ok = ok && GetDouble(&encoded_index, &(dst->bbox.max_x));
    ok = ok && GetDouble(&encoded_index, &(dst->bbox.max_y));
    ok = ok && GetVarint32(&encoded_index, &(dst->tile_bits));
    return ok ? Status::OK() : Status::Corruption("Index encoding corrupted");
  }

 private:
  DB* db_;
  ColumnFamilyHandle* cf_;
};

Status SpatialDB::Create(
    const SpatialDBOptions& options, const std::string& name,
    const std::vector<SpatialIndexOptions>& spatial_indexes) {
  Options rocksdb_options = GetRocksDBOptionsFromOptions(options);
  rocksdb_options.create_if_missing = true;
  rocksdb_options.create_missing_column_families = true;
  rocksdb_options.error_if_exists = true;

  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(ColumnFamilyDescriptor(
      kDefaultColumnFamilyName, ColumnFamilyOptions(rocksdb_options)));
  column_families.push_back(ColumnFamilyDescriptor(
      kMetadataColumnFamilyName, ColumnFamilyOptions(rocksdb_options)));

  for (const auto& index : spatial_indexes) {
    column_families.emplace_back(GetSpatialIndexColumnFamilyName(index.name),
                                 ColumnFamilyOptions(rocksdb_options));
  }

  std::vector<ColumnFamilyHandle*> handles;
  DB* base_db;
  Status s = DB::Open(DBOptions(rocksdb_options), name, column_families,
                      &handles, &base_db);
  if (!s.ok()) {
    return s;
  }
  MetadataStorage metadata(base_db, handles[1]);
  for (const auto& index : spatial_indexes) {
    s = metadata.AddIndex(index);
    if (!s.ok()) {
      break;
    }
  }

  for (auto h : handles) {
    delete h;
  }
  delete base_db;

  return s;
}

Status SpatialDB::Open(const SpatialDBOptions& options, const std::string& name,
                       SpatialDB** db, bool read_only) {
  Options rocksdb_options = GetRocksDBOptionsFromOptions(options);

  Status s;
  std::vector<std::string> existing_column_families;
  std::vector<std::string> spatial_indexes;
  s = DB::ListColumnFamilies(DBOptions(rocksdb_options), name,
                             &existing_column_families);
  if (!s.ok()) {
    return s;
  }
  for (const auto& cf_name : existing_column_families) {
    Slice spatial_index;
    if (GetSpatialIndexName(cf_name, &spatial_index)) {
      spatial_indexes.emplace_back(spatial_index.data(), spatial_index.size());
    }
  }

  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(ColumnFamilyDescriptor(
      kDefaultColumnFamilyName, ColumnFamilyOptions(rocksdb_options)));
  column_families.push_back(ColumnFamilyDescriptor(
      kMetadataColumnFamilyName, ColumnFamilyOptions(rocksdb_options)));

  for (const auto& index : spatial_indexes) {
    column_families.emplace_back(GetSpatialIndexColumnFamilyName(index),
                                 ColumnFamilyOptions(rocksdb_options));
  }
  std::vector<ColumnFamilyHandle*> handles;
  DB* base_db;
  if (read_only) {
    s = DB::OpenForReadOnly(DBOptions(rocksdb_options), name, column_families,
                            &handles, &base_db);
  } else {
    s = DB::Open(DBOptions(rocksdb_options), name, column_families, &handles,
                 &base_db);
  }
  if (!s.ok()) {
    return s;
  }

  MetadataStorage metadata(base_db, handles[1]);

  std::vector<std::pair<SpatialIndexOptions, ColumnFamilyHandle*>> index_cf;
  assert(handles.size() == spatial_indexes.size() + 2);
  for (size_t i = 0; i < spatial_indexes.size(); ++i) {
    SpatialIndexOptions index_options;
    s = metadata.GetIndex(spatial_indexes[i], &index_options);
    if (!s.ok()) {
      break;
    }
    index_cf.emplace_back(index_options, handles[i + 2]);
  }
  uint64_t next_id = 1;
  if (s.ok()) {
    // find next_id
    Iterator* iter = base_db->NewIterator(ReadOptions(), handles[0]);
    iter->SeekToLast();
    if (iter->Valid()) {
      uint64_t last_id = 0;
      if (!GetFixed64BigEndian(iter->key(), &last_id)) {
        s = Status::Corruption("Invalid key in data column family");
      } else {
        next_id = last_id + 1;
      }
    }
    delete iter;
  }
  if (!s.ok()) {
    for (auto h : handles) {
      delete h;
    }
    delete db;
    return s;
  }

  // I don't need metadata column family any more, so delete it
  delete handles[1];
  *db = new SpatialDBImpl(base_db, handles[0], index_cf, next_id);
  return Status::OK();
}

}  // namespace spatial
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
