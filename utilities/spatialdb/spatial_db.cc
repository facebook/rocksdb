//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include <string>
#include <vector>
#include <algorithm>
#include <set>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/spatial_db.h"
#include "util/coding.h"

namespace rocksdb {
namespace spatial {

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
        double d = iter.second.get_double();
        output->append(reinterpret_cast<char*>(&d), sizeof(double));
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
        if (s.size() < sizeof(double)) {
          return false;
        }
        double d;
        memcpy(&d, s.data(), sizeof(double));
        map_.insert({key.ToString(), Variant(d)});
        s.remove_prefix(sizeof(double));
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

namespace {
// indexing idea from http://msdn.microsoft.com/en-us/library/bb259689.aspx
inline uint64_t GetTileFromCoord(double x, double start, double end,
                                 uint32_t tile_bits) {
  if (x < start) {
    return 0;
  }
  uint64_t tiles = static_cast<uint64_t>(1) << tile_bits;
  uint64_t r = ((x - start) / (end - start)) * tiles;
  return std::min(r, tiles - 1);
}
inline uint64_t GetQuadKeyFromTile(uint64_t tile_x, uint64_t tile_y,
                                   uint32_t tile_bits) {
  uint64_t quad_key = 0;
  for (uint32_t i = 0; i < tile_bits; ++i) {
    uint32_t mask = (1LL << i);
    quad_key |= (tile_x & mask) << i;
    quad_key |= (tile_y & mask) << (i + 1);
  }
  return quad_key;
}
inline BoundingBox<uint64_t> GetTileBoundingBox(
    const SpatialIndexOptions& spatial_index, BoundingBox<double> bbox) {
  return BoundingBox<uint64_t>(
      GetTileFromCoord(bbox.min_x, spatial_index.bbox.min_x,
                       spatial_index.bbox.max_x, spatial_index.tile_bits),
      GetTileFromCoord(bbox.min_y, spatial_index.bbox.min_y,
                       spatial_index.bbox.max_y, spatial_index.tile_bits),
      GetTileFromCoord(bbox.max_x, spatial_index.bbox.min_x,
                       spatial_index.bbox.max_x, spatial_index.tile_bits),
      GetTileFromCoord(bbox.max_y, spatial_index.bbox.min_y,
                       spatial_index.bbox.max_y, spatial_index.tile_bits));
}

// big endian can be compared using memcpy
inline void PutFixed64BigEndian(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  buf[0] = (value >> 56) & 0xff;
  buf[1] = (value >> 48) & 0xff;
  buf[2] = (value >> 40) & 0xff;
  buf[3] = (value >> 32) & 0xff;
  buf[4] = (value >> 24) & 0xff;
  buf[5] = (value >> 16) & 0xff;
  buf[6] = (value >> 8) & 0xff;
  buf[7] = value & 0xff;
  dst->append(buf, sizeof(buf));
}
// big endian can be compared using memcpy
inline bool GetFixed64BigEndian(const Slice& input, uint64_t* value) {
  if (input.size() < sizeof(uint64_t)) {
    return false;
  }
  auto ptr = input.data();
  *value = (static_cast<uint64_t>(static_cast<unsigned char>(ptr[0])) << 56) |
           (static_cast<uint64_t>(static_cast<unsigned char>(ptr[1])) << 48) |
           (static_cast<uint64_t>(static_cast<unsigned char>(ptr[2])) << 40) |
           (static_cast<uint64_t>(static_cast<unsigned char>(ptr[3])) << 32) |
           (static_cast<uint64_t>(static_cast<unsigned char>(ptr[4])) << 24) |
           (static_cast<uint64_t>(static_cast<unsigned char>(ptr[5])) << 16) |
           (static_cast<uint64_t>(static_cast<unsigned char>(ptr[6])) << 8) |
           static_cast<uint64_t>(static_cast<unsigned char>(ptr[7]));
  return true;
}

}  // namespace

class SpatialIndexCursor : public Cursor {
 public:
  SpatialIndexCursor(Iterator* spatial_iterator, Iterator* data_iterator,
                     const BoundingBox<uint64_t>& tile_bbox, uint32_t tile_bits)
      : spatial_iterator_(spatial_iterator),
        data_iterator_(data_iterator),
        tile_bbox_(tile_bbox),
        tile_bits_(tile_bits),
        valid_(true) {
    current_x_ = tile_bbox.min_x;
    current_y_ = tile_bbox.min_y;
    UpdateQuadKey();
    ReSeek();
    if (valid_) {
      // this is the first ID returned, so I don't care about return value of
      // Dedup
      Dedup();
    }
    if (valid_) {
      ExtractData();
    }
  }

  virtual bool Valid() const override { return valid_; }

  virtual void Next() override {
    assert(valid_);

    // this do-while loop deals only with deduplication
    do {
      spatial_iterator_->Next();
      if (ExtractID()) {
        // OK, found what we needed
        continue;
      }

      // move to the next tile
      Increment();

      if (ExtractID()) {
        // no need to reseek, found what we needed
        continue;
      }
      // reseek, find next good tile
      ReSeek();
    } while (valid_ && !Dedup() && valid_);

    if (valid_) {
      ExtractData();
    }
  }

  virtual const Slice blob() override { return current_blob_; }
  virtual const FeatureSet& feature_set() override {
    return current_feature_set_;
  }

  virtual Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    if (!spatial_iterator_->status().ok()) {
      return spatial_iterator_->status();
    }
    return data_iterator_->status();
  }

 private:
  // returns true if OK, false if already returned (duplicate)
  bool Dedup() {
    assert(valid_);
    uint64_t id;
    bool ok = GetFixed64BigEndian(current_id_, &id);
    if (!ok) {
      valid_ = false;
      status_ = Status::Corruption("Spatial index corruption");
      return false;
    }
    if (returned_ids_.find(id) != returned_ids_.end()) {
      return false;
    }
    returned_ids_.insert(id);
    return true;
  }
  void ReSeek() {
    while (valid_) {
      spatial_iterator_->Seek(current_quad_key_);
      if (ExtractID()) {
        // found what we're looking for!
        break;
      }
      Increment();
    }
  }

  void Increment() {
    ++current_x_;
    if (current_x_ > tile_bbox_.max_x) {
      current_x_ = tile_bbox_.min_x;
      ++current_y_;
    }
    if (current_y_ > tile_bbox_.max_y) {
      valid_ = false;
    } else {
      UpdateQuadKey();
    }
  }
  void UpdateQuadKey() {
    current_quad_key_.clear();
    PutFixed64BigEndian(&current_quad_key_,
                        GetQuadKeyFromTile(current_x_, current_y_, tile_bits_));
  }
  // * returns true if spatial iterator is on the current quad key and all is
  // well. Caller will call Next() to get new data
  // * returns false if spatial iterator is not on current, or invalid or status
  // bad. Caller will need to reseek to get new data
  bool ExtractID() {
    if (!spatial_iterator_->Valid()) {
      // caller needs to reseek
      return false;
    }
    if (spatial_iterator_->key().size() != 2 * sizeof(uint64_t)) {
      status_ = Status::Corruption("Invalid spatial index key");
      valid_ = false;
      return false;
    }
    Slice quad_key(spatial_iterator_->key().data(), sizeof(uint64_t));
    if (quad_key != current_quad_key_) {
      // caller needs to reseek
      return false;
    }
    // if we come to here, we have found the quad key
    current_id_ = Slice(spatial_iterator_->key().data() + sizeof(uint64_t),
                        sizeof(uint64_t));
    return true;
  }
  // doesn't return anything, but sets valid_ and status_ on corruption
  void ExtractData() {
    assert(valid_);
    data_iterator_->Seek(current_id_);

    if (!data_iterator_->Valid() || data_iterator_->key() != current_id_) {
      status_ = Status::Corruption("Inconsistency in data column family");
      valid_ = false;
      return;
    }

    Slice data = data_iterator_->value();
    current_feature_set_.Clear();
    if (!GetLengthPrefixedSlice(&data, &current_blob_) ||
        !current_feature_set_.Deserialize(data)) {
      status_ = Status::Corruption("Data column family corruption");
      valid_ = false;
      return;
    }
  }

  unique_ptr<Iterator> spatial_iterator_;
  unique_ptr<Iterator> data_iterator_;
  BoundingBox<uint64_t> tile_bbox_;
  uint32_t tile_bits_;
  uint64_t current_x_;
  uint64_t current_y_;
  std::string current_quad_key_;
  Slice current_id_;
  bool valid_;
  Status status_;

  FeatureSet current_feature_set_;
  Slice current_blob_;

  // used for deduplicating results
  std::set<uint64_t> returned_ids_;
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

// Column families are used to store element's data and spatial indexes. We use
// [default] column family to store the element data. This is the format of
// [default] column family:
// * id (fixed 64 big endian) -> blob (length prefixed slice) feature_set
// (serialized)
// We have one additional column family for each spatial index. The name of the
// column family is [spatial$<spatial_index_name>]. The format is:
// * quad_key (fixed 64 bit big endian) id (fixed 64 bit big endian) -> ""
class SpatialDBImpl : public SpatialDB {
 public:
  // * db -- base DB that needs to be forwarded to StackableDB
  // * data_column_family -- column family used to store the data
  // * spatial_indexes -- a list of spatial indexes together with column
  // families that correspond to those spatial indexes
  // * next_id -- next ID in auto-incrementing ID. This is usually
  // `max_id_currenty_in_db + 1`
  SpatialDBImpl(DB* db, ColumnFamilyHandle* data_column_family,
                const std::vector<
                    std::pair<const SpatialIndexOptions&, ColumnFamilyHandle*>>
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
      t = CompactRange(iter.second.column_family, nullptr, nullptr);
      if (!t.ok()) {
        s = t;
      }
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

Status SpatialDB::Open(const SpatialDBOptions& options, const std::string& name,
                       const std::vector<SpatialIndexOptions>& spatial_indexes,
                       SpatialDB** db, bool read_only) {
  Options rocksdb_options = GetRocksDBOptionsFromOptions(options);
  rocksdb_options.create_if_missing = true;
  rocksdb_options.create_missing_column_families = true;

  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(ColumnFamilyDescriptor(
      kDefaultColumnFamilyName, ColumnFamilyOptions(rocksdb_options)));

  for (const auto& index : spatial_indexes) {
    column_families.emplace_back("spatial$" + index.name,
                                 ColumnFamilyOptions(rocksdb_options));
  }
  std::vector<ColumnFamilyHandle*> handles;
  DB* base_db;
  Status s;
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

  std::vector<std::pair<const SpatialIndexOptions&, ColumnFamilyHandle*>>
      index_cf;
  assert(handles.size() == spatial_indexes.size() + 1);
  for (size_t i = 0; i < spatial_indexes.size(); ++i) {
    index_cf.emplace_back(spatial_indexes[i], handles[i + 1]);
  }
  uint64_t next_id;
  {
    // find next_id
    Iterator* iter = base_db->NewIterator(ReadOptions(), handles[0]);
    iter->SeekToLast();
    if (iter->Valid()) {
      uint64_t last_id;
      bool ok = GetFixed64BigEndian(iter->key(), &last_id);
      if (!ok) {
        return Status::Corruption("Invalid key in data column family");
      }
      next_id = last_id + 1;
    } else {
      next_id = 1;
    }
    delete iter;
  }

  *db = new SpatialDBImpl(base_db, handles[0], index_cf, next_id);
  return Status::OK();
}

}  // namespace spatial
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
