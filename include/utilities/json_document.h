//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <map>
#include <unordered_map>
#include <vector>

#include "rocksdb/slice.h"

// We use JSONDocument for DocumentDB API
// Implementation inspired by folly::dynamic and rapidjson

namespace rocksdb {

// NOTE: none of this is thread-safe
class JSONDocument {
 public:
  // return nullptr on parse failure
  static JSONDocument* ParseJSON(const char* json);

  enum Type {
    kNull,
    kArray,
    kBool,
    kDouble,
    kInt64,
    kObject,
    kString,
  };

  JSONDocument();  // null
  /* implicit */ JSONDocument(bool b);
  /* implicit */ JSONDocument(double d);
  /* implicit */ JSONDocument(int64_t i);
  /* implicit */ JSONDocument(const std::string& s);
  /* implicit */ JSONDocument(const char* s);
  // constructs JSONDocument of specific type with default value
  explicit JSONDocument(Type type);

  // copy constructor
  JSONDocument(const JSONDocument& json_document);

  ~JSONDocument();

  Type type() const;

  // REQUIRES: IsObject()
  bool Contains(const std::string& key) const;
  // Returns nullptr if !Contains()
  // don't delete the returned pointer
  // REQUIRES: IsObject()
  const JSONDocument* Get(const std::string& key) const;
  // REQUIRES: IsObject()
  JSONDocument& operator[](const std::string& key);
  // REQUIRES: IsObject()
  const JSONDocument& operator[](const std::string& key) const;
  // returns `this`, so you can chain operations.
  // Copies value
  // REQUIRES: IsObject()
  JSONDocument* Set(const std::string& key, const JSONDocument& value);

  // REQUIRES: IsArray() == true || IsObject() == true
  size_t Count() const;

  // REQUIRES: IsArray()
  const JSONDocument* GetFromArray(size_t i) const;
  // REQUIRES: IsArray()
  JSONDocument& operator[](size_t i);
  // REQUIRES: IsArray()
  const JSONDocument& operator[](size_t i) const;
  // returns `this`, so you can chain operations.
  // Copies the value
  // REQUIRES: IsArray() && i < Count()
  JSONDocument* SetInArray(size_t i, const JSONDocument& value);
  // REQUIRES: IsArray()
  JSONDocument* PushBack(const JSONDocument& value);

  bool IsNull() const;
  bool IsArray() const;
  bool IsBool() const;
  bool IsDouble() const;
  bool IsInt64() const;
  bool IsObject() const;
  bool IsString() const;

  // REQUIRES: IsBool() == true
  bool GetBool() const;
  // REQUIRES: IsDouble() == true
  double GetDouble() const;
  // REQUIRES: IsInt64() == true
  int64_t GetInt64() const;
  // REQUIRES: IsString() == true
  const std::string& GetString() const;

  bool operator==(const JSONDocument& rhs) const;

  std::string DebugString() const;

 private:
  class ItemsIteratorGenerator;

 public:
  // REQUIRES: IsObject()
  ItemsIteratorGenerator Items() const;

  // appends serialized object to dst
  void Serialize(std::string* dst) const;
  // returns nullptr if Slice doesn't represent valid serialized JSONDocument
  static JSONDocument* Deserialize(const Slice& src);

 private:
  void SerializeInternal(std::string* dst, bool type_prefix) const;
  // returns false if Slice doesn't represent valid serialized JSONDocument.
  // Otherwise, true
  bool DeserializeInternal(Slice* input);

  typedef std::vector<JSONDocument*> Array;
  typedef std::unordered_map<std::string, JSONDocument*> Object;

  // iteration on objects
  class const_item_iterator {
   public:
    typedef Object::const_iterator It;
    typedef Object::value_type value_type;
    /* implicit */ const_item_iterator(It it) : it_(it) {}
    It& operator++() { return ++it_; }
    bool operator!=(const const_item_iterator& other) {
      return it_ != other.it_;
    }
    value_type operator*() { return *it_; }

   private:
    It it_;
  };
  class ItemsIteratorGenerator {
   public:
    /* implicit */ ItemsIteratorGenerator(const Object& object)
        : object_(object) {}
    const_item_iterator begin() { return object_.begin(); }
    const_item_iterator end() { return object_.end(); }

   private:
    const Object& object_;
  };

  union Data {
    Data() : n(nullptr) {}
    ~Data() {}

    void* n;
    Array a;
    bool b;
    double d;
    int64_t i;
    std::string s;
    Object o;
  } data_;
  const Type type_;

  // Our serialization format's first byte specifies the encoding version. That
  // way, we can easily change our format while providing backwards
  // compatibility. This constant specifies the current version of the
  // serialization format
  static const char kSerializationFormatVersion;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
