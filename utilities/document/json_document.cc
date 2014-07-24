//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/json_document.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <cassert>
#include <string>
#include <map>
#include <vector>

#include "third-party/rapidjson/reader.h"
#include "util/coding.h"

namespace rocksdb {

JSONDocument::JSONDocument() : type_(kNull) {}
JSONDocument::JSONDocument(bool b) : type_(kBool) { data_.b = b; }
JSONDocument::JSONDocument(double d) : type_(kDouble) { data_.d = d; }
JSONDocument::JSONDocument(int64_t i) : type_(kInt64) { data_.i = i; }
JSONDocument::JSONDocument(const std::string& s) : type_(kString) {
  new (&data_.s) std::string(s);
}
JSONDocument::JSONDocument(const char* s) : type_(kString) {
  new (&data_.s) std::string(s);
}
JSONDocument::JSONDocument(Type type) : type_(type) {
  // TODO(icanadi) make all of this better by using templates
  switch (type) {
    case kNull:
      break;
    case kObject:
      new (&data_.o) Object;
      break;
    case kBool:
      data_.b = false;
      break;
    case kDouble:
      data_.d = 0.0;
      break;
    case kArray:
      new (&data_.a) Array;
      break;
    case kInt64:
      data_.i = 0;
      break;
    case kString:
      new (&data_.s) std::string();
      break;
    default:
      assert(false);
  }
}

JSONDocument::JSONDocument(const JSONDocument& json_document)
    : JSONDocument(json_document.type_) {
  switch (json_document.type_) {
    case kNull:
      break;
    case kArray:
      data_.a.reserve(json_document.data_.a.size());
      for (const auto& iter : json_document.data_.a) {
        // deep copy
        data_.a.push_back(new JSONDocument(*iter));
      }
      break;
    case kBool:
      data_.b = json_document.data_.b;
      break;
    case kDouble:
      data_.d = json_document.data_.d;
      break;
    case kInt64:
      data_.i = json_document.data_.i;
      break;
    case kObject: {
      for (const auto& iter : json_document.data_.o) {
        // deep copy
        data_.o.insert({iter.first, new JSONDocument(*iter.second)});
      }
      break;
    }
    case kString:
      data_.s = json_document.data_.s;
      break;
    default:
      assert(false);
  }
}

JSONDocument::~JSONDocument() {
  switch (type_) {
    case kObject:
      for (auto iter : data_.o) {
        delete iter.second;
      }
      (&data_.o)->~Object();
      break;
    case kArray:
      for (auto iter : data_.a) {
        delete iter;
      }
      (&data_.a)->~Array();
      break;
    case kString:
      using std::string;
      (&data_.s)->~string();
      break;
    default:
      // we're cool, no need for destructors for others
      break;
  }
}

JSONDocument::Type JSONDocument::type() const { return type_; }

bool JSONDocument::Contains(const std::string& key) const {
  assert(type_ == kObject);
  auto iter = data_.o.find(key);
  return iter != data_.o.end();
}

const JSONDocument* JSONDocument::Get(const std::string& key) const {
  assert(type_ == kObject);
  auto iter = data_.o.find(key);
  if (iter == data_.o.end()) {
    return nullptr;
  }
  return iter->second;
}

JSONDocument& JSONDocument::operator[](const std::string& key) {
  assert(type_ == kObject);
  auto iter = data_.o.find(key);
  assert(iter != data_.o.end());
  return *(iter->second);
}

const JSONDocument& JSONDocument::operator[](const std::string& key) const {
  assert(type_ == kObject);
  auto iter = data_.o.find(key);
  assert(iter != data_.o.end());
  return *(iter->second);
}

JSONDocument* JSONDocument::Set(const std::string& key, const JSONDocument& value) {
  assert(type_ == kObject);
  auto itr = data_.o.find(key);
  if (itr == data_.o.end()) {
    // insert
    data_.o.insert({key, new JSONDocument(value)});
  } else {
    // overwrite
    delete itr->second;
    itr->second = new JSONDocument(value);
  }
  return this;
}

size_t JSONDocument::Count() const {
  assert(type_ == kArray || type_ == kObject);
  if (type_ == kArray) {
    return data_.a.size();
  } else if (type_ == kObject) {
    return data_.o.size();
  }
  assert(false);
  return 0;
}

const JSONDocument* JSONDocument::GetFromArray(size_t i) const {
  assert(type_ == kArray);
  return data_.a[i];
}

JSONDocument& JSONDocument::operator[](size_t i) {
  assert(type_ == kArray && i < data_.a.size());
  return *data_.a[i];
}

const JSONDocument& JSONDocument::operator[](size_t i) const {
  assert(type_ == kArray && i < data_.a.size());
  return *data_.a[i];
}

JSONDocument* JSONDocument::SetInArray(size_t i, const JSONDocument& value) {
  assert(IsArray() && i < data_.a.size());
  delete data_.a[i];
  data_.a[i] = new JSONDocument(value);
  return this;
}

JSONDocument* JSONDocument::PushBack(const JSONDocument& value) {
  assert(IsArray());
  data_.a.push_back(new JSONDocument(value));
  return this;
}

bool JSONDocument::IsNull() const { return type() == kNull; }
bool JSONDocument::IsArray() const { return type() == kArray; }
bool JSONDocument::IsBool() const { return type() == kBool; }
bool JSONDocument::IsDouble() const { return type() == kDouble; }
bool JSONDocument::IsInt64() const { return type() == kInt64; }
bool JSONDocument::IsObject() const { return type() == kObject; }
bool JSONDocument::IsString() const { return type() == kString; }

bool JSONDocument::GetBool() const {
  assert(IsBool());
  return data_.b;
}
double JSONDocument::GetDouble() const {
  assert(IsDouble());
  return data_.d;
}
int64_t JSONDocument::GetInt64() const {
  assert(IsInt64());
  return data_.i;
}
const std::string& JSONDocument::GetString() const {
  assert(IsString());
  return data_.s;
}

bool JSONDocument::operator==(const JSONDocument& rhs) const {
  if (type_ != rhs.type_) {
    return false;
  }
  switch (type_) {
    case kNull:
      return true;  // null == null
    case kArray:
      if (data_.a.size() != rhs.data_.a.size()) {
        return false;
      }
      for (size_t i = 0; i < data_.a.size(); ++i) {
        if (!(*data_.a[i] == *rhs.data_.a[i])) {
          return false;
        }
      }
      return true;
    case kBool:
      return data_.b == rhs.data_.b;
    case kDouble:
      return data_.d == rhs.data_.d;
    case kInt64:
      return data_.i == rhs.data_.i;
    case kObject:
      if (data_.o.size() != rhs.data_.o.size()) {
        return false;
      }
      for (const auto& iter : data_.o) {
        auto rhs_iter = rhs.data_.o.find(iter.first);
        if (rhs_iter == rhs.data_.o.end() ||
            !(*(rhs_iter->second) == *iter.second)) {
          return false;
        }
      }
      return true;
    case kString:
      return data_.s == rhs.data_.s;
    default:
      assert(false);
  }
  // it can't come to here, but we don't want the compiler to complain
  return false;
}

std::string JSONDocument::DebugString() const {
  std::string ret;
  switch (type_) {
    case kNull:
      ret = "null";
      break;
    case kArray:
      ret = "[";
      for (size_t i = 0; i < data_.a.size(); ++i) {
        if (i) {
          ret += ", ";
        }
        ret += data_.a[i]->DebugString();
      }
      ret += "]";
      break;
    case kBool:
      ret = data_.b ? "true" : "false";
      break;
    case kDouble: {
      char buf[100];
      snprintf(buf, sizeof(buf), "%lf", data_.d);
      ret = buf;
      break;
    }
    case kInt64: {
      char buf[100];
      snprintf(buf, sizeof(buf), "%" PRIi64, data_.i);
      ret = buf;
      break;
    }
    case kObject: {
      bool first = true;
      ret = "{";
      for (const auto& iter : data_.o) {
        ret += first ? "" : ", ";
        first = false;
        ret += iter.first + ": ";
        ret += iter.second->DebugString();
      }
      ret += "}";
      break;
    }
    case kString:
      ret = "\"" + data_.s + "\"";
      break;
    default:
      assert(false);
  }
  return ret;
}

JSONDocument::ItemsIteratorGenerator JSONDocument::Items() const {
  assert(type_ == kObject);
  return data_.o;
}

// parsing with rapidjson
// TODO(icanadi) (perf) allocate objects with arena
JSONDocument* JSONDocument::ParseJSON(const char* json) {
  class JSONDocumentBuilder {
   public:
    JSONDocumentBuilder() {}

    void Null() { stack_.push_back(new JSONDocument()); }
    void Bool(bool b) { stack_.push_back(new JSONDocument(b)); }
    void Int(int i) { Int64(static_cast<int64_t>(i)); }
    void Uint(unsigned i) { Int64(static_cast<int64_t>(i)); }
    void Int64(int64_t i) { stack_.push_back(new JSONDocument(i)); }
    void Uint64(uint64_t i) { Int64(static_cast<int64_t>(i)); }
    void Double(double d) { stack_.push_back(new JSONDocument(d)); }
    void String(const char* str, size_t length, bool copy) {
      assert(copy);
      stack_.push_back(new JSONDocument(std::string(str, length)));
    }
    void StartObject() { stack_.push_back(new JSONDocument(kObject)); }
    void EndObject(size_t member_count) {
      assert(stack_.size() > 2 * member_count);
      auto object_base_iter = stack_.end() - member_count * 2 - 1;
      assert((*object_base_iter)->type_ == kObject);
      auto& object_map = (*object_base_iter)->data_.o;
      // iter will always be stack_.end() at some point (i.e. will not advance
      // past it) because of the way we calculate object_base_iter
      for (auto iter = object_base_iter + 1; iter != stack_.end(); iter += 2) {
        assert((*iter)->type_ == kString);
        object_map.insert({(*iter)->data_.s, *(iter + 1)});
        delete *iter;
      }
      stack_.erase(object_base_iter + 1, stack_.end());
    }
    void StartArray() { stack_.push_back(new JSONDocument(kArray)); }
    void EndArray(size_t element_count) {
      assert(stack_.size() > element_count);
      auto array_base_iter = stack_.end() - element_count - 1;
      assert((*array_base_iter)->type_ == kArray);
      (*array_base_iter)->data_.a.assign(array_base_iter + 1, stack_.end());
      stack_.erase(array_base_iter + 1, stack_.end());
    }

    JSONDocument* GetDocument() {
      if (stack_.size() != 1) {
        return nullptr;
      }
      return stack_.back();
    }

    void DeleteAllDocumentsOnStack() {
      for (auto document : stack_) {
        delete document;
      }
      stack_.clear();
    }

   private:
    std::vector<JSONDocument*> stack_;
  };

  rapidjson::StringStream stream(json);
  rapidjson::Reader reader;
  JSONDocumentBuilder handler;
  bool ok = reader.Parse<0>(stream, handler);
  if (!ok) {
    handler.DeleteAllDocumentsOnStack();
    return nullptr;
  }
  auto document = handler.GetDocument();
  assert(document != nullptr);
  return document;
}

// serialization and deserialization
// format:
// ------
// document  ::= header(char) object
// object    ::= varint32(n) key_value*(n times)
// key_value ::= string element
// element   ::= 0x01                     (kNull)
//            |  0x02 array               (kArray)
//            |  0x03 byte                (kBool)
//            |  0x04 double              (kDouble)
//            |  0x05 int64               (kInt64)
//            |  0x06 object              (kObject)
//            |  0x07 string              (kString)
// array ::= varint32(n) element*(n times)
// TODO(icanadi) evaluate string vs cstring format
// string ::= varint32(n) byte*(n times)
// double ::= 64-bit IEEE 754 floating point (8 bytes)
// int64  ::= 8 bytes, 64-bit signed integer, little endian

namespace {
inline char GetPrefixFromType(JSONDocument::Type type) {
  static char types[] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7};
  return types[type];
}

inline bool GetNextType(Slice* input, JSONDocument::Type* type) {
  if (input->size() == 0) {
    return false;
  }
  static JSONDocument::Type prefixes[] = {
      JSONDocument::kNull,   JSONDocument::kArray, JSONDocument::kBool,
      JSONDocument::kDouble, JSONDocument::kInt64, JSONDocument::kObject,
      JSONDocument::kString};
  size_t prefix = static_cast<size_t>((*input)[0]);
  if (prefix == 0 || prefix >= 0x8) {
    return false;
  }
  input->remove_prefix(1);
  *type = prefixes[static_cast<size_t>(prefix - 1)];
  return true;
}

// TODO(icanadi): Make sure this works on all platforms we support. Some
// platforms may store double in different binary format (our specification says
// we need IEEE 754)
inline void PutDouble(std::string* dst, double d) {
  dst->append(reinterpret_cast<char*>(&d), sizeof(d));
}

bool DecodeDouble(Slice* input, double* d) {
  if (input->size() < sizeof(double)) {
    return false;
  }
  memcpy(d, input->data(), sizeof(double));
  input->remove_prefix(sizeof(double));

  return true;
}
}  // namespace

void JSONDocument::Serialize(std::string* dst) const {
  // first byte is reserved for header
  // currently, header is only version number. that will help us provide
  // backwards compatility. we might also store more information here if
  // necessary
  dst->push_back(kSerializationFormatVersion);
  SerializeInternal(dst, false);
}

void JSONDocument::SerializeInternal(std::string* dst, bool type_prefix) const {
  if (type_prefix) {
    dst->push_back(GetPrefixFromType(type_));
  }
  switch (type_) {
    case kNull:
      // just the prefix is all we need
      break;
    case kArray:
      PutVarint32(dst, static_cast<uint32_t>(data_.a.size()));
      for (const auto& element : data_.a) {
        element->SerializeInternal(dst, true);
      }
      break;
    case kBool:
      dst->push_back(static_cast<char>(data_.b));
      break;
    case kDouble:
      PutDouble(dst, data_.d);
      break;
    case kInt64:
      PutFixed64(dst, static_cast<uint64_t>(data_.i));
      break;
    case kObject: {
      PutVarint32(dst, static_cast<uint32_t>(data_.o.size()));
      for (const auto& iter : data_.o) {
        PutLengthPrefixedSlice(dst, Slice(iter.first));
        iter.second->SerializeInternal(dst, true);
      }
      break;
    }
    case kString:
      PutLengthPrefixedSlice(dst, Slice(data_.s));
      break;
    default:
      assert(false);
  }
}

const char JSONDocument::kSerializationFormatVersion = 1;

JSONDocument* JSONDocument::Deserialize(const Slice& src) {
  Slice input(src);
  if (src.size() == 0) {
    return nullptr;
  }
  char header = input[0];
  if (header != kSerializationFormatVersion) {
    // don't understand this header (possibly newer version format and we don't
    // support downgrade)
    return nullptr;
  }
  input.remove_prefix(1);
  auto root = new JSONDocument(kObject);
  bool ok = root->DeserializeInternal(&input);
  if (!ok || input.size() > 0) {
    // parsing failure :(
    delete root;
    return nullptr;
  }
  return root;
}

bool JSONDocument::DeserializeInternal(Slice* input) {
  switch (type_) {
    case kNull:
      break;
    case kArray: {
      uint32_t size;
      if (!GetVarint32(input, &size)) {
        return false;
      }
      data_.a.resize(size);
      for (size_t i = 0; i < size; ++i) {
        Type type;
        if (!GetNextType(input, &type)) {
          return false;
        }
        data_.a[i] = new JSONDocument(type);
        if (!data_.a[i]->DeserializeInternal(input)) {
          return false;
        }
      }
      break;
    }
    case kBool:
      if (input->size() < 1) {
        return false;
      }
      data_.b = static_cast<bool>((*input)[0]);
      input->remove_prefix(1);
      break;
    case kDouble:
      if (!DecodeDouble(input, &data_.d)) {
        return false;
      }
      break;
    case kInt64: {
      uint64_t tmp;
      if (!GetFixed64(input, &tmp)) {
        return false;
      }
      data_.i = static_cast<int64_t>(tmp);
      break;
    }
    case kObject: {
      uint32_t num_elements;
      bool ok = GetVarint32(input, &num_elements);
      for (uint32_t i = 0; ok && i < num_elements; ++i) {
        Slice key;
        ok = GetLengthPrefixedSlice(input, &key);
        Type type;
        ok = ok && GetNextType(input, &type);
        if (ok) {
          std::unique_ptr<JSONDocument> value(new JSONDocument(type));
          ok = value->DeserializeInternal(input);
          if (ok) {
            data_.o.insert({key.ToString(), value.get()});
            value.release();
          }
        }
      }
      if (!ok) {
        return false;
      }
      break;
    }
    case kString: {
      Slice key;
      if (!GetLengthPrefixedSlice(input, &key)) {
        return false;
      }
      data_.s = key.ToString();
      break;
    }
    default:
      // this is an assert and not a return because DeserializeInternal() will
      // always be called with a valid type_. In case there has been data
      // corruption, GetNextType() is the function that will detect that and
      // return corruption
      assert(false);
  }
  return true;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
