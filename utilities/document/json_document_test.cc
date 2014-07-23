//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <set>

#include "rocksdb/utilities/json_document.h"

#include "util/testutil.h"
#include "util/testharness.h"

namespace rocksdb {
namespace {
void AssertField(const JSONDocument& json, const std::string& field) {
  ASSERT_TRUE(json.Contains(field));
  ASSERT_TRUE(json[field].IsNull());
}

void AssertField(const JSONDocument& json, const std::string& field,
                 const std::string& expected) {
  ASSERT_TRUE(json.Contains(field));
  ASSERT_TRUE(json[field].IsString());
  ASSERT_EQ(expected, json[field].GetString());
}

void AssertField(const JSONDocument& json, const std::string& field,
                 int64_t expected) {
  ASSERT_TRUE(json.Contains(field));
  ASSERT_TRUE(json[field].IsInt64());
  ASSERT_EQ(expected, json[field].GetInt64());
}

void AssertField(const JSONDocument& json, const std::string& field,
                 bool expected) {
  ASSERT_TRUE(json.Contains(field));
  ASSERT_TRUE(json[field].IsBool());
  ASSERT_EQ(expected, json[field].GetBool());
}

void AssertField(const JSONDocument& json, const std::string& field,
                 double expected) {
  ASSERT_TRUE(json.Contains(field));
  ASSERT_TRUE(json[field].IsDouble());
  ASSERT_EQ(expected, json[field].GetDouble());
}
}  // namespace

class JSONDocumentTest {
 public:
  void AssertSampleJSON(const JSONDocument& json) {
    AssertField(json, "title", std::string("json"));
    AssertField(json, "type", std::string("object"));
    // properties
    ASSERT_TRUE(json.Contains("properties"));
    ASSERT_TRUE(json["properties"].Contains("flags"));
    ASSERT_TRUE(json["properties"]["flags"].IsArray());
    ASSERT_EQ(3u, json["properties"]["flags"].Count());
    ASSERT_TRUE(json["properties"]["flags"][0].IsInt64());
    ASSERT_EQ(10, json["properties"]["flags"][0].GetInt64());
    ASSERT_TRUE(json["properties"]["flags"][1].IsString());
    ASSERT_EQ("parse", json["properties"]["flags"][1].GetString());
    ASSERT_TRUE(json["properties"]["flags"][2].IsObject());
    AssertField(json["properties"]["flags"][2], "tag", std::string("no"));
    AssertField(json["properties"]["flags"][2], std::string("status"));
    AssertField(json["properties"], "age", 110.5e-4);
    AssertField(json["properties"], "depth", static_cast<int64_t>(-10));
    // test iteration
    std::set<std::string> expected({"flags", "age", "depth"});
    for (auto item : json["properties"].Items()) {
      auto iter = expected.find(item.first);
      ASSERT_TRUE(iter != expected.end());
      expected.erase(iter);
    }
    ASSERT_EQ(0U, expected.size());
    ASSERT_TRUE(json.Contains("latlong"));
    ASSERT_TRUE(json["latlong"].IsArray());
    ASSERT_EQ(2u, json["latlong"].Count());
    ASSERT_TRUE(json["latlong"][0].IsDouble());
    ASSERT_EQ(53.25, json["latlong"][0].GetDouble());
    ASSERT_TRUE(json["latlong"][1].IsDouble());
    ASSERT_EQ(43.75, json["latlong"][1].GetDouble());
    AssertField(json, "enabled", true);
  }

  const std::string kSampleJSON =
      "{ \"title\" : \"json\", \"type\" : \"object\", \"properties\" : { "
      "\"flags\": [10, \"parse\", {\"tag\": \"no\", \"status\": null}], "
      "\"age\": 110.5e-4, \"depth\": -10 }, \"latlong\": [53.25, 43.75], "
      "\"enabled\": true }";

  const std::string kSampleJSONDifferent =
      "{ \"title\" : \"json\", \"type\" : \"object\", \"properties\" : { "
      "\"flags\": [10, \"parse\", {\"tag\": \"no\", \"status\": 2}], "
      "\"age\": 110.5e-4, \"depth\": -10 }, \"latlong\": [53.25, 43.75], "
      "\"enabled\": true }";
};

TEST(JSONDocumentTest, Parsing) {
  JSONDocument x(static_cast<int64_t>(5));
  ASSERT_TRUE(x.IsInt64());

  // make sure it's correctly parsed
  auto parsed_json = JSONDocument::ParseJSON(kSampleJSON.c_str());
  ASSERT_TRUE(parsed_json != nullptr);
  AssertSampleJSON(*parsed_json);

  // test deep copying
  JSONDocument copied_json_document(*parsed_json);
  AssertSampleJSON(copied_json_document);
  ASSERT_TRUE(copied_json_document == *parsed_json);
  delete parsed_json;

  auto parsed_different_sample =
      JSONDocument::ParseJSON(kSampleJSONDifferent.c_str());
  ASSERT_TRUE(parsed_different_sample != nullptr);
  ASSERT_TRUE(!(*parsed_different_sample == copied_json_document));
  delete parsed_different_sample;

  // parse error
  const std::string kFaultyJSON =
      kSampleJSON.substr(0, kSampleJSON.size() - 10);
  ASSERT_TRUE(JSONDocument::ParseJSON(kFaultyJSON.c_str()) == nullptr);
}

TEST(JSONDocumentTest, Serialization) {
  auto parsed_json = JSONDocument::ParseJSON(kSampleJSON.c_str());
  ASSERT_TRUE(parsed_json != nullptr);
  std::string serialized;
  parsed_json->Serialize(&serialized);
  delete parsed_json;

  auto deserialized_json = JSONDocument::Deserialize(Slice(serialized));
  ASSERT_TRUE(deserialized_json != nullptr);
  AssertSampleJSON(*deserialized_json);
  delete deserialized_json;

  // deserialization failure
  ASSERT_TRUE(JSONDocument::Deserialize(
                  Slice(serialized.data(), serialized.size() - 10)) == nullptr);
}

TEST(JSONDocumentTest, Mutation) {
  auto sample_json = JSONDocument::ParseJSON(kSampleJSON.c_str());
  ASSERT_TRUE(sample_json != nullptr);
  auto different_json = JSONDocument::ParseJSON(kSampleJSONDifferent.c_str());
  ASSERT_TRUE(different_json != nullptr);

  (*different_json)["properties"]["flags"][2].Set("status", JSONDocument());

  ASSERT_TRUE(*different_json == *sample_json);

  delete different_json;
  delete sample_json;

  auto json1 = JSONDocument::ParseJSON("{\"a\": [1, 2, 3]}");
  auto json2 = JSONDocument::ParseJSON("{\"a\": [2, 2, 3, 4]}");
  ASSERT_TRUE(json1 != nullptr && json2 != nullptr);

  (*json1)["a"].SetInArray(0, static_cast<int64_t>(2))->PushBack(
      static_cast<int64_t>(4));
  ASSERT_TRUE(*json1 == *json2);

  delete json1;
  delete json2;
}

}  //  namespace rocksdb

int main(int argc, char** argv) { return rocksdb::test::RunAllTests(); }
