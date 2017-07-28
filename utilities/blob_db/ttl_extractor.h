//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <memory>
#include <string>

#include "rocksdb/slice.h"

namespace rocksdb {
namespace blob_db {

// TTLExtractor allow applications to extract TTL from key-value pairs.
// This useful for applications using Put or WriteBatch to write keys and
// don't intend to migrate to PutWithTTL or PutUntil.
//
// Applications can implement either ExtractTTL or ExtractExpiration. If both
// are implemented, ExtractExpiration will take precedence.
class TTLExtractor {
 public:
  // Extract TTL from key-value pair.
  // Return true if the key has TTL, false otherwise. If key has TTL,
  // TTL is pass back through ttl. The method can optionally modify the value,
  // pass the result back through new_value, and also set value_changed to true.
  virtual bool ExtractTTL(const Slice& key, const Slice& value, uint64_t* ttl,
                          std::string* new_value, bool* value_changed);

  // Extract expiration time from key-value pair.
  // Return true if the key has expiration time, false otherwise. If key has
  // expiration time, it is pass back through expiration. The method can
  // optionally modify the value, pass the result back through new_value,
  // and also set value_changed to true.
  virtual bool ExtractExpiration(const Slice& key, const Slice& value,
                                 uint64_t now, uint64_t* expiration,
                                 std::string* new_value, bool* value_changed);

  virtual ~TTLExtractor() = default;
};

}  // namespace blob_db
}  // namespace rocksdb
