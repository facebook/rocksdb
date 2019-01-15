#pragma once

#ifndef ROCKSDB_LITE
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/extension_loader.h"

using std::shared_ptr;
using std::unique_ptr;
using std::string;

namespace rocksdb {
template<typename T> void AssertNewUniqueExtension(const DBOptions & dbOpts,
						  const std::string & name,
						  bool isValid,
						   T **extension,
						  std::unique_ptr<T> *guard,
						  bool isGuarded) {
  Status status = NewUniqueExtension(name, dbOpts, nullptr, extension, guard);
  if (isValid) {
    ASSERT_OK(status);
    ASSERT_NE(*extension, nullptr);
    ASSERT_EQ(name, (*extension)->Name());
    if (isGuarded) {
      ASSERT_NE(guard->get(), nullptr);
    } else {
      ASSERT_EQ(guard->get(), nullptr);
    }
  } else {
    ASSERT_TRUE(status.IsInvalidArgument());
    ASSERT_EQ(*extension, nullptr);
    ASSERT_EQ(guard->get(), nullptr);
  }
}

template<typename T> void AssertNewSharedExtension(
				const DBOptions & dbOpts,
				const std::string & name,
				bool isValid, std::shared_ptr<T> *result) {
  Status status = NewSharedExtension(name, dbOpts, nullptr, result);
  if (isValid) {
    ASSERT_OK(status);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(name, (*result)->Name());
  } else {
    ASSERT_TRUE(status.IsInvalidArgument());
    ASSERT_EQ(result->get(), nullptr);
  }
}
} // End namespace

#endif // ROCKSDB_LITE
