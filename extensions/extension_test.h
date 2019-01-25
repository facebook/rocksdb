#pragma once

#ifndef ROCKSDB_LITE
#include "extensions/extension_helper.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/extension_loader.h"

using std::shared_ptr;
using std::unique_ptr;
using std::string;

namespace rocksdb {
template<typename T> void AssertNewUniqueExtension(const DBOptions & dbOpts,
						  const std::string & fullName,
						  bool isValid,
						   T **extension,
						   std::unique_ptr<T> *guard,
						   bool isGuarded,
						   const std::string & shortName) {
  Status status = NewUniqueExtension(fullName, dbOpts, nullptr, extension, guard);
  if (isValid) {
    ASSERT_OK(status);
    ASSERT_NE(*extension, nullptr);
    ASSERT_EQ(shortName, (*extension)->Name());
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

template<typename T> void AssertNewUniqueExtension(const DBOptions & dbOpts,
						  const std::string & fullName,
						  bool isValid,
						   T **extension,
						  std::unique_ptr<T> *guard,
						   bool isGuarded) {
  AssertNewUniqueExtension(dbOpts, fullName, isValid, extension, guard, isGuarded, fullName);
}
  
template<typename T> void AssertNewSharedExtension(
				const DBOptions & dbOpts,
				const std::string & fullName,
				bool isValid,
				std::shared_ptr<T> *result,
				const std::string & shortName) {
  result->reset();
  Status status = NewSharedExtension(fullName, dbOpts, nullptr, result);
  if (isValid) {
    ASSERT_OK(status);
    ASSERT_NE(result->get(), nullptr);
    ASSERT_EQ(shortName, (*result)->Name());
  } else {
    ASSERT_TRUE(status.IsInvalidArgument());
    ASSERT_EQ(result->get(), nullptr);
  }
}
template<typename T> void AssertNewSharedExtension(
				const DBOptions & dbOpts,
				const std::string & fullName,
				bool isValid,
				std::shared_ptr<T> *result) {
  AssertNewSharedExtension(dbOpts, fullName, isValid, result, fullName);
}
} // End namespace

#endif // ROCKSDB_LITE
