#pragma once

#include "rocksdb/cache.h"
#include "util/compression.h"
#include "util/testharness.h"

namespace rocksdb {
namespace titandb {

template <typename T>
void CheckCodec(const T& input) {
  std::string buffer;
  input.EncodeTo(&buffer);
  T output;
  ASSERT_OK(DecodeInto(buffer, &output));
  ASSERT_EQ(output, input);
}

}  // namespace titandb
}  // namespace rocksdb
