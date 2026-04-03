#pragma once

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
namespace debug_read_trace {

inline bool TryParseHexNibble(char c, uint8_t* value) {
  if (c >= '0' && c <= '9') {
    *value = static_cast<uint8_t>(c - '0');
    return true;
  }
  if (c >= 'a' && c <= 'f') {
    *value = static_cast<uint8_t>(10 + c - 'a');
    return true;
  }
  if (c >= 'A' && c <= 'F') {
    *value = static_cast<uint8_t>(10 + c - 'A');
    return true;
  }
  return false;
}

inline std::optional<std::string> ParseHexString(const char* hex) {
  if (hex == nullptr || *hex == '\0') {
    return std::nullopt;
  }

  std::string input(hex);
  if (input.size() >= 2 && input[0] == '0' &&
      (input[1] == 'x' || input[1] == 'X')) {
    input.erase(0, 2);
  }

  if (input.empty() || (input.size() % 2) != 0) {
    return std::nullopt;
  }

  std::string output;
  output.resize(input.size() / 2);
  for (size_t i = 0; i < output.size(); ++i) {
    uint8_t hi = 0;
    uint8_t lo = 0;
    if (!TryParseHexNibble(input[2 * i], &hi) ||
        !TryParseHexNibble(input[2 * i + 1], &lo)) {
      return std::nullopt;
    }
    output[i] = static_cast<char>((hi << 4) | lo);
  }

  return output;
}

inline const std::optional<std::string>& TargetUserKey() {
  static const std::optional<std::string> target =
      ParseHexString(std::getenv("ROCKSDB_DEBUG_USER_KEY_HEX"));
  return target;
}

inline bool ShouldTraceUserKey(const Slice& user_key) {
  const auto& target = TargetUserKey();
  return target.has_value() && user_key.size() >= target->size() &&
         std::memcmp(user_key.data(), target->data(), target->size()) == 0;
}

inline std::string ToHex(const Slice& value) {
  return value.ToString(/* hex */ true);
}

inline const char* BoolToString(bool value) { return value ? "true" : "false"; }

inline void Trace(const char* component, const std::string& message) {
  std::fprintf(stderr, "[rocksdb_debug_user_key][%s] %s\n", component,
               message.c_str());
}

}  // namespace debug_read_trace
}  // namespace ROCKSDB_NAMESPACE
