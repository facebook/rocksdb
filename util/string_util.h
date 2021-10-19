//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

extern std::vector<std::string> StringSplit(const std::string& arg, char delim);

template <typename T>
inline std::string ToString(T value) {
#if !(defined OS_ANDROID) && !(defined CYGWIN) && !(defined OS_FREEBSD)
  return std::to_string(value);
#else
  // Andorid or cygwin doesn't support all of C++11, std::to_string() being
  // one of the not supported features.
  std::ostringstream os;
  os << value;
  return os.str();
#endif
}

// Append a human-readable printout of "num" to *str
extern void AppendNumberTo(std::string* str, uint64_t num);

// Append a human-readable printout of "value" to *str.
// Escapes any non-printable characters found in "value".
extern void AppendEscapedStringTo(std::string* str, const Slice& value);

// Put n digits from v in base kBase to (*buf)[0] to (*buf)[n-1] and
// advance *buf to the position after what was written.
template <size_t kBase>
inline void PutBaseChars(char** buf, size_t n, uint64_t v, bool uppercase) {
  const char* digitChars = uppercase ? "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                     : "0123456789abcdefghijklmnopqrstuvwxyz";
  for (size_t i = n; i > 0; --i) {
    (*buf)[i - 1] = digitChars[static_cast<size_t>(v % kBase)];
    v /= kBase;
  }
  *buf += n;
}

// Parse n digits from *buf in base kBase to *v and advance *buf to the
// position after what was read. On success, true is returned. On failure,
// false is returned, *buf is placed at the first bad character, and *v
// contains the partial parsed data. Overflow is not checked but the
// result is accurate mod 2^64. Requires the starting value of *v to be
// zero or previously accumulated parsed digits, i.e.
//   ParseBaseChars(&b, n, &v);
// is equivalent to n calls to
//   ParseBaseChars(&b, 1, &v);
template <int kBase>
inline bool ParseBaseChars(const char** buf, size_t n, uint64_t* v) {
  while (n) {
    char c = **buf;
    *v *= static_cast<uint64_t>(kBase);
    if (c >= '0' && (kBase >= 10 ? c <= '9' : c < '0' + kBase)) {
      *v += static_cast<uint64_t>(c - '0');
    } else if (kBase > 10 && c >= 'A' && c < 'A' + kBase - 10) {
      *v += static_cast<uint64_t>(c - 'A' + 10);
    } else if (kBase > 10 && c >= 'a' && c < 'a' + kBase - 10) {
      *v += static_cast<uint64_t>(c - 'a' + 10);
    } else {
      return false;
    }
    --n;
    ++*buf;
  }
  return true;
}

// Return a human-readable version of num.
// for num >= 10.000, prints "xxK"
// for num >= 10.000.000, prints "xxM"
// for num >= 10.000.000.000, prints "xxG"
extern std::string NumberToHumanString(int64_t num);

// Return a human-readable version of bytes
// ex: 1048576 -> 1.00 GB
extern std::string BytesToHumanString(uint64_t bytes);

// Return a human-readable version of unix time
// ex: 1562116015 -> "Tue Jul  2 18:06:55 2019"
extern std::string TimeToHumanString(int unixtime);

// Append a human-readable time in micros.
int AppendHumanMicros(uint64_t micros, char* output, int len,
                      bool fixed_format);

// Append a human-readable size in bytes
int AppendHumanBytes(uint64_t bytes, char* output, int len);

// Return a human-readable version of "value".
// Escapes any non-printable characters found in "value".
extern std::string EscapeString(const Slice& value);

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
extern bool ConsumeDecimalNumber(Slice* in, uint64_t* val);

// Returns true if the input char "c" is considered as a special character
// that will be escaped when EscapeOptionString() is called.
//
// @param c the input char
// @return true if the input char "c" is considered as a special character.
// @see EscapeOptionString
bool isSpecialChar(const char c);

// If the input char is an escaped char, it will return the its
// associated raw-char.  Otherwise, the function will simply return
// the original input char.
char UnescapeChar(const char c);

// If the input char is a control char, it will return the its
// associated escaped char.  Otherwise, the function will simply return
// the original input char.
char EscapeChar(const char c);

// Converts a raw string to an escaped string.  Escaped-characters are
// defined via the isSpecialChar() function.  When a char in the input
// string "raw_string" is classified as a special characters, then it
// will be prefixed by '\' in the output.
//
// It's inverse function is UnescapeOptionString().
// @param raw_string the input string
// @return the '\' escaped string of the input "raw_string"
// @see isSpecialChar, UnescapeOptionString
std::string EscapeOptionString(const std::string& raw_string);

// The inverse function of EscapeOptionString.  It converts
// an '\' escaped string back to a raw string.
//
// @param escaped_string the input '\' escaped string
// @return the raw string of the input "escaped_string"
std::string UnescapeOptionString(const std::string& escaped_string);

std::string trim(const std::string& str);

// Returns true if "string" ends with "pattern"
bool EndsWith(const std::string& string, const std::string& pattern);

// Returns true if "string" starts with "pattern"
bool StartsWith(const std::string& string, const std::string& pattern);

#ifndef ROCKSDB_LITE
bool ParseBoolean(const std::string& type, const std::string& value);

uint8_t ParseUint8(const std::string& value);

uint32_t ParseUint32(const std::string& value);

int32_t ParseInt32(const std::string& value);
#endif

uint64_t ParseUint64(const std::string& value);

int ParseInt(const std::string& value);

int64_t ParseInt64(const std::string& value);

double ParseDouble(const std::string& value);

size_t ParseSizeT(const std::string& value);

std::vector<int> ParseVectorInt(const std::string& value);

bool SerializeIntVector(const std::vector<int>& vec, std::string* value);

extern const std::string kNullptrString;

// errnoStr() function returns a string that describes the error code passed in
// the argument err
extern std::string errnoStr(int err);

}  // namespace ROCKSDB_NAMESPACE
