// Copyright (c) 2017 Rockset

#pragma once

#include <string>

//
// These are inlined methods to deal with pathnames and filenames.

namespace {

// trim from start
static std::string& ltrim(std::string& s) {
  s.erase(s.begin(),
          std::find_if(s.begin(), s.end(),
                       std::not1(std::ptr_fun<int, int>(std::isspace))));
  return s;
}

// trim from end
static std::string& rtrim(std::string& s) {
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       std::not1(std::ptr_fun<int, int>(std::isspace)))
              .base(),
          s.end());
  return s;
}

// trim from both ends
inline std::string& trim(std::string& s) { return ltrim(rtrim(s)); }

// Extract basename from a full pathname
struct MatchPathSeparator {
  bool operator()(char ch) const { return ch == '/'; }
};
inline std::string basename(std::string const& pathname) {
  return std::string(
      std::find_if(pathname.rbegin(), pathname.rend(), MatchPathSeparator())
          .base(),
      pathname.end());
}
inline std::string dirname(std::string const& pathname) {
  return std::string(
      pathname.begin(),
      std::find_if(pathname.rbegin(), pathname.rend(), MatchPathSeparator())
          .base());
}

// If the last char of the string is the specified character, then return a
// string
// that has the last character removed.
inline std::string& rtrim_if(std::string& s, char c) {
  if (s.length() > 0 && s[s.length() - 1] == c) {
    s.erase(s.begin() + s.size() - 1);
  }
  return s;
}
}
