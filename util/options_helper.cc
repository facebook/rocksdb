//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include "util/options_helper.h"

#include <cassert>
#include <cctype>
#include <cstdlib>
#include <unordered_set>
#include <vector>
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/block_based_table_factory.h"
#include "table/plain_table_factory.h"
#include "util/logging.h"
#include "util/string_util.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
bool isSpecialChar(const char c) {
  if (c == '\\' || c == '#' || c == ':' || c == '\r' || c == '\n') {
    return true;
  }
  return false;
}

char UnescapeChar(const char c) {
  static const std::unordered_map<char, char> convert_map = {{'r', '\r'},
                                                             {'n', '\n'}};

  auto iter = convert_map.find(c);
  if (iter == convert_map.end()) {
    return c;
  }
  return iter->second;
}

char EscapeChar(const char c) {
  static const std::unordered_map<char, char> convert_map = {{'\n', 'n'},
                                                             {'\r', 'r'}};

  auto iter = convert_map.find(c);
  if (iter == convert_map.end()) {
    return c;
  }
  return iter->second;
}

std::string EscapeOptionString(const std::string& raw_string) {
  std::string output;
  for (auto c : raw_string) {
    if (isSpecialChar(c)) {
      output += '\\';
      output += EscapeChar(c);
    } else {
      output += c;
    }
  }

  return output;
}

std::string UnescapeOptionString(const std::string& escaped_string) {
  bool escaped = false;
  std::string output;

  for (auto c : escaped_string) {
    if (escaped) {
      output += UnescapeChar(c);
      escaped = false;
    } else {
      if (c == '\\') {
        escaped = true;
        continue;
      }
      output += c;
    }
  }
  return output;
}

uint64_t ParseUint64(const std::string& value) {
  size_t endchar;
#ifndef CYGWIN
  uint64_t num = std::stoull(value.c_str(), &endchar);
#else
  char* endptr;
  uint64_t num = std::strtoul(value.c_str(), &endptr, 0);
  endchar = endptr - value.c_str();
#endif

  if (endchar < value.length()) {
    char c = value[endchar];
    if (c == 'k' || c == 'K')
      num <<= 10LL;
    else if (c == 'm' || c == 'M')
      num <<= 20LL;
    else if (c == 'g' || c == 'G')
      num <<= 30LL;
    else if (c == 't' || c == 'T')
      num <<= 40LL;
  }

  return num;
}

namespace {
std::string trim(const std::string& str) {
  if (str.empty()) return std::string();
  size_t start = 0;
  size_t end = str.size() - 1;
  while (isspace(str[start]) != 0 && start <= end) {
    ++start;
  }
  while (isspace(str[end]) != 0 && start <= end) {
    --end;
  }
  if (start <= end) {
    return str.substr(start, end - start + 1);
  }
  return std::string();
}

template <typename T>
bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
               const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

template <typename T>
bool SerializeEnum(const std::unordered_map<std::string, T>& type_map,
                   const T& type, std::string* value) {
  for (const auto& pair : type_map) {
    if (pair.second == type) {
      *value = pair.first;
      return true;
    }
  }
  return false;
}

bool SerializeVectorCompressionType(const std::vector<CompressionType>& types,
                                    std::string* value) {
  std::stringstream ss;
  bool result;
  for (size_t i = 0; i < types.size(); ++i) {
    if (i > 0) {
      ss << ':';
    }
    std::string string_type;
    result = SerializeEnum<CompressionType>(compression_type_string_map,
                                            types[i], &string_type);
    if (result == false) {
      return result;
    }
    ss << string_type;
  }
  *value = ss.str();
  return true;
}

bool ParseBoolean(const std::string& type, const std::string& value) {
  if (value == "true" || value == "1") {
    return true;
  } else if (value == "false" || value == "0") {
    return false;
  }
  throw std::invalid_argument(type);
}

size_t ParseSizeT(const std::string& value) {
  return static_cast<size_t>(ParseUint64(value));
}

uint32_t ParseUint32(const std::string& value) {
  uint64_t num = ParseUint64(value);
  if ((num >> 32LL) == 0) {
    return static_cast<uint32_t>(num);
  } else {
    throw std::out_of_range(value);
  }
}

int ParseInt(const std::string& value) {
  size_t endchar;
#ifndef CYGWIN
  int num = std::stoi(value.c_str(), &endchar);
#else
  char* endptr;
  int num = std::strtoul(value.c_str(), &endptr, 0);
  endchar = endptr - value.c_str();
#endif

  if (endchar < value.length()) {
    char c = value[endchar];
    if (c == 'k' || c == 'K')
      num <<= 10;
    else if (c == 'm' || c == 'M')
      num <<= 20;
    else if (c == 'g' || c == 'G')
      num <<= 30;
  }

  return num;
}

double ParseDouble(const std::string& value) {
#ifndef CYGWIN
  return std::stod(value);
#else
  return std::strtod(value.c_str(), 0);
#endif
}

bool ParseVectorCompressionType(
    const std::string& value,
    std::vector<CompressionType>* compression_per_level) {
  compression_per_level->clear();
  size_t start = 0;
  while (start < value.size()) {
    size_t end = value.find(':', start);
    bool is_ok;
    CompressionType type;
    if (end == std::string::npos) {
      is_ok = ParseEnum<CompressionType>(compression_type_string_map,
                                         value.substr(start), &type);
      if (!is_ok) {
        return false;
      }
      compression_per_level->emplace_back(type);
      break;
    } else {
      is_ok = ParseEnum<CompressionType>(
          compression_type_string_map, value.substr(start, end - start), &type);
      if (!is_ok) {
        return false;
      }
      compression_per_level->emplace_back(type);
      start = end + 1;
    }
  }
  return true;
}

bool ParseSliceTransformHelper(
    const std::string& kFixedPrefixName, const std::string& kCappedPrefixName,
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  static const std::string kNullptrString = "nullptr";
  auto& pe_value = value;
  if (pe_value.size() > kFixedPrefixName.size() &&
      pe_value.compare(0, kFixedPrefixName.size(), kFixedPrefixName) == 0) {
    int prefix_length = ParseInt(trim(value.substr(kFixedPrefixName.size())));
    slice_transform->reset(NewFixedPrefixTransform(prefix_length));
  } else if (pe_value.size() > kCappedPrefixName.size() &&
             pe_value.compare(0, kCappedPrefixName.size(), kCappedPrefixName) ==
                 0) {
    int prefix_length =
        ParseInt(trim(pe_value.substr(kCappedPrefixName.size())));
    slice_transform->reset(NewCappedPrefixTransform(prefix_length));
  } else if (value == kNullptrString) {
    slice_transform->reset();
  } else {
    return false;
  }

  return true;
}

bool ParseSliceTransform(
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  // While we normally don't convert the string representation of a
  // pointer-typed option into its instance, here we do so for backward
  // compatibility as we allow this action in SetOption().

  // TODO(yhchiang): A possible better place for these serialization /
  // deserialization is inside the class definition of pointer-typed
  // option itself, but this requires a bigger change of public API.
  bool result =
      ParseSliceTransformHelper("fixed:", "capped:", value, slice_transform);
  if (result) {
    return result;
  }
  result = ParseSliceTransformHelper(
      "rocksdb.FixedPrefix.", "rocksdb.CappedPrefix.", value, slice_transform);
  if (result) {
    return result;
  }
  // TODO(yhchiang): we can further support other default
  //                 SliceTransforms here.
  return false;
}

bool ParseOptionHelper(char* opt_address, const OptionType& opt_type,
                       const std::string& value) {
  switch (opt_type) {
    case OptionType::kBoolean:
      *reinterpret_cast<bool*>(opt_address) = ParseBoolean("", value);
      break;
    case OptionType::kInt:
      *reinterpret_cast<int*>(opt_address) = ParseInt(value);
      break;
    case OptionType::kUInt:
      *reinterpret_cast<unsigned int*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt32T:
      *reinterpret_cast<uint32_t*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt64T:
      *reinterpret_cast<uint64_t*>(opt_address) = ParseUint64(value);
      break;
    case OptionType::kSizeT:
      *reinterpret_cast<size_t*>(opt_address) = ParseSizeT(value);
      break;
    case OptionType::kString:
      *reinterpret_cast<std::string*>(opt_address) = value;
      break;
    case OptionType::kDouble:
      *reinterpret_cast<double*>(opt_address) = ParseDouble(value);
      break;
    case OptionType::kCompactionStyle:
      return ParseEnum<CompactionStyle>(
          compaction_style_string_map, value,
          reinterpret_cast<CompactionStyle*>(opt_address));
    case OptionType::kCompressionType:
      return ParseEnum<CompressionType>(
          compression_type_string_map, value,
          reinterpret_cast<CompressionType*>(opt_address));
    case OptionType::kVectorCompressionType:
      return ParseVectorCompressionType(
          value, reinterpret_cast<std::vector<CompressionType>*>(opt_address));
    case OptionType::kSliceTransform:
      return ParseSliceTransform(
          value, reinterpret_cast<std::shared_ptr<const SliceTransform>*>(
                     opt_address));
    case OptionType::kChecksumType:
      return ParseEnum<ChecksumType>(
          checksum_type_string_map, value,
          reinterpret_cast<ChecksumType*>(opt_address));
    case OptionType::kBlockBasedTableIndexType:
      return ParseEnum<BlockBasedTableOptions::IndexType>(
          block_base_table_index_type_string_map, value,
          reinterpret_cast<BlockBasedTableOptions::IndexType*>(opt_address));
    case OptionType::kEncodingType:
      return ParseEnum<EncodingType>(
          encoding_type_string_map, value,
          reinterpret_cast<EncodingType*>(opt_address));
    case OptionType::kWALRecoveryMode:
      return ParseEnum<WALRecoveryMode>(
          wal_recovery_mode_string_map, value,
          reinterpret_cast<WALRecoveryMode*>(opt_address));
    case OptionType::kAccessHint:
      return ParseEnum<DBOptions::AccessHint>(
          access_hint_string_map, value,
          reinterpret_cast<DBOptions::AccessHint*>(opt_address));
    case OptionType::kInfoLogLevel:
      return ParseEnum<InfoLogLevel>(
          info_log_level_string_map, value,
          reinterpret_cast<InfoLogLevel*>(opt_address));
    default:
      return false;
  }
  return true;
}

}  // anonymouse namespace

bool SerializeSingleOptionHelper(const char* opt_address,
                                 const OptionType opt_type,
                                 std::string* value) {
  static const std::string kNullptrString = "nullptr";
  assert(value);
  switch (opt_type) {
    case OptionType::kBoolean:
      *value = *(reinterpret_cast<const bool*>(opt_address)) ? "true" : "false";
      break;
    case OptionType::kInt:
      *value = ToString(*(reinterpret_cast<const int*>(opt_address)));
      break;
    case OptionType::kUInt:
      *value = ToString(*(reinterpret_cast<const unsigned int*>(opt_address)));
      break;
    case OptionType::kUInt32T:
      *value = ToString(*(reinterpret_cast<const uint32_t*>(opt_address)));
      break;
    case OptionType::kUInt64T:
      *value = ToString(*(reinterpret_cast<const uint64_t*>(opt_address)));
      break;
    case OptionType::kSizeT:
      *value = ToString(*(reinterpret_cast<const size_t*>(opt_address)));
      break;
    case OptionType::kDouble:
      *value = ToString(*(reinterpret_cast<const double*>(opt_address)));
      break;
    case OptionType::kString:
      *value = EscapeOptionString(
          *(reinterpret_cast<const std::string*>(opt_address)));
      break;
    case OptionType::kCompactionStyle:
      return SerializeEnum<CompactionStyle>(
          compaction_style_string_map,
          *(reinterpret_cast<const CompactionStyle*>(opt_address)), value);
    case OptionType::kCompressionType:
      return SerializeEnum<CompressionType>(
          compression_type_string_map,
          *(reinterpret_cast<const CompressionType*>(opt_address)), value);
    case OptionType::kVectorCompressionType:
      return SerializeVectorCompressionType(
          *(reinterpret_cast<const std::vector<CompressionType>*>(opt_address)),
          value);
      break;
    case OptionType::kSliceTransform: {
      const auto* slice_transform_ptr =
          reinterpret_cast<const std::shared_ptr<const SliceTransform>*>(
              opt_address);
      *value = slice_transform_ptr->get() ? slice_transform_ptr->get()->Name()
                                          : kNullptrString;
      break;
    }
    case OptionType::kTableFactory: {
      const auto* table_factory_ptr =
          reinterpret_cast<const std::shared_ptr<const TableFactory>*>(
              opt_address);
      *value = table_factory_ptr->get() ? table_factory_ptr->get()->Name()
                                        : kNullptrString;
      break;
    }
    case OptionType::kComparator: {
      // it's a const pointer of const Comparator*
      const auto* ptr = reinterpret_cast<const Comparator* const*>(opt_address);
      // Since the user-specified comparator will be wrapped by
      // InternalKeyComparator, we should persist the user-specified one
      // instead of InternalKeyComparator.
      const auto* internal_comparator =
          dynamic_cast<const InternalKeyComparator*>(*ptr);
      if (internal_comparator != nullptr) {
        *value = internal_comparator->user_comparator()->Name();
      } else {
        *value = *ptr ? (*ptr)->Name() : kNullptrString;
      }
      break;
    }
    case OptionType::kCompactionFilter: {
      // it's a const pointer of const CompactionFilter*
      const auto* ptr =
          reinterpret_cast<const CompactionFilter* const*>(opt_address);
      *value = *ptr ? (*ptr)->Name() : kNullptrString;
      break;
    }
    case OptionType::kCompactionFilterFactory: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<CompactionFilterFactory>*>(
              opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kMemTableRepFactory: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<MemTableRepFactory>*>(
              opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kMergeOperator: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<MergeOperator>*>(opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kFilterPolicy: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<FilterPolicy>*>(opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kChecksumType:
      return SerializeEnum<ChecksumType>(
          checksum_type_string_map,
          *reinterpret_cast<const ChecksumType*>(opt_address), value);
    case OptionType::kBlockBasedTableIndexType:
      return SerializeEnum<BlockBasedTableOptions::IndexType>(
          block_base_table_index_type_string_map,
          *reinterpret_cast<const BlockBasedTableOptions::IndexType*>(
              opt_address),
          value);
    case OptionType::kFlushBlockPolicyFactory: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<FlushBlockPolicyFactory>*>(
              opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kEncodingType:
      return SerializeEnum<EncodingType>(
          encoding_type_string_map,
          *reinterpret_cast<const EncodingType*>(opt_address), value);
    case OptionType::kWALRecoveryMode:
      return SerializeEnum<WALRecoveryMode>(
          wal_recovery_mode_string_map,
          *reinterpret_cast<const WALRecoveryMode*>(opt_address), value);
    case OptionType::kAccessHint:
      return SerializeEnum<DBOptions::AccessHint>(
          access_hint_string_map,
          *reinterpret_cast<const DBOptions::AccessHint*>(opt_address), value);
    case OptionType::kInfoLogLevel:
      return SerializeEnum<InfoLogLevel>(
          info_log_level_string_map,
          *reinterpret_cast<const InfoLogLevel*>(opt_address), value);
    default:
      return false;
  }
  return true;
}


template<typename OptionsType>
bool ParseMemtableOptions(const std::string& name, const std::string& value,
                          OptionsType* new_options) {
  if (name == "write_buffer_size") {
    new_options->write_buffer_size = ParseSizeT(value);
  } else if (name == "arena_block_size") {
    new_options->arena_block_size = ParseSizeT(value);
  } else if (name == "memtable_prefix_bloom_bits") {
    // deprecated
  } else if (name == "memtable_prefix_bloom_size_ratio") {
    new_options->memtable_prefix_bloom_size_ratio = ParseDouble(value);
  } else if (name == "memtable_prefix_bloom_probes") {
    // Deprecated
  } else if (name == "memtable_prefix_bloom_huge_page_tlb_size") {
    // Deprecated
  } else if (name == "memtable_huge_page_size") {
    new_options->memtable_huge_page_size = ParseSizeT(value);
  } else if (name == "max_successive_merges") {
    new_options->max_successive_merges = ParseSizeT(value);
  } else if (name == "filter_deletes") {
    // Deprecated
  } else if (name == "max_write_buffer_number") {
    new_options->max_write_buffer_number = ParseInt(value);
  } else if (name == "inplace_update_num_locks") {
    new_options->inplace_update_num_locks = ParseSizeT(value);
  } else {
    return false;
  }
  return true;
}

template<typename OptionsType>
bool ParseCompactionOptions(const std::string& name, const std::string& value,
                            OptionsType* new_options) {
  if (name == "disable_auto_compactions") {
    new_options->disable_auto_compactions = ParseBoolean(name, value);
  } else if (name == "soft_rate_limit") {
    // Deprecated options but still leave it here to avoid older options
    // strings can be consumed.
  } else if (name == "soft_pending_compaction_bytes_limit") {
    new_options->soft_pending_compaction_bytes_limit = ParseUint64(value);
  } else if (name == "hard_pending_compaction_bytes_limit") {
    new_options->hard_pending_compaction_bytes_limit = ParseUint64(value);
  } else if (name == "hard_rate_limit") {
    // Deprecated options but still leave it here to avoid older options
    // strings can be consumed.
  } else if (name == "level0_file_num_compaction_trigger") {
    new_options->level0_file_num_compaction_trigger = ParseInt(value);
  } else if (name == "level0_slowdown_writes_trigger") {
    new_options->level0_slowdown_writes_trigger = ParseInt(value);
  } else if (name == "level0_stop_writes_trigger") {
    new_options->level0_stop_writes_trigger = ParseInt(value);
  } else if (name == "max_grandparent_overlap_factor") {
    new_options->max_grandparent_overlap_factor = ParseInt(value);
  } else if (name == "expanded_compaction_factor") {
    new_options->expanded_compaction_factor = ParseInt(value);
  } else if (name == "source_compaction_factor") {
    new_options->source_compaction_factor = ParseInt(value);
  } else if (name == "target_file_size_base") {
    new_options->target_file_size_base = ParseInt(value);
  } else if (name == "target_file_size_multiplier") {
    new_options->target_file_size_multiplier = ParseInt(value);
  } else if (name == "max_bytes_for_level_base") {
    new_options->max_bytes_for_level_base = ParseUint64(value);
  } else if (name == "max_bytes_for_level_multiplier") {
    new_options->max_bytes_for_level_multiplier = ParseInt(value);
  } else if (name == "max_bytes_for_level_multiplier_additional") {
    new_options->max_bytes_for_level_multiplier_additional.clear();
    size_t start = 0;
    while (true) {
      size_t end = value.find(':', start);
      if (end == std::string::npos) {
        new_options->max_bytes_for_level_multiplier_additional.push_back(
            ParseInt(value.substr(start)));
        break;
      } else {
        new_options->max_bytes_for_level_multiplier_additional.push_back(
            ParseInt(value.substr(start, end - start)));
        start = end + 1;
      }
    }
  } else if (name == "verify_checksums_in_compaction") {
    new_options->verify_checksums_in_compaction = ParseBoolean(name, value);
  } else {
    return false;
  }
  return true;
}

template<typename OptionsType>
bool ParseMiscOptions(const std::string& name, const std::string& value,
                      OptionsType* new_options) {
  if (name == "max_sequential_skip_in_iterations") {
    new_options->max_sequential_skip_in_iterations = ParseUint64(value);
  } else if (name == "paranoid_file_checks") {
    new_options->paranoid_file_checks = ParseBoolean(name, value);
  } else if (name == "report_bg_io_stats") {
    new_options->report_bg_io_stats = ParseBoolean(name, value);
  } else if (name == "compression") {
    bool is_ok = ParseEnum<CompressionType>(compression_type_string_map, value,
                                            &new_options->compression);
    if (!is_ok) {
      return false;
    }
  } else if (name == "min_partial_merge_operands") {
    new_options->min_partial_merge_operands = ParseUint32(value);
  } else {
    return false;
  }
  return true;
}

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableCFOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  for (const auto& o : options_map) {
    try {
      if (ParseMemtableOptions(o.first, o.second, new_options)) {
      } else if (ParseCompactionOptions(o.first, o.second, new_options)) {
      } else if (ParseMiscOptions(o.first, o.second, new_options)) {
      } else {
        return Status::InvalidArgument(
            "unsupported dynamic option: " + o.first);
      }
    } catch (std::exception& e) {
      return Status::InvalidArgument("error parsing " + o.first + ":" +
                                     std::string(e.what()));
    }
  }
  return Status::OK();
}

Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map) {
  assert(opts_map);
  // Example:
  //   opts_str = "write_buffer_size=1024;max_write_buffer_number=2;"
  //              "nested_opt={opt1=1;opt2=2};max_bytes_for_level_base=100"
  size_t pos = 0;
  std::string opts = trim(opts_str);
  while (pos < opts.size()) {
    size_t eq_pos = opts.find('=', pos);
    if (eq_pos == std::string::npos) {
      return Status::InvalidArgument("Mismatched key value pair, '=' expected");
    }
    std::string key = trim(opts.substr(pos, eq_pos - pos));
    if (key.empty()) {
      return Status::InvalidArgument("Empty key found");
    }

    // skip space after '=' and look for '{' for possible nested options
    pos = eq_pos + 1;
    while (pos < opts.size() && isspace(opts[pos])) {
      ++pos;
    }
    // Empty value at the end
    if (pos >= opts.size()) {
      (*opts_map)[key] = "";
      break;
    }
    if (opts[pos] == '{') {
      int count = 1;
      size_t brace_pos = pos + 1;
      while (brace_pos < opts.size()) {
        if (opts[brace_pos] == '{') {
          ++count;
        } else if (opts[brace_pos] == '}') {
          --count;
          if (count == 0) {
            break;
          }
        }
        ++brace_pos;
      }
      // found the matching closing brace
      if (count == 0) {
        (*opts_map)[key] = trim(opts.substr(pos + 1, brace_pos - pos - 1));
        // skip all whitespace and move to the next ';'
        // brace_pos points to the next position after the matching '}'
        pos = brace_pos + 1;
        while (pos < opts.size() && isspace(opts[pos])) {
          ++pos;
        }
        if (pos < opts.size() && opts[pos] != ';') {
          return Status::InvalidArgument(
              "Unexpected chars after nested options");
        }
        ++pos;
      } else {
        return Status::InvalidArgument(
            "Mismatched curly braces for nested options");
      }
    } else {
      size_t sc_pos = opts.find(';', pos);
      if (sc_pos == std::string::npos) {
        (*opts_map)[key] = trim(opts.substr(pos));
        // It either ends with a trailing semi-colon or the last key-value pair
        break;
      } else {
        (*opts_map)[key] = trim(opts.substr(pos, sc_pos - pos));
      }
      pos = sc_pos + 1;
    }
  }

  return Status::OK();
}

Status ParseColumnFamilyOption(const std::string& name,
                               const std::string& org_value,
                               ColumnFamilyOptions* new_options,
                               bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  try {
    if (name == "max_bytes_for_level_multiplier_additional") {
      new_options->max_bytes_for_level_multiplier_additional.clear();
      size_t start = 0;
      while (true) {
        size_t end = value.find(':', start);
        if (end == std::string::npos) {
          new_options->max_bytes_for_level_multiplier_additional.push_back(
              ParseInt(value.substr(start)));
          break;
        } else {
          new_options->max_bytes_for_level_multiplier_additional.push_back(
              ParseInt(value.substr(start, end - start)));
          start = end + 1;
        }
      }
    } else if (name == "block_based_table_factory") {
      // Nested options
      BlockBasedTableOptions table_opt, base_table_options;
      auto block_based_table_factory = dynamic_cast<BlockBasedTableFactory*>(
          new_options->table_factory.get());
      if (block_based_table_factory != nullptr) {
        base_table_options = block_based_table_factory->table_options();
      }
      Status table_opt_s = GetBlockBasedTableOptionsFromString(
          base_table_options, value, &table_opt);
      if (!table_opt_s.ok()) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      new_options->table_factory.reset(NewBlockBasedTableFactory(table_opt));
    } else if (name == "plain_table_factory") {
      // Nested options
      PlainTableOptions table_opt, base_table_options;
      auto plain_table_factory = dynamic_cast<PlainTableFactory*>(
          new_options->table_factory.get());
      if (plain_table_factory != nullptr) {
        base_table_options = plain_table_factory->table_options();
      }
      Status table_opt_s = GetPlainTableOptionsFromString(
          base_table_options, value, &table_opt);
      if (!table_opt_s.ok()) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      new_options->table_factory.reset(NewPlainTableFactory(table_opt));
    } else if (name == "memtable") {
      std::unique_ptr<MemTableRepFactory> new_mem_factory;
      Status mem_factory_s =
          GetMemTableRepFactoryFromString(value, &new_mem_factory);
      if (!mem_factory_s.ok()) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      new_options->memtable_factory.reset(new_mem_factory.release());
    } else if (name == "compression_opts") {
      size_t start = 0;
      size_t end = value.find(':');
      if (end == std::string::npos) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      new_options->compression_opts.window_bits =
          ParseInt(value.substr(start, end - start));
      start = end + 1;
      end = value.find(':', start);
      if (end == std::string::npos) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      new_options->compression_opts.level =
          ParseInt(value.substr(start, end - start));
      start = end + 1;
      if (start >= value.size()) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      end = value.find(':', start);
      new_options->compression_opts.strategy =
          ParseInt(value.substr(start, value.size() - start));
      // max_dict_bytes is optional for backwards compatibility
      if (end != std::string::npos) {
        start = end + 1;
        if (start >= value.size()) {
          return Status::InvalidArgument(
              "unable to parse the specified CF option " + name);
        }
        new_options->compression_opts.max_dict_bytes =
            ParseInt(value.substr(start, value.size() - start));
      }
    } else if (name == "compaction_options_fifo") {
      new_options->compaction_options_fifo.max_table_files_size =
          ParseUint64(value);
    } else {
      auto iter = cf_options_type_info.find(name);
      if (iter == cf_options_type_info.end()) {
        return Status::InvalidArgument(
            "Unable to parse the specified CF option " + name);
      }
      const auto& opt_info = iter->second;
      if (opt_info.verification != OptionVerificationType::kDeprecated &&
          ParseOptionHelper(
              reinterpret_cast<char*>(new_options) + opt_info.offset,
              opt_info.type, value)) {
        return Status::OK();
      }
      switch (opt_info.verification) {
        case OptionVerificationType::kByName:
        case OptionVerificationType::kByNameAllowNull:
          return Status::NotSupported(
              "Deserializing the specified CF option " + name +
                  " is not supported");
        case OptionVerificationType::kDeprecated:
          return Status::OK();
        default:
          return Status::InvalidArgument(
              "Unable to parse the specified CF option " + name);
      }
    }
  } catch (const std::exception&) {
    return Status::InvalidArgument(
        "unable to parse the specified option " + name);
  }
  return Status::OK();
}

bool SerializeSingleDBOption(std::string* opt_string,
                             const DBOptions& db_options,
                             const std::string& name,
                             const std::string& delimiter) {
  auto iter = db_options_type_info.find(name);
  if (iter == db_options_type_info.end()) {
    return false;
  }
  auto& opt_info = iter->second;
  const char* opt_address =
      reinterpret_cast<const char*>(&db_options) + opt_info.offset;
  std::string value;
  bool result = SerializeSingleOptionHelper(opt_address, opt_info.type, &value);
  if (result) {
    *opt_string = name + "=" + value + delimiter;
  }
  return result;
}

Status GetStringFromDBOptions(std::string* opt_string,
                              const DBOptions& db_options,
                              const std::string& delimiter) {
  assert(opt_string);
  opt_string->clear();
  for (auto iter = db_options_type_info.begin();
       iter != db_options_type_info.end(); ++iter) {
    if (iter->second.verification == OptionVerificationType::kDeprecated) {
      // If the option is no longer used in rocksdb and marked as deprecated,
      // we skip it in the serialization.
      continue;
    }
    std::string single_output;
    bool result = SerializeSingleDBOption(&single_output, db_options,
                                          iter->first, delimiter);
    assert(result);
    if (result) {
      opt_string->append(single_output);
    }
  }
  return Status::OK();
}

bool SerializeSingleColumnFamilyOption(std::string* opt_string,
                                       const ColumnFamilyOptions& cf_options,
                                       const std::string& name,
                                       const std::string& delimiter) {
  auto iter = cf_options_type_info.find(name);
  if (iter == cf_options_type_info.end()) {
    return false;
  }
  auto& opt_info = iter->second;
  const char* opt_address =
      reinterpret_cast<const char*>(&cf_options) + opt_info.offset;
  std::string value;
  bool result = SerializeSingleOptionHelper(opt_address, opt_info.type, &value);
  if (result) {
    *opt_string = name + "=" + value + delimiter;
  }
  return result;
}

Status GetStringFromColumnFamilyOptions(std::string* opt_string,
                                        const ColumnFamilyOptions& cf_options,
                                        const std::string& delimiter) {
  assert(opt_string);
  opt_string->clear();
  for (auto iter = cf_options_type_info.begin();
       iter != cf_options_type_info.end(); ++iter) {
    if (iter->second.verification == OptionVerificationType::kDeprecated) {
      // If the option is no longer used in rocksdb and marked as deprecated,
      // we skip it in the serialization.
      continue;
    }
    std::string single_output;
    bool result = SerializeSingleColumnFamilyOption(&single_output, cf_options,
                                                    iter->first, delimiter);
    if (result) {
      opt_string->append(single_output);
    } else {
      return Status::InvalidArgument("failed to serialize %s\n",
                                     iter->first.c_str());
    }
    assert(result);
  }
  return Status::OK();
}

Status GetStringFromCompressionType(std::string* compression_str,
                                    CompressionType compression_type) {
  bool ok = SerializeEnum<CompressionType>(compression_type_string_map,
                                           compression_type, compression_str);
  if (ok) {
    return Status::OK();
  } else {
    return Status::InvalidArgument("Invalid compression types");
  }
}

bool SerializeSingleBlockBasedTableOption(
    std::string* opt_string, const BlockBasedTableOptions& bbt_options,
    const std::string& name, const std::string& delimiter) {
  auto iter = block_based_table_type_info.find(name);
  if (iter == block_based_table_type_info.end()) {
    return false;
  }
  auto& opt_info = iter->second;
  const char* opt_address =
      reinterpret_cast<const char*>(&bbt_options) + opt_info.offset;
  std::string value;
  bool result = SerializeSingleOptionHelper(opt_address, opt_info.type, &value);
  if (result) {
    *opt_string = name + "=" + value + delimiter;
  }
  return result;
}

Status GetStringFromBlockBasedTableOptions(
    std::string* opt_string, const BlockBasedTableOptions& bbt_options,
    const std::string& delimiter) {
  assert(opt_string);
  opt_string->clear();
  for (auto iter = block_based_table_type_info.begin();
       iter != block_based_table_type_info.end(); ++iter) {
    if (iter->second.verification == OptionVerificationType::kDeprecated) {
      // If the option is no longer used in rocksdb and marked as deprecated,
      // we skip it in the serialization.
      continue;
    }
    std::string single_output;
    bool result = SerializeSingleBlockBasedTableOption(
        &single_output, bbt_options, iter->first, delimiter);
    assert(result);
    if (result) {
      opt_string->append(single_output);
    }
  }
  return Status::OK();
}

Status GetStringFromTableFactory(std::string* opts_str, const TableFactory* tf,
                                 const std::string& delimiter) {
  const auto* bbtf = dynamic_cast<const BlockBasedTableFactory*>(tf);
  opts_str->clear();
  if (bbtf != nullptr) {
    return GetStringFromBlockBasedTableOptions(opts_str, bbtf->table_options(),
                                               delimiter);
  }

  return Status::OK();
}

Status ParseDBOption(const std::string& name,
                     const std::string& org_value,
                     DBOptions* new_options,
                     bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  try {
    if (name == "rate_limiter_bytes_per_sec") {
      new_options->rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(ParseUint64(value))));
    } else {
      auto iter = db_options_type_info.find(name);
      if (iter == db_options_type_info.end()) {
        return Status::InvalidArgument("Unrecognized option DBOptions:", name);
      }
      const auto& opt_info = iter->second;
      if (opt_info.verification != OptionVerificationType::kDeprecated &&
          ParseOptionHelper(
              reinterpret_cast<char*>(new_options) + opt_info.offset,
              opt_info.type, value)) {
        return Status::OK();
      }
      switch (opt_info.verification) {
        case OptionVerificationType::kByName:
        case OptionVerificationType::kByNameAllowNull:
          return Status::NotSupported(
              "Deserializing the specified DB option " + name +
                  " is not supported");
        case OptionVerificationType::kDeprecated:
          return Status::OK();
        default:
          return Status::InvalidArgument(
              "Unable to parse the specified DB option " + name);
      }
    }
  } catch (const std::exception&) {
    return Status::InvalidArgument("Unable to parse DBOptions:", name);
  }
  return Status::OK();
}

std::string ParseBlockBasedTableOption(const std::string& name,
                                       const std::string& org_value,
                                       BlockBasedTableOptions* new_options,
                                       bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  if (!input_strings_escaped) {
    // if the input string is not escaped, it means this function is
    // invoked from SetOptions, which takes the old format.
    if (name == "block_cache") {
      new_options->block_cache = NewLRUCache(ParseSizeT(value));
      return "";
    } else if (name == "block_cache_compressed") {
      new_options->block_cache_compressed = NewLRUCache(ParseSizeT(value));
      return "";
    } else if (name == "filter_policy") {
      // Expect the following format
      // bloomfilter:int:bool
      const std::string kName = "bloomfilter:";
      if (value.compare(0, kName.size(), kName) != 0) {
        return "Invalid filter policy name";
      }
      size_t pos = value.find(':', kName.size());
      if (pos == std::string::npos) {
        return "Invalid filter policy config, missing bits_per_key";
      }
      int bits_per_key =
          ParseInt(trim(value.substr(kName.size(), pos - kName.size())));
      bool use_block_based_builder =
          ParseBoolean("use_block_based_builder", trim(value.substr(pos + 1)));
      new_options->filter_policy.reset(
          NewBloomFilterPolicy(bits_per_key, use_block_based_builder));
      return "";
    }
  }
  const auto iter = block_based_table_type_info.find(name);
  if (iter == block_based_table_type_info.end()) {
    return "Unrecognized option";
  }
  const auto& opt_info = iter->second;
  if (!ParseOptionHelper(reinterpret_cast<char*>(new_options) + opt_info.offset,
                         opt_info.type, value)) {
    return "Invalid value";
  }
  return "";
}

std::string ParsePlainTableOptions(const std::string& name,
                                   const std::string& org_value,
                                   PlainTableOptions* new_option,
                                   bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  const auto iter = plain_table_type_info.find(name);
  if (iter == plain_table_type_info.end()) {
    return "Unrecognized option";
  }
  const auto& opt_info = iter->second;
  if (!ParseOptionHelper(reinterpret_cast<char*>(new_option) + opt_info.offset,
                         opt_info.type, value)) {
    return "Invalid value";
  }
  return "";
}

Status GetBlockBasedTableOptionsFromMap(
    const BlockBasedTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    BlockBasedTableOptions* new_table_options, bool input_strings_escaped) {
  assert(new_table_options);
  *new_table_options = table_options;
  for (const auto& o : opts_map) {
    auto error_message = ParseBlockBasedTableOption(
        o.first, o.second, new_table_options, input_strings_escaped);
    if (error_message != "") {
      const auto iter = block_based_table_type_info.find(o.first);
      if (iter == block_based_table_type_info.end() ||
          !input_strings_escaped ||  // !input_strings_escaped indicates
                                     // the old API, where everything is
                                     // parsable.
          (iter->second.verification != OptionVerificationType::kByName &&
           iter->second.verification !=
               OptionVerificationType::kByNameAllowNull &&
           iter->second.verification != OptionVerificationType::kDeprecated)) {
        return Status::InvalidArgument("Can't parse BlockBasedTableOptions:",
                                       o.first + " " + error_message);
      }
    }
  }
  return Status::OK();
}

Status GetBlockBasedTableOptionsFromString(
    const BlockBasedTableOptions& table_options,
    const std::string& opts_str,
    BlockBasedTableOptions* new_table_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  return GetBlockBasedTableOptionsFromMap(table_options, opts_map,
                                          new_table_options);
}

Status GetPlainTableOptionsFromMap(
    const PlainTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    PlainTableOptions* new_table_options, bool input_strings_escaped) {
  assert(new_table_options);
  *new_table_options = table_options;
  for (const auto& o : opts_map) {
    auto error_message = ParsePlainTableOptions(
        o.first, o.second, new_table_options, input_strings_escaped);
    if (error_message != "") {
      const auto iter = plain_table_type_info.find(o.first);
      if (iter == plain_table_type_info.end() ||
          !input_strings_escaped ||  // !input_strings_escaped indicates
                                     // the old API, where everything is
                                     // parsable.
          (iter->second.verification != OptionVerificationType::kByName &&
           iter->second.verification !=
               OptionVerificationType::kByNameAllowNull &&
           iter->second.verification != OptionVerificationType::kDeprecated)) {
        return Status::InvalidArgument("Can't parse PlainTableOptions:",
                                        o.first + " " + error_message);
      }
    }
  }
  return Status::OK();
}

Status GetPlainTableOptionsFromString(
    const PlainTableOptions& table_options,
    const std::string& opts_str,
    PlainTableOptions* new_table_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  return GetPlainTableOptionsFromMap(table_options, opts_map,
                                     new_table_options);
}

Status GetMemTableRepFactoryFromString(const std::string& opts_str,
    std::unique_ptr<MemTableRepFactory>* new_mem_factory) {
  std::vector<std::string> opts_list = StringSplit(opts_str, ':');
  size_t len = opts_list.size();

  if (opts_list.size() <= 0 || opts_list.size() > 2) {
    return Status::InvalidArgument("Can't parse memtable_factory option ",
                                     opts_str);
  }

  MemTableRepFactory* mem_factory = nullptr;

  if (opts_list[0] == "skip_list") {
    // Expecting format
    // skip_list:<lookahead>
    if (2 == len) {
      size_t lookahead = ParseSizeT(opts_list[1]);
      mem_factory = new SkipListFactory(lookahead);
    } else if (1 == len) {
      mem_factory = new SkipListFactory();
    }
  } else if (opts_list[0] == "prefix_hash") {
    // Expecting format
    // prfix_hash:<hash_bucket_count>
    if (2 == len) {
      size_t hash_bucket_count = ParseSizeT(opts_list[1]);
      mem_factory = NewHashSkipListRepFactory(hash_bucket_count);
    } else if (1 == len) {
      mem_factory = NewHashSkipListRepFactory();
    }
  } else if (opts_list[0] == "hash_linkedlist") {
    // Expecting format
    // hash_linkedlist:<hash_bucket_count>
    if (2 == len) {
      size_t hash_bucket_count = ParseSizeT(opts_list[1]);
      mem_factory = NewHashLinkListRepFactory(hash_bucket_count);
    } else if (1 == len) {
      mem_factory = NewHashLinkListRepFactory();
    }
  } else if (opts_list[0] == "vector") {
    // Expecting format
    // vector:<count>
    if (2 == len) {
      size_t count = ParseSizeT(opts_list[1]);
      mem_factory = new VectorRepFactory(count);
    } else if (1 == len) {
      mem_factory = new VectorRepFactory();
    }
  } else if (opts_list[0] == "cuckoo") {
    // Expecting format
    // cuckoo:<write_buffer_size>
    if (2 == len) {
      size_t write_buffer_size = ParseSizeT(opts_list[1]);
      mem_factory= NewHashCuckooRepFactory(write_buffer_size);
    } else if (1 == len) {
      return Status::InvalidArgument("Can't parse memtable_factory option ",
                                     opts_str);
    }
  } else {
    return Status::InvalidArgument("Unrecognized memtable_factory option ",
                                   opts_str);
  }

  if (mem_factory != nullptr){
    new_mem_factory->reset(mem_factory);
  }

  return Status::OK();
}

Status GetColumnFamilyOptionsFromMap(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped) {
  return GetColumnFamilyOptionsFromMapInternal(
      base_options, opts_map, new_options, input_strings_escaped);
}

Status GetColumnFamilyOptionsFromMapInternal(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped,
    std::vector<std::string>* unsupported_options_names) {
  assert(new_options);
  *new_options = base_options;
  if (unsupported_options_names) {
    unsupported_options_names->clear();
  }
  for (const auto& o : opts_map) {
    auto s = ParseColumnFamilyOption(o.first, o.second, new_options,
                                 input_strings_escaped);
    if (!s.ok()) {
      if (s.IsNotSupported()) {
        // If the deserialization of the specified option is not supported
        // and an output vector of unsupported_options is provided, then
        // we log the name of the unsupported option and proceed.
        if (unsupported_options_names != nullptr) {
          unsupported_options_names->push_back(o.first);
        }
        // Note that we still return Status::OK in such case to maintain
        // the backward compatibility in the old public API defined in
        // rocksdb/convenience.h
      } else {
        return s;
      }
    }
  }
  return Status::OK();
}

Status GetColumnFamilyOptionsFromString(
    const ColumnFamilyOptions& base_options,
    const std::string& opts_str,
    ColumnFamilyOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  return GetColumnFamilyOptionsFromMap(base_options, opts_map, new_options);
}

Status GetDBOptionsFromMap(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options, bool input_strings_escaped) {
  return GetDBOptionsFromMapInternal(
      base_options, opts_map, new_options, input_strings_escaped);
}

Status GetDBOptionsFromMapInternal(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options, bool input_strings_escaped,
    std::vector<std::string>* unsupported_options_names) {
  assert(new_options);
  *new_options = base_options;
  if (unsupported_options_names) {
    unsupported_options_names->clear();
  }
  for (const auto& o : opts_map) {
    auto s = ParseDBOption(o.first, o.second,
                           new_options, input_strings_escaped);
    if (!s.ok()) {
      if (s.IsNotSupported()) {
        // If the deserialization of the specified option is not supported
        // and an output vector of unsupported_options is provided, then
        // we log the name of the unsupported option and proceed.
        if (unsupported_options_names != nullptr) {
          unsupported_options_names->push_back(o.first);
        }
        // Note that we still return Status::OK in such case to maintain
        // the backward compatibility in the old public API defined in
        // rocksdb/convenience.h
      } else {
        return s;
      }
    }
  }
  return Status::OK();
}

Status GetDBOptionsFromString(
    const DBOptions& base_options,
    const std::string& opts_str,
    DBOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  return GetDBOptionsFromMap(base_options, opts_map, new_options);
}

Status GetOptionsFromString(const Options& base_options,
                            const std::string& opts_str, Options* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  DBOptions new_db_options(base_options);
  ColumnFamilyOptions new_cf_options(base_options);
  for (const auto& o : opts_map) {
    if (ParseDBOption(o.first, o.second, &new_db_options).ok()) {
    } else if (ParseColumnFamilyOption(
        o.first, o.second, &new_cf_options).ok()) {
    } else {
      return Status::InvalidArgument("Can't parse option " + o.first);
    }
  }
  *new_options = Options(new_db_options, new_cf_options);
  return Status::OK();
}

Status GetTableFactoryFromMap(
    const std::string& factory_name,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::shared_ptr<TableFactory>* table_factory) {
  Status s;
  if (factory_name == BlockBasedTableFactory().Name()) {
    BlockBasedTableOptions bbt_opt;
    s = GetBlockBasedTableOptionsFromMap(BlockBasedTableOptions(), opt_map,
                                         &bbt_opt, true);
    if (!s.ok()) {
      return s;
    }
    table_factory->reset(new BlockBasedTableFactory(bbt_opt));
    return Status::OK();
  } else if (factory_name == PlainTableFactory().Name()) {
    PlainTableOptions pt_opt;
    s = GetPlainTableOptionsFromMap(PlainTableOptions(), opt_map, &pt_opt,
                                    true);
    if (!s.ok()) {
      return s;
    }
    table_factory->reset(new PlainTableFactory(pt_opt));
    return Status::OK();
  }
  // Return OK for not supported table factories as TableFactory
  // Deserialization is optional.
  table_factory->reset();
  return Status::OK();
}

ColumnFamilyOptions BuildColumnFamilyOptions(
    const Options& options, const MutableCFOptions& mutable_cf_options) {
  ColumnFamilyOptions cf_opts(options);

  // Memtable related options
  cf_opts.write_buffer_size = mutable_cf_options.write_buffer_size;
  cf_opts.max_write_buffer_number = mutable_cf_options.max_write_buffer_number;
  cf_opts.arena_block_size = mutable_cf_options.arena_block_size;
  cf_opts.memtable_prefix_bloom_size_ratio =
      mutable_cf_options.memtable_prefix_bloom_size_ratio;
  cf_opts.memtable_huge_page_size = mutable_cf_options.memtable_huge_page_size;
  cf_opts.max_successive_merges = mutable_cf_options.max_successive_merges;
  cf_opts.inplace_update_num_locks =
      mutable_cf_options.inplace_update_num_locks;

  // Compaction related options
  cf_opts.disable_auto_compactions =
      mutable_cf_options.disable_auto_compactions;
  cf_opts.level0_file_num_compaction_trigger =
      mutable_cf_options.level0_file_num_compaction_trigger;
  cf_opts.level0_slowdown_writes_trigger =
      mutable_cf_options.level0_slowdown_writes_trigger;
  cf_opts.level0_stop_writes_trigger =
      mutable_cf_options.level0_stop_writes_trigger;
  cf_opts.max_grandparent_overlap_factor =
      mutable_cf_options.max_grandparent_overlap_factor;
  cf_opts.expanded_compaction_factor =
      mutable_cf_options.expanded_compaction_factor;
  cf_opts.source_compaction_factor =
      mutable_cf_options.source_compaction_factor;
  cf_opts.target_file_size_base = mutable_cf_options.target_file_size_base;
  cf_opts.target_file_size_multiplier =
      mutable_cf_options.target_file_size_multiplier;
  cf_opts.max_bytes_for_level_base =
      mutable_cf_options.max_bytes_for_level_base;
  cf_opts.max_bytes_for_level_multiplier =
      mutable_cf_options.max_bytes_for_level_multiplier;

  cf_opts.max_bytes_for_level_multiplier_additional.clear();
  for (auto value :
       mutable_cf_options.max_bytes_for_level_multiplier_additional) {
    cf_opts.max_bytes_for_level_multiplier_additional.emplace_back(value);
  }

  cf_opts.verify_checksums_in_compaction =
      mutable_cf_options.verify_checksums_in_compaction;

  // Misc options
  cf_opts.max_sequential_skip_in_iterations =
      mutable_cf_options.max_sequential_skip_in_iterations;
  cf_opts.paranoid_file_checks = mutable_cf_options.paranoid_file_checks;
  cf_opts.report_bg_io_stats = mutable_cf_options.report_bg_io_stats;

  cf_opts.table_factory = options.table_factory;
  // TODO(yhchiang): find some way to handle the following derived options
  // * max_file_size

  return cf_opts;
}

#endif  // !ROCKSDB_LITE
}  // namespace rocksdb
