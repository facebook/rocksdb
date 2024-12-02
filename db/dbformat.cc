//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/dbformat.h"

#include <cinttypes>
#include <cstdio>

#include "db/lookup_key.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
const ValueType kValueTypeForSeek = kTypeValuePreferredSeqno;
const ValueType kValueTypeForSeekForPrev = kTypeDeletion;
const std::string kDisableUserTimestamp;

EntryType GetEntryType(ValueType value_type) {
  switch (value_type) {
    case kTypeValue:
      return kEntryPut;
    case kTypeDeletion:
      return kEntryDelete;
    case kTypeDeletionWithTimestamp:
      return kEntryDeleteWithTimestamp;
    case kTypeSingleDeletion:
      return kEntrySingleDelete;
    case kTypeMerge:
      return kEntryMerge;
    case kTypeRangeDeletion:
      return kEntryRangeDeletion;
    case kTypeBlobIndex:
      return kEntryBlobIndex;
    case kTypeWideColumnEntity:
      return kEntryWideColumnEntity;
    case kTypeValuePreferredSeqno:
      return kEntryTimedPut;
    default:
      return kEntryOther;
  }
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

void AppendInternalKeyWithDifferentTimestamp(std::string* result,
                                             const ParsedInternalKey& key,
                                             const Slice& ts) {
  assert(key.user_key.size() >= ts.size());
  result->append(key.user_key.data(), key.user_key.size() - ts.size());
  result->append(ts.data(), ts.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

void AppendUserKeyWithDifferentTimestamp(std::string* result, const Slice& key,
                                         const Slice& ts) {
  assert(key.size() >= ts.size());
  result->append(key.data(), key.size() - ts.size());
  result->append(ts.data(), ts.size());
}

void AppendInternalKeyFooter(std::string* result, SequenceNumber s,
                             ValueType t) {
  PutFixed64(result, PackSequenceAndType(s, t));
}

void AppendKeyWithMinTimestamp(std::string* result, const Slice& key,
                               size_t ts_sz) {
  assert(ts_sz > 0);
  const std::string kTsMin(ts_sz, static_cast<unsigned char>(0));
  result->append(key.data(), key.size());
  result->append(kTsMin.data(), ts_sz);
}

void AppendKeyWithMaxTimestamp(std::string* result, const Slice& key,
                               size_t ts_sz) {
  assert(ts_sz > 0);
  const std::string kTsMax(ts_sz, static_cast<unsigned char>(0xff));
  result->append(key.data(), key.size());
  result->append(kTsMax.data(), ts_sz);
}

void AppendUserKeyWithMinTimestamp(std::string* result, const Slice& key,
                                   size_t ts_sz) {
  assert(ts_sz > 0);
  result->append(key.data(), key.size() - ts_sz);
  result->append(ts_sz, static_cast<unsigned char>(0));
}

void AppendUserKeyWithMaxTimestamp(std::string* result, const Slice& key,
                                   size_t ts_sz) {
  assert(ts_sz > 0);
  result->append(key.data(), key.size() - ts_sz);

  static constexpr char kTsMax[] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
  if (ts_sz < strlen(kTsMax)) {
    result->append(kTsMax, ts_sz);
  } else {
    result->append(std::string(ts_sz, '\xff'));
  }
}

void PadInternalKeyWithMinTimestamp(std::string* result, const Slice& key,
                                    size_t ts_sz) {
  assert(ts_sz > 0);
  assert(key.size() >= kNumInternalBytes);
  size_t user_key_size = key.size() - kNumInternalBytes;
  result->reserve(key.size() + ts_sz);
  result->append(key.data(), user_key_size);
  result->append(ts_sz, static_cast<unsigned char>(0));
  result->append(key.data() + user_key_size, kNumInternalBytes);
}

void PadInternalKeyWithMaxTimestamp(std::string* result, const Slice& key,
                                    size_t ts_sz) {
  assert(ts_sz > 0);
  assert(key.size() >= kNumInternalBytes);
  size_t user_key_size = key.size() - kNumInternalBytes;
  result->reserve(key.size() + ts_sz);
  result->append(key.data(), user_key_size);
  result->append(std::string(ts_sz, '\xff'));
  result->append(key.data() + user_key_size, kNumInternalBytes);
}

void StripTimestampFromInternalKey(std::string* result, const Slice& key,
                                   size_t ts_sz) {
  assert(key.size() >= ts_sz + kNumInternalBytes);
  result->reserve(key.size() - ts_sz);
  result->append(key.data(), key.size() - kNumInternalBytes - ts_sz);
  result->append(key.data() + key.size() - kNumInternalBytes,
                 kNumInternalBytes);
}

void ReplaceInternalKeyWithMinTimestamp(std::string* result, const Slice& key,
                                        size_t ts_sz) {
  const size_t key_sz = key.size();
  assert(key_sz >= ts_sz + kNumInternalBytes);
  result->reserve(key_sz);
  result->append(key.data(), key_sz - kNumInternalBytes - ts_sz);
  result->append(ts_sz, static_cast<unsigned char>(0));
  result->append(key.data() + key_sz - kNumInternalBytes, kNumInternalBytes);
}

std::string ParsedInternalKey::DebugString(bool log_err_key, bool hex,
                                           const Comparator* ucmp) const {
  std::string result = "'";
  size_t ts_sz_for_debug = ucmp == nullptr ? 0 : ucmp->timestamp_size();
  if (log_err_key) {
    if (ts_sz_for_debug == 0) {
      result += user_key.ToString(hex);
    } else {
      assert(user_key.size() >= ts_sz_for_debug);
      Slice user_key_without_ts = user_key;
      user_key_without_ts.remove_suffix(ts_sz_for_debug);
      result += user_key_without_ts.ToString(hex);
      Slice ts = Slice(user_key.data() + user_key.size() - ts_sz_for_debug,
                       ts_sz_for_debug);
      result += "|timestamp:";
      result += ucmp->TimestampToString(ts);
    }
  } else {
    result += "<redacted>";
  }

  result += "' seq:" + std::to_string(sequence);
  result += ", type:" + std::to_string(type);

  return result;
}

std::string InternalKey::DebugString(bool hex, const Comparator* ucmp) const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed, false /* log_err_key */).ok()) {
    result = parsed.DebugString(true /* log_err_key */, hex, ucmp);  // TODO
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

int InternalKeyComparator::Compare(const ParsedInternalKey& a,
                                   const ParsedInternalKey& b) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_.Compare(a.user_key, b.user_key);
  if (r == 0) {
    if (a.sequence > b.sequence) {
      r = -1;
    } else if (a.sequence < b.sequence) {
      r = +1;
    } else if (a.type > b.type) {
      r = -1;
    } else if (a.type < b.type) {
      r = +1;
    }
  }
  return r;
}

int InternalKeyComparator::Compare(const Slice& a,
                                   const ParsedInternalKey& b) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_.Compare(ExtractUserKey(a), b.user_key);
  if (r == 0) {
    const uint64_t anum =
        DecodeFixed64(a.data() + a.size() - kNumInternalBytes);
    const uint64_t bnum = (b.sequence << 8) | b.type;
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

int InternalKeyComparator::Compare(const ParsedInternalKey& a,
                                   const Slice& b) const {
  return -Compare(b, a);
}

LookupKey::LookupKey(const Slice& _user_key, SequenceNumber s,
                     const Slice* ts) {
  size_t usize = _user_key.size();
  size_t ts_sz = (nullptr == ts) ? 0 : ts->size();
  size_t needed = usize + ts_sz + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  // NOTE: We don't support users keys of more than 2GB :)
  dst = EncodeVarint32(dst, static_cast<uint32_t>(usize + ts_sz + 8));
  kstart_ = dst;
  memcpy(dst, _user_key.data(), usize);
  dst += usize;
  if (nullptr != ts) {
    memcpy(dst, ts->data(), ts_sz);
    dst += ts_sz;
  }
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

void IterKey::EnlargeBuffer(size_t key_size) {
  // If size is smaller than buffer size, continue using current buffer,
  // or the inline one, as default
  assert(key_size > buf_size_);
  // Need to enlarge the buffer.
  ResetBuffer();
  buf_ = new char[key_size];
  buf_size_ = key_size;
}

void IterKey::EnlargeSecondaryBufferIfNeeded(size_t key_size) {
  // If size is smaller than buffer size, continue using current buffer,
  // or the inline one, as default
  if (key_size <= secondary_buf_size_) {
    return;
  }
  // Need to enlarge the secondary buffer.
  ResetSecondaryBuffer();
  secondary_buf_ = new char[key_size];
  secondary_buf_size_ = key_size;
}
}  // namespace ROCKSDB_NAMESPACE
