#include "db/blob_log_format.h"
#include "util/coding.h"


namespace rocksdb {
namespace blob_log {


BlobLogHeader::BlobLogHeader()
: magic_number_(kMagicNumber),
  has_ttl_(false),
  has_ts_(false)
{
}

BlobLogFooter::BlobLogFooter()
: magic_number_(kMagicNumber),
  blob_count_(0),
  has_ttl_(false), has_ts_(false)
{
}
  
Status BlobLogFooter::DecodeFrom(Slice* input)
{
  Status s;
  uint32_t offset = 0;

  const char *ptr = input->data();
  uint32_t mn = DecodeFixed32(ptr);
  if (mn != kMagicNumber)
  {
    return Status::Corruption("bad magic number");
  }

  magic_number_ = mn;

  offset += sizeof(uint32_t);
  uint32_t val = (DecodeFixed32(ptr + offset) >> 8);
  has_ttl_ = has_ts_ = false;
  RecordSubType st = (RecordSubType)val;
  if (st == kTTLType) {
    has_ttl_= true;
  } else if (st == kTimestampType) {
    has_ts_ = true;
  } else {
    return Status::Corruption("bad sub type");
  }

  offset += sizeof(uint32_t);
  blob_count_ = DecodeFixed64(ptr + offset);

  offset += sizeof(uint64_t);
  ttl_range_.first = DecodeFixed32(ptr + offset);
  offset += sizeof(uint32_t);
  ttl_range_.second = DecodeFixed32(ptr + offset);
  offset += sizeof(uint32_t);

  sn_range_.first = DecodeFixed64(ptr + offset);
  offset += sizeof(uint64_t);
  sn_range_.second = DecodeFixed64(ptr + offset);
  offset += sizeof(uint64_t);

  ts_range_.first = DecodeFixed64(ptr + offset);
  offset += sizeof(uint64_t);
  ts_range_.second = DecodeFixed64(ptr + offset);

  return s;
}

void BlobLogFooter::EncodeTo(std::string* dst) const {

  dst->reserve(kFooterSize);
  PutFixed32(dst, magic_number_);

  RecordType rt = kFullType;
  RecordSubType st = kRegularType;
  if (has_ttl_) {
    st = kTTLType;
  } else if (has_ts_) {
    st = kTimestampType;
  }

  uint32_t val = static_cast<uint32_t>(rt) | (static_cast<uint32_t>(st) << 8);
  PutFixed32(dst, val);
  PutFixed64(dst, blob_count_);
  PutFixed32(dst, ttl_range_.first);
  PutFixed32(dst, ttl_range_.second);
  PutFixed64(dst, sn_range_.first);
  PutFixed64(dst, sn_range_.second);

  PutFixed64(dst, ts_range_.first);
  PutFixed64(dst, ts_range_.second);
}

void BlobLogHeader::EncodeTo(std::string *dst) const {

  dst->reserve(kHeaderSize);
  PutFixed32(dst, magic_number_);

  RecordSubType st = kRegularType;
  if (has_ttl_) {
    st = kTTLType;
  } else if (has_ts_) {
    st = kTimestampType;
  }

  uint32_t val = static_cast<uint32_t>(st);
  PutFixed32(dst, val);

  PutFixed32(dst, ttl_guess_.first);
  PutFixed32(dst, ttl_guess_.second);

  PutFixed64(dst, ts_guess_.first);
  PutFixed64(dst, ts_guess_.second);
}

Status BlobLogHeader::DecodeFrom(Slice* input)
{
  Status s;
  uint32_t offset = 0;

  const char *ptr = input->data();
  uint32_t mn = DecodeFixed32(ptr);
  if (mn != kMagicNumber)
  {
    return Status::Corruption("bad magic number");
  }

  magic_number_ = mn;

  offset += sizeof(uint32_t);
  uint32_t val = DecodeFixed32(ptr + offset);
  has_ttl_ = has_ts_ = false;
  RecordSubType st = (RecordSubType)val;
  if (st == kTTLType) {
    has_ttl_= true;
  } else if (st == kTimestampType) {
    has_ts_ = true;
  } else {
    return Status::Corruption("bad sub type");
  }
  offset += sizeof(uint32_t);

  ttl_guess_.first = DecodeFixed32(ptr + offset);
  offset += sizeof(uint32_t);
  ttl_guess_.second = DecodeFixed32(ptr + offset);
  offset += sizeof(uint32_t);

  ts_guess_.first = DecodeFixed64(ptr + offset);
  offset += sizeof(uint64_t);
  ts_guess_.second = DecodeFixed64(ptr + offset);

  return s;
}

}}
