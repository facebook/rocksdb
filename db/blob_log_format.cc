#include "db/blob_log_format.h"
#include "util/coding.h"
#include "util/crc32c.h"


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

BlobLogRecord::BlobLogRecord()
: checksum_(0), header_cksum_(0), key_size_(0), blob_size_(0),
  time_val_(0), ttl_val_(0), sn_(0), type_(0), subtype_(0),
  key_buffer_(nullptr), kbs_(0), blob_buffer_(nullptr),
  bbs_(0)
{
}

BlobLogRecord::~BlobLogRecord()
{
  if (key_buffer_) {
    delete [] key_buffer_;
    key_buffer_ = nullptr;
  }

  if (blob_buffer_) {
    delete [] blob_buffer_;
    blob_buffer_ = nullptr;
  }
}

void BlobLogRecord::resizeKeyBuffer(uint64_t kbs) {
  if (kbs > kbs_) {
    delete [] key_buffer_;
    key_buffer_ = new char[kbs];
    kbs_ = kbs;
  }
}

void BlobLogRecord::resizeBlobBuffer(uint64_t bbs) {
  if (bbs > bbs_) {
    delete [] blob_buffer_;
    blob_buffer_ = new char[bbs];
    bbs_ = bbs;
  }
}

void BlobLogRecord::clear()
{
  checksum_ = 0;
  header_cksum_ = 0;
  key_size_ = 0;
  blob_size_ = 0;
  time_val_ = 0;
  ttl_val_ = 0;
  sn_ = 0;
  type_ = subtype_ = 0;
  key_.clear();
  blob_.clear();
}

Status BlobLogRecord::DecodeHeaderFrom(Slice* input)
{
  if (input->size() < kHeaderSize) {
    return Status::Corruption("bad header");
  }

  Status s;
  uint32_t offset = 0;
  const char *ptr = input->data();
  checksum_ = DecodeFixed32(ptr + offset);
  offset += sizeof(uint32_t);
  header_cksum_ = DecodeFixed32(ptr + offset);

  offset += sizeof(uint32_t);
  key_size_ = DecodeFixed32(ptr + offset);
  offset += sizeof(uint32_t);
  blob_size_ = DecodeFixed64(ptr + offset);
  offset += sizeof(uint64_t);
  ttl_val_ = DecodeFixed32(ptr + offset);
  offset += sizeof(uint32_t);
  time_val_ = DecodeFixed64(ptr + offset);
  offset += sizeof(uint64_t);
  type_ = static_cast<char>(*(ptr + offset));
  offset++;
  subtype_ = static_cast<char>(*(ptr + offset));
  //offset++;
  {
    uint32_t hcksum = 0;
    hcksum = crc32c::Extend(hcksum, ptr + 2*sizeof(uint32_t), (size_t)(kHeaderSize - 2*sizeof(uint32_t)));
    hcksum = crc32c::Mask(hcksum);
    if (hcksum != header_cksum_ ) {
       return Status::Corruption("bad header");
    }
  }
  return s;
}

}}
