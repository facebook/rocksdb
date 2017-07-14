//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "util/random_read_context.h"

#include "monitoring/iostats_context_imp.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace async {

/////////////////////////////////////////////////////////////////////////
/// RandomFileReadContext

void RandomFileReadContext::PrepareRead(uint64_t offset, size_t n,
  Slice* result, char * buffer) {

  result_ = result;
  result_buffer_ = buffer;
  n_ = n;

  IOSTATS_METER_START(read_nanos);

  if (direct_io_) {
    auto alignment = buf_.Alignment();
    read_offset_ = TruncateToPageBoundary(alignment, offset);
    offset_advance_ = offset - read_offset_;
    read_size_ = Roundup(offset + n, alignment) - read_offset_;

    buf_.AllocateNewBuffer(read_size_);
  } else {
    read_offset_ = offset;
    read_size_ = n;
  }
}

Status RandomFileReadContext::RandomRead() {

  Status s;

  if (direct_io_) {
    assert(buf_.Capacity() >= read_size_);
    s = ra_file_->Read(read_offset_, read_size_, result_, buf_.BufferStart());
  } else {
    s = ra_file_->Read(read_offset_, read_size_, result_, result_buffer_);
  }

  return s;
}

Status RandomFileReadContext::RequestRandomRead(const RandomAccessCallback & iocb) {

  Status s;

  IOSTATS_METER_MEASURE(read_nanos);

  if (direct_io_) {
    assert(buf_.Capacity() >= read_size_);
    s = ra_file_->Read(iocb, read_offset_, read_size_, result_, buf_.BufferStart());
  } else {
    s = ra_file_->Read(iocb, read_offset_, read_size_, result_, result_buffer_);
  }

  return s;
}

void RandomFileReadContext::OnRandomReadComplete(const Status& status,
  const Slice& slice) {

  // This may or may not point to our buffer
  *result_ = slice;

  // Means there was a read with direct IO
  // which requires additional handling
  // namely, we need to copy aligned buffer content to
  // the actual destination
  if (direct_io_) {
    // result_ may now have more bytes than requested
    // or less data than requested
    size_t r = 0;
    if (status.ok()) {
      // Data was placed into our aligned buffer
      // copy it to the user supplied buffer
      if (result_->data() == buf_.BufferStart()) {
        if (offset_advance_ < result_->size()) {
          buf_.Size(result_->size());
          r = buf_.Read(result_buffer_, offset_advance_,
            std::min(result_->size() - offset_advance_, n_));
        }
        *result_ = Slice(result_buffer_, r);
      } else {
        // result does not point to our intermediate buffer
        // but possibly to a readahead buffer
        // We want to keep that optimization but need to adjust
        // the start and length
        if (offset_advance_ < result_->size()) {
          r = std::min(result_->size() - offset_advance_, n_);
          auto start = result_->data() + offset_advance_;
          *result_ = Slice(start, r);
        } else {
          *result_ = Slice(result_buffer_, r);
        }
      }
    } else {
      // Failure to read
      *result_ = Slice(result_buffer_, r);
    }
  }

  IOSTATS_METER_STOP(read_nanos);
  sw_.elapse_and_disarm();

  if (stats_ != nullptr && hist_ != nullptr) {
    hist_->Add(elapsed_);
  }

  // On direct io we read more, should we count the truth?
  IOSTATS_ADD_IF_POSITIVE(bytes_read, result_->size());
}

RandomReadContext::RandomReadContext(RandomAccessFileReader* file,
  uint64_t offset, size_t n,
  Slice* result, char* buf) {

  auto data = file->GetReadContextData();

  new (&ra_context_) RandomFileReadContext(file->file(), data.env_, data.stats_,
    data.file_read_hist_, data.hist_type_, file->use_direct_io(),
    file->file()->GetRequiredBufferAlignment());

  GetCtxRef().PrepareRead(offset, n, result, buf);
}


} // namespace async
} // namespace rocksdb
