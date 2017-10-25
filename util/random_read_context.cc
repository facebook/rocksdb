//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "util/random_read_context.h"

#include "monitoring/iostats_context_imp.h"
#include "util/file_reader_writer.h"

#include <algorithm>

namespace rocksdb {
namespace async {

/////////////////////////////////////////////////////////////////////////
/// RandomFileReadContext
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
                        std::min(result_->size() - size_t(offset_advance_), n_));
        }
        *result_ = Slice(result_buffer_, r);
      } else {
        // result does not point to our intermediate buffer
        // but possibly to a readahead buffer
        // We want to keep that optimization but need to adjust
        // the start and length
        if (offset_advance_ < result_->size()) {
          r = std::min(result_->size() - size_t(offset_advance_), n_);
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
} // namespace async
} // namespace rocksdb
