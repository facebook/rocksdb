//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include "rocksdb/async/callables.h"
#include "rocksdb/status.h"

#include "table/format.h"
#include "util/aligned_buffer.h"
#include "util/iostats_context_imp.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"

namespace rocksdb {

class Env;
class HistogramImpl;
class RandomAccessFile;
class RandomAccessFileReader;
class Statistics;

namespace async {

using
RandomAccessCallback = async::Callable<void, Status&&, const Slice&>;

class RandomFileReadContext {

  // Callback is empty when request is sync
  RandomAccessFile* ra_file_;
  Statistics*       stats_;
  HistogramImpl*    hist_;
  StopWatch         sw_;
  uint64_t          elapsed_;
  IOSTATS_TIMER_DECL(read_nanos);
  bool              direct_io_;

  Slice*            result_;
  char*             result_buffer_;
  uint64_t          read_offset_;    // disk offset to read
  uint64_t          offset_advance_; // offset in the direct_io buffer if used
  size_t            read_size_;  // how much to read
  size_t            n_;          // actual requested read size
                              // Needed as intermediate for direct reads only
  AlignedBuffer     buf_;

public:

  // Constructor argument values come from the reader class
  RandomFileReadContext(RandomAccessFile* file, Env* env, Statistics* stats,
    HistogramImpl* hist, uint32_t hist_type,
    bool direct_io,
    size_t alignment) :
    ra_file_(file),
    stats_(stats),
    hist_(hist),
    sw_(env, stats, hist_type, (stats_ != nullptr) ? &elapsed_ : nullptr),
    elapsed_(0),
    IOSTATS_TIMER_INIT(read_nanos),
    direct_io_(direct_io),
    result_(nullptr),
    result_buffer_(nullptr),
    read_offset_(0),
    offset_advance_(0),
    read_size_(0),
    n_(0) {
    buf_.Alignment(alignment);
  }

  const Slice& GetResult() const {
    return *result_;
  }

  size_t GetRequestedSize() const {
    return n_;
  }

  void PrepareRead(uint64_t offset, size_t n, Slice* result, char* buffer);

  // Sync read
  Status RandomRead();

  Status RequestRandomRead(const RandomAccessCallback& iocb);

  ~RandomFileReadContext() {}

  // Must be called by the supplied iocb no matter what
  void  OnRandomReadComplete(const Status& status, const Slice& slice);
};

// This is a base class for all random-reads
// async reads
// At present the context does not own
// the file since it is expected to be cached
// with a newly created TableReader
class RandomReadContext {
public:

  RandomReadContext(const RandomReadContext&) = delete;
  RandomReadContext& operator-(const RandomReadContext&) = delete;

private:

  std::unique_ptr<RandomFileReadContext> ra_context_;

protected:

  RandomReadContext(RandomAccessFileReader* file, 
                    uint64_t offset, size_t n,
                    Slice* result, char* buf);

  const Slice& GetResult() const {
    return ra_context_->GetResult();
  }

  size_t GetRequestedSize() const {
    return ra_context_->GetRequestedSize();
  }


  Status RequestRead(const RandomAccessCallback& iocb) {
    return ra_context_->RequestRandomRead(iocb);
  }

  Status Read() {
    return ra_context_->RandomRead();
  }

  void  OnRandomReadComplete(const Status& status, const Slice& slice) {
    ra_context_->OnRandomReadComplete(status, slice);
  }

  // Non-virtual because protected
  // we want to be lightweight
  ~RandomReadContext()  {
  }
};

class ReadFooterContext : protected RandomReadContext {
public:
  using
  FooterReadCallback = async::Callable<void, Status&&>;

protected:

  Slice& GetFooterInput() {
    return footer_input_;
  }

  Status OnReadFooterComplete(const Status& s, const Slice& slice);

  ReadFooterContext(const FooterReadCallback& footer_cb,
    RandomAccessFileReader* file, uint64_t offset,
    Footer* footer, uint64_t enforce_table_magic_number) :
    RandomReadContext(file, offset, Footer::kMaxEncodedLength,
                      &footer_input_, footer_space_),
    footer_cb_(footer_cb),
    footer_(footer),
    enforce_table_magic_number_(enforce_table_magic_number) {
  }


public:

  ReadFooterContext(const ReadFooterContext&) = delete;
  ReadFooterContext& operator=(const ReadFooterContext&) = delete;

  static
  Status RequestFooterRead(const FooterReadCallback& footer_cb, 
                           RandomAccessFileReader* file, uint64_t file_size,
                           Footer* footer, uint64_t enforce_table_magic_number) {

      if (file_size < Footer::kMinEncodedLength) {
        return Status::Corruption("file is too short to be an sstable");
      }

    size_t read_offset =
      (file_size > Footer::kMaxEncodedLength) ? 
      static_cast<size_t>(file_size - Footer::kMaxEncodedLength)
      : 0;

      std::unique_ptr<ReadFooterContext> context(new ReadFooterContext(footer_cb,
                                                 file,
                                                 read_offset, footer,
                                                 enforce_table_magic_number));

      auto iocb = context->GetIOCallback();
      Status s = context->RequestRead(iocb);

      if (s.IsIOPending()) {
        context.release();
        return s;
      }

      return context->OnReadFooterComplete(s, context->GetFooterInput());
  }

  static
  Status ReadFooter(RandomAccessFileReader * file, uint64_t file_size,
    Footer * footer, uint64_t enforce_table_magic_number) {

      if (file_size < Footer::kMinEncodedLength) {
        return Status::Corruption("file is too short to be an sstable");
      }

    size_t read_offset =
      (file_size > Footer::kMaxEncodedLength)
      ? static_cast<size_t>(file_size - Footer::kMaxEncodedLength)
      : 0;

    ReadFooterContext context(FooterReadCallback(), file, read_offset, footer,
                              enforce_table_magic_number);
    Status s = context.Read();
    return context.OnReadFooterComplete(s, context.GetFooterInput());
  }


private:

  RandomAccessCallback GetIOCallback() {
    async::CallableFactory<ReadFooterContext, void, Status&&, const Slice&> fac(this);
    return fac.GetCallable<&ReadFooterContext::OnIOCompletion>();
  }

  void OnIOCompletion(Status&&, const Slice&);

  FooterReadCallback footer_cb_;
  Footer*            footer_;     // Pointer to class to decode to
  Slice              footer_input_;
  uint64_t           enforce_table_magic_number_;
  char               footer_space_[Footer::kMaxEncodedLength];
};

// This class specialized on read an entire block
class ReadBlockContext : protected RandomReadContext {
public:
  using
  ReadBlockCallback = async::Callable<void, Status&&, const Slice&>;

protected:

  ReadBlockContext(const ReadBlockCallback& client_cb,
    RandomAccessFileReader* file, ChecksumType checksum_type,
    bool verify_check_sum, uint64_t offset, size_t n, Slice* result, char* buf) :
    RandomReadContext(file, offset, n, result, buf),
    PERF_TIMER_INIT(block_read_time),
    checksum_type_(checksum_type),
    verify_checksums_(verify_check_sum) {
    PERF_TIMER_START(block_read_time);
  }

  // Performs after read tasks in both sync and async cases
  Status OnReadBlockComplete(const Status& s, const Slice&);

public:

  ReadBlockContext(const ReadBlockContext&) = delete;
  ReadBlockContext& operator=(const ReadBlockContext&) = delete;

  ~ReadBlockContext() {
  }

  // Async block read request
  static
  Status RequestBlockRead(const ReadBlockCallback& cb,
                          RandomAccessFileReader* file, const Footer& footer,
                          const ReadOptions& options, const BlockHandle& handle,
                          Slice* contents, /* result of reading */ char* buf);

  // Sync version of the API
  static
  Status ReadBlock(RandomAccessFileReader* file, const Footer& footer,
                  const ReadOptions& options, const BlockHandle& handle,
                  Slice* contents, /* result of reading */ char* buf);

private:

  // This is invoked only if this class represents
  // the concrete class and is to be destroyed after the callback
  // completes. Derived classes supply their own callback where they
  // dictate their own policies but can reuse the logic of this class
  void OnIoCompletion(Status&&, const Slice&);

  RandomAccessCallback GetIOCallback() {
    async::CallableFactory<ReadBlockContext, void, Status&&, const Slice&>
     factory(this);
    return factory.GetCallable<&ReadBlockContext::OnIoCompletion>();
  }

  ReadBlockCallback  client_cb_;
  PERF_TIMER_DECL(block_read_time);
  ChecksumType       checksum_type_;
  bool               verify_checksums_;
};

} // namespace async
} // namespace rocksdb
