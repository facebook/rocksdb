//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "util/random_read_context.h"

#include "monitoring/iostats_context_imp.h"
#include "table/block_based_table_reader.h"
#include "table/persistent_cache_helper.h"

#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/stop_watch.h"
#include "util/xxhash.h"

namespace rocksdb {
namespace async {

Status ReadFooterContext::OnReadFooterComplete(const Status& status, const Slice& slice) {

  OnRandomReadComplete(status, slice);

  if (!status.ok()) return status;

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input_.size() < Footer::kMinEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  Status s = footer_->DecodeFrom(&footer_input_);
  if (!s.ok()) {
    return s;
  }

  if (enforce_table_magic_number_ != 0 &&
    enforce_table_magic_number_ != footer_->table_magic_number()) {
    return Status::Corruption("Bad table magic number");
  }
  return Status::OK();
}

Status ReadFooterContext::OnIOCompletion(const Status& s, const Slice& slice) {
  std::unique_ptr<ReadFooterContext> self(this);
  Status status = OnReadFooterComplete(s, slice);
  // In these classes OnIOComplletion is only invoked when async
  // simply enforce this
  status.async(true);
  footer_cb_.Invoke(status);
  return status;
}

/////////////////////////////////////////////////////////////////////////////////////////
// ReadBlockContext
Status ReadBlockContext::RequestBlockRead(const ReadBlockCallback& cb,
  RandomAccessFileReader* file, const Footer& footer,
  const ReadOptions& options, const BlockHandle& handle,
  Slice* contents, /* result of reading */ char* buf) {

  std::unique_ptr<ReadBlockContext> ctx(new ReadBlockContext(cb, file,
    footer.checksum(), options.verify_checksums,
    handle, contents, buf));

  auto iocb = ctx->GetIOCallback();
  Status s = ctx->RequestRead(iocb);

  if (s.IsIOPending()) {
    ctx.release();
    return s;
  }

  ctx->OnReadBlockComplete(s, *contents);

  return s;
}

Status ReadBlockContext::ReadBlock(RandomAccessFileReader * file,
  const Footer& footer, const ReadOptions & options,
  const BlockHandle & handle, Slice * contents, char * buf) {

  ReadBlockContext ctx(ReadBlockCallback(), file, footer.checksum(),
    options.verify_checksums, handle,
    contents, buf);

  Status s = ctx.Read();

  s = ctx.OnReadBlockComplete(s, *contents);

  return s;
}

Status ReadBlockContext::OnReadBlockComplete(const Status& status, const Slice& raw_slice) {

  OnRandomReadComplete(status, raw_slice);

  PERF_METER_STOP(block_read_time);
  PERF_COUNTER_ADD(block_read_count, 1);
  PERF_COUNTER_ADD(block_read_byte, raw_slice.size());

  if (!status.ok()) {
    return status;
  }

  Status s(status);

  const Slice& slice = GetResult();
  auto n = GetRequestedSize();

  if (slice.size() != n) {
    return Status::Corruption("truncated block read");
  }

  // Bring back the original value
  n -= kBlockTrailerSize;

  // Check the crc of the type and the block contents
  const char* data = slice.data();  // Pointer to where Read put the data
  if (verify_checksums_) {
    PERF_TIMER_GUARD(block_checksum_time);
    uint32_t value = DecodeFixed32(data + n + 1);
    uint32_t actual = 0;
    switch (checksum_type_) {
    case kCRC32c:
      value = crc32c::Unmask(value);
      actual = crc32c::Value(data, n + 1);
      break;
    case kxxHash:
      actual = XXH32(data, static_cast<int>(n) + 1, 0);
      break;
    default:
      s = Status::Corruption("unknown checksum type");
    }
    if (s.ok() && actual != value) {
      s = Status::Corruption("block checksum mismatch");
    }
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

Status ReadBlockContext::OnIoCompletion(const Status& status, const Slice& raw_slice) {

  std::unique_ptr<ReadBlockContext> self(this);
  Status s = OnReadBlockComplete(status, raw_slice);
  // In these classes OnIOComplletion is only invoked when async
  // simply enforce this
  s.async(true);
  client_cb_.Invoke(s, GetResult());
  return s;
}

/////////////////////////////////////////////////////////////////////////////////
/// ReadBlockContentsContext
Status ReadBlockContentsContext::CheckPersistentCache(bool&
    need_decompression) {

  size_t n = GetN();
  Status status;

  need_decompression = true;

  if (cache_options_->persistent_cache &&
    !cache_options_->persistent_cache->IsCompressed()) {
    status = PersistentCacheHelper::LookupUncompressedPage(*cache_options_,
      handle_, contents_);
    if (status.ok()) {
      // uncompressed page is found for the block handle
      need_decompression = false;
      return status;
    } else {
      // uncompressed page is not found
      if (ioptions_->info_log && !status.IsNotFound()) {
        assert(!status.ok());
        ROCKS_LOG_INFO(ioptions_->info_log,
          "Error reading from persistent cache. %s",
          status.ToString().c_str());
      }
    }
  }

  if (cache_options_->persistent_cache &&
    cache_options_->persistent_cache->IsCompressed()) {
    // lookup uncompressed cache mode p-cache
    status = PersistentCacheHelper::LookupRawPage(
      *cache_options_, handle_, &heap_buf_, n + kBlockTrailerSize);
  } else {
    status = Status::NotFound();
  }

  if (status.ok()) {
    // cache hit
    result_ = Slice(heap_buf_.get(), n + kBlockTrailerSize);
  } else if (ioptions_->info_log && !status.IsNotFound()) {
    assert(!status.ok());
    ROCKS_LOG_INFO(ioptions_->info_log,
      "Error reading from persistent cache. %s",
      status.ToString().c_str());
  }

  return status;
}

Status ReadBlockContentsContext::RequestContentstRead(const
  ReadBlockContCallback& client_cb_,
  RandomAccessFileReader* file,
  const Footer& footer,
  const ReadOptions& read_options,
  const BlockHandle & handle,
  BlockContents* contents,
  const ImmutableCFOptions& ioptions,
  bool decompression_requested,
  const Slice& compression_dict,
  const PersistentCacheOptions& cache_options) {


  std::unique_ptr<ReadBlockContentsContext> context(new ReadBlockContentsContext(
    client_cb_, footer,
    read_options, handle, contents, ioptions, decompression_requested,
    compression_dict, cache_options));

  bool need_decompression = false;
  Status status = context->CheckPersistentCache(need_decompression);

  if (status.ok()) {
    if (need_decompression) {
      return context->OnReadBlockContentsComplete(status, context->result_);
    }
    return status;
  }

  // Proceed with reading the block from disk
  context->ConstructReadBlockContext(file);

  auto iocb = context->GetIOCallback();
  status = context->RequestRead(iocb);

  if (status.IsIOPending()) {
    context.release();
    return status;
  }

  return context->OnReadBlockContentsComplete(status, context->result_);
}

Status ReadBlockContentsContext::ReadContents(RandomAccessFileReader* file,
  const Footer& footer,
  const ReadOptions& read_options,
  const BlockHandle& handle,
  BlockContents* contents,
  const ImmutableCFOptions& ioptions,
  bool decompression_requested,
  const Slice & compression_dict,
  const PersistentCacheOptions & cache_options) {

  ReadBlockContentsContext context(ReadBlockContCallback(), footer, read_options,
    handle, contents, ioptions, decompression_requested,
    compression_dict, cache_options);

  bool need_decompression = false;
  Status status = context.CheckPersistentCache(need_decompression);

  if (status.ok()) {
    if (need_decompression) {
      return context.OnReadBlockContentsComplete(status, context.result_);
    }
    return status;
  }

  // Proceed with reading the block from disk
  context.ConstructReadBlockContext(file);

  status = context.Read();

  return context.OnReadBlockContentsComplete(status, context.result_);
}

Status ReadBlockContentsContext::OnReadBlockContentsComplete(const Status& s,
  const Slice& raw_slice) {

  Status status(s);

  if (is_read_block_) {
    status = GetReadBlock()->OnReadBlockComplete(s, raw_slice);
  }

  if (!status.ok()) {
    return status;
  }

  // This is a size w/o a trailer
  // but the result has the total size
  // raw_slice may not point to our buffer in case of the following:
  // - direct_io read is performed to an intermediate buffer
  // - may point to a memory mapped file in memory
  // - may point to a read_ahead buffer
  // The result is properly set after the above OnReadBlockComplete
  // is handled or if the data is obtained from the persistent cache
  // then raw_slice is same as a result
  const Slice& slice = result_;
  size_t n = GetN();

  // We only allocate heap_buf_ if necessary
  char* used_buf = (heap_buf_) ? heap_buf_.get() : inclass_buf_;
  assert(used_buf != nullptr);

  if (read_options_->fill_cache &&
    cache_options_->persistent_cache &&
    cache_options_->persistent_cache->IsCompressed()) {
    // insert to raw cache
    PersistentCacheHelper::InsertRawPage(*cache_options_, handle_, slice.data(),
      slice.size());
  }

  PERF_TIMER_GUARD(block_decompress_time);

  rocksdb::CompressionType compression_type =
    static_cast<rocksdb::CompressionType>(slice.data()[n]);

  if (decompression_requested_ && compression_type != kNoCompression) {
    // compressed page, uncompress, update cache
    status = UncompressBlockContents(slice.data(), n, contents_,
      footer_->version(), compression_dict_,
      *ioptions_);
  } else if (slice.data() != used_buf) {
    // the slice content is not the buffer provided
    *contents_ = BlockContents(Slice(slice.data(), n), false, compression_type);
  } else {
    // page is uncompressed, the buffer either stack or heap provided
    if (used_buf == inclass_buf_) {
      heap_buf_.reset(new char[n]);
      memcpy(heap_buf_.get(), inclass_buf_, n);
    }
    *contents_ = BlockContents(std::move(heap_buf_), n, true, compression_type);
  }

  if (status.ok() && read_options_->fill_cache &&
    cache_options_->persistent_cache &&
    !cache_options_->persistent_cache->IsCompressed()) {
    // insert to uncompressed cache
    PersistentCacheHelper::InsertUncompressedPage(*cache_options_, handle_,
      *contents_);
  }

  return status;
}

Status ReadBlockContentsContext::OnIoCompletion(const Status& status,
  const Slice& slice) {

  std::unique_ptr<ReadBlockContentsContext> self(this);
  Status s = OnReadBlockContentsComplete(status, slice);
  // In these classes OnIOComplletion is only invoked when async
  // simply enforce this
  s.async(true);
  client_cb_.Invoke(s);
  return s;
}


}
}