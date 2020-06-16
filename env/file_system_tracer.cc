// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "env/file_system_tracer.h"

namespace ROCKSDB_NAMESPACE {

namespace {
using namespace std::chrono;
uint64_t Timestamp() {
  return duration_cast<microseconds>(system_clock::now().time_since_epoch())
      .count();
}
}  // namespace

IOStatus FileSystemTracingWrapper::NewWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  IOStatus s = target()->NewWritableFile(fname, file_opts, result, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileName,
                          "NewWritableFile", s.ToString(), fname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::NewDirectory(
    const std::string& name, const IOOptions& io_opts,
    std::unique_ptr<FSDirectory>* result, IODebugContext* dbg) {
  IOStatus s = target()->NewDirectory(name, io_opts, result, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileName, "NewDirectory",
                          s.ToString(), name);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::GetChildren(const std::string& dir,
                                               const IOOptions& io_opts,
                                               std::vector<std::string>* r,
                                               IODebugContext* dbg) {
  IOStatus s = target()->GetChildren(dir, io_opts, r, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileName, "GetChildren",
                          s.ToString(), dir);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::DeleteFile(const std::string& fname,
                                              const IOOptions& options,
                                              IODebugContext* dbg) {
  IOStatus s = target()->DeleteFile(fname, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileName, "DeleteFile",
                          s.ToString(), fname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::CreateDir(const std::string& dirname,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  IOStatus s = target()->CreateDir(dirname, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileName, "CreateDir",
                          s.ToString(), dirname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::CreateDirIfMissing(
    const std::string& dirname, const IOOptions& options, IODebugContext* dbg) {
  IOStatus s = target()->CreateDirIfMissing(dirname, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileName,
                          "CreateDirIfMissing", s.ToString(), dirname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::DeleteDir(const std::string& dirname,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  IOStatus s = target()->DeleteDir(dirname, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileName, "DeleteDir",
                          s.ToString(), dirname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::GetFileSize(const std::string& fname,
                                               const IOOptions& options,
                                               uint64_t* file_size,
                                               IODebugContext* dbg) {
  IOStatus s = target()->GetFileSize(fname, options, file_size, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileNameAndFileSize,
                          "GetFileSize", s.ToString(), fname, *file_size);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::Read(size_t n,
                                              const IOOptions& options,
                                              Slice* result, char* scratch,
                                              IODebugContext* dbg) {
  IOStatus s = target()->Read(n, options, result, scratch, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOLen, "Read", s.ToString(),
                          0 /* offset */, result->size());
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::InvalidateCache(size_t offset,
                                                         size_t length) {
  IOStatus s = target()->InvalidateCache(offset, length);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffsetAndLen,
                          "InvalidateCache", s.ToString(), offset, length);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::PositionedRead(
    uint64_t offset, size_t n, const IOOptions& options, Slice* result,
    char* scratch, IODebugContext* dbg) {
  IOStatus s =
      target()->PositionedRead(offset, n, options, result, scratch, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffsetAndLen,
                          "PositionedRead", s.ToString(), offset,
                          result->size());
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::Read(uint64_t offset, size_t n,
                                                const IOOptions& options,
                                                Slice* result, char* scratch,
                                                IODebugContext* dbg) const {
  IOStatus s = target()->Read(offset, n, options, result, scratch, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffsetAndLen, "Read",
                          s.ToString(), offset, n);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::MultiRead(FSReadRequest* reqs,
                                                     size_t num_reqs,
                                                     const IOOptions& options,
                                                     IODebugContext* dbg) {
  IOStatus s = target()->MultiRead(reqs, num_reqs, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOGeneral, "MultiRead",
                          s.ToString());
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::Prefetch(uint64_t offset, size_t n,
                                                    const IOOptions& options,
                                                    IODebugContext* dbg) {
  IOStatus s = target()->Prefetch(offset, n, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffsetAndLen, "Prefetch",
                          s.ToString(), offset, n);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::InvalidateCache(size_t offset,
                                                           size_t length) {
  IOStatus s = target()->InvalidateCache(offset, length);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffsetAndLen,
                          "InvalidateCache", s.ToString(),
                          static_cast<uint64_t>(offset), length);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Append(const Slice& data,
                                              const IOOptions& options,
                                              IODebugContext* dbg) {
  IOStatus s = target()->Append(data, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOGeneral, "Append",
                          s.ToString());
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSWritableFileTracingWrapper::PositionedAppend(
    const Slice& data, uint64_t offset, const IOOptions& options,
    IODebugContext* dbg) {
  IOStatus s = target()->PositionedAppend(data, offset, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffset, "PositionedAppend",
                          s.ToString(), offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Truncate(uint64_t size,
                                                const IOOptions& options,
                                                IODebugContext* dbg) {
  IOStatus s = target()->Truncate(size, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOLen, "Truncate",
                          s.ToString(), 0 /*offset*/, size);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Close(const IOOptions& options,
                                             IODebugContext* dbg) {
  IOStatus s = target()->Close(options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOGeneral, "Close",
                          s.ToString());
  io_tracer_->WriteIOOp(io_record);
  return s;
}

uint64_t FSWritableFileTracingWrapper::GetFileSize(const IOOptions& options,
                                                   IODebugContext* dbg) {
  uint64_t file_size = target()->GetFileSize(options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOFileSize, "GetFileSize",
                          "" /* file_name */, file_size);
  io_tracer_->WriteIOOp(io_record);
  return file_size;
}

IOStatus FSWritableFileTracingWrapper::InvalidateCache(size_t offset,
                                                       size_t length) {
  IOStatus s = target()->InvalidateCache(offset, length);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffsetAndLen,
                          "InvalidateCache", s.ToString(),
                          static_cast<uint64_t>(offset), length);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Write(uint64_t offset, const Slice& data,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  IOStatus s = target()->Write(offset, data, options, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffset, "Write",
                          s.ToString(), offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Read(uint64_t offset, size_t n,
                                            const IOOptions& options,
                                            Slice* result, char* scratch,
                                            IODebugContext* dbg) const {
  IOStatus s = target()->Read(offset, n, options, result, scratch, dbg);
  IOTraceRecord io_record(Timestamp(), TraceType::kIOOffsetAndLen, "Read",
                          s.ToString(), offset, n);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
