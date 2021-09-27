// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env/file_system_tracer.h"

#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

IOStatus FileSystemTracingWrapper::NewSequentialFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->NewSequentialFile(fname, file_opts, result, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          fname.substr(fname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::NewRandomAccessFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->NewRandomAccessFile(fname, file_opts, result, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          fname.substr(fname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::NewWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->NewWritableFile(fname, file_opts, result, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          fname.substr(fname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::ReopenWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->ReopenWritableFile(fname, file_opts, result, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          fname.substr(fname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& file_opts, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s =
      target()->ReuseWritableFile(fname, old_fname, file_opts, result, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          fname.substr(fname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::NewRandomRWFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->NewRandomRWFile(fname, file_opts, result, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          fname.substr(fname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::NewDirectory(
    const std::string& name, const IOOptions& io_opts,
    std::unique_ptr<FSDirectory>* result, IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->NewDirectory(name, io_opts, result, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          name.substr(name.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::GetChildren(const std::string& dir,
                                               const IOOptions& io_opts,
                                               std::vector<std::string>* r,
                                               IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->GetChildren(dir, io_opts, r, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          dir.substr(dir.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::DeleteFile(const std::string& fname,
                                              const IOOptions& options,
                                              IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->DeleteFile(fname, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          fname.substr(fname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::CreateDir(const std::string& dirname,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->CreateDir(dirname, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          dirname.substr(dirname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::CreateDirIfMissing(
    const std::string& dirname, const IOOptions& options, IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->CreateDirIfMissing(dirname, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          dirname.substr(dirname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::DeleteDir(const std::string& dirname,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->DeleteDir(dirname, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          dirname.substr(dirname.find_last_of("/\\") + 1));
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::GetFileSize(const std::string& fname,
                                               const IOOptions& options,
                                               uint64_t* file_size,
                                               IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->GetFileSize(fname, options, file_size, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOFileSize);
  IOTraceRecord io_record(
      clock_->NowNanos(), TraceType::kIOTracer, io_op_data, __func__, elapsed,
      s.ToString(), fname.substr(fname.find_last_of("/\\") + 1), *file_size);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FileSystemTracingWrapper::Truncate(const std::string& fname,
                                            size_t size,
                                            const IOOptions& options,
                                            IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Truncate(fname, size, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOFileSize);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(),
                          fname.substr(fname.find_last_of("/\\") + 1), size);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::Read(size_t n,
                                              const IOOptions& options,
                                              Slice* result, char* scratch,
                                              IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Read(n, options, result, scratch, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_,
                          result->size(), 0 /*Offset*/);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::InvalidateCache(size_t offset,
                                                         size_t length) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->InvalidateCache(offset, length);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_, length,
                          offset);
  io_tracer_->WriteIOOp(io_record, nullptr /*dbg*/);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::PositionedRead(
    uint64_t offset, size_t n, const IOOptions& options, Slice* result,
    char* scratch, IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s =
      target()->PositionedRead(offset, n, options, result, scratch, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_,
                          result->size(), offset);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::Read(uint64_t offset, size_t n,
                                                const IOOptions& options,
                                                Slice* result, char* scratch,
                                                IODebugContext* dbg) const {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Read(offset, n, options, result, scratch, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_, n,
                          offset);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::MultiRead(FSReadRequest* reqs,
                                                     size_t num_reqs,
                                                     const IOOptions& options,
                                                     IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->MultiRead(reqs, num_reqs, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t latency = elapsed;
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  for (size_t i = 0; i < num_reqs; i++) {
    IOTraceRecord io_record(
        clock_->NowNanos(), TraceType::kIOTracer, io_op_data, __func__, latency,
        reqs[i].status.ToString(), file_name_, reqs[i].len, reqs[i].offset);
    io_tracer_->WriteIOOp(io_record, dbg);
  }
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::Prefetch(uint64_t offset, size_t n,
                                                    const IOOptions& options,
                                                    IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Prefetch(offset, n, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_, n,
                          offset);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::InvalidateCache(size_t offset,
                                                           size_t length) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->InvalidateCache(offset, length);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_, length,
                          static_cast<uint64_t>(offset));
  io_tracer_->WriteIOOp(io_record, nullptr /*dbg*/);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Append(const Slice& data,
                                              const IOOptions& options,
                                              IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Append(data, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_,
                          data.size(), 0 /*Offset*/);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSWritableFileTracingWrapper::PositionedAppend(
    const Slice& data, uint64_t offset, const IOOptions& options,
    IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->PositionedAppend(data, offset, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_,
                          data.size(), offset);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Truncate(uint64_t size,
                                                const IOOptions& options,
                                                IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Truncate(size, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_, size,
                          0 /*Offset*/);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Close(const IOOptions& options,
                                             IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Close(options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          file_name_);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

uint64_t FSWritableFileTracingWrapper::GetFileSize(const IOOptions& options,
                                                   IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  uint64_t file_size = target()->GetFileSize(options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOFileSize);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, "OK", file_name_, file_size);
  io_tracer_->WriteIOOp(io_record, dbg);
  return file_size;
}

IOStatus FSWritableFileTracingWrapper::InvalidateCache(size_t offset,
                                                       size_t length) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->InvalidateCache(offset, length);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_, length,
                          static_cast<uint64_t>(offset));
  io_tracer_->WriteIOOp(io_record, nullptr /*dbg*/);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Write(uint64_t offset, const Slice& data,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Write(offset, data, options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_,
                          data.size(), offset);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Read(uint64_t offset, size_t n,
                                            const IOOptions& options,
                                            Slice* result, char* scratch,
                                            IODebugContext* dbg) const {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Read(offset, n, options, result, scratch, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  uint64_t io_op_data = 0;
  io_op_data |= (1 << IOTraceOp::kIOLen);
  io_op_data |= (1 << IOTraceOp::kIOOffset);
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer, io_op_data,
                          __func__, elapsed, s.ToString(), file_name_, n,
                          offset);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Flush(const IOOptions& options,
                                             IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Flush(options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          file_name_);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Close(const IOOptions& options,
                                             IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Close(options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          file_name_);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Sync(const IOOptions& options,
                                            IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Sync(options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          file_name_);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Fsync(const IOOptions& options,
                                             IODebugContext* dbg) {
  StopWatchNano timer(clock_);
  timer.Start();
  IOStatus s = target()->Fsync(options, dbg);
  uint64_t elapsed = timer.ElapsedNanos();
  IOTraceRecord io_record(clock_->NowNanos(), TraceType::kIOTracer,
                          0 /*io_op_data*/, __func__, elapsed, s.ToString(),
                          file_name_);
  io_tracer_->WriteIOOp(io_record, dbg);
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
