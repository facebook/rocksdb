//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <aio.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

#include "rocksdb/file_system.h"
#include "test_util/sync_point.h"

// The AsyncFileSystem and AsyncRandomAccessFile classes implement a
// Posix AIO based MultiReadAsync. The asynchronous IO completions
// are handles in a seperate thread created by the Posix library.
//
namespace ROCKSDB_NAMESPACE {

class AsyncRandomAccessFile : public FSRandomAccessFileWrapper {
 public:
  explicit AsyncRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& t,
                                 int fd)
      : FSRandomAccessFileWrapper(t.get()), file_(std::move(t)), fd_(fd) {}

  ~AsyncRandomAccessFile() { close(fd_); }

  static void PosixAIOCallback(union sigval val) {
    std::pair<MultiReadContext*, struct aiocb>* aio_ctx =
        static_cast<std::pair<MultiReadContext*, struct aiocb>*>(val.sival_ptr);
    MultiReadContext* ctx = aio_ctx->first;
    size_t expected = ctx->completed.load();
    while (!ctx->completed.compare_exchange_weak(expected, expected + 1,
                                                 std::memory_order_relaxed)) {
      expected = ctx->completed.load();
    }
    if (expected == ctx->aiocbs.size() - 1) {
      for (size_t i = 0; i < ctx->aiocbs.size(); ++i) {
        FSReadResponse& resp = ctx->resps[i];
        struct aiocb& aio_req = ctx->aiocbs[i].second;
        ssize_t ret = aio_return(&aio_req);
        if (ret < 0) {
          resp.status = IOStatus::IOError();
          continue;
        }
        resp.result = Slice(ctx->reqs[i].scratch, static_cast<size_t>(ret));
      }

      IOCallback cb = ctx->cb;
      (*cb)(ctx->resps.data(), ctx->resps.size(),
            const_cast<void*>(ctx->cb_arg1), const_cast<void*>(ctx->cb_arg2));
    }
  }

  IOStatus MultiReadAsync(const IOOptions& /*opts*/, IOCallback cb,
                          const void* cb_arg1, const void* cb_arg2,
                          const size_t num_reqs, const FSReadRequest* reqs,
                          std::unique_ptr<void, IOHandleDeleter>* handle,
                          IODebugContext* /*dbg*/) override {
    MultiReadContext* ctx =
        new MultiReadContext(num_reqs, cb, cb_arg1, cb_arg2);
    for (size_t i = 0; i < num_reqs; ++i) {
      const FSReadRequest& req = reqs[i];
      struct aiocb& aio_req = ctx->aiocbs[i].second;
      ctx->reqs[i] = reqs[i];
      memset(&aio_req, 0, sizeof(struct aiocb));
      aio_req.aio_fildes = fd_;
      aio_req.aio_offset = req.offset;
      aio_req.aio_buf = req.scratch;
      aio_req.aio_nbytes = req.len;
      aio_req.aio_reqprio = 0;
      aio_req.aio_sigevent.sigev_notify = SIGEV_THREAD;
      aio_req.aio_sigevent.sigev_value.sival_ptr = &ctx->aiocbs[i];
      aio_req.aio_sigevent.sigev_notify_function =
          AsyncRandomAccessFile::PosixAIOCallback;
      ctx->aiocbs[i].first = ctx;

      int ret = aio_read(&aio_req);
      assert(!ret);
    }

    std::unique_ptr<void, IOHandleDeleter> hndl(
        ctx, [](void* ptr) { delete static_cast<MultiReadContext*>(ptr); });
    *handle = std::move(hndl);
    return IOStatus::OK();
  }

 private:
  struct MultiReadContext {
    std::vector<std::pair<MultiReadContext*, struct aiocb>> aiocbs;
    std::vector<FSReadRequest> reqs;
    std::vector<FSReadResponse> resps;
    std::atomic<size_t> completed;
    IOCallback cb;
    const void* cb_arg1;
    const void* cb_arg2;

    MultiReadContext(size_t num_reqs, IOCallback _cb, const void* _cb_arg1,
                     const void* _cb_arg2)
        : aiocbs(num_reqs),
          reqs(num_reqs),
          resps(num_reqs),
          completed(0),
          cb(_cb),
          cb_arg1(_cb_arg1),
          cb_arg2(_cb_arg2) {}
  };

  std::unique_ptr<FSRandomAccessFile> file_;
  int fd_;
};

class AsyncFileSystem : public FileSystemWrapper {
 public:
  AsyncFileSystem(std::shared_ptr<FileSystem> t) : FileSystemWrapper(t) {}

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    IOStatus s =
        FileSystemWrapper::NewRandomAccessFile(fname, options, result, dbg);
    if (!s.ok()) {
      return s;
    }

    std::unique_ptr<FSRandomAccessFile> file = std::move(*result);
    int fd;
    int flags = O_RDONLY;
    if (options.use_direct_reads) {
      flags |= O_DIRECT;
      TEST_SYNC_POINT_CALLBACK("NewRandomAccessFile:O_DIRECT", &flags);
    }
    fd = open(fname.c_str(), flags, 0644);
    if (fd < 0) {
      s = IOStatus::IOError("Failed to open file for async read");
      file.reset();
      return s;
    }

    AsyncRandomAccessFile* new_file =
        new AsyncRandomAccessFile(std::move(file), fd);
    result->reset(new_file);
    return IOStatus::OK();
  }
};

std::unique_ptr<Env> NewAsyncEnv() {
  return NewCompositeEnv(
      std::make_shared<AsyncFileSystem>(FileSystem::Default()));
}

}  // namespace ROCKSDB_NAMESPACE
