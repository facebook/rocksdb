// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <rocksdb/async/callables.h>
#include <rocksdb/iterator.h>

#include "async/block_based_table_request.h"
#include "ms_internal/ms_threadpool.h"
#include "ms_internal/ms_taskpoolhandle.h"



#include "port/stack_trace.h"
#include "port/win/io_win.h"
#include "port/win/iocompletion.h"

#include "table/block_based_table_builder.h"
#include "table/block_based_table_factory.h"
#include "table/block_based_table_reader.h"
#include "table/sst_file_writer_collectors.h"

#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

#include <algorithm>
#include <fstream>

namespace rocksdb {

// This interface extends the standard Iterator
// and introduces async versions of the API
// which may incur IO and therefore complete async
// on another thread
class AsyncIterator : public Iterator {
public:
  AsyncIterator() {}

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  // bool - true is returned if the call has completed synchronously
  // false - if an async operation was submitted. In this case,
  // the current code path of iteration must exit the current
  // thread and preserve its state on the heap. The execution will
  // continue when IO completes and the callable is invoked
  virtual bool SeekToFirst(const async::Callable<void,const Slice&>&) = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual bool SeekToLast(const async::Callable<void,const Slice&>&) = 0;

  // Position at the first key in the source that at or past target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual bool Seek(const Slice& target,
      const async::Callable<void,const Slice&>&) = 0;

  // Position at the last key in the source that at or before target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or before target.
  virtual bool SeekForPrev(const Slice& target, 
       const async::Callable<void,const Slice&>&) {}

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual bool Next(const async::Callable<void,const Slice&>&) = 0;

};


class AsyncTest : public testing::Test {};

static
void* test_ctx = reinterpret_cast<void*>(0x10000);

void CallbackTest(void* ctx) {
  ASSERT_EQ(test_ctx, ctx);
}

void CallbackTestWithArg(void* ctx, int) {
  ASSERT_EQ(test_ctx, ctx);
}

int CallbackTestWithRet(void* ctx) {
  return 10;
}

int CallbackWithRValue(void*, std::string&& s) {
  std::string a(std::move(s));
  return 0;
}

struct MethodCallbackTest {

  bool invoked_;

  MethodCallbackTest() : 
    invoked_(false) {}

  void TestMethod(int) {
    invoked_ = true;
  }

  int TestMethodWithRet(int a) {
    invoked_ = true;
    return a;
  }

  int TestMethodWithRValue(std::string&&) {
    invoked_ = true;
    return 10;
  }

};

// Compilation test only
void Indirect(async::Callable<int, int>& cb, int arg) {
  cb.Invoke(arg);
}

TEST_F(AsyncTest, SimleTest) {
  {
    // Simple callable direct use
    // nothing to return and no arguments
    async::Callable<void> simple(test_ctx, &CallbackTest);
    simple.Invoke();
  }

  {
    // Simple callable direct use
    // nothing to return one int argument by value
    async::Callable<void,int> simple(test_ctx, &CallbackTestWithArg);
    simple.Invoke(10);
  }

  {
    // Simple callable direct use
    // no arguments and integer return type
    async::Callable<int> simple(test_ctx, &CallbackTestWithRet);
    int result = simple.Invoke();
  }

  {
    // Simple callable direct use
    // test rvalue parameter invocation
    async::Callable<int,std::string&&> simple(test_ctx, &CallbackWithRValue);
    std::string s("10");
    int result = simple.Invoke(std::move(s));
  }

  {
    MethodCallbackTest testObj;
    async::CallableFactory<MethodCallbackTest,void,int> 
      methodCallback(&testObj);

    auto callable = methodCallback.GetCallable<
      &MethodCallbackTest::TestMethod>();

    ASSERT_FALSE(testObj.invoked_);
    callable.Invoke(10);
    ASSERT_TRUE(testObj.invoked_);

    testObj.invoked_ = false;

    async::CallableFactory<MethodCallbackTest, int, int> methodRet(&testObj);
    auto callableWithRet = methodRet.GetCallable<&MethodCallbackTest::TestMethodWithRet>();

    int result = callableWithRet.Invoke(30);
    ASSERT_TRUE(testObj.invoked_);
    ASSERT_EQ(result, 30);


    testObj.invoked_ = false;
    async::CallableFactory<MethodCallbackTest, int, std::string&&> methodRValue(&testObj);
    auto callableRValue = methodRValue.GetCallable<&MethodCallbackTest::TestMethodWithRValue>();
    std::string s("10");
    callableRValue.Invoke(std::move(s));
    ASSERT_TRUE(testObj.invoked_);
  }
}

// Helper for quickly generating random data.
class RandomGenerator {
private:
  std::string data_;
  size_t      pos_;

public:
  RandomGenerator(size_t valsize, double compression_ratio) :
    pos_(0) {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < std::max<size_t>(1048576U, valsize)) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, compression_ratio, 100, &piece);
      data_.append(piece);
    }
  }

  Slice Generate(size_t len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

struct WinIOOverlapped : public OVERLAPPED {

  using
  RequestCallback = async::Callable<void, const Status&, const Slice&>;

  port::MemoryArenaId arena_;
  RequestCallback     cb_;
  char*               buffer_;

  WinIOOverlapped(const RequestCallback& cb, uint64_t offset,
    char* buffer) :
    cb_(cb),
    buffer_(buffer) {

    ZeroMemory(this, sizeof(OVERLAPPED));

    ULARGE_INTEGER offsetUnion;
    offsetUnion.QuadPart = offset;
    Offset = offsetUnion.LowPart;
    OffsetHigh = offsetUnion.HighPart;
  }

  ~WinIOOverlapped() {
  }

  Slice GetSlice(uint64_t len) {
    return Slice(buffer_, len);
  }

  const port::MemoryArenaId& GetArenaId() const {
    return arena_;
  }
};

// Test callback here
class TestIOCompletionCallback {
  HANDLE                              hFile_;
  std::unique_ptr<port::IOCompletion> io_compl_handle_;
public:

  TestIOCompletionCallback(const std::string& fileName,
    port::TaskPoolHandle& tp) :
    hFile_(NULL) {

    DWORD fileFlags = FILE_ATTRIBUTE_READONLY | FILE_FLAG_RANDOM_ACCESS |
      FILE_FLAG_OVERLAPPED;

    HANDLE hFile =
      CreateFileA(fileName.c_str(), GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        NULL, OPEN_EXISTING, fileFlags, NULL);

    hFile_ = hFile;
    assert(hFile_ != INVALID_HANDLE_VALUE);

    // Do not require setting an event since we are using a compl port
    // and in case the operation completes sync we do not want
    // a callback dispatch
    SetFileCompletionNotificationModes(hFile, 
      FILE_SKIP_SET_EVENT_ON_HANDLE | FILE_SKIP_COMPLETION_PORT_ON_SUCCESS);

    io_compl_handle_ = tp.CreateIOCompletion(hFile_, OnIoCompletion);
    assert(io_compl_handle_ != nullptr);
  }

  ~TestIOCompletionCallback() {
    io_compl_handle_.reset();
    CloseHandle(hFile_);
  }

  TestIOCompletionCallback(const TestIOCompletionCallback&) = delete;
  TestIOCompletionCallback& operator-(const TestIOCompletionCallback&) = delete;

  // A sketch of a async read API for random access
  // Takes callback that returns nothing but accepts Status and Slice
  // which would be the return values for such an API
  // This API returns status either OK - meaning things completed inline
  // OR - IOPending which means async operation is in progress, act accordingly
  // vacate the thread
  Status Read(const WinIOOverlapped::RequestCallback& cb,
    uint64_t offset, size_t n, char* buffer, Slice* result) {

    Status s;

    std::unique_ptr<WinIOOverlapped> overlapped(new WinIOOverlapped(cb, offset,
      buffer));

    unsigned long bytesRead = 0;

    // Let the TP know we are about to start
    // Must do it on every IO
    io_compl_handle_->Start();

    BOOL ret = ReadFile(hFile_, buffer, static_cast<DWORD>(n), &bytesRead,
      overlapped.get());

    if (ret == TRUE) {
      // Operation completed sync
      io_compl_handle_->Cancel();
      // We choose not to invoke callback and return in sync
      *result = Slice(buffer, bytesRead);
      return s;
    }

    DWORD lastError = GetLastError();
    if (lastError == ERROR_IO_PENDING) {
      // Async operation has been initiated
      overlapped.release();
      // We may want to mention file name here
      return Status::IOPending();
    }

    // Operation completed sync
    io_compl_handle_->Cancel();

    // We handle EOF as a success with zero bytes read
    if (lastError == ERROR_HANDLE_EOF) {
      *result = Slice(buffer, 0);
      return s;
    }

    // We have some other error
    return port::IOErrorFromWindowsError("Failed to initiate async Read()",
      lastError);
  }

  static
  void CALLBACK OnIoCompletion(
    PTP_CALLBACK_INSTANCE /* Instance */,
    PVOID                 /* Context */,
    PVOID                 Overlapped,
    ULONG                 IoResult,
    ULONG_PTR             NumberOfBytesTransferred,
    PTP_IO                /* ptp_io */) {

    std::unique_ptr<WinIOOverlapped> overlapped(
      reinterpret_cast<WinIOOverlapped*>(Overlapped));

    port::MemoryArenaSwitch arena(overlapped->GetArenaId());

    Status status;
    Slice slice;

    if (IoResult == ERROR_HANDLE_EOF) {
      slice = overlapped->GetSlice(0);
    } else if (IoResult == NO_ERROR) {
      slice = overlapped->GetSlice(NumberOfBytesTransferred);
    } else {
      slice = overlapped->GetSlice(0);
      status = port::IOErrorFromWindowsError("Async read failed: ",
        IoResult);
    }

    auto cb = overlapped->cb_;
    // Free the structure here as no longer needed
    // When pooling overlapped structures we we will want
    // to release it asap
    // Also release in this arena
    overlapped.reset();

    // Invoke callback and further processing continues on this thread
    cb.Invoke(status, slice);
  }
};

struct Customer {
  HANDLE hEvent_;
  std::unique_ptr<TableReader> table_reader_;

  Customer() {
    hEvent_ = CreateEvent(NULL, TRUE, FALSE, NULL);
    assert(NULL != hEvent_);
  }

  ~Customer() {
    if (hEvent_ != NULL) {
      CloseHandle(hEvent_);
    }
  }

  void Wait() {
    auto ret = WaitForSingleObject(hEvent_, INFINITE);
    assert(ret == WAIT_OBJECT_0);
  }

  void Reset() {
    ResetEvent(hEvent_);
  }

  async::Callable<void, const Status&, const Slice&>
  GetIOCallback() {
    async::CallableFactory<Customer,void, const Status&, const Slice&>
      factory(this);
    return factory.GetCallable<&Customer::OnIOCompletion>();
  }

  // IO COmpletion callback
  void OnIOCompletion(const Status& status, const Slice& slice) {
    std::cout << "Async Bytes read: " << slice.size() <<
              " status: " << status.ToString() << std::endl;
    SetEvent(hEvent_);
  }

  Status OnTableOpen(const Status& status,
                     std::unique_ptr<TableReader>&& table_reader) {
    std::cout << "OnTableOpen: async: " << std::boolalpha << status.async() <<
              " status: " << status.ToString() << " reader: " << ((table_reader) ?
                  "not null" : "null") <<
              std::endl;

    assert(!table_reader_);
    table_reader_ = std::move(table_reader);
    SetEvent(hEvent_);
    return status;
  }
};


TEST_F(AsyncTest, IOCompletionTest) {

  // Generate a file for reading
  std::string fileName = test::TmpDir();
  fileName += "/io_completion.bin";

  {
    std::ofstream os(fileName, std::ios::binary | std::ios::out);
    ASSERT_TRUE(os.is_open());

    RandomGenerator gen(32 * 1024U, 0.5);
    uint64_t sizeWritten = 0;
    while (sizeWritten < 1024 * 10U) {
      Slice data = gen.Generate(4096);
      os.write(data.data(), data.size());
      sizeWritten += data.size();
    }
    os.flush();
    ASSERT_FALSE(os.fail());
    os.close();
  }

  using namespace port;
  // Create 5 threads in the current arena
  // We use task pools to submit IOs always on behalf of
  // specific instances

  VistaThreadPool threadPool(MemoryArenaId(), 5);

  // Test creation and then destroy the taskpool first
  // and cleaning all the instances
  // TODO: After we removed the indirection from the
  // callback context and zeroing out the handles that the
  // task pool closes, we should always destroy the database
  // first as it will wait for all the handles
  {
    TaskPoolHandle taskPool = threadPool.CreateTaskPool();
    TestIOCompletionCallback dummy(fileName, taskPool);
    taskPool.Destroy();
  }

  // Reverse the destruction. File object destroys first.
  {
    TaskPoolHandle taskPool = threadPool.CreateTaskPool();
    TestIOCompletionCallback dummy(fileName, taskPool);
  }

  // Test the actual overlapped IO
  {
    TaskPoolHandle taskPool = threadPool.CreateTaskPool();
    TestIOCompletionCallback fileIO(fileName, taskPool);
    Customer customer;
    char buffer[4096 * 2];
    Slice result;
    Status status = fileIO.Read(customer.GetIOCallback(),
      4096U, sizeof(buffer), buffer, &result);

    if (status.IsIOPending()) {
      customer.Wait();
    }  else if(status.ok()) {
      std::cout << 
        "Sync read bytes: " << result.size() << std::endl;
    }     else {
      std::cout << "Sync read failed: " << status.ToString() << std::endl;
    }
  }

  remove(fileName.c_str());
}

TEST_F(AsyncTest, BlockBasedTableOpen) {

  Env* env = Env::Default();
  std::string folderName = test::TmpDir() + "/async_blocktable_open";
  ASSERT_OK(env->CreateDirIfMissing(folderName));

  const EnvOptions envDefault;
  std::string fileName = folderName + "/async_open_test.sst";
  std::unique_ptr<WritableFile> file;
  ASSERT_OK(NewWritableFile(env, fileName, &file, envDefault));
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(file), envDefault));

  Options options;
  BlockBasedTableOptions bbto;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  const ImmutableCFOptions ioptions(options);
  InternalKeyComparator ikc(options.comparator);

  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
    int_tbl_prop_collector_factories;
  int_tbl_prop_collector_factories.emplace_back(
    new SstFileWriterPropertiesCollectorFactory(2 /* version */,
      0 /* global_seqno*/));

  std::string column_family_name;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
    TableBuilderOptions(ioptions, ikc, &int_tbl_prop_collector_factories,
      kNoCompression, CompressionOptions(),
      nullptr /* compression_dict */,
      false /* skip_filters */, column_family_name, -1),
    TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
    file_writer.get()));

  for (char c = 'a'; c <= 'z'; ++c) {
    std::string key(8, c);
    std::string value = key;
    InternalKey ik(key, 0, kTypeValue);

    builder->Add(ik.Encode(), value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush());
  ASSERT_OK(file_writer->Close());

  builder.reset();
  file_writer.reset();

  // This test attempts to do basic Async open for
  // the BlockBased table
  // The basic scenario is that the main thread creates an
  // event which is signalled by a callback when table opens completes
  // We get back BlockBasedTableReader pointer
  {
    using namespace port;
    VistaThreadPool threadPool(MemoryArenaId(), 5, 10);
    TaskPoolHandle taskPool = threadPool.CreateTaskPool();
    auto asyncTp = env->CreateAsyncThreadPool(&taskPool);

    EnvOptions env_options;
    env_options.use_async_reads = true;
    env_options.async_threadpool = asyncTp.get();

    std::unique_ptr<RandomAccessFile> ra_file;
    ASSERT_OK(env->NewRandomAccessFile(fileName, &ra_file, env_options));
    std::unique_ptr<RandomAccessFileReader> ra_reader(new RandomAccessFileReader(std::move(ra_file)));
    uint64_t file_size = 0;
    ASSERT_OK(env->GetFileSize(fileName, &file_size));

    Customer c;
    // This will receive result in case of sync completion
    std::unique_ptr<TableReader> table_reader;
    using namespace async;
    CallableFactory<Customer, Status, const Status&, std::unique_ptr<TableReader>&&> f(&c);
    auto on_table_create_cb = f.GetCallable<&Customer::OnTableOpen>();
    Status s = async::TableOpenRequestContext::RequestOpen(on_table_create_cb, ioptions, env_options, bbto,
      ikc, std::move(ra_reader), file_size, &table_reader, true, false, -1);

    if (s.IsIOPending()) {
      c.Wait();
    } else {
      std::cout << "TableReader creation has completed sync. Status: " << s.ToString() << std::endl;
      std::cout << "Reader ptr is: " << ((table_reader) ? "not null" : "null") << std::endl;
    }
  }

}

} // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
