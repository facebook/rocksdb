
#include "port/win/iocompletion.h"
#include "port/win/asyncthreadpoolimpl.h"

#include <Windows.h>

namespace rocksdb {
namespace port {

//////////////////////////////////////////////////////////////////////////////
// IOCompletion

IOCompletion::~IOCompletion() {
  Close();
}

inline
PTP_IO GetHandle(void* h) {
  return reinterpret_cast<PTP_IO>(h);
}

void IOCompletion::Start() const {
  ::StartThreadpoolIo(GetHandle(io_compl_));
}

// Can only be called if Start()
// was called
void IOCompletion::Cancel() const {
  ::CancelThreadpoolIo(GetHandle(io_compl_));
}

void IOCompletion::Close() {

  if (io_compl_ != nullptr) {
    // Most of the file closures
    // and, therefore, ThredPoolIo closures will
    // take place from within the IO callbacks
    // often originating from the file being closed.
    // We, therefore, can not wait here for completion
    // of all the events, it would result in a deadlock.
    // Thus we choose to close the file
    // first and then close the ThredPoolIo below and just
    // let the pending IO drain async OR close
    // the TakPool handle before the db
    ::CloseThreadpoolIo(GetHandle(io_compl_));
    io_compl_ = nullptr;
  }

  if (tp_tasktoken_ != nullptr) {
    reinterpret_cast<ThreadPoolTaskToken*>(tp_tasktoken_)->Destroy();
    tp_tasktoken_ = nullptr;
  }
}
} // namespace async
} //namespace rocksdb
