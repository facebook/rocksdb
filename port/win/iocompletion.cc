
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
    // Wait for all of them to finish
    ::WaitForThreadpoolIoCallbacks(GetHandle(io_compl_), FALSE);
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
