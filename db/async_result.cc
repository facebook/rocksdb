#include "rocksdb/async_result.h"

namespace ROCKSDB_NAMESPACE {

template <class T>
void async_result<T>::await_suspend(std::coroutine_handle<async_result<T>::promise_type> h) {
  if (!async_) 
    h_.promise().prev_ = &h.promise();
  else
    context_->promise = &h.promise();
}
}  // namespace ROCKSDB_NAMESPACE