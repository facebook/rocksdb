#include "rocksdb/async_result.h"

namespace ROCKSDB_NAMESPACE {

void async_result::await_suspend(
    std::coroutine_handle<async_result::promise_type> h) {
  if (!async_)
    h_.promise().prev_ = &h.promise();
  else
    context_->promise = &h.promise();
}

}  // namespace ROCKSDB_NAMESPACE