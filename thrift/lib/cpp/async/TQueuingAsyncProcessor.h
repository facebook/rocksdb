#ifndef _THRIFT_TQUEUINGASYNCPROCESSOR_H_
#define _THRIFT_TQUEUINGASYNCPROCESSOR_H_ 1

#include <tr1/functional>
#include <boost/shared_ptr.hpp>
#include "thrift/lib/cpp/TProcessor.h"
#include "thrift/lib/cpp/async/TAsyncProcessor.h"
#include "thrift/lib/cpp/async/TEventTask.h"
#include "thrift/lib/cpp/concurrency/Exception.h"

namespace apache { namespace thrift { namespace async {

/**
 * Adapter to allow a TProcessor to be used as a TAsyncProcessor.
 *
 * Note: this is not intended for use outside of TEventConnection since the
 * callback mechanism used in TEventTask will invoke handleAsyncTaskComplete()
 * regardless of what is passed in as the cob.
 *
 * Uses a per-server task queue for all calls.
 */
class TQueuingAsyncProcessor : public TAsyncProcessor {
 public:
  TQueuingAsyncProcessor(
   boost::shared_ptr<apache::thrift::server::TProcessor> processor,
   boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager,
   int64_t taskExpireTime,
   TEventConnection* connection)
    : processor_(processor)
    , threadManager_(threadManager)
    , taskExpireTime_(taskExpireTime)
    , connection_(connection)
  {}

  virtual void process(
                std::tr1::function<void(bool success)> cob,
                boost::shared_ptr<apache::thrift::protocol::TProtocol> in,
                boost::shared_ptr<apache::thrift::protocol::TProtocol> out,
                TConnectionContext* context) {

    boost::shared_ptr<apache::thrift::concurrency::Runnable> task =
      boost::shared_ptr<apache::thrift::concurrency::Runnable>(
                                                 new TEventTask(connection_));

    try {
      threadManager_->add(task, 0LL, taskExpireTime_);
    } catch (apache::thrift::concurrency::IllegalStateException & ise) {
      T_ERROR("IllegalStateException: TQueuingAsyncProcessor::process() %s",
              ise.what());
      // no task will be making a callback
      return cob(false);
    }
  }

 private:
  boost::shared_ptr<apache::thrift::server::TProcessor> processor_;

  /// For processing via thread pool
  boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager_;

  /// Time in milliseconds before an unperformed task expires (0 == infinite).
  int64_t taskExpireTime_;

  /// The worker that started us
  TEventConnection* connection_;
};

}}} // apache::thrift::async

#endif // #ifndef _THRIFT_TQUEUINGASYNCPROCESSOR_H_
