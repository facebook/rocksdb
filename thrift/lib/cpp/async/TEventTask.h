#ifndef _THRIFT_TEVENTTASK_H_
#define _THRIFT_TEVENTTASK_H_ 1

#include "thrift/lib/cpp/Thrift.h"
#include "thrift/lib/cpp/server/TServer.h"
#include "thrift/lib/cpp/transport/TBufferTransports.h"
#include "thrift/lib/cpp/concurrency/ThreadManager.h"
#include "thrift/lib/cpp/async/TEventConnection.h"
#include <tr1/functional>
#include <boost/shared_ptr.hpp>
#include "thrift/lib/cpp/TProcessor.h"
#include "thrift/lib/cpp/protocol/TProtocol.h"

namespace apache { namespace thrift { namespace async {

class TEventTask : public apache::thrift::concurrency::Runnable {
 public:
  explicit TEventTask(TEventConnection* connection);

  void run();

  TEventConnection* getConnection() const {
    return connection_;
  }

 private:
  boost::shared_ptr<apache::thrift::server::TProcessor> processor_;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> input_;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> output_;
  TEventConnection* connection_;
  TConnectionContext* connectionContext_;
};

class TaskCompletionMessage {
 public:
  explicit TaskCompletionMessage(TEventConnection *inConnection)
      : connection(inConnection) {}

  TaskCompletionMessage(TaskCompletionMessage &&msg)
      : connection(msg.connection) {
    msg.connection = NULL;
  }

  TEventConnection *connection;
};

} } } // namespace apache::thrift::async

#endif // !_THRIFT_TEVENTTASK_H_
