// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef THRIFT_TASYNCPROCESSOR_H
#define THRIFT_TASYNCPROCESSOR_H 1

#include <tr1/functional>
#include <boost/shared_ptr.hpp>
#include "thrift/lib/cpp/TProcessor.h"
#include "thrift/lib/cpp/server/TConnectionContext.h"
#include "thrift/lib/cpp/protocol/TProtocol.h"

using apache::thrift::server::TConnectionContext;

namespace apache { namespace thrift { namespace async {

/**
 * Async version of a TProcessor.  It is not expected to complete by the time
 * the call to process returns.  Instead, it calls a cob to signal completion.
 *
 * @author David Reiss <dreiss@facebook.com>
 */

class TEventServer; // forward declaration

class TAsyncProcessor : public TProcessorBase {
 public:
  virtual ~TAsyncProcessor() {}

  virtual void process(std::tr1::function<void(bool success)> _return,
                       boost::shared_ptr<protocol::TProtocol> in,
                       boost::shared_ptr<protocol::TProtocol> out,
                       TConnectionContext* context = NULL) = 0;

  void process(std::tr1::function<void(bool success)> _return,
               boost::shared_ptr<apache::thrift::protocol::TProtocol> io) {
    return process(_return, io, io);
  }

  const TEventServer* getAsyncServer() {
    return asyncServer_;
  }
 protected:
  TAsyncProcessor() {}

  const TEventServer* asyncServer_;
 private:
  friend class TEventServer;
  void setAsyncServer(const TEventServer* server) {
    asyncServer_ = server;
  }
};

class TAsyncProcessorFactory {
 public:
  virtual ~TAsyncProcessorFactory() {}

  /**
   * Get the TAsyncProcessor to use for a particular connection.
   */
  virtual boost::shared_ptr<TAsyncProcessor> getProcessor(
      server::TConnectionContext* ctx) = 0;
};

class TAsyncSingletonProcessorFactory : public TAsyncProcessorFactory {
 public:
  explicit TAsyncSingletonProcessorFactory(
    const boost::shared_ptr<TAsyncProcessor>& processor) :
      processor_(processor) {}

  boost::shared_ptr<TAsyncProcessor> getProcessor(server::TConnectionContext*) {
    return processor_;
  }

 private:
  boost::shared_ptr<TAsyncProcessor> processor_;
};


}}} // apache::thrift::async

// XXX I'm lazy for now
namespace apache { namespace thrift {
using apache::thrift::async::TAsyncProcessor;
}}

#endif // #ifndef THRIFT_TASYNCPROCESSOR_H
