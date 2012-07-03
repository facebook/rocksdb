// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TSYNCTOASYNCPROCESSOR_H_
#define _THRIFT_TSYNCTOASYNCPROCESSOR_H_ 1

#include <tr1/functional>
#include <boost/shared_ptr.hpp>
#include "thrift/lib/cpp/TProcessor.h"
#include "thrift/lib/cpp/async/TAsyncProcessor.h"

namespace apache { namespace thrift { namespace async {

/**
 * Adapter to allow a TProcessor to be used as a TAsyncProcessor.
 *
 * Note that this should only be used for handlers that return quickly without
 * blocking, since async servers can be stalled by a single blocking operation.
 */
class TSyncToAsyncProcessor : public TAsyncProcessor {
 public:
  TSyncToAsyncProcessor(boost::shared_ptr<TProcessor> processor)
    : processor_(processor)
  {}

  virtual void process(std::tr1::function<void(bool success)> _return,
                       boost::shared_ptr<protocol::TProtocol> in,
                       boost::shared_ptr<protocol::TProtocol> out,
                       TConnectionContext* context) {
    return _return(processor_->process(in, out, context));
  }

 private:
  boost::shared_ptr<TProcessor> processor_;
};

}}} // apache::thrift::async

#endif // #ifndef _THRIFT_TSYNCTOASYNCPROCESSOR_H_
