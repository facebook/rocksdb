/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef THRIFT_ASYNC_TASYNCDISPATCHPROCESSOR_H_
#define THRIFT_ASYNC_TASYNCDISPATCHPROCESSOR_H_ 1

#include "thrift/lib/cpp/async/TAsyncProcessor.h"

namespace apache { namespace thrift { namespace async {

/**
 * TAsyncDispatchProcessor is a helper class to parse the message header then
 * call another function to dispatch based on the function name.
 *
 * Subclasses must implement dispatchCall() to dispatch on the function name.
 */
template <class Protocol_>
class TAsyncDispatchProcessorT : public TAsyncProcessor {
 public:
  virtual void process(std::tr1::function<void(bool success)> _return,
                       boost::shared_ptr<protocol::TProtocol> in,
                       boost::shared_ptr<protocol::TProtocol> out,
                       TConnectionContext* context) {
    protocol::TProtocol* inRaw = in.get();
    protocol::TProtocol* outRaw = out.get();

    // Try to dynamic cast to the template protocol type
    Protocol_* specificIn = dynamic_cast<Protocol_*>(inRaw);
    Protocol_* specificOut = dynamic_cast<Protocol_*>(outRaw);
    if (specificIn && specificOut) {
      return processFast(_return, specificIn, specificOut, context);
    }

    // Log the fact that we have to use the slow path
    T_GENERIC_PROTOCOL(this, inRaw, specificIn);
    T_GENERIC_PROTOCOL(this, outRaw, specificOut);

    std::string fname;
    protocol::TMessageType mtype;
    int32_t seqid;
    try {
      inRaw->readMessageBegin(fname, mtype, seqid);
    } catch (const TException &ex) {
      GlobalOutput.printf("received invalid message from client: %s",
                          ex.what());
      _return(false);
      return;
    }

    // If this doesn't look like a valid call, log an error and return false so
    // that the server will close the connection.
    //
    // (The old generated processor code used to try to skip a T_STRUCT and
    // continue.  However, that seems unsafe.)
    if (mtype != protocol::T_CALL && mtype != protocol::T_ONEWAY) {
      GlobalOutput.printf("received invalid message type %d from client",
                          mtype);
      _return(false);
      return;
    }

    return this->dispatchCall(_return, inRaw, outRaw, fname, seqid, context);
  }

  void processFast(std::tr1::function<void(bool success)> _return,
                   Protocol_* in, Protocol_* out,
                   TConnectionContext* context) {
    std::string fname;
    protocol::TMessageType mtype;
    int32_t seqid;
    try {
      in->readMessageBegin(fname, mtype, seqid);
    } catch (const TException &ex) {
      GlobalOutput.printf("received invalid message from client: %s",
                          ex.what());
      _return(false);
      return;
    }

    if (mtype != protocol::T_CALL && mtype != protocol::T_ONEWAY) {
      GlobalOutput.printf("received invalid message type %d from client",
                          mtype);
      _return(false);
      return;
    }

    return this->dispatchCallTemplated(_return, in, out, fname, seqid, context);
  }

  virtual void dispatchCall(std::tr1::function<void(bool ok)> _return,
                            apache::thrift::protocol::TProtocol* in,
                            apache::thrift::protocol::TProtocol* out,
                            const std::string& fname, int32_t seqid,
                            TConnectionContext* context) = 0;

  virtual void dispatchCallTemplated(std::tr1::function<void(bool ok)> _return,
                                     Protocol_* in, Protocol_* out,
                                     const std::string& fname, int32_t seqid,
                                     TConnectionContext* context) = 0;
};

/**
 * Non-templatized version of TAsyncDispatchProcessor,
 * that doesn't bother trying to perform a dynamic_cast.
 */
class TAsyncDispatchProcessor : public TAsyncProcessor {
 public:
  virtual void process(std::tr1::function<void(bool success)> _return,
                       boost::shared_ptr<protocol::TProtocol> in,
                       boost::shared_ptr<protocol::TProtocol> out,
                       TConnectionContext* context) {
    protocol::TProtocol* inRaw = in.get();
    protocol::TProtocol* outRaw = out.get();

    std::string fname;
    protocol::TMessageType mtype;
    int32_t seqid;
    try {
      inRaw->readMessageBegin(fname, mtype, seqid);
    } catch (const TException &ex) {
      GlobalOutput.printf("received invalid message from client: %s",
                          ex.what());
      _return(false);
      return;
    }

    // If this doesn't look like a valid call, log an error and return false so
    // that the server will close the connection.
    //
    // (The old generated processor code used to try to skip a T_STRUCT and
    // continue.  However, that seems unsafe.)
    if (mtype != protocol::T_CALL && mtype != protocol::T_ONEWAY) {
      GlobalOutput.printf("received invalid message type %d from client",
                          mtype);
      _return(false);
      return;
    }

    return dispatchCall(_return, inRaw, outRaw, fname, seqid, context);
  }

  virtual void dispatchCall(std::tr1::function<void(bool ok)> _return,
                            apache::thrift::protocol::TProtocol* in,
                            apache::thrift::protocol::TProtocol* out,
                            const std::string& fname, int32_t seqid,
                            TConnectionContext* context) = 0;
};

// Specialize TAsyncDispatchProcessorT for TProtocol and TDummyProtocol just to
// use the generic TDispatchProcessor.
template <>
class TAsyncDispatchProcessorT<protocol::TDummyProtocol> :
  public TAsyncDispatchProcessor {};
template <>
class TAsyncDispatchProcessorT<protocol::TProtocol> :
  public TAsyncDispatchProcessor {};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TASYNCDISPATCHPROCESSOR_H_
