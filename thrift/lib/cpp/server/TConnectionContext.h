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
#ifndef THRIFT_TCONNECTIONCONTEXT_H_
#define THRIFT_TCONNECTIONCONTEXT_H_ 1

#include <stddef.h>

#include <boost/shared_ptr.hpp>
#include "thrift/lib/cpp/protocol/TProtocol.h"

namespace apache { namespace thrift {

namespace transport {
class TSocketAddress;
}

namespace protocol {
class TProtocol;
}

namespace server {

class TConnectionContext {
 public:
  TConnectionContext()
    : userData_(NULL)
    , destructor_(NULL) {}

  virtual ~TConnectionContext() {
    cleanupUserData();
  }

  // expose getPeerAddress() defined in TRpcTransportContext

  virtual const transport::TSocketAddress* getPeerAddress() const = 0;

  virtual boost::shared_ptr<protocol::TProtocol> getInputProtocol() const = 0;

  virtual boost::shared_ptr<protocol::TProtocol> getOutputProtocol() const = 0;
  /**
   * Get the user data field.
   */
  void* getUserData() const {
    return userData_;
  }

  /**
   * Set the user data field.
   *
   * @param data         The new value for the user data field.
   * @param destructor   A function pointer to invoke when the connection
   *                     context is destroyed.  It will be invoked with the
   *                     contents of the user data field.
   *
   * @return Returns the old user data value.
   */
  virtual void* setUserData(void* data, void (*destructor)(void*) = NULL) {
    void* oldData = userData_;
    userData_  = data;
    destructor_ = destructor;
    return oldData;
  }

 protected:
  void cleanupUserData() {
    if (destructor_) {
      destructor_(userData_);
      destructor_ = NULL;
    }
    userData_ = NULL;
  }

 private:
  void* userData_;
  void (*destructor_)(void*);
};

}}} // apache::thrift::server

#endif // THRIFT_TCONNECTIONCONTEXT_H_
