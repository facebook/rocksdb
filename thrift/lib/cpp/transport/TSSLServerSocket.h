// Copyright (c) 2009- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TSSLSERVERSOCKET_H_
#define _THRIFT_TRANSPORT_TSSLSERVERSOCKET_H_ 1

#include <boost/shared_ptr.hpp>
#include "thrift/lib/cpp/transport/TServerSocket.h"

namespace apache { namespace thrift { namespace transport {

class TSSLSocketFactory;

/**
 * Server socket that accepts SSL connections.
 */
class TSSLServerSocket: public TServerSocket {
 public:
  /**
   * Constructor.
   *
   * @param port    Listening port
   * @param factory SSL socket factory implementation
   */
  TSSLServerSocket(int port, boost::shared_ptr<TSSLSocketFactory> factory);
  /**
   * Constructor.
   *
   * @param port        Listening port
   * @param sendTimeout Socket send timeout
   * @param recvTimeout Socket receive timeout
   * @param factory     SSL socket factory implementation
   */
  TSSLServerSocket(int port, int sendTimeout, int recvTimeout,
                   boost::shared_ptr<TSSLSocketFactory> factory);
 protected:
  boost::shared_ptr<TSocket> createSocket(int socket);
  boost::shared_ptr<TSSLSocketFactory> factory_;
};

}}}

#endif
