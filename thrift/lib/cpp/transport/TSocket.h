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

#ifndef _THRIFT_TRANSPORT_TSOCKET_H_
#define _THRIFT_TRANSPORT_TSOCKET_H_ 1

#include <string>
#include <sys/time.h>

#include "thrift/lib/cpp/transport/TRpcTransport.h"
#include "thrift/lib/cpp/transport/TVirtualTransport.h"
#include "thrift/lib/cpp/transport/TServerSocket.h"
#include "thrift/lib/cpp/transport/TSocketAddress.h"

namespace apache { namespace thrift { namespace transport {

/**
 * TCP Socket implementation of the TTransport interface.
 *
 */
class TSocket : public TVirtualTransport< TSocket,
                                          TTransportDefaults<TRpcTransport> > {

public:
  /**
   * Struct that contains socket options related stuff
   */
  struct Options {

    Options():
      connTimeout(0),
      sendTimeout(0),
      recvTimeout(0),
      sendBufSize(0),
      recvBufSize(0),
      lingerOn(false),
      lingerVal(0),
      noDelay(true),
      reuseAddr(false) {
      }

    /** Connect timeout in ms */
    int connTimeout;

    /** Send timeout in ms */
    int sendTimeout;

    /** Recv timeout in ms */
    int recvTimeout;

    /** Send Buffer Size in Bytes */
    size_t sendBufSize;

    /** Recv Buffer Size in Bytes */
    size_t recvBufSize;

    /** Linger on */
    bool lingerOn;

    /** Linger val */
    int lingerVal;

    /** Nodelay */
    bool noDelay;

    /** SO_REUSEADDR **/
    bool reuseAddr;
  };

  /**
   * Constructs a new socket. Note that this does NOT actually connect the
   * socket.
   *
   */
  TSocket();

  /**
   * Constructs a new socket. Note that this does NOT actually connect the
   * socket.
   *
   * If a hostname is provided, and it resolves to multiple IPs, connect() will
   * attempt to connect to each one in sequence, until one connection succeeds.
   *
   * @param host An IP address or hostname to connect to
   * @param port The port to connect on
   */
  TSocket(std::string host, int port);

  /**
   * Constructs a new socket. Note that this does NOT actually connect the
   * socket.
   *
   * @param adddress The address to connect to
   */
  explicit TSocket(const TSocketAddress* address);

  /**
   * Constructs a new socket. Note that this does NOT actually connect the
   * socket.
   *
   * @param adddress The address to connect to
   */
  explicit TSocket(const TSocketAddress& address);

  /**
   * Constructor to create socket from raw UNIX handle.
   *
   * This is used by the TServerSocket class to create a TSocket from file
   * descriptors returned by accept().
   */
  explicit TSocket(int socket);


  /**
   * Destroyes the socket object, closing it if necessary.
   */
  virtual ~TSocket();

  /**
   * Whether the socket is alive.
   *
   * @return Is the socket alive?
   */
  bool isOpen();

  /**
   * Calls select on the socket to see if there is more data available.
   */
  bool peek();

  /**
   * Creates and opens the UNIX socket.
   *
   * @throws TTransportException If the socket could not connect
   */
  virtual void open();

  /**
   * Shuts down communications on the socket.
   */
  virtual void close();

  /**
   * Reads from the underlying socket.
   */
  uint32_t read(uint8_t* buf, uint32_t len);

  /**
   * Writes to the underlying socket.  Loops until done or fail.
   */
  void write(const uint8_t* buf, uint32_t len);

  /**
   * Writes to the underlying socket.  Does single send() and returns result.
   */
  uint32_t write_partial(const uint8_t* buf, uint32_t len);

  /**
   * Get the host that the socket is connected to
   *
   * @return string host identifier
   */
  std::string getHost();

  /**
   * Get the port that the socket is connected to
   *
   * @return int port number
   */
  int getPort();

  /**
   * Set the host that socket will connect to
   *
   * @param host host identifier
   */
  void setHost(std::string host);

  /**
   * Set the port that socket will connect to
   *
   * @param port port number
   */
  void setPort(int port);


  /**
   * Sets the socket options enabled in the
   * TSocket::Options object options_;
   * Note you can try to individually set any option
   * using the methods provided below e.g. setSendBufSize
   */
  void setSocketOptions(const Options& oh );

  /**
   * get the options_ object
   *
   * @return TSocket::Options options_;
   */
  TSocket::Options getSocketOptions();

  /**
   * get the currently set socket options
   * this function returns the currently set socket options
   * using the getsockopt() function rather than trusting
   * what is set in the options_ member
   * setting socket options via setSocketOption calls can fail
   * because of settings in the sysctl
   * e.g. trying to set the sendBufSize to some value greater
   * than the wmem_max value in sysctl
   *
   * @return TSocket::Options
   */
  TSocket::Options getCurrentSocketOptions();

  /**
   * Controls whether the linger option is set on the socket.
   *
   * @param on      Whether SO_LINGER is on
   * @param linger  If linger is active, the number of seconds to linger for
   */
  void setLinger(bool on, int linger);

  /**
   * Whether to enable/disable Nagle's algorithm.
   *
   * @param noDelay Whether or not to disable the algorithm.
   * @return
   */
  void setNoDelay(bool noDelay);

  /**
   * Set the connect timeout
   */
  void setConnTimeout(int ms);

  /**
   * Set the receive timeout
   */
  void setRecvTimeout(int ms);

  /**
   * Set the send bufsize
   */
  void setSendBufSize(size_t bufsize);

  /**
   * Set the recv bufsize
   */
  void setRecvBufSize(size_t bufsize);

  /**
   * Set the send timeout
   */
  void setSendTimeout(int ms);

  /**
   * Set the max number of recv retries in case of an EAGAIN
   * error
   */
  void setMaxRecvRetries(int maxRecvRetries);

  /**
   * Set the SO_REUSEADDR socket option.
   */
  void setReuseAddress(bool reuseAddr);

  /**
   * Get socket information formated as a string <Host: x Port: x>
   */
  std::string getSocketInfo();

  /*
   * Returns the address of the host to which the socket is connected
   */
  const TSocketAddress* getPeerAddress();

  /**
   * Returns the DNS name of the host to which the socket is connected
   */
  std::string getPeerHost();

  /**
   * Returns a string representation of the IP address to which the
   * socket is connected
   */
  std::string getPeerAddressStr();

  /**
   * Returns the port of the host to which the socket is connected
   **/
  uint16_t getPeerPort();

  /**
   * Returns the underlying socket file descriptor.
   */
  int getSocketFD() {
    return socket_;
  }

  /**
   * (Re-)initialize a TSocket for the supplied descriptor.  This is only
   * intended for use by TNonblockingServer -- other use may result in
   * unfortunate surprises.
   *
   * @param fd the descriptor for an already-connected socket
   */
  void setSocketFD(int fd);

  /**
   * Sets whether to use a low minimum TCP retransmission timeout.
   */
  static void setUseLowMinRto(bool useLowMinRto);

  /**
   * Gets whether to use a low minimum TCP retransmission timeout.
   */
  static bool getUseLowMinRto();

  /**
   * Set a cache of the peer address (used when trivially available: e.g.
   * accept() or connect()). Only caches IPV4 and IPV6; unset for others.
   */
  void setCachedAddress(const sockaddr* addr, socklen_t len);

 protected:
  /** connect, called by open */
  void openConnection(struct addrinfo *res);

  /** Host to connect to */
  std::string host_;

  /** Peer hostname */
  std::string peerHost_;

  /** Peer address */
  std::string peerAddressStr_;

  /** Port number to connect on */
  int port_;

  /** Underlying UNIX socket handle */
  int socket_;

  /** Socket Options Helper */
  Options options_;

  /** Recv EGAIN retries */
  int maxRecvRetries_;

  /** Cached peer address */
  TSocketAddress cachedPeerAddr_;

  /** Connection start time */
  timespec startTime_;

  /** Whether to use low minimum TCP retransmission timeout */
  static bool useLowMinRto_;
};

std::ostream& operator<<(std::ostream& os, const TSocket::Options& o);

}}} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TSOCKET_H_

