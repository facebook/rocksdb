// Copyright (c) 2009- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef THRIFT_TRANSPORT_TSSLSOCKET_H_
#define THRIFT_TRANSPORT_TSSLSOCKET_H_ 1

#include <string>
#include <boost/shared_ptr.hpp>
#include <openssl/ssl.h>
#include "thrift/lib/cpp/concurrency/Mutex.h"
#include "thrift/lib/cpp/transport/TSocket.h"

namespace apache { namespace thrift { namespace transport {

class PasswordCollector;
class SSLContext;
class TSocketAddress;

/**
 * OpenSSL implementation for SSL socket interface.
 */
class TSSLSocket: public TVirtualTransport<TSSLSocket, TSocket> {
 public:
  /**
   * Constructor.
   */
  explicit TSSLSocket(const boost::shared_ptr<SSLContext>& ctx);
  /**
   * Constructor, create an instance of TSSLSocket given an existing socket.
   *
   * @param socket An existing socket
   */
  TSSLSocket(const boost::shared_ptr<SSLContext>& ctx, int socket);
  /**
   * Constructor.
   *
   * @param host  Remote host name
   * @param port  Remote port number
   */
  TSSLSocket(const boost::shared_ptr<SSLContext>& ctx,
             const std::string& host,
             int port);
  /**
   * Constructor.
   */
  TSSLSocket(const boost::shared_ptr<SSLContext>& ctx,
             const TSocketAddress& address);
  /**
   * Destructor.
   */
  ~TSSLSocket();

  /**
   * TTransport interface.
   */
  bool     isOpen();
  bool     peek();
  void     open();
  void     close();
  uint32_t read(uint8_t* buf, uint32_t len);
  void     write(const uint8_t* buf, uint32_t len);
  void     flush();

  /**
   * Set whether to use client or server side SSL handshake protocol.
   *
   * @param flag  Use server side handshake protocol if true.
   */
  void server(bool flag) { server_ = flag; }
  /**
   * Determine whether the SSL socket is server or client mode.
   */
  bool server() const { return server_; }

protected:
  /**
   * Verify peer certificate after SSL handshake completes.
   */
  virtual void verifyCertificate();

  /**
   * Initiate SSL handshake if not already initiated.
   */
  void checkHandshake();

  bool server_;
  SSL* ssl_;
  boost::shared_ptr<SSLContext> ctx_;
};

/**
 * SSL socket factory. SSL sockets should be created via SSL factory.
 */
class TSSLSocketFactory {
 public:
  /**
   * Constructor/Destructor
   */
  explicit TSSLSocketFactory(const boost::shared_ptr<SSLContext>& context);
  virtual ~TSSLSocketFactory();

  /**
   * Create an instance of TSSLSocket with a fresh new socket.
   */
  virtual boost::shared_ptr<TSSLSocket> createSocket();
  /**
   * Create an instance of TSSLSocket with the given socket.
   *
   * @param socket An existing socket.
   */
  virtual boost::shared_ptr<TSSLSocket> createSocket(int socket);
   /**
   * Create an instance of TSSLSocket.
   *
   * @param host  Remote host to be connected to
   * @param port  Remote port to be connected to
   */
  virtual boost::shared_ptr<TSSLSocket> createSocket(const std::string& host,
                                                     int port);
  /**
   * Set/Unset server mode.
   *
   * @param flag  Server mode if true
   */
  virtual void server(bool flag) { server_ = flag; }
  /**
   * Determine whether the socket is in server or client mode.
   *
   * @return true, if server mode, or, false, if client mode
   */
  virtual bool server() const { return server_; }

 private:
  boost::shared_ptr<SSLContext> ctx_;
  bool server_;
};

/**
 * SSL exception.
 */
class TSSLException: public TTransportException {
 public:
  explicit TSSLException(const std::string& message):
    TTransportException(TTransportException::INTERNAL_ERROR, message) {}

  virtual const char* what() const throw() {
    if (message_.empty()) {
      return "TSSLException";
    } else {
      return message_.c_str();
    }
  }
};

/**
 * Wrap OpenSSL SSL_CTX into a class.
 */
class SSLContext {
 public:

  enum SSLVersion {
     SSLv2,
     SSLv3,
     TLSv1
  };

  /**
   * Constructor.
   *
   * @param version The lowest or oldest SSL version to support.
   */
  explicit SSLContext(SSLVersion version = TLSv1);
  virtual ~SSLContext();

  /**
   * Set ciphers to be used in SSL handshake process.
   *
   * @param ciphers  A list of ciphers
   */
  virtual void ciphers(const std::string& enable);
  /**
   * Enable/Disable authentication. Peer name validation can only be done
   * if checkPeerCert is true.
   *
   * @param checkPeerCert If true, require peer to present valid certificate
   * @param checkPeerName If true, validate that the certificate common name
   *                      or alternate name(s) of peer matches the hostname
   *                      used to connect.
   * @param peerName      If non-empty, validate that the certificate common
   *                      name of peer matches the given string (altername
   *                      name(s) are not used in this case).
   */
  virtual void authenticate(bool checkPeerCert, bool checkPeerName,
                            const std::string& peerName = std::string());
  /**
   * Load server certificate.
   *
   * @param path   Path to the certificate file
   * @param format Certificate file format
   */
  virtual void loadCertificate(const char* path, const char* format = "PEM");
  /**
   * Load private key.
   *
   * @param path   Path to the private key file
   * @param format Private key file format
   */
  virtual void loadPrivateKey(const char* path, const char* format = "PEM");
  /**
   * Load trusted certificates from specified file.
   *
   * @param path Path to trusted certificate file
   */
  virtual void loadTrustedCertificates(const char* path);
  /**
   * Load trusted certificates from specified X509 certificate store.
   *
   * @param store X509 certificate store.
   */
  virtual void loadTrustedCertificates(X509_STORE* store);
  /**
   * Default randomize method.
   */
  virtual void randomize();
  /**
   * Override default OpenSSL password collector.
   *
   * @param collector Instance of user defined password collector
   */
  virtual void passwordCollector(boost::shared_ptr<PasswordCollector> collector);
  /**
   * Obtain password collector.
   *
   * @return User defined password collector
   */
  virtual boost::shared_ptr<PasswordCollector> passwordCollector() {
    return collector_;
  }

  /**
   * Create an SSL object from this context.
   */
  SSL* createSSL() const;

  /**
   * Possibly validate the peer's certificate name, depending on how this
   * SSLContext was configured by authenticate().
   *
   * @return True if the peer's name is acceptable, false otherwise
   */
  bool validatePeerName(TSSLSocket* sock, SSL* ssl) const;

  /**
   * Set the options on the SSL_CTX object.
   */
  void setOptions(long options);

#ifdef OPENSSL_NPN_NEGOTIATED
  /**
   * Set the list of protocols that a TLS server should advertise for
   * Next Protocol Negotiation (NPN).
   *
   * @param protocols   List of protocol names, or NULL to disable NPN.
   *                    Note: if non-null, this method makes a copy, so
   *                    the caller needn't keep the list in scope after
   *                    the call completes.
   */
  void setAdvertisedNextProtocols(const std::list<std::string>* protocols);
#endif // OPENSSL_NPN_NEGOTIATED

  /**
   * Gets the underlying SSL_CTX for advanced usage
   */
  SSL_CTX *getSSLCtx() const {
    return ctx_;
  }

  enum SSLLockType {
    LOCK_MUTEX,
    LOCK_SPINLOCK,
    LOCK_NONE
  };

  /**
   * Set preferences for how to treat locks in OpenSSL.  This must be
   * called before the instantiation of any SSLContext objects, otherwise
   * the defaults will be used.
   *
   * OpenSSL has a lock for each module rather than for each object or
   * data that needs locking.  Some locks protect only refcounts, and
   * might be better as spinlocks rather than mutexes.  Other locks
   * may be totally unnecessary if the objects being protected are not
   * shared between threads in the application.
   *
   * By default, all locks are initialized as mutexes.  OpenSSL's lock usage
   * may change from version to version and you should know what you are doing
   * before disabling any locks entirely.
   *
   * Example: if you don't share SSL sessions between threads in your
   * application, you may be able to do this
   *
   * setSSLLockTypes({{CRYPTO_LOCK_SSL_SESSION, SSLContext::LOCK_NONE}})
   */
  static void setSSLLockTypes(std::map<int, SSLLockType> lockTypes);

 protected:
  SSL_CTX* ctx_;

 private:
  bool checkPeerName_;
  std::string peerFixedName_;
  boost::shared_ptr<PasswordCollector> collector_;

  static concurrency::Mutex mutex_;
  static uint64_t count_;

#ifdef OPENSSL_NPN_NEGOTIATED
  /**
   * Wire-format list of advertised protocols for use in NPN.
   */
  unsigned char* advertisedNextProtocols_;
  unsigned advertisedNextProtocolsLength_;

  static int advertisedNextProtocolCallback(SSL* ssl,
      const unsigned char** out, unsigned int* outlen, void* data);
#endif // OPENSSL_NPN_NEGOTIATED

  static int passwordCallback(char* password, int size, int, void* data);

  static void initializeOpenSSL();
  static void cleanupOpenSSL();

  /**
   * Helper to match a hostname versus a pattern.
   */
  static bool matchName(const char* host, const char* pattern, int size);
};

typedef boost::shared_ptr<SSLContext> SSLContextPtr;

/**
 * Override the default password collector.
 */
class PasswordCollector {
 public:
  virtual ~PasswordCollector() {}
  /**
   * Interface for customizing how to collect private key password.
   *
   * By default, OpenSSL prints a prompt on screen and request for password
   * while loading private key. To implement a custom password collector,
   * implement this interface and register it with TSSLSocketFactory.
   *
   * @param password Pass collected password back to OpenSSL
   * @param size     Maximum length of password including NULL character
   */
  virtual void getPassword(std::string& password, int size) = 0;
};

}}}

#endif
