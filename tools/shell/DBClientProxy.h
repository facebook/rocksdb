
#ifndef TOOLS_SHELL_DBCLIENTPROXY
#define TOOLS_SHELL_DBCLIENTPROXY

#include <vector>
#include <map>
#include <string>
#include <boost/utility.hpp>
#include <boost/shared_ptr.hpp>

#include "DB.h"

/*
 * class DBClientProxy maintains:
 * 1. a connection to rocksdb service
 * 2. a map from db names to opened db handles
 *
 * it's client codes' responsibility to catch all possible exceptions.
 */

namespace rocksdb {

class DBClientProxy : private boost::noncopyable {
 public:
  // connect to host_:port_
  void connect(void);

  // return true on success, false otherwise
  bool get(const std::string & db,
           const std::string & key,
           std::string & value);

  // return true on success, false otherwise
  bool put(const std::string & db,
           const std::string & key,
           const std::string & value);

  // return true on success, false otherwise
  bool scan(const std::string & db,
            const std::string & start_key,
            const std::string & end_key,
            const std::string & limit,
            std::vector<std::pair<std::string, std::string> > & kvs);

  // return true on success, false otherwise
  bool create(const std::string & db);

  DBClientProxy(const std::string & host, int port);
  ~DBClientProxy();

 private:
  // some internal help functions
  void cleanUp(void);
  void open(const std::string & db);
  std::map<std::string, Trocksdb::DBHandle>::iterator getHandle(const std::string & db);

  const std::string host_;
  const int port_;
  std::map<std::string, Trocksdb::DBHandle> dbToHandle_;
  boost::shared_ptr<Trocksdb::DBClient> dbClient_;
};

} // namespace
#endif
