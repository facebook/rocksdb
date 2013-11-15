#ifndef TOOLS_SHELL_SHELLCONTEXT
#define TOOLS_SHELL_SHELLCONTEXT

#include <map>
#include <string>
#include <boost/utility.hpp>
#include <boost/shared_ptr.hpp>

#include "DB.h"
#include "DBClientProxy.h"

class ShellState;

class ShellContext : private boost::noncopyable {
 public:
  void changeState(ShellState * pState);

  void stop(void);

  bool ParseInput(void);

  void connect(void);

  void get(const std::string & db,
           const std::string & key);

  void put(const std::string & db,
           const std::string & key,
           const std::string & value);

  void scan(const std::string & db,
            const std::string & start_key,
            const std::string & end_key,
            const std::string & limit);

  void create(const std::string & db);

  void run(void);

  ShellContext(int argc, char ** argv);

 private:
  ShellState * pShellState_;
  bool exit_;
  int argc_;
  char ** argv_;
  int port_;
  boost::shared_ptr<rocksdb::DBClientProxy> clientProxy_;
};

#endif
