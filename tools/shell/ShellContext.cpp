
#include <iostream>
#include <boost/shared_ptr.hpp>

#include "ShellContext.h"
#include "ShellState.h"



#include "thrift/lib/cpp/protocol/TBinaryProtocol.h"
#include "thrift/lib/cpp/transport/TSocket.h"
#include "thrift/lib/cpp/transport/TTransportUtils.h"



using namespace std;
using namespace boost;
using namespace Tleveldb;
using namespace rocksdb;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

void ShellContext::changeState(ShellState * pState) {
  pShellState_ = pState;
}

void ShellContext::stop(void) {
  exit_ = true;
}

bool ShellContext::ParseInput(void) {
  if(argc_ != 3) {
    printf("leveldb_shell host port\n");
    return false;
  }

  port_ = atoi(argv_[2]);
  if(port_ <= 0) {
    printf("Error while parse port : %s\n", argv_[2]);
    return false;
  }

  clientProxy_.reset(new DBClientProxy(argv_[1], port_));
  if(!clientProxy_.get()) {
    return false;
  } else {
    return true;
  }
}

void ShellContext::connect(void) {
  clientProxy_->connect();
}

void ShellContext::create(const string & db) {
  if (clientProxy_->create(db)) {
    printf("%s created\n", db.c_str());
  }
}

void ShellContext::get(const string & db,
                       const string & key) {
  string v;
  if (clientProxy_->get(db, key, v)) {
    printf("%s\n", v.c_str());
  }
}

void ShellContext::put(const string & db,
                       const string & key,
                       const string & value) {
  if (clientProxy_->put(db, key, value)) {
    printf("(%s, %s) has been set\n", key.c_str(), value.c_str());
  }
}

void ShellContext::scan(const string & db,
                        const string & start_key,
                        const string & end_key,
                        const string & limit) {
  vector<pair<string, string> > kvs;
  if (clientProxy_->scan(db, start_key, end_key, limit, kvs)) {
    for(unsigned int i = 0; i < kvs.size(); ++i) {
      printf("%d (%s, %s)\n", i, kvs[i].first.c_str(), kvs[i].second.c_str());
    }
  }
}

void ShellContext::run(void) {
  while(!exit_) {
    pShellState_->run(this);
  }
}

ShellContext::ShellContext(int argc, char ** argv) :
  pShellState_(ShellStateStart::getInstance()),
  exit_(false),
  argc_(argc),
  argv_(argv),
  port_(-1),
  clientProxy_() {
}


