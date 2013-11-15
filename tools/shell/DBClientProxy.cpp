
#include <boost/shared_ptr.hpp>

#include "DBClientProxy.h"


#include "thrift/lib/cpp/protocol/TBinaryProtocol.h"
#include "thrift/lib/cpp/transport/TSocket.h"
#include "thrift/lib/cpp/transport/TTransportUtils.h"



using namespace std;
using namespace boost;
using namespace Tleveldb;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace rocksdb {

DBClientProxy::DBClientProxy(const string & host, int port) :
  host_(host),
  port_(port),
  dbToHandle_(),
  dbClient_() {
}

DBClientProxy::~DBClientProxy() {
  cleanUp();
}


void DBClientProxy::connect(void) {
  cleanUp();
  printf("Connecting to %s:%d\n", host_.c_str(), port_);
  try {
    boost::shared_ptr<TSocket> socket(new TSocket(host_, port_));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));
    dbClient_.reset(new DBClient(protocol));

    transport->open();
  } catch (const std::exception & e) {
    dbClient_.reset();
    throw;
  }
}

void DBClientProxy::cleanUp(void) {
  if(dbClient_.get()) {
    for(map<string, DBHandle>::iterator itor = dbToHandle_.begin();
        itor != dbToHandle_.end();
        ++itor) {
      dbClient_->Close(itor->second, itor->first);
    }
    dbClient_.reset();
  }
  dbToHandle_.clear();
}

void DBClientProxy::open(const string & db) {
  if(!dbClient_.get()) {
    printf("please connect() first\n");
    return;
  }

  //  printf("opening database : %s\n", db.c_str());
  // we use default DBOptions here
  DBOptions opt;
  DBHandle handle;
  try {
    dbClient_->Open(handle, db, opt);
  } catch (const LeveldbException & e) {
    printf("%s\n", e.message.c_str());
    if(kIOError == e.errorCode) {
      printf("no such database : %s\n", db.c_str());
      return;
    }else {
      printf("Unknown error : %d\n", e.errorCode);
      return;
    }
  }

  dbToHandle_[db] = handle;
}


bool DBClientProxy::create(const string & db) {
  if(!dbClient_.get()) {
    printf("please connect() first\n");
    return false;
  }

  printf("creating database : %s\n", db.c_str());
  DBOptions opt;
  opt.create_if_missing = true;
  opt.error_if_exists = true;
  DBHandle handle;
  try {
    dbClient_->Open(handle, db, opt);
  }catch (const LeveldbException & e) {
    printf("%s\n", e.message.c_str());
    printf("error code : %d\n", e.errorCode);
    if(kNotFound == e.errorCode) {
      printf("no such database : %s\n", db.c_str());
      return false;;
    } else {
      printf("Unknown error : %d\n", e.errorCode);
      return false;
    }
  }

  dbToHandle_[db] = handle;
  return true;
}


map<string, DBHandle>::iterator
DBClientProxy::getHandle(const string & db) {
  map<string, DBHandle>::iterator itor = dbToHandle_.find(db);
  if(dbToHandle_.end() == itor) {
    open(db);
    itor = dbToHandle_.find(db);
  }

  return itor;
}


bool DBClientProxy::get(const string & db,
                        const string & key,
                        string & value) {
  if(!dbClient_.get()) {
    printf("please connect() first\n");
    return false;
  }

  map<string, DBHandle>::iterator itor = getHandle(db);
  if(dbToHandle_.end() == itor) {
    return false;
  }

  ResultItem ret;
  Slice k;
  k.data = key;
  k.size = key.size();
  // we use default values of options here
  ReadOptions opt;
  dbClient_->Get(ret,
                 itor->second,
                 k,
                 opt);
  if(kOk == ret.status) {
    value = ret.value.data;
    return true;
  } else if(kNotFound == ret.status) {
    printf("no such key : %s\n", key.c_str());
    return false;
  } else {
    printf("get data error : %d\n", ret.status);
    return false;
  }
}



bool DBClientProxy::put(const string & db,
                        const string & key,
                        const string & value) {
  if(!dbClient_.get()) {
    printf("please connect() first\n");
    return false;
  }

  map<string, DBHandle>::iterator itor = getHandle(db);
  if(dbToHandle_.end() == itor) {
    return false;
  }
 
  kv temp;
  temp.key.data = key;
  temp.key.size = key.size();
  temp.value.data = value;
  temp.value.size = value.size();
  WriteOptions opt;
  opt.sync = true;
  Code code;
  code = dbClient_->Put(itor->second,
                        temp,
                        opt);


  if(kOk == code) {
    //    printf("set value finished\n");
    return true;
  } else {
    printf("put data error : %d\n", code);
    return false;
  }
}

bool DBClientProxy::scan(const string & db,
                         const string & start_key,
                         const string & end_key,
                         const string & limit,
                         vector<pair<string, string> > & kvs) {
  if(!dbClient_.get()) {
    printf("please connect() first\n");
    return false;
  }

  int limitInt = -1;
  limitInt = atoi(limit.c_str());
  if(limitInt <= 0) {
    printf("Error while parse limit : %s\n", limit.c_str());
    return false;
  }

  if(start_key > end_key) {
    printf("empty range.\n");
    return false;
  }

  map<string, DBHandle>::iterator itor = getHandle(db);
  if(dbToHandle_.end() == itor) {
    return false;
  }

  ResultIterator ret;
  // we use the default values of options here
  ReadOptions opt;
  Slice k;
  k.data = start_key;
  k.size = start_key.size();
  dbClient_->NewIterator(ret,
                         itor->second,
                         opt,
                         seekToKey,
                         k);
  Iterator it;
  if(kOk == ret.status) {
    it = ret.iterator;
  } else {
    printf("get iterator error : %d\n", ret.status);
    return false;
  }

  int idx = 0;
  string ck = start_key;
  while(idx < limitInt && ck < end_key) {
    ResultPair retPair;
    dbClient_->GetNext(retPair, itor->second, it);
    if(kOk == retPair.status) {
      ++idx;
      ck = retPair.keyvalue.key.data;
      if (ck < end_key) {
        kvs.push_back(make_pair(retPair.keyvalue.key.data,
                                retPair.keyvalue.value.data));
      }
    } else if(kEnd == retPair.status) {
      printf("not enough values\n");
      return true;
    } else {
      printf("GetNext() error : %d\n", retPair.status);
      return false;
    }
  }
  return true;
}

} // namespace
