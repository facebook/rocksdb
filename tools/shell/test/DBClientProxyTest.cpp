/**
 * Tests for DBClientProxy class for leveldb
 * @author Bo Liu (newpoo.liu@gmail.com)
 * Copyright 2012 Facebook
 */

#include <algorithm>
#include <vector>
#include <string>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <util/testharness.h>
#include <DB.h>
#include <AssocService.h>
#include <leveldb_types.h>

#include "server_options.h"


#include "../DBClientProxy.h"
using namespace rocksdb;


using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using boost::shared_ptr;
using namespace Tleveldb;
using namespace std;



extern "C" void startServer(int argc, char**argv);
extern "C" void stopServer(int port);
extern  ServerOptions server_options;

static const string db1("db1");


static void testDBClientProxy(DBClientProxy & dbcp) {
  bool flag;
  const int NOK = 100;
  const int BUFSIZE = 16;
  int testcase = 0;

  vector<string> keys, values;
  vector<pair<string, string> > kvs, correctKvs;
  string k, v;

  for(int i = 0; i < NOK; ++i) {
    char bufKey[BUFSIZE];
    char bufValue[BUFSIZE];
    snprintf(bufKey, BUFSIZE, "key%d", i);
    snprintf(bufValue, BUFSIZE, "value%d", i);
    keys.push_back(bufKey);
    values.push_back(bufValue);
    correctKvs.push_back((make_pair(string(bufKey), string(bufValue))));
  }

  sort(correctKvs.begin(), correctKvs.end());


  // can not do get(), put(), scan() or create() before connected.
  flag = dbcp.get(db1, keys[0], v);
  ASSERT_TRUE(false == flag);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);
  flag = dbcp.put(db1, keys[0], keys[1]);
  ASSERT_TRUE(false == flag);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);
  flag = dbcp.scan(db1, "a", "w", "100", kvs);
  ASSERT_TRUE(false == flag);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);
  flag = dbcp.create(db1);
  ASSERT_TRUE(false == flag);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);

  dbcp.connect();

  // create a database
  flag = dbcp.create(db1);
  ASSERT_TRUE(true == flag);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);

  // no such key
  flag = dbcp.get(db1, keys[0], v);
  ASSERT_TRUE(false == flag);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);


  // scan() success with empty returned key-value pairs
  kvs.clear();
  flag = dbcp.scan(db1, "a", "w", "100", kvs);
  ASSERT_TRUE(true == flag);
  ASSERT_TRUE(kvs.empty());
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);


  // put()
  for(int i = 0; i < NOK; ++i) {
    flag = dbcp.put(db1, keys[i], values[i]);
    ASSERT_TRUE(true == flag);
  }
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);


  // scan all of key-value pairs
  kvs.clear();
  flag = dbcp.scan(db1, "a", "w", "100", kvs);
  ASSERT_TRUE(true == flag);
  ASSERT_TRUE(kvs == correctKvs);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);


  // scan the first 20 key-value pairs
  {
    kvs.clear();
    flag = dbcp.scan(db1, "a", "w", "20", kvs);
    ASSERT_TRUE(true == flag);
    vector<pair<string, string> > tkvs(correctKvs.begin(), correctKvs.begin() + 20);
    ASSERT_TRUE(kvs == tkvs);
    printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);
  }

  // scan key[10] to key[50]
  {
    kvs.clear();
    flag = dbcp.scan(db1, correctKvs[10].first, correctKvs[50].first, "100", kvs);
    ASSERT_TRUE(true == flag);

    vector<pair<string, string> > tkvs(correctKvs.begin() + 10, correctKvs.begin() + 50);
    ASSERT_TRUE(kvs == tkvs);
    printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);
  }

  // scan "key10" to "key40" by limit constraint
  {
    kvs.clear();
    flag = dbcp.scan(db1, correctKvs[10].first.c_str(), "w", "30", kvs);
    ASSERT_TRUE(true == flag);
    vector<pair<string, string> > tkvs(correctKvs.begin() + 10, correctKvs.begin() + 40);
    ASSERT_TRUE(kvs == tkvs);
    printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);
  }


  // get()
  flag = dbcp.get(db1, "unknownKey", v);
  ASSERT_TRUE(false == flag);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);

  flag = dbcp.get(db1, keys[0], v);
  ASSERT_TRUE(true == flag);
  ASSERT_TRUE(v == values[0]);
  printf("\033[01;40;32mTEST CASE %d passed\033[01;40;37m\n", ++testcase);
}



static void cleanupDir(std::string dir) {
  // remove old data, if any
  char* cleanup = new char[100];
  snprintf(cleanup, 100, "rm -rf %s", dir.c_str());
  system(cleanup);
}

int main(int argc, char **argv) {
  // create a server
  startServer(argc, argv);
  printf("Server thread created.\n");

  // give some time to the server to initialize itself
  while (server_options.getPort() == 0) {
    sleep(1);
  }

  cleanupDir(server_options.getDataDirectory(db1));

  DBClientProxy dbcp("localhost", server_options.getPort());
  testDBClientProxy(dbcp);
}

