/**
 * Tests for Thrift server for leveldb
 * @author Dhruba Borthakur (dhruba@gmail.com)
 * Copyright 2012 Facebook
 */
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <util/testharness.h>
#include <DB.h>
#include <AssocService.h>
#include <leveldb_types.h>
#include "server_options.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using boost::shared_ptr;
using namespace Tleveldb;

extern "C" void startServer(int argc, char**argv);
extern "C" void stopServer(int port);
extern  ServerOptions server_options;

static DBHandle dbhandle;
static DBClient* dbclient;
static AssocServiceClient* aclient;
static const Text dbname = "test-dhruba";
static pthread_t serverThread;
static int ARGC;
static char** ARGV;

static void cleanupDir(std::string dir) {
  // remove old data, if any
  int ret = unlink(dir.c_str());
  char* cleanup = new char[100];
  snprintf(cleanup, 100, "rm -rf %s", dir.c_str());
  system(cleanup);
}

static void createDatabase() {
  DBOptions options;
  options.create_if_missing = true; // create
  options.error_if_exists = false;  // overwrite
  options.write_buffer_size = (4<<20); // 4 MB
  options.max_open_files = 1000;
  options.block_size = 4096;
  options.block_restart_interval = 16;
  options.compression = kSnappyCompression;

  cleanupDir(server_options.getDataDirectory(dbname));

  // create the database
  dbclient->Open(dbhandle, dbname, options);
}

static void initialize(int port) {
  boost::shared_ptr<TSocket> socket1(new TSocket("localhost", port));
  boost::shared_ptr<TTransport> transport1(new TBufferedTransport(socket1));
  boost::shared_ptr<TProtocol> protocol1(new TBinaryProtocol(transport1));

  // open database
  dbclient = new DBClient(protocol1);
  transport1->open();

  boost::shared_ptr<TSocket> socket2(new TSocket("localhost", port+1));
  boost::shared_ptr<TTransport> transport2(new TBufferedTransport(socket2));
  boost::shared_ptr<TProtocol> protocol2(new TBinaryProtocol(transport2));
  aclient = new AssocServiceClient(protocol2);
  transport2->open();

  createDatabase();
  printf("Database created.\n");
}

//
// Run base leveldb thrift server get/put/iter/scan tests
//
static void testClient() {
  WriteOptions writeOptions;
  printf("Running base leveldb operations .................\n");

  // insert record into leveldb
  Slice key;
  key.data = "Key1";
  key.size = 4;
  Slice value;
  value.data = "value1";
  value.size = 6;
  kv keyvalue;
  keyvalue.key = key;
  keyvalue.value = value;
  int ret = dbclient->Put(dbhandle, keyvalue, writeOptions);
  ASSERT_TRUE(ret == Code::kOk);
  printf("Put Key1 suceeded.\n");

  //read it back
  ReadOptions readOptions;
  ResultItem rValue;
  dbclient->Get(rValue, dbhandle, key, readOptions);
  ASSERT_TRUE(rValue.status == Code::kOk);
  ASSERT_TRUE(value.data.compare(rValue.value.data) == 0);
  ASSERT_TRUE(value.size == rValue.value.size);
  printf("Get suceeded.\n");

  // get a snapshot
  ResultSnapshot rsnap;
  dbclient->GetSnapshot(rsnap, dbhandle);
  ASSERT_TRUE(rsnap.status == Code::kOk);
  ASSERT_TRUE(rsnap.snapshot.snapshotid > 0);
  printf("Snapshot suceeded.\n");

  // insert a new record into leveldb
  Slice key2;
  key2.data = "Key2";
  key2.size = 4;
  Slice value2;
  value2.data = "value2";
  value2.size = 6;
  keyvalue.key = key2;
  keyvalue.value = value2;
  ret = dbclient->Put(dbhandle, keyvalue, writeOptions);
  ASSERT_TRUE(ret == Code::kOk);
  printf("Put Key2 suceeded.\n");

  // verify that a get done with a previous snapshot does not find Key2
  readOptions.snapshot = rsnap.snapshot;
  dbclient->Get(rValue, dbhandle, key2, readOptions);
  ASSERT_TRUE(rValue.status == Code::kNotFound);
  printf("Get with snapshot succeeded.\n");

  // release snapshot
  ret = dbclient->ReleaseSnapshot(dbhandle, rsnap.snapshot);
  ASSERT_TRUE(ret == Code::kOk);
  printf("Snapshot released.\n");

  // if we try to re-release the same snapshot, it should fail
  ret = dbclient->ReleaseSnapshot(dbhandle, rsnap.snapshot);
  ASSERT_TRUE(ret == Code::kNotFound);
  
  // compact whole database
  Slice range;
  range.size = 0;
  ret = dbclient->CompactRange(dbhandle, range, range);
  ASSERT_TRUE(ret == Code::kOk);
  printf("Compaction trigger suceeded.\n");

  // create a new iterator to scan all keys from the start
  Slice target;
  ResultIterator ri;
  readOptions.snapshot.snapshotid = 0;
  dbclient->NewIterator(ri, dbhandle, readOptions,
                        IteratorType::seekToFirst, target);
  ASSERT_TRUE(ri.status == Code::kOk);
  int foundPairs = 0;
  while (true) {
    ResultPair pair;
    dbclient->GetNext(pair, dbhandle, ri.iterator);
    if (pair.status == Code::kOk) {
      foundPairs++;
    } else {
      break;
    }
  }
  ASSERT_TRUE(foundPairs == 2);
  ret = dbclient->DeleteIterator(dbhandle, ri.iterator);
  ASSERT_TRUE(ret == Code::kOk);
  printf("Iterator scan-all forward passes.\n");

  // create a new iterator, position at end and scan backwards
  readOptions.snapshot.snapshotid = 0;
  dbclient->NewIterator(ri, dbhandle, readOptions,
                        IteratorType::seekToLast, target);
  ASSERT_TRUE(ri.status == Code::kOk);
  foundPairs = 0;
  while (true) {
    ResultPair pair;
    dbclient->GetPrev(pair, dbhandle, ri.iterator);
    if (pair.status == Code::kOk) {
      foundPairs++;
    } else {
      break;
    }
  }
  ASSERT_TRUE(foundPairs == 2);
  ret = dbclient->DeleteIterator(dbhandle, ri.iterator);
  ASSERT_TRUE(ret == Code::kOk);
  printf("Iterator scan-all backward passes.\n");
  
  // create a new iterator, position at middle
  readOptions.snapshot.snapshotid = 0;
  target = key;
  dbclient->NewIterator(ri, dbhandle, readOptions,
                        IteratorType::seekToKey, target);
  ASSERT_TRUE(ri.status == Code::kOk);
  foundPairs = 0;
  while (true) {
    ResultPair pair;
    dbclient->GetPrev(pair, dbhandle, ri.iterator);
    if (pair.status == Code::kOk) {
      foundPairs++;
    } else {
      break;
    }
  }
  ASSERT_TRUE(foundPairs == 1);
  ret = dbclient->DeleteIterator(dbhandle, ri.iterator);
  ASSERT_TRUE(ret == Code::kOk);
  printf("Iterator scan-selective backward passes.\n");
}

//
// Run assoc tests
//
static void testAssocs() {
  WriteOptions writeOptions;
  printf("Running assoc leveldb operations ................\n");

  // insert record into leveldb
  int64_t assocType = 100;
  int64_t id1 = 1;
  int64_t id2 = 2;
  int64_t id1Type = 101;
  int64_t id2Type = 102;
  int64_t ts =3333;
  AssocVisibility vis = AssocVisibility::VISIBLE;
  bool update_count = true;
  int64_t dataVersion = 5;
  const Text data = "data......";
  const Text wormhole_comments = "wormhole...";
  int64_t count = aclient->taoAssocPut(dbname, assocType,
           id1, id2, id1Type, id2Type, 
           ts, vis, update_count,
           dataVersion, data, wormhole_comments);
  ASSERT_GE(count, 0);
  printf("Put AssocPut suceeded.\n");

  // verify assoc counts.
  int64_t cnt = aclient->taoAssocCount(dbname, assocType, id1); 
  ASSERT_EQ(cnt, 1);
  printf("AssocCount suceeded.\n");

  // verify that we can read back what we inserted earlier
  std::vector<int64_t> id2list(1);
  id2list[0] = id2;
  std::vector<TaoAssocGetResult> readback(1);
  aclient->taoAssocGet(readback, dbname,
                       assocType, id1, id2list);
  printf("AssocGet suceeded.\n");
  printf("size = %lld\n", readback.size());
  ASSERT_EQ(1, readback.size());
  ASSERT_EQ(id1Type, readback[0].id1Type);
  printf("XXX %lld %lld\n", id1Type, readback[0].id1Type);
  ASSERT_EQ(id2Type, readback[0].id2Type);
  ASSERT_EQ(ts, readback[0].time);
  ASSERT_EQ(dataVersion, readback[0].dataVersion);
  ASSERT_EQ(readback[0].data.compare(wormhole_comments), 0);

}

//
// close all resources
//
static void close() {
  // close database
  dbclient->Close(dbhandle, dbname);
  // transport->close();
}


int main(int argc, char **argv) {

  ARGC = argc;
  ARGV = argv;

  // create a server
  startServer(argc, argv);
  printf("Server thread created.\n");

  // give some time to the server to initialize itself
  while (server_options.getPort() == 0) {
    sleep(1);
  }

  // test client
  initialize(server_options.getPort());

  // run all tests
  testClient();
  testAssocs();

  // done all tests
  close();

  return 0;
}

