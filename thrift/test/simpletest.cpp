/**
 * Tests for Thrift server for leveldb
 * @author Dhruba Borthakur (dhruba@gmail.com)
 * Copyright 2012 Facebook
 */
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <DB.h>
#include <leveldb_types.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using boost::shared_ptr;
using namespace Tleveldb;

extern "C" void startServer(int port);
extern "C" void stopServer(int port);

static DBHandle dbhandle;
static DBClient* dbclient;
static const Text dbname = "/tmp/leveldb/test";
static const int myport = 9091;
static pthread_t serverThread;

static void createDatabase() {
  DBOptions options;
  options.create_if_missing = true; // create
  options.error_if_exists = false;  // overwrite
  options.write_buffer_size = (4<<20); // 4 MB
  options.max_open_files = 1000;
  options.block_size = 4096;
  options.block_restart_interval = 16;
  options.compression = kSnappyCompression;

  // create the database
  dbclient->Open(dbhandle, dbname, options);
}

static void testClient(int port) {
  boost::shared_ptr<TSocket> socket(new TSocket("localhost", port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  WriteOptions writeOptions;

  // open database
  dbclient = new DBClient(protocol);
  transport->open();
  createDatabase();
  printf("Database created.\n");

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
  assert(ret == Code::kOk);
  printf("Put Key1 suceeded.\n");

  //read it back
  ReadOptions readOptions;
  ResultItem rValue;
  dbclient->Get(rValue, dbhandle, key, readOptions);
  assert(rValue.status == Code::kOk);
  assert(value.data.compare(rValue.value.data) == 0);
  assert(value.size == rValue.value.size);
  printf("Get suceeded.\n");

  // get a snapshot
  ResultSnapshot rsnap;
  dbclient->GetSnapshot(rsnap, dbhandle);
  assert(rsnap.status == Code::kOk);
  assert(rsnap.snapshot.snapshotid > 0);
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
  assert(ret == Code::kOk);
  printf("Put Key2 suceeded.\n");

  // verify that a get done with a previous snapshot does not find Key2
  readOptions.snapshot = rsnap.snapshot;
  dbclient->Get(rValue, dbhandle, key2, readOptions);
  assert(rValue.status == Code::kNotFound);
  printf("Get with snapshot succeeded.\n");

  // release snapshot
  ret = dbclient->ReleaseSnapshot(dbhandle, rsnap.snapshot);
  assert(ret == Code::kOk);
  printf("Snapshot released.\n");

  // if we try to re-release the same snapshot, it should fail
  ret = dbclient->ReleaseSnapshot(dbhandle, rsnap.snapshot);
  assert(ret == Code::kNotFound);
  
  // compact whole database
  Slice range;
  range.size = 0;
  ret = dbclient->CompactRange(dbhandle, range, range);
  assert(ret == Code::kOk);
  printf("Compaction trigger suceeded.\n");

  // create a new iterator to scan all keys from the start
  Slice target;
  ResultIterator ri;
  readOptions.snapshot.snapshotid = 0;
  dbclient->NewIterator(ri, dbhandle, readOptions,
                        IteratorType::seekToFirst, target);
  assert(ri.status == Code::kOk);
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
  assert(foundPairs == 2);
  ret = dbclient->DeleteIterator(dbhandle, ri.iterator);
  assert(ret == Code::kOk);
  printf("Iterator scan-all forward passes.\n");

  // create a new iterator, position at end and scan backwards
  readOptions.snapshot.snapshotid = 0;
  dbclient->NewIterator(ri, dbhandle, readOptions,
                        IteratorType::seekToLast, target);
  assert(ri.status == Code::kOk);
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
  assert(foundPairs == 2);
  ret = dbclient->DeleteIterator(dbhandle, ri.iterator);
  assert(ret == Code::kOk);
  printf("Iterator scan-all backward passes.\n");
  
  // create a new iterator, position at middle
  readOptions.snapshot.snapshotid = 0;
  target = key;
  dbclient->NewIterator(ri, dbhandle, readOptions,
                        IteratorType::seekToKey, target);
  assert(ri.status == Code::kOk);
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
  assert(foundPairs == 1);
  ret = dbclient->DeleteIterator(dbhandle, ri.iterator);
  assert(ret == Code::kOk);
  printf("Iterator scan-selective backward passes.\n");
  
  // close database
  dbclient->Close(dbhandle, dbname);
  transport->close();
}


static void* startTestServer(void *arg) {
  printf("Server starting on port %d...\n", myport);
  startServer(myport);
}

int main(int argc, char **argv) {

  // remove old data, if any
  int ret = unlink(dbname.c_str());
  char* cleanup = new char[100];
  snprintf(cleanup, 100, "rm -rf %s", dbname.c_str());
  system(cleanup);

  // create a server
  int rc = pthread_create(&serverThread, NULL, startTestServer, NULL);
  printf("Server thread created.\n");

  // give some time to the server to initialize itself
  sleep(1);

  // test client
  testClient(myport);

  return 0;
}

