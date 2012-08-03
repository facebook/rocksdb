/**
 * Thrift server for leveldb
 * @author Dhruba Borthakur (dhruba@gmail.com)
 * Copyright 2012 Facebook
 */

#include <signal.h>
#include <DB.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TConnectionContext.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <async/TEventServer.h>
#include <async/TAsyncProcessor.h>
#include <async/TSyncToAsyncProcessor.h>
#include <util/TEventServerCreator.h>
#include <leveldb_types.h>
#include "openhandles.h"
#include "server_options.h"
#include "assoc.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

using namespace apache::thrift;
using namespace apache::thrift::util;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::async;
using namespace  Tleveldb;
using boost::shared_ptr;

extern "C" void startServer(int argc, char** argv);
extern "C" void stopServer(int port);

static boost::shared_ptr<TServer> baseServer;
static boost::shared_ptr<TServer> assocServer;

// The global object that stores the default configuration of the server
ServerOptions server_options;

class DBHandler : virtual public DBIf {
 public:
  DBHandler(OpenHandles* oh) {
    openHandles = oh;
  }

  void Open(DBHandle& _return, const Text& dbname, 
    const DBOptions& dboptions) {
    printf("Open %s\n", dbname.c_str());
    if (!server_options.isValidName(dbname)) {
      LeveldbException e;
      e.errorCode = Code::kInvalidArgument;
      e.message = "Bad DB name";
      fprintf(stderr, "Bad DB name %s\n", dbname.c_str());
      throw e;
    }
    std::string dbdir = server_options.getDataDirectory(dbname);
    leveldb::Options options;

    // fill up per-server options
    options.block_cache = server_options.getCache();

    // fill up  per-DB options
    options.create_if_missing = dboptions.create_if_missing;
    options.error_if_exists = dboptions.error_if_exists;
    options.write_buffer_size = dboptions.write_buffer_size;
    options.max_open_files = dboptions.max_open_files;
    options.block_size = dboptions.block_size;
    options.block_restart_interval = dboptions.block_restart_interval;
    if (dboptions.compression == kNoCompression) {
      options.compression = leveldb::kNoCompression;
    } else if (dboptions.compression == kSnappyCompression) {
      options.compression = leveldb::kSnappyCompression;
    }
    openHandles->add(options, dbname, dbdir);
    _return.dbname = dbname;
  }

  Code Close(const DBHandle& dbhandle, const Text& dbname) {
    //
    // We do not close any handles for now, otherwise we have to do
    // some locking that will degrade performance in the normal case.
    //
    return Code::kNotSupported;
  }

  Code Put(const DBHandle& dbhandle, const kv& kv, 
    const WriteOptions& options) {
    leveldb::WriteOptions woptions;
    woptions.sync = options.sync;
    leveldb::Slice key, value;
    key.data_ = kv.key.data.data();
    key.size_ = kv.key.size;
    value.data_ = kv.value.data.data();
    value.size_ = kv.value.size;
    leveldb::DB* db = openHandles->get(dbhandle.dbname, NULL);
    if (db == NULL) {
      return Code::kNotFound;
    }
    leveldb::Status status = db->Put(woptions, key, value);
    if (status.ok()) {
      return Code::kOk;
    }
    return Code::kIOError;
  }

  Code Delete(const DBHandle& dbhandle, const Slice& kv, 
    const WriteOptions& options) {
    leveldb::WriteOptions woptions;
    woptions.sync = options.sync;
    leveldb::Slice key;
    key.data_ = kv.data.data();
    key.size_ = kv.size;
    leveldb::DB* db = openHandles->get(dbhandle.dbname, NULL);
    if (db == NULL) {
      return Code::kNotFound;
    }
    leveldb::Status status = db->Delete(woptions, key);
    if (status.ok()) {
      return Code::kOk;
    }
    return Code::kIOError;
  }

  Code Write(const DBHandle& dbhandle, const std::vector<kv> & batch, 
    const WriteOptions& options) {
    leveldb::WriteOptions woptions;
    leveldb::WriteBatch lbatch;
    woptions.sync = options.sync;
    leveldb::Slice key, value;
    for (unsigned int i = 0; i < batch.size(); i++) {
      kv one = batch[i];
      key.data_ = one.key.data.data();
      key.size_ = one.key.size;
      value.data_ = one.value.data.data();
      value.size_ = one.value.size;
      lbatch.Put(key, value);
    }
    leveldb::DB* db = openHandles->get(dbhandle.dbname, NULL);
    if (db == NULL) {
      return Code::kNotFound;
    }
    leveldb::Status status = db->Write(woptions, &lbatch);
    if (status.ok()) {
      return Code::kOk;
    }
    return Code::kIOError;
  }

  void Get(ResultItem& _return, const DBHandle& dbhandle, const Slice& inputkey, 
    const ReadOptions& options) {
    struct onehandle* thishandle;
    _return.status = Code::kNotFound;
    std::string ret;
    leveldb::Slice ikey;
    ikey.data_ = inputkey.data.data();
    ikey.size_ = inputkey.size;
    leveldb::DB* db = openHandles->get(dbhandle.dbname, &thishandle);
    if (db == NULL) {
      return;
    }
    assert(thishandle != NULL);
    const leveldb::Snapshot* s = NULL;
    if (options.snapshot.snapshotid > 0) {
      s = thishandle->lookupSnapshot(options.snapshot.snapshotid);
      assert(s != NULL);
      if (s == NULL) {
        return;
      }
    }
    leveldb::ReadOptions roptions;
    roptions.verify_checksums = options.verify_checksums;
    roptions.fill_cache = options.fill_cache;
    roptions.snapshot = s;

    leveldb::Status status = db->Get(roptions, ikey, &ret);
    if (status.ok()) {
      _return.value.data = ret.data();
      _return.value.size = ret.size();
      _return.status  = Code::kOk;
    }
  }

  void NewIterator(ResultIterator& _return, const DBHandle& dbhandle, 
     const ReadOptions& options, IteratorType iteratorType, 
     const Slice& target)  {
    struct onehandle* thishandle;
    _return.status = Code::kNotFound;
    leveldb::DB* db = openHandles->get(dbhandle.dbname, &thishandle);
    if (db == NULL) {
      return;
    }
    assert(thishandle != NULL);

    // check to see if snapshot is specified
    const leveldb::Snapshot* s = NULL;
    if (options.snapshot.snapshotid > 0) {
      s = thishandle->lookupSnapshot(options.snapshot.snapshotid);
      assert(s != NULL);
      if (s == NULL) {
        return;
      }
    }

    // create leveldb iterator
    leveldb::ReadOptions roptions;
    roptions.verify_checksums = options.verify_checksums;
    roptions.fill_cache = options.fill_cache;
    roptions.snapshot = s;
    leveldb::Iterator* iter = db->NewIterator(roptions);
    if (iter == NULL) {
      return;
    }

    // position iterator at right place
    if (iteratorType == IteratorType::seekToFirst) {
      iter->SeekToFirst();
    } else if (iteratorType == IteratorType::seekToLast) {
      iter->SeekToLast();
    } else if (iteratorType == IteratorType::seekToKey) {
      leveldb::Slice key;
      key.data_ = target.data.data();
      key.size_ = target.size;
      iter->Seek(key);
    } else {
      delete iter;
      _return.status = Code::kInvalidArgument;
      return;
    }

    // insert iterator into openhandle list, get unique id
    int64_t id = thishandle->addIterator(iter);
    _return.iterator.iteratorid = id;
    _return.status = kOk;
  }

  // Delete existing iterator
  Code DeleteIterator(const DBHandle& dbhandle, const Iterator& iterator) {
    // find the db
    struct onehandle* thishandle;
    leveldb::DB* db = openHandles->get(dbhandle.dbname, &thishandle);
    if (db == NULL) {
      return kNotFound;
    }
    assert(thishandle != NULL);

    // find the leveldb iterator for this db
    leveldb::Iterator* it = 
      thishandle->lookupIterator(iterator.iteratorid);
    if (it == NULL) {
      // this must have been cleaned up by the last call to GetNext
      return kOk;
    }
    thishandle->removeIterator(iterator.iteratorid);
    delete it;                  // cleanup
    return kOk;
  }

  // read the next value from the iterator
  void GetAnother(ResultPair& _return, const DBHandle& dbhandle, 
    const Iterator& iterator, const bool doNext)  {

    // find the db
    struct onehandle* thishandle;
    _return.status = Code::kNotFound;
    leveldb::DB* db = openHandles->get(dbhandle.dbname, &thishandle);
    if (db == NULL) {
      return;
    }
    assert(thishandle != NULL);

    // find the leveldb iterator for this db
    leveldb::Iterator* it = 
      thishandle->lookupIterator(iterator.iteratorid);
    assert(it != NULL);
    if (it == NULL) {
      return;
    }

    // If the iterator has reached the end close it rightaway.
    // There is no need for the application to make another thrift
    // call to cleanup the iterator.
    if (!it->Valid()) {
       thishandle->removeIterator(iterator.iteratorid);
       delete it;                  // cleanup
      _return.status = Code::kEnd; // no more elements
      return;
    }

    // if iterator has encountered any corruption
    if (!it->status().ok()) {
       thishandle->removeIterator(iterator.iteratorid);
       delete it;                  // cleanup
      _return.status = Code::kIOError; // error in data
      return;
    }

    // find current key-value
    leveldb::Slice key = it->key();
    leveldb::Slice value = it->value();

    // pack results back to client
    _return.keyvalue.key.data.assign(key.data_, key.size_);
    _return.keyvalue.key.size = key.size_;
    _return.keyvalue.value.data.assign(value.data_, value.size_);
    _return.keyvalue.value.size = value.size_;
    _return.status = Code::kOk;  // success

    // move to next or previous value
    if (doNext) {
      it->Next();
    } else {
      it->Prev();
    }
  }

  // read the next value from the iterator
  void GetNext(ResultPair& _return, const DBHandle& dbhandle, 
    const Iterator& iterator)  {
    GetAnother(_return, dbhandle, iterator, 1);
  }

  // read the prev value from the iterator
  void GetPrev(ResultPair& _return, const DBHandle& dbhandle, 
    const Iterator& iterator)  {
    GetAnother(_return, dbhandle, iterator, 0);
  }

  void GetSnapshot(ResultSnapshot& _return, const DBHandle& dbhandle) {
    _return.status = kIOError;
    struct onehandle* thishandle;
    leveldb::DB* db = openHandles->get(dbhandle.dbname, &thishandle);
    if (db == NULL) {
      return;
    }
    // create leveldb snapshot
    const leveldb::Snapshot* s = db->GetSnapshot();

    // store snapshot in dbhandle, get unique id.
    int64_t id = thishandle->addSnapshot(s);
    _return.snapshot.snapshotid = id;
    _return.status = kOk;
  }

  Code ReleaseSnapshot(const DBHandle& dbhandle, const Snapshot& snapshot) {
    struct onehandle* thishandle;
    leveldb::DB* db = openHandles->get(dbhandle.dbname, &thishandle);
    if (db == NULL) {
      return kNotFound;
    }
    const leveldb::Snapshot* s = thishandle->removeSnapshot(snapshot.snapshotid);
    if (s == NULL) {
      return Code::kNotFound;
    }
    db->ReleaseSnapshot(s); // release leveldb snapshot
    return Code::kOk;
  }

  Code CompactRange(const DBHandle& dbhandle, const Slice& begin, 
    const Slice& end) {
    leveldb::DB* db = openHandles->get(dbhandle.dbname, NULL);
    if (db == NULL) {
      return Code::kNotFound;
    }
    leveldb::Slice k1, *start = &k1;
    k1.data_ = begin.data.data();
    k1.size_ = begin.size;
    leveldb::Slice k2, *stop = &k2;
    k2.data_ = begin.data.data();
    k2.size_ = begin.size;
    
    // check special ranges.
    if (start->size_ == 0) {
      start = NULL;
    }
    if (stop->size_ == 0) {
      stop = NULL;
    }
    db->CompactRange(start, stop);
    return Code::kOk;
  }

 private:
  OpenHandles* openHandles;
};

//
// starts a service
static void* startOneService(void *arg) {
  TSimpleServer* t = (TSimpleServer *)arg;
  t->serve();
  return NULL;
}


// Starts a very simple thrift server
void startServer(int argc, char** argv) {

  // process command line options
  if (!server_options.parseOptions(argc, argv)) {
    exit(1);
  }

  // create directories for server
  if (!server_options.createDirectories()) {
    exit(1);
  }
  // create the server's block cache
  server_options.createCache();

  // data structure to record ope databases
  OpenHandles* openHandles = new OpenHandles();

  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  // create the service to process the normal get/put to leveldb.
  int port = server_options.getPort();
  fprintf(stderr, "Server starting on port %d\n", port);
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<DBHandler> handler(new DBHandler(openHandles));
  shared_ptr<TProcessor> processor(new DBProcessor(handler));
  TSimpleServer* baseServer = new TSimpleServer(processor, serverTransport, 
                                             transportFactory, protocolFactory);
  pthread_t dbServerThread;
  int rc = pthread_create(&dbServerThread, NULL, startOneService, 
                          (void *)baseServer);
  if (rc != 0) {
    fprintf(stderr, "Unable to start DB server on port %d\n.", port);
    exit(1);
  }

  // create the service to process the assoc get/put to leveldb.
  int assocport = server_options.getAssocPort();
  fprintf(stderr, "Server starting on port %d\n", assocport);
  shared_ptr<TServerTransport> assocTransport(new TServerSocket(assocport));
  shared_ptr<AssocServiceHandler> assocHandler(new AssocServiceHandler(openHandles));
  shared_ptr<TProcessor> assocProcessor(new AssocServiceProcessor(assocHandler));
  TSimpleServer* assocServer = new TSimpleServer(assocProcessor, 
                     assocTransport, transportFactory, protocolFactory);
  pthread_t assocServerThread;
  rc = pthread_create(&assocServerThread, NULL, startOneService, 
                      (void *)assocServer);
  if (rc != 0) {
    fprintf(stderr, "Unable to start assoc server on port %d\n.", port);
    exit(1);
  }
}

/**
void startEventServer(int port) {
  shared_ptr<DBHandler> handler(new DBHandler());
  shared_ptr<TProcessor> processor(new DBProcessor(handler));
  shared_ptr<TAsyncProcessor> asyncProcessor(new TSyncToAsyncProcessor(processor));
  TEventServerCreator creator(asyncProcessor, (uint16_t)port, 2);
  tServer = creator.createServer();
  tServer.serve();
}
**/

// Stops the thrift server
void stopServer(int port) {
  baseServer->stop();
  assocServer->stop();
}
