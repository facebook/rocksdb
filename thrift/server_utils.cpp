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

extern "C" void startServer(int port);
extern "C" void stopServer(int port);

static boost::shared_ptr<TServer> tServer;

class DBHandler : virtual public DBIf {
 public:
  DBHandler() {
    openHandles = new OpenHandles();
  }

  void Open(DBHandle& _return, const Text& dbname, 
    const DBOptions& dboptions) {
    printf("Open\n");
    leveldb::Options options;
    leveldb::DB* db;
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
    int64_t session = openHandles->add(options, dbname);
    _return.dbname = dbname;
    _return.handleid = session;
  }

  Code Close(const DBHandle& dbhandle, const Text& dbname) {
    /**
     * We do not close any handles for now, otherwise we have to do
     * some locking that will degrade performance in the normal case.
    if (openHandles->remove(dbname, dbhandle.handleid) == false) {
       return Code::kIOError;
    }
    */
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid, NULL);
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid, NULL);
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
    for (int i = 0; i < batch.size(); i++) {
      kv one = batch[i];
      key.data_ = one.key.data.data();
      key.size_ = one.key.size;
      value.data_ = one.value.data.data();
      value.size_ = one.value.size;
      lbatch.Put(key, value);
    }
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid, NULL);
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid, 
                                       &thishandle);
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid, 
                                       &thishandle);
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid, 
                                       &thishandle);
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid, 
                                       &thishandle);
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

    // If the iterator has reached the endm close it rightaway.
    // There is no need for the application to make another thrift
    // call to cleanup the iterator.
    if (!it->Valid()) {
       thishandle->removeIterator(iterator.iteratorid);
       delete it;                  // cleanup
      _return.status = Code::kEnd; // no more elements
      return;
    }

    // find current key-value
    leveldb::Slice key = it->key();
    leveldb::Slice value = it->value();

    // move to next or previous value
    if (doNext) {
      it->Next();
    } else {
      it->Prev();
    }

    // pack results back to client
    _return.keyvalue.key.data = key.data_;
    _return.keyvalue.key.size = key.size_;
    _return.keyvalue.value.data = value.data_;
    _return.keyvalue.value.size = value.size_;
    _return.status = Code::kOk;  // success
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid,
                                       &thishandle);
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid,
                                       &thishandle);
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
    leveldb::DB* db = openHandles->get(dbhandle.dbname, dbhandle.handleid, NULL);
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
      start == NULL;
    }
    if (stop->size_ == 0) {
      stop == NULL;
    }
    db->CompactRange(start, stop);
    return Code::kOk;
  }

 private:
  OpenHandles* openHandles;
};

// Starts a very simple thrift server
void startServer(int port) {
  shared_ptr<DBHandler> handler(new DBHandler());
  shared_ptr<TProcessor> processor(new DBProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer tServer(processor, serverTransport, transportFactory, protocolFactory);
  tServer.serve();
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
  tServer->stop();
}
