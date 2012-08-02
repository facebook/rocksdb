/**
 * Thrift server for leveldb
 * @author Dhruba Borthakur (dhruba@gmail.com)
 * Copyright 2012 Facebook
 */

#ifndef THRIFT_LEVELDB_SERVER_H_
#define THRIFT_LEVELDB_SERVER_H_

#include <unordered_map>
#include <atomic>
#include "DB.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TConnectionContext.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

#include "leveldb/db.h"
#include "util/random.h"

using boost::shared_ptr;
using std::unordered_map;

using namespace  ::Tleveldb;

// List of snapshots. Each entry has a unique snapshot id.
struct snapshotEntry {
  int64_t snapshotid;
  const leveldb::Snapshot* lsnap;

  snapshotEntry() : snapshotid(-1), lsnap(NULL)  {
  }

  private:
   snapshotEntry(const snapshotEntry&);
   snapshotEntry& operator= (const snapshotEntry&);
};

// List of iterators. Each entry has a unique iterator id.
struct iteratorEntry {
  int64_t iteratorid;
  leveldb::Iterator* liter;

  iteratorEntry() : iteratorid(-1), liter(NULL)  {
  }

  private:
   iteratorEntry(const iteratorEntry&);
   iteratorEntry& operator= (const iteratorEntry&);
};


//
// This is the information stored for each open database. Each open instance
// of the database has a list of snapshots and a list of iterators that are
// currenty open
//
struct onehandle {
  Text name;
  leveldb::DB* onedb;    // locate the localleveldb instance
  int refcount;          // currently not used
  std::atomic<uint64_t> currentSnapshotId; // valid snapshotids > 0
  std::atomic<uint64_t> currentIteratorId; // valid iterators > 0
  unordered_map<uint64_t, struct snapshotEntry*> snaplist; 
                        // list of snapshots for this database
  unordered_map<uint64_t, struct iteratorEntry*> iterlist; 
                        // list of iterators for this database

  onehandle() : currentSnapshotId(1), currentIteratorId(1) {
  }

  // stores a new leveldb snapshot and returns an unique id
  int64_t addSnapshot(const leveldb::Snapshot* l) {
    struct snapshotEntry* news = new snapshotEntry;
    news->snapshotid = currentSnapshotId++;
    news->lsnap = l;
    snaplist[news->snapshotid] = news;
    return news->snapshotid;
  }

  // lookup a snapshot from its ids
  const leveldb::Snapshot* lookupSnapshot(int64_t id) {
    auto p = snaplist.find(id);
    if (p == snaplist.end()) {
      fprintf(stderr, "get:No snaphot with id %ld\n", id);
      return NULL;
    }
    return p->second->lsnap;
  }

  // remove a snapshot from this database
  const leveldb::Snapshot* removeSnapshot(int64_t id) {
    const leveldb::Snapshot* l = lookupSnapshot(id);
    if (l != NULL) {
      int numRemoved = snaplist.erase(id);
      assert(numRemoved == 1);
      return l;
    }
    return NULL;  // not found
  }

  // stores a new leveldb iterator and returns an unique id
  int64_t addIterator(leveldb::Iterator* l) {
    struct iteratorEntry* news = new iteratorEntry;
    news->iteratorid = currentIteratorId++;
    news->liter = l;
    iterlist[news->iteratorid] = news;
    return news->iteratorid;
  }

  // lookup a iterator from its ids
  leveldb::Iterator* lookupIterator(int64_t id) {
    auto p = iterlist.find(id);
    if (p == iterlist.end()) {
      fprintf(stderr, "lookupIterator:No iterator with id %ld\n", id);
      return NULL;
    }
    return p->second->liter;
  }

  // remove a iterator from this database
  leveldb::Iterator* removeIterator(int64_t id) {
    leveldb::Iterator* i = lookupIterator(id);
    if (i != NULL) {
      int numRemoved = iterlist.erase(id);
      assert(numRemoved == 1); 
      return i;
    }
    return NULL;  // not found
  }

  private:
   onehandle(const onehandle&); 
   onehandle& operator= (const onehandle&); 
};

class OpenHandles {
 public:

  OpenHandles() {
  }

  // Inserts a new database into the list.
  // If the database is already open, increase refcount.  
  // If the database is not already open, open and insert into list.
  void add(leveldb::Options& options, Text dbname, std::string dbdir) {
    struct onehandle* found = head_[dbname];
    if (found == NULL) {
      found = new onehandle;
      found->name = dbname;
      fprintf(stderr, "openhandle.add: Opening leveldb DB %s\n", 
              dbname.c_str());
      leveldb::Status status = leveldb::DB::Open(options, dbdir, &found->onedb);
      if (!status.ok()) {
        LeveldbException e;
        e.errorCode = Code::kIOError;
        e.message = "Unable to open database";
        fprintf(stderr, "openhandle.add: Unable to open database %s\n", 
                dbname.c_str());
        throw e;
      }
      assert(found->onedb != NULL);
      head_[dbname] = found;
    }
    found->refcount++;
  }

  leveldb::DB* get(Text dbname, struct onehandle** f) {
    auto p = head_.find(dbname);
    if (p == head_.end()) {
      fprintf(stderr, "get:No db with name\n");
      return NULL;
    }
    struct onehandle* found = p->second;
    if (found->refcount <= 0) {
      fprintf(stderr, "get:bad refcount\n.");
      return NULL;
    }
    // returns the onehandle if asked to do so
    if (f != NULL) {
      *f = found;
    }
    return found->onedb;
  }

  bool remove(Text dbname) {
    auto p = head_.find(dbname);
    if (p == head_.end()) {
      fprintf(stderr, "get:No db with name\n");
      return false;
    }
    struct onehandle* found = p->second;
    if (found->refcount == 1) {
      delete found->onedb; // close database
      int numRemoved = head_.erase(dbname);
      assert (numRemoved == 1);
    } else {
      found->refcount--;  // decrement refcount
    }
    return true;
  }

 private:
  unordered_map<std::string, struct onehandle*> head_; // all open databases

  struct onehandle* lookup(Text dbname) {
    auto p = head_.find(dbname);
    if (p == head_.end()) {
      fprintf(stderr, "get:No db with name\n");
      return NULL;
    }
    return p->second;
  }
};

#endif // THRIFT_LEVELDB_SERVER_H_

   
