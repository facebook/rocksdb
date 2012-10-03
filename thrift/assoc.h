/**
* Thrift server that supports operations on the 
* Facebook TAO Graph database
* @author Dhruba Borthakur (dhruba@gmail.com)
* Copyright 2012 Facebook
*/
#ifndef THRIFT_LEVELDB_ASSOC_SERVER_H_
#define THRIFT_LEVELDB_ASSOC_SERVER_H_

#include <AssocService.h>
#include <leveldb_types.h>
#include "openhandles.h"
#include "server_options.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "util/testharness.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/murmurhash.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

using boost::shared_ptr;

using namespace  ::Tleveldb;

//
// These are the service methods that processes Association Data.
// Native types are stored in big-endian format, i.e. first bytes
// have most significant bits.

class AssocServiceHandler : virtual public AssocServiceIf {
 public:

  AssocServiceHandler(OpenHandles* openhandles) {
    openhandles_ = openhandles;
    woptions_sync_.sync = true;
  }
  
  int64_t taoAssocPut(const Text& tableName, int64_t assocType, int64_t id1, 
                      int64_t id2, int64_t id1Type, int64_t id2Type, 
                      int64_t timestamp, AssocVisibility visibility, 
                      bool update_count, int64_t dataVersion, const Text& data, 
                      const Text& wormhole_comment) {
    leveldb::DB* db = openhandles_->get(tableName, NULL);
    if (db == NULL) {
      return Code::kNotFound;
    }
    int64_t ret = assocPutInternal(tableName,
                           db, assocType, id1, id2, id1Type, id2Type,
                           timestamp, visibility, update_count, dataVersion,
                           data, wormhole_comment);
    return ret;
  }

  int64_t taoAssocDelete(const Text& tableName, int64_t assocType, int64_t id1, 
                         int64_t id2, AssocVisibility visibility, bool update_count, 
                         const Text& wormhole_comment) {
    leveldb::DB* db = openhandles_->get(tableName, NULL);
    if (db == NULL) {
      return Code::kNotFound;
    }
    return assocDeleteInternal(tableName, db, assocType, id1, id2, visibility,
                               update_count, wormhole_comment);
    return 0;
  }

  void taoAssocRangeGet(std::vector<TaoAssocGetResult> & _return, 
                        const Text& tableName, int64_t assocType, int64_t id1, 
                        int64_t start_time, int64_t end_time, int64_t offset, 
                        int64_t limit) {
    leveldb::DB* db = openhandles_->get(tableName, NULL);
    if (db == NULL) {
      throw generate_exception(tableName, Code::kNotFound,
               "taoAssocRangeGet: Unable to open database " ,
               assocType, id1, 0, 0, 0, 0, Tleveldb::UNUSED1);
    }
    assocRangeGetBytimeInternal(_return, tableName, db, assocType, id1,
                              start_time, end_time, offset, limit);
  }

  void taoAssocGet(std::vector<TaoAssocGetResult> & _return, 
                   const Text& tableName, int64_t assocType, int64_t id1, 
                   const std::vector<int64_t> & id2s) {
    leveldb::DB* db = openhandles_->get(tableName, NULL);
    if (db == NULL) {
      throw generate_exception(tableName, Code::kNotFound,
               "taoAssocGet:Unable to open database " ,
               assocType, id1, 0, 0, 0, 0, Tleveldb::UNUSED1);
    }
    assocGetInternal(_return, tableName, db, assocType, id1, id2s);
  }

  int64_t taoAssocCount(const Text& tableName, int64_t assocType, int64_t id1) {
    leveldb::DB* db = openhandles_->get(tableName, NULL);
    if (db == NULL) {
      return Code::kNotFound;
    }
    return assocCountInternal(tableName, db, assocType, id1);
  }

 private:
  OpenHandles* openhandles_;
  leveldb::ReadOptions roptions_;
  leveldb::WriteOptions woptions_;      // write with no sync
  leveldb::WriteOptions woptions_sync_; // write with sync

  // the maximum values returned in a rangeget/multiget call.
  const static unsigned int MAX_RANGE_SIZE = 10000;

  // the seed for murmur hash (copied from Hadoop)
  const static unsigned int HASHSEED = 0x5bd1e995;

  // A bunch of rowlocks, sharded over the entire rowkey range
  // Each rowkey is deterministically mapped to one of these locks.
  leveldb::port::RWMutex rowlocks_[1000];

  // A helper method that hashes the row key to a lock
  leveldb::port::RWMutex* findRowLock(char* str, int size) {
    int index = MurmurHash(str, size, HASHSEED) % sizeof(rowlocks_);
    return &rowlocks_[index];
  }

  //
  // Inserts an assoc
  // If update_count, returns the updated count of the assoc.
  // If !update_count, return zero.
  // On failure, throws exception
  // 
  int64_t assocPutInternal(const Text& tableName, leveldb::DB* db,
                      int64_t assocType, int64_t id1, 
                      int64_t id2, int64_t id1Type, int64_t id2Type, 
                      int64_t ts, AssocVisibility vis, 
                      bool update_count, int64_t dataVersion, const Text& data, 
                      const Text& wormhole_comment) {
    leveldb::WriteBatch batch;
    ts = convertTime(ts); // change time to numberofmillis till MAXLONG

    // create the payload for this assoc
    int payloadsize = sizeof(id1Type) + sizeof(id2Type) + sizeof(dataVersion) +
                      sizeof(int32_t) +    // store the data size
                      sizeof(int32_t) +    // store the wormhole comment size
                      data.size() + wormhole_comment.size();
    std::string payload;
    payload.reserve(payloadsize);
    payload.resize(payloadsize);
    makePayload(&payload[0], id1Type, id2Type, dataVersion, data, 
                wormhole_comment);

    int64_t count = 0;
    int64_t oldts;
    int8_t  oldvis;
    bool newassoc = false; // is this assoc new or an overwrite
    leveldb::Status status;
    std::string value;

    // create RowKey for 'c'
    int maxkeysize = sizeof(id1) + sizeof(assocType) + 1;
    std::string dummy1;
    dummy1.reserve(maxkeysize);
    dummy1.resize(maxkeysize);
    char* keybuf = &dummy1[0];
    int rowkeysize = makeRowKey(keybuf, id1, assocType);
    int keysize = appendRowKeyForCount(rowkeysize, keybuf);
    leveldb::Slice ckey(keybuf, keysize);

    // find the row lock
    leveldb::port::RWMutex* rowlock = findRowLock(keybuf, rowkeysize);
    {
      // acquire the row lock
      leveldb::WriteLock l(rowlock);

      // Scan 'c'  to get $count if $update_count == true
      if (update_count) {
        status = db->Get(roptions_, ckey, &value);
        if (status.IsNotFound()) {
          // nothing to do
        } else if (!status.ok() || (value.size() != sizeof(int64_t))) {
          throw generate_exception(tableName, Code::kNotFound,
                  "AssocPut Unable to extract count ", 
                  assocType, id1, id2, id1Type, id2Type, ts, vis);
        } else {
          extract_int64(&count, (char *)value.c_str());
        }
      }

      // Scan 'm'$id2 to get $ts and $vis
      maxkeysize = sizeof(id1) + sizeof(assocType) + 1 + sizeof(id2);
      std::string dummy2;
      dummy2.reserve(maxkeysize);
      dummy2.resize(maxkeysize);
      keybuf = &dummy2[0];
      rowkeysize = makeRowKey(keybuf, id1, assocType);
      keysize = appendRowKeyForMeta(rowkeysize, keybuf, id2);
      leveldb::Slice mkey(keybuf, keysize);
      status = db->Get(roptions_, mkey, &value);
      if (status.IsNotFound()) {
        newassoc = true;
        oldvis = UNUSED1;
      } else if (!status.ok() || 
                 (value.size() != sizeof(int64_t) + sizeof(int8_t))) {
        throw generate_exception(tableName, Code::kNotFound,
                  "AssocPut Unable to extract m$id2 ", 
                  assocType, id1, id2, id1Type, id2Type, ts, vis);
      }

      // make the key 'p'$old_ts$id2
      maxkeysize = sizeof(id1) + sizeof(assocType) + 1 +
                   sizeof(ts) + sizeof(id2);
      std::string dummy3;
      dummy3.reserve(maxkeysize);
      dummy3.resize(maxkeysize);
      keybuf = &dummy3[0];
      rowkeysize = makeRowKey(keybuf, id1, assocType);

      // if ts != oldts, then delete 'p'$old_ts$id2
      if (!newassoc) {
        extractTsVisString(&oldts, &oldvis, (char *)value.c_str());
        keysize = appendRowKeyForPayload(rowkeysize, keybuf, oldts, id2);
        leveldb::Slice pkey(keybuf, keysize);
        if (ts != oldts) {
          batch.Delete(pkey);
        }
      }

      // store in m$id2 the value of $ts$vis
      std::string myvalue;
      myvalue.reserve(sizeof(int64_t) + sizeof(int8_t));
      myvalue.resize(sizeof(int64_t) + sizeof(int8_t));
      makeTsVisString(&myvalue[0], ts, vis);
      leveldb::Slice sl(myvalue);
      batch.Put(mkey, leveldb::Slice(myvalue));

      // store in p$ts$id2 the payload
      keybuf = &dummy3[0];
      keysize = appendRowKeyForPayload(rowkeysize, keybuf, ts, id2);
      leveldb::Slice pkeynew(keybuf, keysize);
      batch.Put(pkeynew, leveldb::Slice(payload));

      // increment count
      if (update_count && (newassoc || oldvis != VISIBLE)) {
        assert(count >= 0);
        count++;
        myvalue.reserve(sizeof(int64_t));
        myvalue.resize(sizeof(int64_t));
        makeCountString(&myvalue[0], count);
        batch.Put(ckey, leveldb::Slice(myvalue));
      }

      // We do a write here without sync. This writes it to the
      // transaction log but does not sync it. It also makes these
      // changes readable by other threads.
      status = db->Write(woptions_, &batch);
      if (!status.ok()) {
        throw generate_exception(tableName, Code::kNotFound,
                "AssocPut Unable to batch write ", 
                assocType, id1, id2, id1Type, id2Type, ts, vis);
      }
    } // release rowlock

    // Do a sync to the transaction log without holding the rowlock.
    // This improves updates for hotrows. The disadvantage is that
    // uncommiitted reads might be read by other threads, but that 
    // should be ok.
    batch.Clear();
    status = db->Write(woptions_sync_, &batch);
    if (!status.ok()) {
      throw generate_exception(tableName, Code::kNotFound,
                "AssocPut Unable to batch sync write ", 
                assocType, id1, id2, id1Type, id2Type, ts, vis);
    }
    if (update_count) {
      assert(count > 0);
      return count;
    }
    return 0;
  }

  //
  // Deletes an assoc
  // If count changes return 1, else returns zero
  // On failure, thrws exception
  // 
  int64_t assocDeleteInternal(const Text& tableName, leveldb::DB* db,
                      int64_t assocType, int64_t id1, 
                      int64_t id2, AssocVisibility vis, 
                      bool update_count, const Text& wormhole_comment) {
    leveldb::WriteBatch batch;
    int return_value = 0;
    int64_t count = 0;
    int64_t oldts;
    int8_t  oldvis;
    std::string value;

    // make a key for count
    int maxkeysize = sizeof(id1) + sizeof(assocType) + 1;
    std::string dummy;
    dummy.reserve(maxkeysize);
    dummy.resize(maxkeysize);
    char* keybuf = &dummy[0];
    int rowkeysize = makeRowKey(keybuf, id1, assocType);
    leveldb::Status status;
    int keysize = appendRowKeyForCount(rowkeysize, keybuf);
    leveldb::Slice ckey(keybuf, keysize);

    // find the row lock
    leveldb::port::RWMutex* rowlock = findRowLock(keybuf, rowkeysize);
    {
      // acquire the row lock
      leveldb::WriteLock l(rowlock);

      // Scan 'c'  to get $count if $update_count == true
      if (update_count) {
        status = db->Get(roptions_, ckey, &value);
        if (status.IsNotFound()) {
          throw generate_exception(tableName, Code::kNotFound,
                  "assocDelete: Unable to find count ", 
                  assocType, id1, id2, 0, 0, 0, vis);
        } else if (!status.ok() || (value.size() != sizeof(int64_t))) {
          throw generate_exception(tableName, Code::kNotFound,
                  "assocDelete: Unable to extract count ", 
                  assocType, id1, id2, 0, 0, 0, vis);
        } else {
          extract_int64(&count, (char *)value.c_str());
        }
      }

      // Scan 'm'$id2 to get $ts and $vis
      maxkeysize = sizeof(id1) + sizeof(assocType) + 1 + sizeof(id2);
      std::string dummy2;
      dummy2.reserve(maxkeysize);
      dummy2.resize(maxkeysize);
      keybuf = &dummy2[0];
      rowkeysize = makeRowKey(keybuf, id1, assocType);
      keysize = appendRowKeyForMeta(rowkeysize, keybuf, id2);
      leveldb::Slice mkey(keybuf, keysize);
      status = db->Get(roptions_, mkey, &value);
      if (status.IsNotFound()) {
        throw generate_exception(tableName, Code::kNotFound,
                  "assocDelete Unable to find column m ", 
                  assocType, id1, id2, 0, 0, 0, vis);
      } else if (!status.ok() ||
                 (value.size() != sizeof(int64_t) + sizeof(int8_t))) {
        throw generate_exception(tableName, Code::kNotFound,
                  "assocDelete Unable to extract m$id2 ",
                  assocType, id1, id2, 0, 0, 0, vis);
      }
      extractTsVisString(&oldts, &oldvis, (char *)value.c_str());

      // Create d'$id2
      maxkeysize = sizeof(id1) + sizeof(assocType) + 1 + sizeof(id2);
      std::string dummy3;
      dummy3.reserve(maxkeysize);
      dummy3.resize(maxkeysize);
      keybuf = &dummy3[0];
      rowkeysize = makeRowKey(keybuf, id1, assocType);
      keysize = appendRowKeyForDelete(rowkeysize, keybuf, id2);
      leveldb::Slice dkey(keybuf, keysize);

      // create key for 'p'
      maxkeysize = sizeof(id1) + sizeof(assocType) + 1 +
                       sizeof(oldts) + sizeof(id2);
      std::string dummy4;
      dummy4.reserve(maxkeysize);
      dummy4.resize(maxkeysize);
      keybuf = &dummy4[0];
      rowkeysize = makeRowKey(keybuf, id1, assocType);
      keysize = appendRowKeyForPayload(rowkeysize, keybuf, oldts, id2);
      leveldb::Slice pkey(keybuf, keysize);
  
      // if this is a hard delete, then delete all columns
      if (vis == AssocVisibility::HARD_DELETE) {
        batch.Delete(ckey);
        batch.Delete(mkey);
        batch.Delete(dkey);
        batch.Delete(pkey);
      } else if (vis == AssocVisibility::DELETED) {
        if (oldvis != AssocVisibility::DELETED) {
          // change vis in m$id2
          std::string mvalue;
          mvalue.reserve(sizeof(int64_t) + sizeof(int8_t));
          mvalue.resize(sizeof(int64_t) + sizeof(int8_t));
          makeTsVisString(&mvalue[0], oldts, vis);
          batch.Put(mkey, leveldb::Slice(mvalue));
        }

        // scan p$tsid2 to get payload
        // do we need to modify payload with new wormhole comments?
        std::string pvalue;
        status = db->Get(roptions_, pkey, &pvalue);
        if (status.IsNotFound()) {
          throw generate_exception(tableName, Code::kNotFound,
                  "assocDelete Unable to find p ",
                  assocType, id1, id2, 0, 0, oldts, vis);
        } else if (!status.ok() ||
                   (value.size() != sizeof(int64_t) + sizeof(int8_t))) {
          throw generate_exception(tableName, Code::kNotFound,
                  "assocDelete Unable to extract p ",
                  assocType, id1, id2, 0, 0, oldts, vis);
        }

        // store payload in d$id2
        batch.Put(dkey, leveldb::Slice(pvalue));
  
        // delete p$ts$id2
        batch.Delete(pkey);
      }
      if (update_count && oldvis == AssocVisibility::VISIBLE) {
        return_value = 1;
        assert(count >= 1);
        count--;
        std::string myvalue;
        myvalue.reserve(sizeof(int64_t));
        myvalue.resize(sizeof(int64_t));
        makeCountString(&myvalue[0], count);
        batch.Put(ckey, leveldb::Slice(myvalue));
      }
      status = db->Write(woptions_, &batch); // write with no sync
      if (!status.ok()) {
        throw generate_exception(tableName, Code::kNotFound,
                  "assocDelete Unable to Batch Write ",
                  assocType, id1, id2, 0, 0, oldts, vis);
      }
    }  // release rowlock

    // Do a sync write after releasing the rowlock. This
    // improves performance for hotrow updates.
    batch.Clear();
    status = db->Write(woptions_sync_, &batch);
    if (!status.ok()) {
      throw generate_exception(tableName, Code::kNotFound,
                "assocDelete Unable to Batch sync Write ",
                assocType, id1, id2, 0, 0, oldts, vis);
     }
    if (update_count) {
      assert(count >= 0);
      return count;
    }
    return return_value;
  }

  int64_t assocCountInternal(const Text& tableName, leveldb::DB* db,
                                int64_t assocType, int64_t id1) {
    // create key to query
    int maxkeysize = sizeof(id1) + sizeof(assocType) + 1;
    std::string dummy;
    dummy.reserve(maxkeysize);
    dummy.resize(maxkeysize);
    char* keybuf = &dummy[0];
    int rowkeysize = makeRowKey(keybuf, id1, assocType);
    int keysize = appendRowKeyForCount(rowkeysize, keybuf); // column 'c'
    leveldb::Slice ckey(keybuf, keysize);

    // Query database to find value
    leveldb::Status status;
    std::string value;
    int64_t count;
    status = db->Get(roptions_, ckey, &value);

    // parse results retrieved from database
    if (status.IsNotFound()) {
      return 0;              // non existant assoc
    } else if (!status.ok()) {
      throw generate_exception(tableName, Code::kNotFound,
             "assocCountInternal Unable to find count ",
             assocType, id1, 0, 0, 0, 0, Tleveldb::UNUSED1);
    }
    if (value.size() != sizeof(int64_t)) {
      printf("expected %ld got %ld\n", sizeof(int64_t), value.size());
      throw generate_exception(tableName, Code::kNotFound,
             "assocCountInternal Bad sizes for count ",
             assocType, id1, 0, 0, 0, 0, Tleveldb::UNUSED1);
    }
    extract_int64(&count, (char *)value.c_str());
    return count;
  }

  void assocRangeGetBytimeInternal(std::vector<TaoAssocGetResult> & _return, 
                        const Text& tableName, leveldb::DB* db,
                        int64_t assocType, int64_t id1, 
                        int64_t start_time, int64_t end_time, int64_t offset, 
                        int64_t limit) {
    if (start_time < end_time) {
      throw generate_exception(tableName, Code::kNotFound,
             "assocRangeGetBytimeInternal:Bad starttime and endtime\n",
             assocType, id1, 0, 0, 0, 0, Tleveldb::UNUSED1);
    }
    
    int64_t ts, id2;
    std::string wormhole;

    // convert times to time-till-LONGMAX
    int64_t startTime = convertTime(start_time);
    int64_t endTime = convertTime(end_time);

    // create max key to query
    int maxkeysize = sizeof(id1) + sizeof(assocType) + 1 + sizeof(ts) +
                     sizeof(id2);
    std::string dummy;
    dummy.reserve(maxkeysize);
    dummy.resize(maxkeysize);

    // create rowkey
    char* keybuf = &dummy[0];
    int rowkeysize = makeRowKey(keybuf, id1, assocType);

    // Position scan at 'p'$ts$id2 where ts = startTime and id2 = 0
    id2 = 0;
    int keysize = appendRowKeyForPayload(rowkeysize, keybuf, startTime, id2); 
    leveldb::Slice pkey(keybuf, keysize);
    leveldb::Iterator* iter = db->NewIterator(roptions_);

    for (iter->Seek(pkey); iter->Valid() && limit > 0 ; iter->Next()) {
      // skip over records that the caller is not interested in
      if (offset > 0) {
        offset--;
        continue;
      }
      ASSERT_GE(iter->key().size_, (unsigned int)rowkeysize);

      // extract the timestamp and id1 from the key
      extractRowKeyP(&ts, &id2, rowkeysize, (char*)(iter->key().data_));
      ASSERT_GE(ts, startTime);
      if (ts > endTime) {
        break;
      }

      // allocate a new slot in the result set.
      _return.resize(_return.size() + 1);
      TaoAssocGetResult* result = &_return.back();

      // Fill up new element in result set. 
      result->id2 = id2;
      result->time = convertTime(ts);
      extractPayload((char*)iter->value().data_, &result->id1Type,
                     &result->id2Type,
                     &result->dataVersion, result->data, wormhole);
      limit--;
    }
  }

  void assocGetInternal(std::vector<TaoAssocGetResult> & _return, 
                   const Text& tableName, 
                   leveldb::DB* db,
                   int64_t assocType, int64_t id1, 
                   const std::vector<int64_t> & id2s) {
    int64_t ts, id2;

    if (id2s.size() > MAX_RANGE_SIZE) {
      throw generate_exception(tableName, Code::kNotFound,
               "assocGetInternal Ids2 cannot be gteater than 10K.",
               assocType, id1, 0, 0, 0, 0, Tleveldb::UNUSED1);
    }
    // allocate the entire result buffer.
    _return.reserve(id2s.size());

    // create max key to query
    int maxkeysize = sizeof(id1) + sizeof(assocType) + 1 + sizeof(ts) +
                     sizeof(id2);
    std::string dummy;
    dummy.reserve(maxkeysize);
    dummy.resize(maxkeysize);

    // create rowkey
    char* keybuf = &dummy[0];
    int rowkeysize = makeRowKey(keybuf, id1, assocType);
    leveldb::Iterator* iter = db->NewIterator(roptions_);

    for (unsigned int index = 0; index < id2s.size(); index++) {
      int64_t ts;
      int8_t  oldvis;
      leveldb::Status status;
      std::string wormhole;

      // query column 'm'$id2
      id2 = id2s[index];
      int keysize = appendRowKeyForMeta(rowkeysize, keybuf, id2); 
      leveldb::Slice ckey(keybuf, keysize);
      iter->Seek(ckey);
      if (!iter->Valid()) {
        throw generate_exception(tableName, Code::kNotFound,
               "Unable to find m$id2 ",
               assocType, id1, id2, 0, 0, 0, Tleveldb::UNUSED1);
      }
      if (ckey != iter->key()) {
        continue;              // non existant assoc
      }
      leveldb::Slice value = iter->value();
      if (value.size() != sizeof(int64_t) + sizeof(int8_t)) {
        throw generate_exception(tableName, Code::kNotFound,
               "Unable to find m$id2 ",
               assocType, id1, id2, 0, 0, 0, Tleveldb::UNUSED1);
      }

      extractTsVisString(&ts, &oldvis, (char*)value.data_);
      if(oldvis != AssocVisibility::VISIBLE) {
        continue;
      }
      ASSERT_NE(ts, 0);

      // this assoc is visible, scan 'p'$ts$id2 to retrieve payload.
      keysize = appendRowKeyForPayload(rowkeysize, keybuf, ts, id2); 
      leveldb::Slice pkey(keybuf, keysize);
      iter->Seek(pkey);
      if (!iter->Valid() || (pkey != iter->key())) {
        throw generate_exception(tableName, Code::kNotFound,
               "Unable to find p$ts$id2 ",
               assocType, id1, id2, 0, 0, ts, Tleveldb::UNUSED1);
      }

      // allocate a new slot in the result set.
      _return.resize(_return.size() + 1);
      TaoAssocGetResult* result = &_return.back();

      // Fill up new element in result set. 
      result->id2 = id2;
      result->time = convertTime(ts);
      extractPayload((char *)iter->value().data_, &result->id1Type, 
                     &result->id2Type,
                     &result->dataVersion, result->data, wormhole);
    }
  }

  // fill the row key and returns the size of the key
  inline int makeRowKey(char* dest, int64_t id1, int64_t assocType) {
    dest = copy_int64_switch_endian(dest, id1);
    dest = copy_int64_switch_endian(dest, assocType);
    return sizeof(id1) + sizeof(assocType);
  }

  // fill the row key +'c' and returns the size of the key
  inline int appendRowKeyForCount(int rowkeysize, char* dest) {
    dest += rowkeysize;
    *dest = 'c';
    return rowkeysize + 1;
  }

  // fill the row key +'p' + $ts$id2 and returns the size of the key
  inline int appendRowKeyForPayload(int rowkeysize, char* dest,
                                int64_t ts, int64_t id2) {
    dest += rowkeysize;
    *dest++ = 'p';
    dest = copy_int64_switch_endian(dest, ts);
    dest = copy_int64_switch_endian(dest, id2);
    return rowkeysize + sizeof(ts) + sizeof(id2) + 1;
  }

  // extract the timestamp and id2 from the key p$ts$id2
  inline void extractRowKeyP(int64_t* ts, int64_t* id, 
                             int rowkeysize, char* src) {
    src += rowkeysize; // skip over the rowkey
    ASSERT_EQ(*src, 'p');
    src++;
    extract_int64(ts, src); src += sizeof(*ts);
    extract_int64(id, src); src += sizeof(*id);
  }

  // fill the row key +'m' + id2 and returns the size of the key
  inline int appendRowKeyForMeta(int rowkeysize, char* dest, 
                                 int64_t id2) {
    dest += rowkeysize;
    *dest++ = 'm';
    dest = copy_int64_switch_endian(dest, id2);
    return rowkeysize + sizeof(id2) + 1;
  }

  // fill the row key +'d' + id2 and returns the size of the key
  inline int appendRowKeyForDelete(int rowkeysize, char* dest, 
                                   int64_t id2) {
    dest += rowkeysize;
    *dest++ = 'd';
    dest = copy_int64_switch_endian(dest, id2);
    return rowkeysize + sizeof(id2) + 1;
  }

  // encode id1Type, id2Type, dataversion, etc into the payload
  void makePayload(char* dest, int64_t id1Type, int64_t id2Type,
                      int64_t dataVersion, const Text& data, 
                      const Text& wormhole_comment) {
    int32_t datasize = data.size();
    int32_t wormhole_commentsize = wormhole_comment.size();

    dest = copy_int64_switch_endian(dest, id1Type);
    dest = copy_int64_switch_endian(dest, id2Type);
    dest = copy_int64_switch_endian(dest, dataVersion);
    dest = copy_int32(dest, datasize);
    dest = copy_int32(dest, wormhole_commentsize);
    memcpy(dest, data.data(), data.size());
    dest += data.size();
    memcpy(dest, wormhole_comment.data(), wormhole_comment.size());
    dest += wormhole_comment.size();
  }

  // extract id1Type, id2Type, dataversion, etc from payload
  void extractPayload(char* dest, int64_t* id1Type, int64_t* id2Type,
                      int64_t* dataVersion, Text& data, 
                      Text& wormhole_comment) {
    int32_t datasize, wormsize;
    extract_int64(id1Type, dest); dest += sizeof(*id1Type);
    extract_int64(id2Type, dest); dest += sizeof(*id2Type);
    extract_int64(dataVersion, dest); dest += sizeof(*dataVersion);
    extract_int32(&datasize, dest); dest += sizeof(datasize);
    extract_int32(&wormsize, dest); dest += sizeof(wormsize);

    data.assign(dest, datasize); dest += datasize;
    wormhole_comment.assign(dest, wormsize); dest += wormsize;
  }

  // fill the timestamp and visibility
  inline void makeTsVisString(char* dest, int64_t ts, int8_t vis) {
    dest = copy_int64_switch_endian(dest, ts);
    *dest = vis;
  }

  // extracts the timestamp and visibility from a byte stream
  inline void extractTsVisString(int64_t* ts, int8_t* vis, char* src) {
    extract_int64(ts, src);
    extract_int8(vis, src + sizeof(*ts));
  }

  // fill the count value
  inline void makeCountString(char* dest, int64_t count) {
    dest = copy_int64_switch_endian(dest, count);
  }

  //
  // Switch endianess of the id and copy it to dest.
  // Returns the updated destination address
  //
  inline char* copy_int64_switch_endian(char* dest, int64_t id) {
    char* src = (char *)&id + sizeof(id) - 1;
    for (unsigned int i = 0; i < sizeof(id); i++) {
      *dest++ = *src--;      
    }
    return dest;
  }

  // extracts a int64 type from the char stream. Swaps endianness.
  inline void extract_int64(int64_t* dest, char* src) {
    char* d = (char *)dest;
    src += sizeof(int64_t) - 1;
    for (unsigned int i = 0; i < sizeof(uint64_t); i++) {
      *d++ = *src--;      
    }
  }

  //
  // copy a 4 byte quantity to byte stream. swap endianess.
  //
  inline char* copy_int32(char* dest, int32_t id) {
    char* src = (char *)&id + sizeof(id) - 1;
    for (unsigned int i = 0; i < sizeof(id); i++) {
      *dest++ = *src--;      
    }
    return dest;
  }

  // extract a 4 byte quantity from a byte stream
  inline void extract_int32(int32_t* dest, char* src) {
    char* d = (char *)dest;
    src += sizeof(int32_t) - 1;
    for (unsigned int i = 0; i < sizeof(*dest); i++) {
      *d++ = *src--;      
    }
  }

  // extracts a 1 byte integer from the char stream.
  inline void extract_int8(int8_t* dest, char* src) {
    *dest = *(int8_t *)src;
  }

  // convert a timestamp from an ever-increasing number to
  // a decreasing number. All stored timestamps in this database
  // are MAXLONG - timestamp. Thus, a backward-scan in time
  // is converted to a forward scan in the database.
  inline int64_t convertTime(int64_t ts) {
    return LONG_MAX - ts;
  }


  // generate an exception message
  LeveldbException generate_exception(const Text& tableName,
                      Code errorCode, const char* message,
                      int64_t assocType, int64_t id1, 
                      int64_t id2, int64_t id1Type, int64_t id2Type, 
                      int64_t ts, AssocVisibility vis) {
    char result[1024];
    sprintf(result, 
            "id1=%ld assocType=%ld id2=%ld id1Type=%ld id2Type=%ld ts=%ld vis=%d ", 
            id1, assocType, id2, id1Type, id2Type, ts, vis);
    fprintf(stderr, "assoc_server error table %s: %s errorCode=%d %s",
            tableName.c_str(), message, errorCode, result);

    LeveldbException e;
    e.errorCode = errorCode;
    e.message = message;
    throw e;
  }

};

#endif // THRIFT_LEVELDB_ASSOC_SERVER_H_
