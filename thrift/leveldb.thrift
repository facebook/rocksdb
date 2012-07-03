#!/usr/local/bin/thrift --gen cpp

namespace java Tleveldb
namespace cpp Tleveldb 
namespace rb Tleveldb
namespace py Tleveldb
namespace perl Tleveldb

// Types
typedef binary Text
typedef binary Bytes

// A basic object needed for storing keys and values
struct Slice {
  1:Text data;
  2:i32  size
}

// Different compression types supported
enum CompressionType {
  kNoCompression     = 0x0,
  kSnappyCompression = 0x1
}

// Error codes
enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kEnd = 6
}

// A range object
struct Range {
  1:Slice start;          // Included in the range
  2:Slice limit          // Not included in the range
}

// Options to creating a database
struct DBOptions {
  1:bool create_if_missing;
  2:bool error_if_exists;
  3:i32 write_buffer_size;
  4:i32 max_open_files;
  5:i32 block_size;
  6:i32 block_restart_interval;
  7:CompressionType compression
}

// Options for writing
struct WriteOptions {
  1:bool sync
}

struct Snapshot {
  1:i64 snapshotid     // server generated
}

// Options for reading. If you do not have a
// snapshot, set snapshot.snapshotid = 0
struct ReadOptions {
  1:bool verify_checksums;
  2:bool fill_cache,
  3:Snapshot snapshot
}

// Represents a open database object
struct DBHandle {
  1:Text dbname;        //name of the database
  2:i64 handleid        // server generated
}

struct Iterator {
  1:i64 iteratorid     // server generated
}

// flags for the iterator
enum IteratorType {
    seekToFirst = 0,
    seekToLast = 1,
    seekToKey = 2 
}

struct kv {
  1:Slice key;
  2:Slice value
}

// Return a single value from the Get call
struct ResultItem {
  1:Code status;
  2:Slice value
}

// Return a key,value from a Scan call
struct ResultPair {
  1:Code status;
  2:kv   keyvalue
}

// Snapshot result
struct ResultSnapshot {
  1:Code status;
  2:Snapshot   snapshot
}

// Iterator result
struct ResultIterator {
  1:Code status;
  2:Iterator   iterator
}

exception LeveldbException {
  1:Text message,
  2:Code errorCode
}

// The Database service
service DB {
  
  // opens the database
  DBHandle Open(1:Text dbname, 2:DBOptions dboptions)
    throws (1:LeveldbException se),

  // closes the database
  Code Close(1:DBHandle dbhandle, 2:Text dbname),

  // puts a key in the database
  Code Put(1:DBHandle dbhandle, 2:kv keyvalue, 3:WriteOptions options),
  
  // deletes a key from the database
  Code Delete(1:DBHandle dbhandle, 2:Slice key, 3:WriteOptions options),

  // writes batch of keys into the database
  Code Write(1:DBHandle dbhandle, 2:list<kv> batch, 3:WriteOptions options),

  // fetch a key from the DB. 
  // ResultItem.status == kNotFound means key is non existant
  // ResultItem.status == kOk means key is found
  ResultItem Get(1:DBHandle dbhandle, 2:Slice inputkey, 
                 3:ReadOptions options),

  // start iteration over a set of keys. If iteratorType.seekToFirst
  // is set, then position the iterator at the first key in the source.
  // If iteratorType.seekToLast is set, then position at the last key in the
  // source. If iteratorType.seekToKey is set, then position at the first 
  // key in the source that is at or past target.
  // If any two of iteratorType.seekToFirst & iteratorType.seekToLast
  // and iteratorType.seekToKey are set, then error.
  // If either iteratorType.seekToFirst or iteratorType.seekToLast is set,
  // then target is not used.
  ResultIterator NewIterator(1:DBHandle dbhandle, 2:ReadOptions options,
                             3:IteratorType iteratorType,
                             4:Slice target),

  // Release resources associated with an iterator allocated previously
  // via a call to NewIterator. The call to this method may be skipped
  // if the iterator had already traversed all the keys in the specified
  // range. If the application wants to terminate a scan before processing
  // all the resulting keys, then it is essential to invoke this method.
  Code DeleteIterator(1:DBHandle dbhandle, 2:Iterator iterator),

  // Return the previous/next from this iteration
  ResultPair GetNext(1:DBHandle dbhandle, 2:Iterator iterator),
  ResultPair GetPrev(1:DBHandle dbhandle, 2:Iterator iterator),

  // Create snapshot.
  ResultSnapshot GetSnapshot(1:DBHandle dbhandle),

  // Release snapshots
  Code     ReleaseSnapshot(1:DBHandle dbhandle, 2:Snapshot snapshot),

  // compact a range of keys
  // begin.size == 0 to start at a range earlier than the first existing key
  // end.size == 0 to end at a range later than the last existing key
  Code CompactRange(1:DBHandle dbhandle, 2:Slice begin, 3:Slice end),
}
