#ifndef STORAGE_LEVELDB_INCLUDE_TYPES_H_
#define STORAGE_LEVELDB_INCLUDE_TYPES_H_

#include <stdint.h>

namespace leveldb {

// Define all public custom types here.

//  Represents a sequence number in a WAL file.
typedef uint64_t SequenceNumber;

}  //  namespace leveldb
#endif //  STORAGE_LEVELDB_INCLUDE_TYPES_H_
