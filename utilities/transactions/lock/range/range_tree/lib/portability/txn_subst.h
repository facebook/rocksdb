//
// A substitute for ft/txn/txn.h
//
#pragma once

#include <set>

#include "../util/omt.h"

typedef uint64_t TXNID;
#define TXNID_NONE ((TXNID)0)

// A set of transactions
//  (TODO: consider using class toku::txnid_set. The reason for using STL
//   container was that its API is easier)
class TxnidVector : public std::set<TXNID> {
 public:
  bool contains(TXNID txnid) { return find(txnid) != end(); }
};

// A value for lock structures with a meaning "the lock is owned by multiple
// transactions (and one has to check the TxnidVector to get their ids)
#define TXNID_SHARED (TXNID(-1))

// Auxiliary value meaning "any transaction id will do".  No real transaction
// may have this is as id.
#define TXNID_ANY (TXNID(-2))
