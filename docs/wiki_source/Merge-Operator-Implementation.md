This page describes the internal implementation details for the RocksDB Merge feature. It is aimed at expert RocksDB engineers and/or other Facebook engineers who are interested in understanding how Merge works.

>If you are a user of RocksDB and you only want to know how to use Merge in production, go to the [Client Interface](Merge-Operator) page. Otherwise, it is assumed that you have already read that page.

# Context

Here is a high-level overview of the code-changes that we needed in order to implement Merge:
* We created an abstract base class called MergeOperator that the user needs to inherit from. 
* We updated that Get(), iteration, and Compaction() call paths to call the MergeOperator's FullMerge() and PartialMerge() functions when neccessary. 
* The major change needed was to implement "stacking" of merge operands, which we describe below. 
* We introduced some other interface changes (i.e.: updated the Options class and DB class to support MergeOperator) 
* We created a simpler AssociativeMergeOperator to make the user's lives easier under a very common use-case. Note this can be much more inefficient.

For the reader, if any of the above statements do not make sense at a high level, YOU PROBABLY SHOULD READ THE [Client Interface page first](Merge-Operator). Otherwise, we dive directly into the details below, and also talk about some design decisions and rationale for picking the implementations we did.

# The Interface

A quick reiteration of the interface (it is assumed that the reader is somewhat familiar with it already):

	// The Merge Operator
	//
	// Essentially, a MergeOperator specifies the SEMANTICS of a merge, which only
	// client knows. It could be numeric addition, list append, string
	// concatenation, edit data structure, ... , anything.
	// The library, on the other hand, is concerned with the exercise of this
	// interface, at the right time (during get, iteration, compaction...)
	class MergeOperator {
	 public:
	  virtual ~MergeOperator() {}
 
	  // Gives the client a way to express the read -> modify -> write semantics
	  // key:         (IN) The key that's associated with this merge operation.
	  // existing:    (IN) null indicates that the key does not exist before this op
	  // operand_list:(IN) the sequence of merge operations to apply, front() first.
	  // new_value:  (OUT) Client is responsible for filling the merge result here
	  // logger:      (IN) Client could use this to log errors during merge.
	  //
	  // Return true on success, false on failure/corruption/etc.
	  virtual bool FullMerge(const Slice& key,
	                         const Slice* existing_value,
	                         const std::deque<std::string>& operand_list,
	                         std::string* new_value,
	                         Logger* logger) const = 0;
 
	  // This function performs merge(left_op, right_op)
	  // when both the operands are themselves merge operation types.
	  // Save the result in *new_value and return true. If it is impossible
	  // or infeasible to combine the two operations, return false instead.
	  virtual bool PartialMerge(const Slice& key,
	                            const Slice& left_operand,
	                            const Slice& right_operand,
	                            std::string* new_value,
	                            Logger* logger) const = 0;
 
	  // The name of the MergeOperator. Used to check for MergeOperator
	  // mismatches (i.e., a DB created with one MergeOperator is
	  // accessed using a different MergeOperator)
	  virtual const char* Name() const = 0;
	};

# RocksDB Data Model

Before going into the gory details of how merge works, let's try to understand the data model of RocksDB first.

In a nutshell, RocksDB is a versioned key-value store. Every change to the db is globally ordered and assigned a monotonically increasing sequence number. For each key, RocksDB keeps the history of operations. We denote each operation as OPi. A key (K) that experienced n changes, looks like this logically (physically, the changes could be in the active memtable, the immutable memtables, or the level files).

	K:   OP1   OP2   OP3   ...   OPn

An operation has three properties: its type - either a Delete or a Put (now we have Merge too), its sequence number and its value (Delete can be treated as a degenerate case without a value). Sequence numbers will be increasing but not contiguous with regard to a single key, as they are globally shared by all keys.

When a client issues db->Put or db->Delete, the library literally appends the operation to the history. No checking of the existing value is done, probably for performance consideration (no wonder Delete remains silent if the key does not pre-exist...)

What about db->Get? It returns the state of a key with regard to a point in time, specified by a sequence number. The state of a key could be either non-existent or an opaque string value. It starts as non-existent. Each operation moves the key to a new state. In this sense, each key is a state machine with operations as transitions.

From the state machine point of view, Merge is really a generic transition that looks at the current state (existing value, or non-existence of that), combines it with the operand (the value associated with the Merge operation) and then produces a new value (state). Put, is a degenerate case of Merge, that pays no attention to the current state, and produces the new state solely based on its operand. Delete goes one step further - it doesn't even have an operand and always bring the key back to its original state - non-existent.

## Get

In principal, Get returns the state of a key at a specific time.

	K:   OP1    OP2   OP3   ....   OPk  .... OPn
                                ^
                                |
                             Get.seq

Suppose OPk is the most recent operation that's visible to Get:

`k = max(i) {seq(OPi) <= Get.seq}`

Then, if OPk is a Put or Delete, Get should simply return the value specified (if Put) or a NotFound status (if Delete). It can ignore previous values.

With the new Merge operation, we actually need to look backward. And how far do we go? Up to a Put or Delete (history beyond that is not essential anyways).

	K:   OP1    OP2   OP3   ....    OPk  .... OPn
	            Put  Merge  Merge  Merge
	                                 ^
	                                 |
	                              Get.seq
	             -------------------->

For the above example, Get should return something like:

`Merge(...Merge(Merge(operand(OP2), operand(OP3)), operand(OP4))..., operand(OPk))`

Internally, RocksDB traverses the key history from new to old. The internal data structures for RocksDB support a nice "binary-search" style Seek function. So, provided a sequence number, it can actually return: k = max(i) {seq(OPi) <= Get.seq} relatively efficiently. Then, beginning with OPk, it will iterate through history from new to old as mentioned until a Put/Delete is found.

In order to actually enact the merging, rocksdb makes use of the two specified Merge-Operator methods: FullMerge() and PartialMerge(). The Client Interface page gives a good overview on what these functions mean at a high-level. But, for the sake of completeness, it should be known that PartialMerge() is an optional function, used to combine two merge operations (operands) into a single operand. For example, combining OP(k-1) with OPk to produce some OP', which is also a merge-operation type. Whenever PartialMerge() is unable to combine two operands, it returns false, signaling to rocksdb to handle the operands itself. How is this done? Well, internally, rocksdb provides an in-memory stack-like data structure (we actually use an STL Deque) to stack the operands, maintaining their relative order, until a Put/Delete is found, in which case FullMerge() is used to apply the list of operands onto the base-value.

The algorithm for Get() is as follows:

	Get(key):
	  Let stack = [ ];       // in reality, this should be a "deque", but stack is simpler to conceptualize for this pseudocode
	  for each entry OPi from newest to oldest:
	    if OPi.type is "merge_operand":
	      push OPi to stack
	        while (stack has at least 2 elements and (stack.top() and stack.second_from_top() can be partial-merged)
	          OP_left = stack.pop()
	          OP_right = stack.pop()
	          result_OP = client_merge_operator.PartialMerge(OP_left, OP_right)
	          push result_OP to stack
	    else if OPi.type is "put":
	      return client_merge_operator.FullMerge(v, stack);
	    else if v.type is "delete":
	      return client_merge_operator.FullMerge(nullptr, stack);
 
	  // We've reached the end (OP0) and we have no Put/Delete, just interpret it as empty (like Delete would)
	  return client_merge_operator.FullMerge(nullptr, stack);

Thus, RocksDB will "stack up" the operations until it reaches a Put or a Delete (or the beginning of the key history), and will then call the user-defined FullMerge() operation with the sequence/stack of operations passed in as a parameter. So, with the above example, it will start at OPk, then go to OPk-1, ..., etc. When RocksDB encounters OP2, it will have a stack looking like [OP3, OP4, ..., OPk] of Merge operands (with OP3 being the front/top of stack). It will then call the user-defined MergeOperator::FullMerge(key, existing_value = OP2, operands = [OP3, OP4, ..., OPk]). This should return the result to the user.

## Compaction

Here comes the fun part, the most crucial background process of rocksdb. Compaction is a process of reducing the history of a key, without affecting any externally observable state. What's an externally observable state? A snapshot basically, represented by a sequence number. Let's look at an example:

	K:   OP1     OP2     OP3     OP4     OP5  ... OPn
	              ^               ^                ^
	              |               |                |
	           snapshot1       snapshot2       snapshot3

For each snapshot, we could define the Supporting operation as the most recent operation that's visible to the snapshot (OP2 is the Supporting operation of snapshot1, OP4 is the Supporting operation of snapshot2...).

Obviously, we could not drop any Supporting operations, without affecting externally observable states. What about other operations? Before the introduction of Merge operation, we could say bye to ALL non-supporting operations. In the above example, a full Compaction would reduce the history of K to OP2 OP4 and OPn. The reason is simple: Put's and Delete's are shortcuts, they hide previous operations.

With merge, the procedure is a bit different. Even if some merge operand is not a Supporting operation for any snapshot, we cannot simply drop it, because later merge operations may rely on it for correctness. Also, in fact, this means that we cannot even drop past Put or Delete operations, because there may be later merge operands that rely on them as well.

So what do we do? We proceed from newest to oldest, "stacking" (and/or PartialMerging) the merge operands. We stop the stacking and process the stack in any one of the following cases (whichever occurs first):
1. a Put/Delete is encountered - we call FullMerge(value or nullptr, stack)
2. End-of-key-history is encountered - we call FullMerge(nullptr, stack)
3. a Supporting operation (snapshot) is encountered - see below
4. End-of-file is encountered - see below 

The first two cases are more-or-less similar to Get(). If you see a Put, call FullMerge(value of put, stack). If you see a delete, likewise.

Compaction introduces two new cases, however. First, if a snapshot is encountered, we must stop the merging process. When this happens, we simply write out the un-merged operands, clear the stack, and continue compacting (starting with the Supporting operation). Similarly, if we have completed compaction ("end-of-file"), we can't simply apply FullMerge(nullptr, stack) because we may not have seen the beginning of the key's history; there may be some entries in some files that happen to not be included in compaction at the time. Hence, in this case, we also have to simply write out the un-merged operands, and clear the stack. In both of these cases, all merge operands become like "Supporting operations", and cannot be dropped.

The role of Partial Merge here is to facilitate compaction. Since it can be very likely that a supporting operation or end-of-file is reached, it can be likely that most merge operands will not be compacted away for a long time. Hence, Merge Operators that support partial merge make it easier for compaction, because the left-over operands will not be stacked, but will be combined into single merge operands before being written out to the new file.

### Example

Let's walk through a concrete example as we come up with rules. Say a counter K starts out as 0, goes through a bunch of Add's, gets reset to 2, and then goes through some more Add's. Now a full Compaction is due (with some externally observable snapshots in place) - what happens?

(Note: In this example we assume associativity, but the idea is the same without PartialMerge as well)

	K:    0    +1    +2    +3    +4     +5      2     +1     +2
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3

	We show it step by step, as we scan from the newest operation to the oldest operation

	K:    0    +1    +2    +3    +4     +5      2    (+1     +2)
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	A Merge operation consumes a previous Merge Operation and produces a new Merge operation (or a stack)
	      (+1  +2) => PartialMerge(1,2) => +3
 
	K:    0    +1    +2    +3    +4     +5      2            +3
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	K:    0    +1    +2    +3    +4     +5     (2            +3)
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	A Merge operation consumes a previous Put operation and produces a new Put operation
	      (2   +3) =>  FullMerge(2, 3) => 5
 
	K:    0    +1    +2    +3    +4     +5                    5
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	A newly produced Put operation is still a Put, thus hides any non-Supporting operations
	      (+5   5) => 5
 
	K:    0    +1    +2   (+3    +4)                          5
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	(+3  +4) => PartialMerge(3,4) => +7
 
	K:    0    +1    +2          +7                           5
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	A Merge operation cannot consume a previous Supporting operation.
	       (+2   +7) can not be combined
 
	K:    0   (+1    +2)         +7                           5
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	(+1  +2) => PartialMerge(1,2) => +3
 
	K:    0          +3          +7                           5
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	K:   (0          +3)         +7                           5
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3
 
	(0   +3) => FullMerge(0,3) => 3
 
	K:               3           +7                           5
	                 ^           ^                            ^
	                 |           |                            |
	              snapshot1   snapshot2                   snapshot3

To sum it up: During Compaction, if a Supporting operation is Merge, it will combine previous operations (via PartialMerge or stacking) until
* another Supporting operation is reached (in others words, we crossed snapshot boundary)
* a Put or a Delete operation is reached, where we convert the Merge operation to a Put.
* end-of-key-history is reached, where we convert Merge operation to a Put
* end-of-Compaction-Files is reached, where we treat it as crossing snapshot boundary 

Note that we assumed that the Merge operation defined PartialMerge() in the example above. For operations without PartialMerge(), the operands will instead be combined on a stack until one of the cases are encountered.

### Issues with this compaction model

In the event that a Put/Delete is not found, for example, if the Put/Delete happens to be in a different file that is not undergoing compaction, then the compaction will simply write out the keys one-by-one as if they were not compacted. The major issue with this is that we did unnecessary work in pushing them to the deque.

Similarly, if there is a single key with MANY merge operations applied to it, then all of these operations must be stored in memory if there is no PartialMerge. In the end, this could lead to a memory overflow or something similar.

**Possible future solution**: To avoid the memory overhead of maintaining a stack/deque, it might be more beneficial to traverse the list twice, once forward to find a Put/Delete, and then once in reverse. This would likely require a lot of disk IO, but it is just a suggestion. In the end we decided not to do this, because for most (if not all) workloads, the in-memory handling should be enough for individual keys. There is room for debate and benchmarking around this in the future.

### Compaction Algorithm

Algorithmically, compaction now works as follows:

	Compaction(snaps, files):
	  // <snaps> is the set of snapshots (i.e.: a list of sequence numbers)
	  // <files> is the set of files undergoing compaction
	  Let input = a file composed of the union of all files
	  Let output = a file to store the resulting entries
 
	  Let stack = [];       // in reality, this should be a "deque", but stack is simpler to conceptualize in this pseudo-code
	  for each v from newest to oldest in input:
	    clear_stack = false
	    if v.sequence_number is in snaps:
	      clear_stack = true
	    else if stack not empty && v.key != stack.top.key:
	      clear_stack = true
 
	    if clear_stack:
	      write out all operands on stack to output (in the same order as encountered)
	      clear(stack)
 
	    if v.type is "merge_operand":
	      push v to stack
	        while (stack has at least 2 elements and (stack.top and stack.second_from_top can be partial-merged)):
	          v1 = stack.pop();
	          v2 = stack.pop();
	          result_v = client_merge_operator.PartialMerge(v1,v2)
	          push result_v to stack
	    if v.type is "put":
	      write client_merge_operator.FullMerge(v, stack) to output
	      clear stack
	    if v.type is "delete":
	      write client_merge_operator.FullMerge(nullptr, stack) to output
	      clear stack
 
	  If stack not empty:
	    if end-of-key-history for key on stack:
	      write client_merge_operator.FullMerge(nullptr, stack) to output
	      clear(stack)
	    else
	      write out all operands on stack to output
	      clear(stack)
 
	  return output

### Picking upper-level files in Compaction

Notice that the relative order of all merge operands should always stay fixed. Since all iterators search the database "level-by-level", we never want to have older merge-operands in an earlier level than newer merge-operands. So, we also had to update compaction so that when it selects its file for compaction, it expands its upper-level files to also include all "earlier" merge-operands. Why does this change things? Because, when any entry is compacted, it will always move to a lower level. So if the merge-operands for a given key are spread over multiple files in the same level, but only some of those files undergo compaction, then that circumstance can happen where the newer merge-operands get pushed down to a later level.

**This was technically a bug in classic rocksdb!** Specifically, this issue was always there, but for most (if not all) applications without merge-operator, it can be assumed that there will be one version of each key per level (except for level-0), since compaction would always compress duplicate Puts to simply contain the latest put value. So this concept of swapping orders was irrelevant, except in level-0 (where compaction always includes all overlapping files). So this has been fixed.

**Some issues**: On malicious inputs, this could lead to always having to include many files during compaction whenever the system really only wanted to pick one file. This could slow things down, but benchmarking suggested that this wasn't really an issue.


# Notes on Efficiency

A quick discussion about efficiency with merge and partial-merge.

**Having a stack of operands can be more efficient.** For instance, in a string-append example (assuming NO partial-merge), providing the user with a stacked set of string operands to append allows the user to amortize the cost of constructing the final string. For example, if I am given a list of 1000 small strings that I need to append, I can use these strings to compute the final size of the string, reserve/allocate space for this result, and then proceed and copy all of the data into the newly allocated memory array. Instead, if I were forced to always partial merge, the system would have to perform the order of 1000 reallocations, once per string operand, each a relatively large size, and the number of bytes having to be allocated in total might be extremely large. In most use-cases this is probably not an issue; and in our benchmarking we found that it really only mattered on a largely in-memory database with hot-keys, but this is something to consider for "growing data". In any case, we provide the user with a choice nonetheless.

The key point to take away from this is that there are cases where having a stack of operands (rather than a single operand) provides an asymptotic increase in the overall efficiency of the operations. For example, in the string-append operator above, the merge-operator can theoretically make the string append operation an amortized O(N)-time operation (where N is the size of the final string after all operations), whereas without a stack, we can have an O(N^2) time operation.

For more information, contact the rocksdb team. Or see the RocksDB wiki page.