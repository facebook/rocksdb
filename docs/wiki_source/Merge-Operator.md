This page describes the Atomic Read-Modify-Write operation in RocksDB, known as the "Merge" operation. It is an interface overview, aimed at the client or RocksDB user who has the questions: when and why should I use Merge; and how do I use Merge?

# Why
RocksDB is a high-performance embedded persistent key-value store. It traditionally provides three simple operations Get, Put and Delete to allow an elegant Lookup-table-like interface. [https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h)

Often times, it's a common pattern to update an existing value in some ways. To do this in rocksdb, the client would have to read (Get) the existing value, modify it and then write (Put) it back to the db. Let's look at a concrete example.

Imagine we are maintaining a set of uint64 counters. Each counter has a distinct name. We would like to support four high level operations: Set, Add, Get and Remove.

First, we define the interface and get the semantics right. Error handling is brushed aside for clarity.

```cpp
class Counters {
 public:
  // (re)set the value of a named counter
  virtual void Set(const string& key, uint64_t value);

  // remove the named counter
  virtual void Remove(const string& key);

  // retrieve the current value of the named counter, return false if not found
  virtual bool Get(const string& key, uint64_t *value);

  // increase the named counter by value.
  // if the counter does not exist,  treat it as if the counter was initialized to zero
  virtual void Add(const string& key, uint64_t value);
  };
```

Second, we implement it with the existing rocksdb support. Pseudo-code follows:

```cpp
    class RocksCounters : public Counters {
     public:
      static uint64_t kDefaultCount = 0;
      RocksCounters(std::shared_ptr<DB> db);

      // mapped to a RocksDB Put
      virtual void Set(const string& key, uint64_t value) {
        string serialized = Serialize(value);
        db_->Put(put_option_, key,  serialized));
      }

      // mapped to a RocksDB Delete
      virtual void Remove(const string& key) {
        db_->Delete(delete_option_, key);
      }

      // mapped to a RocksDB Get
      virtual bool Get(const string& key, uint64_t *value) {
        string str;
        auto s = db_->Get(get_option_, key,  &str);
        if (s.ok()) {
          *value = Deserialize(str);
          return true;
        } else {
          return false;
        }
      }

      // implemented as get -> modify -> set
      virtual void Add(const string& key, uint64_t value) {
        uint64_t base;
        if (!Get(key, &base)) {
          base = kDefaultCount;
        }
        Set(key, base + value);
      }
    };
```

Note that, other than the Add operation, all other three operations can be mapped directly to a single operation in rocksdb. Coding-wise, it's not that bad. However, a conceptually single operation Add is nevertheless mapped to two rocksdb operations. This has performance implication too - random Get is relatively slow in rocksdb.

Now, suppose we are going to host Counters as a service. Given the number of cores of servers nowadays, our service is almost certainly multithreaded. If the threads are not partitioned by the key space, it's possible that multiple Add requests of the same counter, be picked up by different threads and executed concurrently. Well, if we also have strict consistency requirement (missing an update is not acceptable), we would have to wrap Add with external synchronization, a lock of some sort. The overhead adds up.

What if RocksDb directly supports the Add functionality? We might come up with something like this then:

```cpp
    virtual void Add(const string& key, uint64_t value) {
      string serialized = Serialize(value);
      db->Add(add_option, key, serialized);
    }
```

This seems reasonable for Counters. But not everything you store in RocksDB is a counter. Say we need to track the locations where a user has been to. We could store a (serialized) list of locations as the value of a user key. It would be a common operation to add a new location to the existing list. We might want an Append operation in this case: db->Append(user_key, serialize(new_location)). This suggests that the semantics of the read-modify-write operation are really client value-type determined. To keep the library generic, we better abstract out this operation, and allow the client to specify the semantics. That brings us to our proposal: Merge.

# What

**We have developed a generic Merge operation as a new first-class operation in RocksDB to capture the read-modify-write semantics.**

This Merge operation:
* Encapsulates the semantics for read-modify-write into a simple abstract interface.
* Allows user to avoid incurring extra cost from repeated Get() calls.
* Performs back-end optimizations in deciding when/how to combine the operands without changing the underlying semantics.
* Can, in some cases, amortize the cost over all incremental updates to provide asymptotic increases in efficiency.

# How to Use It

In the following sections, the client-specific code changes are explained. We also provide a brief outline of how to use Merge.
It is assumed that the reader already knows how to use classic RocksDB (or LevelDB), including:
* The DB class (including construction, DB::Put(), DB::Get(), and DB::Delete())
* The Options and ColumnFamilyOptions class (and how to specify them)
* Knowledge that all keys/values written to the database are simple strings of bytes.

## Overview of the Interface

We have defined a new interface/abstract-base-class: MergeOperator.
It exposes some functions telling RocksDB how to combine incremental update operations (called "merge operands") with base-values (Put/Delete).
These functions can also be used to tell RocksDB how to combine merge operands with _each other_ to form new merge operands (called "Partial" or "Associative" merging).

For simplicity, we will temporarily ignore this concept of Partial vs. non-Partial merging.
So we have provided a separate interface called AssociativeMergeOperator which encapsulates and hides all of the details around partial merging.
And, for most simple applications (such as in our 64-Bit Counters example above), this will suffice.

So the reader should assume that all merging is handled via an interface called AssociativeMergeOperator.
Here is the public interface:

```cpp
    // The Associative Merge Operator interface.
    // Client needs to provide an object implementing this interface.
    // Essentially, this class specifies the SEMANTICS of a merge, which only
    // client knows. It could be numeric addition, list append, string
    // concatenation, ... , anything.
    // The library, on the other hand, is concerned with the exercise of this
    // interface, at the right time (during get, iteration, compaction...)
    class AssociativeMergeOperator : public MergeOperator {
     public:
      virtual ~AssociativeMergeOperator() {}

      // Gives the client a way to express the read -> modify -> write semantics
      // key:           (IN) The key that's associated with this merge operation.
      // existing_value:(IN) null indicates the key does not exist before this op
      // value:         (IN) the value to update/merge the existing_value with
      // new_value:    (OUT) Client is responsible for filling the merge result here
      // logger:        (IN) Client could use this to log errors during merge.
      //
      // Return true on success. Return false failure / error / corruption.
      virtual bool Merge(const Slice& key,
                         const Slice* existing_value,
                         const Slice& value,
                         std::string* new_value,
                         Logger* logger) const = 0;

      // The name of the MergeOperator. Used to check for MergeOperator
      // mismatches (i.e., a DB created with one MergeOperator is
      // accessed using a different MergeOperator)
      virtual const char* Name() const = 0;

     private:
      ...
    };
```

**Some Notes:**
* AssociativeMergeOperator is a sub-class of a class called MergeOperator. We will see later that the more generic MergeOperator class can be more powerful in certain cases. The AssociativeMergeOperator we use here is, on the other hand, a simpler interface.
* existing_value could be nullptr. This is useful in case the Merge operation is the first operation of a key. nullptr indicates that the 'existing' value does not exist. This basically defers to the client to interpret the semantics of a merge operation without a pre-value. Client could do whatever reasonable. For example, Counters::Add assumes a zero value, if none exists.
* We pass in the key so that client could multiplex the merge operator based on it, if the key space is partitioned and different subspaces refer to different types of data which have different merge operation semantics. For example, the client might choose to store the current balance (a number) of a user account under the key "BAL:uid" and the history of the account activities (a list) under the key "HIS:uid", in the same DB. (Whether or not this is a good practice is debatable). For current balance, numeric addition is a perfect merge operator; for activity history, we would need a list append though. Thus, by passing the key back to the Merge callback, we allow the client to differentiate between the two types.

Example:

```cpp
     void Merge(...) {
       if (key start with "BAL:") {
         NumericAddition(...)
       } else if (key start with "HIS:") {
         ListAppend(...);
       }
     }
```

## Other Changes to the client-visible interface

To use Merge in an application, the client must first define a class which inherits from the AssociativeMergeOperator interface (or the MergeOperator interface as we will see later).
This object class should implement the functions of the interface, which will (eventually) be called by RocksDB at the appropriate time, whenever it needs to apply merging. In this way, the merge-semantics are completely client-specified.

After defining this class, the user should have a way to specify to RocksDB to use this merge operator for its merges. We have introduced additional fields/methods to the DB class and the ColumnFamilyOptions class for this purpose:

```cpp
    // In addition to Get(), Put(), and Delete(), the DB class now also has an additional method: Merge().
    class DB {
      ...
      // Merge the database entry for "key" with "value". Returns OK on success,
      // and a non-OK status on error. The semantics of this operation is
      // determined by the user provided merge_operator when opening DB.
      // Returns Status::NotSupported if DB does not have a merge_operator.
      virtual Status Merge(
        const WriteOptions& options,
        const Slice& key,
        const Slice& value) = 0;
      ...
    };

    Struct ColumnFamilyOptions {
      ...
      // REQUIRES: The client must provide a merge operator if Merge operation
      // needs to be accessed. Calling Merge on a DB without a merge operator
      // would result in Status::NotSupported. The client must ensure that the
      // merge operator supplied here has the same name and *exactly* the same
      // semantics as the merge operator provided to previous open calls on
      // the same DB. The only exception is reserved for upgrade, where a DB
      // previously without a merge operator is introduced to Merge operation
      // for the first time. It's necessary to specify a merge operator when
      // opening the DB in this case.
      // Default: nullptr
      const std::shared_ptr<MergeOperator> merge_operator;
      ...
    };
```

**Note:** The ColumnFamilyOptions::merge_operator field is defined as a shared-pointer to a MergeOperator. As specified above, the AssociativeMergeOperator inherits from MergeOperator, so it is okay to specify an AssociativeMergeOperator here. This is the approach used in the following example.

## Client code change:

Given the above interface change, the client can implement a version of Counters that directly utilizes the built-in Merge operation.

**Counters v2:**

```cpp
    // A 'model' merge operator with uint64 addition semantics
    class UInt64AddOperator : public AssociativeMergeOperator {
     public:
      virtual bool Merge(
        const Slice& key,
        const Slice* existing_value,
        const Slice& value,
        std::string* new_value,
        Logger* logger) const override {

        // assuming 0 if no existing value
        uint64_t existing = 0;
        if (existing_value) {
          if (!Deserialize(*existing_value, &existing)) {
            // if existing_value is corrupted, treat it as 0
            Log(logger, "existing value corruption");
            existing = 0;
          }
        }

        uint64_t oper;
        if (!Deserialize(value, &oper)) {
          // if operand is corrupted, treat it as 0
          Log(logger, "operand value corruption");
          oper = 0;
        }

        auto new = existing + oper;
        *new_value = Serialize(new);
        return true;        // always return true for this, since we treat all errors as "zero".
      }

      virtual const char* Name() const override {
        return "UInt64AddOperator";
       }
    };

    // Implement 'add' directly with the new Merge operation
    class MergeBasedCounters : public RocksCounters {
     public:
      MergeBasedCounters(std::shared_ptr<DB> db);

      // mapped to a leveldb Merge operation
      virtual void Add(const string& key, uint64_t value) override {
        string serialized = Serialize(value);
        db_->Merge(merge_option_, key, serialized);
      }
    };

    // How to use it
    DB* dbp;
    Options options;
    options.merge_operator.reset(new UInt64AddOperator);
    DB::Open(options, "/tmp/db", &dbp);
    std::shared_ptr<DB> db(dbp);
    MergeBasedCounters counters(db);
    counters.Add("a", 1);
    ...
    uint64_t v;
    counters.Get("a", &v);
```

The user interface change is relatively small. And the RocksDB back-end takes care of the rest.

# Associativity vs. Non-Associativity
Up until now, we have used the relatively simple example of maintaining a database of counters. And it turns out that the aforementioned AssociativeMergeOperator interface is generally pretty good for handling many use-cases such as this. For instance, if you wanted to maintain a set of strings, with an "append" operation, then what we've seen so far could be easily adapted to handle that as well.

So, why are these cases considered "simple"? Well, implicitly, we have assumed something about the data: associativity. This means we have assumed that:
* The values that are Put() into the RocksDB database have the same format as the merge operands called with Merge(); and
* It is okay to combine multiple merge operands into a single merge operand using the same user-specified merge operator.

For example, look at the Counters case. The RocksDB database internally stores each value as a serialized 8-byte integer. So, when the client calls Counters::Set (corresponding to a DB::Put()), the argument is exactly in that format. And similarly, when the client calls Counters::Add (corresponding to a DB::Merge()), the merge operand is also a serialized 8-byte integer. This means that, in the client's UInt64AddOperator, the *existing_value may have corresponded to the original Put(), or it may have corresponded to a merge operand; it doesn't really matter! In all cases, as long as the *existing_value and value are given, the UInt64AddOperator behaves in the same way: it adds them together and computes the *new_value. And in turn, this *new_value may be fed into the merge operator later, upon subsequent merge calls.

By contrast, it turns out that RocksDB merge can be used in more powerful ways than this. For example, suppose we wanted our database to store a set of json strings (such as PHP arrays or objects). Then, within the database, we would want them to be stored and retrieved as fully formatted json strings, but we might want the "update" operation to correspond to updating a property of the json object. So we might be able to write code like:

```cpp
    ...
    // Put/store the json string into to the database
    db_->Put(put_option_, "json_obj_key",
             "{ employees: [ {first_name: john, last_name: doe}, {first_name: adam, last_name: smith}] }");

    ...

    // Use a pre-defined "merge operator" to incrementally update the value of the json string
    db_->Merge(merge_option_, "json_obj_key", "employees[1].first_name = lucy");
    db_->Merge(merge_option_, "json_obj_key", "employees[0].last_name = dow");
```

In the above pseudo-code, we see that the data would be stored in RocksDB as a json string (corresponding to the original Put()), but when the client wants to update the value, a "javascript-like" assignment-statement string is passed as the merge-operand. The database would store all of these strings as-is, and would expect the user's merge operator to be able to handle it.

Now, the AssociativeMergeOperator model cannot handle this, simply because it assumes the associativity constraints as mentioned above. That is, in this case, we have to distinguish between the base-values (json strings) and the merge-operands (the assignment statements); and we also don't have an (intuitive) way of combining the merge-operands into a single merge-operand. So this use-case does not fit into our "associative" merge model. That is where the generic MergeOperator interface becomes useful.

# The Generic MergeOperator interface

The MergeOperator interface is designed to support generality and also to exploit some of the key ways in which RocksDB operates in order to provide an efficient solution for "incremental updates". As noted above in the json example, it is possible for the base-value types (Put() into the database) to be formatted completely differently than the merge operands that are used to update them. Also, we will see that it is sometimes beneficial to exploit the fact that some merge operands can be combined to form a single merge operand, while some others may not. It all depends on the client's specific semantics. The MergeOperator interface provides a relatively simple way of providing these semantics as a client.

```cpp
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
      // Return true on success. Return false failure / error / corruption.
      virtual bool FullMerge(const Slice& key,
                             const Slice* existing_value,
                             const std::deque<std::string>& operand_list,
                             std::string* new_value,
                             Logger* logger) const = 0;

      struct MergeOperationInput { ... };
      struct MergeOperationOutput { ... };
      virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                               MergeOperationOutput* merge_out) const;

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

      // Determines whether the MergeOperator can be called with just a single
      // merge operand.
      // Override and return true for allowing a single operand. FullMergeV2 and
      // PartialMerge/PartialMergeMulti should be implemented accordingly to handle
      // a single operand.
      virtual bool AllowSingleOperand() const { return false; }
    };
```

**Some Notes:**
* MergeOperator has two methods, FullMerge() and PartialMerge(). The first method is used when a Put/Delete is the *existing_value (or nullptr). The latter method is used to combine two-merge operands (if possible).
* AssociativeMergeOperator simply inherits from MergeOperator and provides private default implementations of these methods, while exposing a wrapper function for simplicity.
* In MergeOperator, the "FullMerge()" function takes in an *existing_value and a sequence (std::deque) of merge operands, rather than a single operand. We explain below.

### How do these methods work?

On a high level, it should be noted that any call to DB::Put() or DB::Merge() does not necessarily force the value to be computed or the merge to occur immediately. RocksDB will more-or-less lazily decide when to actually apply the operations (e.g.: the next time the user calls Get(), or when the system decides to do its clean-up process called "Compaction"). This means that, when the MergeOperator is actually invoked, it may have several "stacked" operands that need to be applied. Hence, the MergeOperator::FullMerge() function is given an *existing_value and a list of operands that have been stacked. The MergeOperator should then apply the operands one-by-one (or in whatever optimized way the client decides so that the final *new_value is computed _as if_ the operands were applied one-by-one).

### Partial Merge vs. Stacking

Sometimes, it may be useful to start combining the merge-operands as soon as the system encounters them, instead of stacking them. The MergeOperator::PartialMerge() function is supplied for this case. If the client-specified operator can logically handle "combining" two merge-operands into a single operand, the semantics for doing so should be provided in this method, which should then return true. If it is not logically possible, then it should simply leave *new_value unchanged and return false.

Conceptually, when the library decides to begin its stacking and applying process, it first tries to apply the client-specified PartialMerge() on each pair of operands it encounters. Whenever this returns false, it will instead resort to stacking, until it finds a Put/Delete base-value, in which case it will call the FullMerge() function, passing the operands as a list parameter. Generally speaking, this final FullMerge() call should return true. It should really only return false if there is some form of corruption or bad-data.

### How AssociativeMergeOperator fits in

As alluded to above, AssociativeMergeOperator inherits from MergeOperator and allows the client to specify a single merge function. It overrides PartialMerge() and FullMerge() to use this AssociativeMergeOperator::Merge(). It is then used for combining operands, and also when a base-value is encountered. That is why it only works under the "associativity" assumptions described above (and it also explains the name).

### When to allow a single merge operand

Typically a merge operator is invoked only if there are at least two merge operands to act on. Override `AllowSingleOperand()` to return true if you need the merge operator to be invoked even with a single operand. An example use case for this is if you are using merge operator to change the value based on a TTL so that it could be dropped during later compactions (may be using a compaction filter).

## JSON Example

Using our generic MergeOperator interface, we now have the ability to implement the json example.

```cpp
    // A 'model' pseudo-code merge operator with json update semantics
    // We pretend we have some in-memory data-structure (called JsonDataStructure) for
    // parsing and serializing json strings.
    class JsonMergeOperator : public MergeOperator {          // not associative
     public:
      virtual bool FullMerge(const Slice& key,
                             const Slice* existing_value,
                             const std::deque<std::string>& operand_list,
                             std::string* new_value,
                             Logger* logger) const override {
        JsonDataStructure obj;
        if (existing_value) {
          obj.ParseFrom(existing_value->ToString());
        }

        if (obj.IsInvalid()) {
          Log(logger, "Invalid json string after parsing: %s", existing_value->ToString().c_str());
          return false;
        }

        for (const auto& value : operand_list) {
          auto split_vector = Split(value, " = ");      // "xyz[0] = 5" might return ["xyz[0]", 5] as an std::vector, etc.
          obj.SelectFromHierarchy(split_vector[0]) = split_vector[1];
          if (obj.IsInvalid()) {
            Log(logger, "Invalid json after parsing operand: %s", value.c_str());
            return false;
          }
        }

        obj.SerializeTo(new_value);
        return true;
      }


      // Partial-merge two operands if and only if the two operands
      // both update the same value. If so, take the "later" operand.
      virtual bool PartialMerge(const Slice& key,
                                const Slice& left_operand,
                                const Slice& right_operand,
                                std::string* new_value,
                                Logger* logger) const override {
        auto split_vector1 = Split(left_operand, " = ");   // "xyz[0] = 5" might return ["xyz[0]", 5] as an std::vector, etc.
        auto split_vector2 = Split(right_operand, " = ");

        // If the two operations update the same value, just take the later one.
        if (split_vector1[0] == split_vector2[0]) {
          new_value->assign(right_operand.data(), right_operand.size());
          return true;
        } else {
          return false;
        }
      }

      virtual const char* Name() const override {
        return "JsonMergeOperator";
       }
    };

    ...

    // How to use it
    DB* dbp;
    Options options;
    options.merge_operator.reset(new JsonMergeOperator);
    DB::Open(options, "/tmp/db", &dbp);
    std::shared_ptr<DB> db_(dbp);
    ...
    // Put/store the json string into to the database
    db_->Put(put_option_, "json_obj_key",
             "{ employees: [ {first_name: john, last_name: doe}, {first_name: adam, last_name: smith}] }");

    ...

    // Use the "merge operator" to incrementally update the value of the json string
    db_->Merge(merge_option_, "json_obj_key", "employees[1].first_name = lucy");
    db_->Merge(merge_option_, "json_obj_key", "employees[0].last_name = dow");
```

# Error Handling

If the MergeOperator::PartialMerge() returns false, this is a signal to RocksDB that the merging should be deferred (stacked) until we find a Put/Delete value to FullMerge() with.
However, if FullMerge() returns false, then this is treated as "corruption" or error. This means that RocksDB will usually reply to the client with a Status::Corruption message or something similar.
Hence, the MergeOperator::FullMerge() method should only return false if there is absolutely no robust way of handling the error within the client logic itself.
(See the JsonMergeOperator example)

For AssociativeMergeOperator, the Merge() method follows the same "error" rules as MergeOperator::FullMerge() in terms of error-handling. Return false only if there is no logical way of dealing with the values. In the Counters example above, our Merge() always returns true, since we can interpret any bad value as 0.

# Get Merge Operands
This is an API to allow for fetching all merge operands associated with a Key. The main motivation for this API is to support use cases where doing a full online merge is not necessary as it is performance sensitive. This API is available from version 6.4.0.                                                                               
Example use-cases:
1. Storing a KV pair where V is a collection of sorted integers and new values may get appended to the collection and subsequently users want to search for a value in the collection.                                        
Example KV:                                                                                                      
Key: ‘Some-Key’                                                                                                 Value: [2], [3,4,5], [21,100], [1,6,8,9]                                                                                                                                                                  
To store such a KV pair users would typically call the Merge API as:                                             
           a. db→Merge(WriteOptions(), 'Some-Key', '2');                                                         
           b. db→Merge(WriteOptions(), 'Some-Key', '3,4,5');                                                     
           c. db→Merge(WriteOptions(), 'Some-Key', '21,100');                                                    
           d. db→Merge(WriteOptions(), 'Some-Key', '1,6,8,9');                                                             
and implement a Merge Operator that would simply convert the Value to [2,3,4,5,21,100,1,6,8,9] upon a Get API call and then search in the resultant value. In such a case doing the merge online is unnecessary and simply returning all the operands [2], [3,4,5], [21, 100] and [1,6,8,9] and then search through the sub-lists proves to be faster while saving CPU and achieving the same outcome.                                                                                                                                                                                          

2. Update subset of columns and read subset of columns -
    Imagine a SQL Table, a row may be encoded as a KV pair. If there are many columns and users only updated one of them, we can use merge operator to reduce write amplification. While users only read one or two columns in the read query, this feature can avoid a full merging of the whole row, and save some CPU.

3. Updating very few attributes in a value which is a JSON-like document -
    Updating one attribute can be done efficiently using merge operator, while reading back one attribute can be done more efficiently if we don't need to do a full merge.

```cpp
 API: 
  // Returns all the merge operands corresponding to the key. If the
  // number of merge operands in DB is greater than
  // merge_operands_options.expected_max_number_of_operands
  // no merge operands are returned and status is Incomplete. Merge operands
  // returned are in the order of insertion.
  // merge_operands- Points to an array of at-least
  //             merge_operands_options.expected_max_number_of_operands and the
  //             caller is responsible for allocating it. If the status
  //             returned is Incomplete then number_of_operands will contain
  //             the total number of merge operands found in DB for key.
  virtual Status GetMergeOperands(
      const ReadOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, PinnableSlice* merge_operands,
      GetMergeOperandsOptions* get_merge_operands_options,
      int* number_of_operands) = 0;

  Example: 
  int size = 100;
  int number_of_operands = 0;
  std::vector values(size);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = size;
  db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), "k1", values.data(), merge_operands_info, 
  &number_of_operands);
```
The above API returns all the merge operands corresponding to the key. If the number of merge operands in DB is greater than merge_operands_options.expected_max_number_of_operands, no merge operands are returned and status is Incomplete. Merge operands returned are in the order of insertion.

DB Bench has a benchmark that uses Example 1 to demonstrate the performance difference of doing an online merge and then operating on the collection vs simply returning the sub-lists and operating on the sub-lists. To run the benchmark the command is :                                                                                        
`./db_bench -benchmarks=getmergeoperands --merge_operator=sortlist`  
The merge_operator used above is used to sort the data across all the sublists for the online merge case which happens automatically when Get API is called.   

# Review and Best Practices
Altogether, we have described the Merge Operator, and how to use it. Here are a couple tips on when/how to use the MergeOperator and AssociativeMergeOperator depending on use-cases.

## When to use merge

If the following are true:
* You have data that needs to be incrementally updated.
* You would usually need to read the data before knowing what the new value would be.

Then use one of the two Merge operators as specified in this wiki.

## Associative Data

If the following are true:
* Your merge operands are formatted the same as your Put values, AND
* It is okay to combine multiple operands into one (as long as they are in the same order)

Then use **AssociativeMergeOperator**.

## Generic Merge
If either of the two associativity constraints do not hold, then use **MergeOperator**.

If there are some times where it is okay to combine multiple operands into one (but not always):
* Use **MergeOperator**
* Have the PartialMerge() function return true in cases where the operands can be combined.

## Tips

**Multiplexing:** While a RocksDB DB object can only be passed 1 merge-operator at the time of construction, your user-defined merge operator class can behave differently depending on the data passed to it. The key, as well as the values themselves, will be passed to the merge operator; so one can encode different "operations" in the operands themselves, and get the MergeOperator to perform different functions accordingly.

**Is my use-case Associative?:** If you are unsure of whether the "associativity" constraints apply to your use-case, you can ALWAYS use the generic MergeOperator. The AssociativeMergeOperator is a direct subclass of MergeOperator, so any use-case that can be solved with the AssociativeMergeOperator can be solved with the more generic MergeOperator. The AssociativeMergeOperator is mostly provided for convenience.

## Useful Links
* [[Merge+Compaction Implementation Details|Merge-Operator-Implementation]]: For RocksDB engineers who want to know how MergeOperator affects their code.
