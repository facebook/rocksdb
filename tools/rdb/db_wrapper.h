#ifndef DBWRAPPER_H
#define DBWRAPPER_H

#include <map>
#include <node.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace v8;

class DBWrapper : public node::ObjectWrap {
  public:
    static void Init(Handle<Object> exports);

  private:
    explicit DBWrapper(std::string db_file, std::vector<std::string> cfs);
    ~DBWrapper();

    // Helper methods
    static bool hasFamilyNamed(std::string name, DBWrapper* db);
    static bool addToBatch(rocksdb::WriteBatch& batch, bool del,
                           Handle<Array> array);
    static bool addToBatch(rocksdb::WriteBatch& batch, bool del,
                           Handle<Array> array,DBWrapper* db_wrapper,
                           std::string cf);
    static Handle<Value> CompactRangeDefault(const v8::Arguments& args);
    static Handle<Value> CompactColumnFamily(const Arguments& args);
    static Handle<Value> CompactOptions(const Arguments& args);
    static Handle<Value> CompactAll(const Arguments& args);


    // C++ mappings of API methods
    static Persistent<v8::Function> constructor;
    static Handle<Value> New(const Arguments& args);
    static Handle<Value> Get(const Arguments& args);
    static Handle<Value> Put(const Arguments& args);
    static Handle<Value> Delete(const Arguments& args);
    static Handle<Value> Dump(const Arguments& args);
    static Handle<Value> WriteBatch(const Arguments& args);
    static Handle<Value> CreateColumnFamily(const Arguments& args);
    static Handle<Value> CompactRange(const Arguments& args);
    static Handle<Value> Close(const Arguments& args);

    // Internal fields
    rocksdb::Options options;
    rocksdb::Status status;
    rocksdb::DB* db;
    std::map<std::string, rocksdb::ColumnFamilyHandle*> columnFamilies;
};

#endif
