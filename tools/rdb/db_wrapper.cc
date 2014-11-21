#include <iostream>
#include <vector>
#include <v8.h>
#include <node.h>

#include "db_wrapper.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

namespace DBHelpers {
  void printWithBackSlashes(std::string str) {
    for (std::string::size_type i = 0; i < str.size(); i++) {
      if (str[i] == '\\' || str[i] == '"') {
        std::cout << "\\";
      }

      std::cout << str[i];
    }
  }

  bool has_key_for_array(Local<Object> obj, std::string key) {
    return obj->Has(String::NewSymbol(key.c_str())) &&
      obj->Get(String::NewSymbol(key.c_str()))->IsArray();
  }
}

using namespace v8;


Persistent<Function> DBWrapper::constructor;

DBWrapper::DBWrapper(std::string db_file, std::vector<std::string> cfs) {
  std::string db_path = db_file;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.disable_auto_compactions = true;
  options.create_if_missing        = true;

  if (cfs.size() == 1) {
    status = rocksdb::DB::Open(options, db_path, &db);
  } else {
    std::vector<rocksdb::ColumnFamilyDescriptor> families;

    for (std::vector<int>::size_type i = 0; i < cfs.size(); i++) {
      families.push_back(rocksdb::ColumnFamilyDescriptor(
                           cfs[i], rocksdb::ColumnFamilyOptions()));
    }

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    status = rocksdb::DB::Open(options, db_path, families, &handles, &db);

    if (!status.ok()) {
      throw 1;
    }

    for (std::vector<int>::size_type i = 0; i < handles.size(); i++) {
      columnFamilies[cfs[i]] = handles[i];
    }
  }
}

DBWrapper::~DBWrapper() {
  delete db;
}

bool DBWrapper::hasFamilyNamed(std::string name, DBWrapper* db) {
  return db->columnFamilies.find(name) != db->columnFamilies.end();
}


void DBWrapper::Init(Handle<Object> exports) {
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("DBWrapper"));
  tpl->InstanceTemplate()->SetInternalFieldCount(7);
  tpl->PrototypeTemplate()->Set(String::NewSymbol("get"),
    FunctionTemplate::New(Get)->GetFunction()
  );
  tpl->PrototypeTemplate()->Set(String::NewSymbol("put"),
    FunctionTemplate::New(Put)->GetFunction()
  );
  tpl->PrototypeTemplate()->Set(String::NewSymbol("delete"),
    FunctionTemplate::New(Delete)->GetFunction()
  );
  tpl->PrototypeTemplate()->Set(String::NewSymbol("dump"),
    FunctionTemplate::New(Dump)->GetFunction()
  );
  tpl->PrototypeTemplate()->Set(String::NewSymbol("createColumnFamily"),
    FunctionTemplate::New(CreateColumnFamily)->GetFunction()
  );
  tpl->PrototypeTemplate()->Set(String::NewSymbol("writeBatch"),
    FunctionTemplate::New(WriteBatch)->GetFunction()
  );
  tpl->PrototypeTemplate()->Set(String::NewSymbol("compactRange"),
    FunctionTemplate::New(CompactRange)->GetFunction()
  );

  constructor = Persistent<Function>::New(tpl->GetFunction());
  exports->Set(String::NewSymbol("DBWrapper"), constructor);
}

Handle<Value> DBWrapper::New(const Arguments& args) {
  HandleScope scope;
  Handle<Value> to_return;

  if (!(args[0]->IsString() &&
       (args[1]->IsUndefined() || args[1]->IsArray()))) {
    ThrowException(Exception::TypeError(
      String::New("Parameters are incorrect types")
    ));

    return scope.Close(Null());
  }

  std::vector<std::string> cfs = { rocksdb::kDefaultColumnFamilyName };

  if (!args[1]->IsUndefined()) {
    Handle<Array> array = Handle<Array>::Cast(args[1]);
    for (uint i = 0; i < array->Length(); i++) {
      if (!array->Get(i)->IsString()) {
        ThrowException(Exception::TypeError(
          String::New("Non-string column name given")
        ));
        return scope.Close(Null());
      }

      cfs.push_back(*v8::String::Utf8Value(array->Get(i)->ToString()));
    }
  }

  try {
    if (args.IsConstructCall()) {
      DBWrapper* db_wrapper = new DBWrapper(
        *v8::String::Utf8Value(args[0]->ToString()),
        cfs
      );
      db_wrapper->Wrap(args.This());
      return args.This();
    } else {
      const int argc = 2;
      Local<Value> argv[argc] = { args[0], args[1] };
      return scope.Close(constructor->NewInstance(argc, argv));
    }
  } catch(int n) {
    if (n == 1) {
      HandleScope empty_scope;
      ThrowException(Exception::ReferenceError(
        String::New("Column families names do not match those in database")
      ));
      return empty_scope.Close(Null());
    }
  }

  return scope.Close(Null());
}

Handle<Value> DBWrapper::Get(const Arguments& args) {
  HandleScope scope;

  if (!(args[0]->IsString() &&
        (args[1]->IsUndefined() || args[1]->IsString()))) {
    return scope.Close(Null());
  }

  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  std::string key       = *v8::String::Utf8Value(args[0]->ToString());
  std::string cf        = *v8::String::Utf8Value(args[1]->ToString());
  std::string* value    = new std::string();

  if (args[1]->IsUndefined()) {
    db_wrapper->status = db_wrapper->db->Get(
      rocksdb::ReadOptions(), key, value
    );
  } else if (db_wrapper->hasFamilyNamed(cf, db_wrapper)) {
    db_wrapper->status = db_wrapper->db->Get(
      rocksdb::ReadOptions(),
      db_wrapper->columnFamilies[cf],
      key,
      value
    );
  } else {
    return scope.Close(Null());
  }

  Handle<Value> v = db_wrapper->status.ok() ?
    String::NewSymbol(value->c_str()) : Null();

  delete value;

  return scope.Close(v);
}

Handle<Value> DBWrapper::Put(const Arguments& args) {
  HandleScope scope;

  if (!(args[0]->IsString() && args[1]->IsString() &&
       (args[2]->IsUndefined() || args[2]->IsString()))) {
    return scope.Close(Boolean::New(false));
  }

  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  std::string key       = *v8::String::Utf8Value(args[0]->ToString());
  std::string value     = *v8::String::Utf8Value(args[1]->ToString());
  std::string cf        = *v8::String::Utf8Value(args[2]->ToString());

  if (args[2]->IsUndefined()) {
    db_wrapper->status  = db_wrapper->db->Put(
      rocksdb::WriteOptions(), key, value
    );
  } else if (db_wrapper->hasFamilyNamed(cf, db_wrapper)) {
    db_wrapper->status = db_wrapper->db->Put(
      rocksdb::WriteOptions(),
      db_wrapper->columnFamilies[cf],
      key,
      value
    );
  } else {
    return scope.Close(Boolean::New(false));
  }


  return scope.Close(Boolean::New(db_wrapper->status.ok()));
}

Handle<Value> DBWrapper::Delete(const Arguments& args) {
  HandleScope scope;

  if (!args[0]->IsString()) {
    return scope.Close(Boolean::New(false));
  }

  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  std::string arg0      = *v8::String::Utf8Value(args[0]->ToString());
  std::string arg1      = *v8::String::Utf8Value(args[1]->ToString());

  if (args[1]->IsUndefined()) {
    db_wrapper->status = db_wrapper->db->Delete(rocksdb::WriteOptions(), arg0);
  } else {
    if (!db_wrapper->hasFamilyNamed(arg1, db_wrapper)) {
      return scope.Close(Boolean::New(false));
    }
    db_wrapper->status = db_wrapper->db->Delete(
      rocksdb::WriteOptions(),
      db_wrapper->columnFamilies[arg1],
      arg0
    );
  }

  return scope.Close(Boolean::New(db_wrapper->status.ok()));
}

Handle<Value> DBWrapper::Dump(const Arguments& args) {
  HandleScope scope;
  rocksdb::Iterator* iterator;
  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  std::string arg0      = *v8::String::Utf8Value(args[0]->ToString());

  if (args[0]->IsUndefined()) {
    iterator = db_wrapper->db->NewIterator(rocksdb::ReadOptions());
  } else {
    if (!db_wrapper->hasFamilyNamed(arg0, db_wrapper)) {
      return scope.Close(Boolean::New(false));
    }

    iterator = db_wrapper->db->NewIterator(
      rocksdb::ReadOptions(),
      db_wrapper->columnFamilies[arg0]
    );
  }

  iterator->SeekToFirst();

  while (iterator->Valid()) {
    std::cout << "\"";
    DBHelpers::printWithBackSlashes(iterator->key().ToString());
    std::cout << "\" => \"";
    DBHelpers::printWithBackSlashes(iterator->value().ToString());
    std::cout << "\"\n";
    iterator->Next();
  }

  delete iterator;

  return scope.Close(Boolean::New(true));
}

Handle<Value> DBWrapper::CreateColumnFamily(const Arguments& args) {
  HandleScope scope;

  if (!args[0]->IsString()) {
    return scope.Close(Boolean::New(false));
  }

  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  std::string cf_name   = *v8::String::Utf8Value(args[0]->ToString());

  if (db_wrapper->hasFamilyNamed(cf_name, db_wrapper)) {
    return scope.Close(Boolean::New(false));
  }

  rocksdb::ColumnFamilyHandle* cf;
  db_wrapper->status = db_wrapper->db->CreateColumnFamily(
    rocksdb::ColumnFamilyOptions(), cf_name, &cf
  );

  if (!db_wrapper->status.ok()) {
    return scope.Close(Boolean::New(false));
  }

  db_wrapper->columnFamilies[cf_name] = cf;

  return scope.Close(Boolean::New(true));
}

bool DBWrapper::addToBatch(rocksdb::WriteBatch& batch, bool del,
                           Handle<Array> array) {
  Handle<Array> put_pair;
  for (uint i = 0; i < array->Length(); i++) {
    if (del) {
      if (!array->Get(i)->IsString()) {
        return false;
      }

      batch.Delete(*v8::String::Utf8Value(array->Get(i)->ToString()));
      continue;
    }

    if (!array->Get(i)->IsArray()) {
      return false;
    }

    put_pair = Handle<Array>::Cast(array->Get(i));

    if (!put_pair->Get(0)->IsString() || !put_pair->Get(1)->IsString()) {
      return false;
    }

    batch.Put(
      *v8::String::Utf8Value(put_pair->Get(0)->ToString()),
      *v8::String::Utf8Value(put_pair->Get(1)->ToString())
    );
  }

  return true;
}

bool DBWrapper::addToBatch(rocksdb::WriteBatch& batch, bool del,
                           Handle<Array> array, DBWrapper* db_wrapper,
                           std::string cf) {
  Handle<Array> put_pair;
  for (uint i = 0; i < array->Length(); i++) {
    if (del) {
      if (!array->Get(i)->IsString()) {
        return false;
      }
      batch.Delete(
        db_wrapper->columnFamilies[cf],
        *v8::String::Utf8Value(array->Get(i)->ToString())
      );
      continue;
    }

    if (!array->Get(i)->IsArray()) {
      return false;
    }

    put_pair = Handle<Array>::Cast(array->Get(i));

    if (!put_pair->Get(0)->IsString() || !put_pair->Get(1)->IsString()) {
      return false;
    }

    batch.Put(
      db_wrapper->columnFamilies[cf],
      *v8::String::Utf8Value(put_pair->Get(0)->ToString()),
      *v8::String::Utf8Value(put_pair->Get(1)->ToString())
    );
  }

  return true;
}

Handle<Value> DBWrapper::WriteBatch(const Arguments& args) {
  HandleScope scope;

  if (!args[0]->IsArray()) {
    return scope.Close(Boolean::New(false));
  }

  DBWrapper* db_wrapper     = ObjectWrap::Unwrap<DBWrapper>(args.This());
  Handle<Array> sub_batches = Handle<Array>::Cast(args[0]);
  Local<Object> sub_batch;
  rocksdb::WriteBatch batch;
  bool well_formed;

  for (uint i = 0; i < sub_batches->Length(); i++) {
    if (!sub_batches->Get(i)->IsObject()) {
      return scope.Close(Boolean::New(false));
    }
    sub_batch = sub_batches->Get(i)->ToObject();

    if (sub_batch->Has(String::NewSymbol("column_family"))) {
      if (!DBHelpers::has_key_for_array(sub_batch, "put") &&
          !DBHelpers::has_key_for_array(sub_batch, "delete")) {
        return scope.Close(Boolean::New(false));
      }

      well_formed = db_wrapper->addToBatch(
        batch,
        false,
        Handle<Array>::Cast(sub_batch->Get(String::NewSymbol("put"))),
        db_wrapper,
        *v8::String::Utf8Value(sub_batch->Get(
          String::NewSymbol("column_family"))
        )
      );

      well_formed = db_wrapper->addToBatch(
        batch,
        true,
        Handle<Array>::Cast(sub_batch->Get(String::NewSymbol("delete"))),
        db_wrapper,
        *v8::String::Utf8Value(sub_batch->Get(
          String::NewSymbol("column_family"))
        )
      );
    } else {
      well_formed = db_wrapper->addToBatch(
        batch,
        false,
        Handle<Array>::Cast(sub_batch->Get(String::NewSymbol("put")))
      );
      well_formed = db_wrapper->addToBatch(
        batch,
        true,
        Handle<Array>::Cast(sub_batch->Get(String::NewSymbol("delete")))
      );

      if (!well_formed) {
        return scope.Close(Boolean::New(false));
      }
    }
  }

  db_wrapper->status = db_wrapper->db->Write(rocksdb::WriteOptions(), &batch);

  return scope.Close(Boolean::New(db_wrapper->status.ok()));
}

Handle<Value> DBWrapper::CompactRangeDefault(const Arguments& args) {
  HandleScope scope;

  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  rocksdb::Slice begin     = *v8::String::Utf8Value(args[0]->ToString());
  rocksdb::Slice end       = *v8::String::Utf8Value(args[1]->ToString());
  db_wrapper->status    = db_wrapper->db->CompactRange(&end, &begin);

  return scope.Close(Boolean::New(db_wrapper->status.ok()));
}

Handle<Value> DBWrapper::CompactColumnFamily(const Arguments& args) {
  HandleScope scope;

  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  rocksdb::Slice begin  = *v8::String::Utf8Value(args[0]->ToString());
  rocksdb::Slice end    = *v8::String::Utf8Value(args[1]->ToString());
  std::string cf        = *v8::String::Utf8Value(args[2]->ToString());
  db_wrapper->status    = db_wrapper->db->CompactRange(
    db_wrapper->columnFamilies[cf], &begin, &end
  );

  return scope.Close(Boolean::New(db_wrapper->status.ok()));
}

Handle<Value> DBWrapper::CompactOptions(const Arguments& args) {
  HandleScope scope;

  if (!args[2]->IsObject()) {
    return scope.Close(Boolean::New(false));
  }

  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  rocksdb::Slice begin     = *v8::String::Utf8Value(args[0]->ToString());
  rocksdb::Slice end       = *v8::String::Utf8Value(args[1]->ToString());
  Local<Object> options  = args[2]->ToObject();
  int target_level = -1, target_path_id = 0;

  if (options->Has(String::NewSymbol("target_level")) &&
      options->Get(String::NewSymbol("target_level"))->IsInt32()) {
    target_level = (int)(options->Get(
      String::NewSymbol("target_level")
    )->ToInt32()->Value());

    if (options->Has(String::NewSymbol("target_path_id")) ||
        options->Get(String::NewSymbol("target_path_id"))->IsInt32()) {
      target_path_id = (int)(options->Get(
        String::NewSymbol("target_path_id")
      )->ToInt32()->Value());
    }
  }

  db_wrapper->status = db_wrapper->db->CompactRange(
    &begin, &end, true, target_level, target_path_id
  );

  return scope.Close(Boolean::New(db_wrapper->status.ok()));
}

Handle<Value> DBWrapper::CompactAll(const Arguments& args) {
  HandleScope scope;

  if (!args[2]->IsObject() || !args[3]->IsString()) {
    return scope.Close(Boolean::New(false));
  }

  DBWrapper* db_wrapper = ObjectWrap::Unwrap<DBWrapper>(args.This());
  rocksdb::Slice begin  = *v8::String::Utf8Value(args[0]->ToString());
  rocksdb::Slice end    = *v8::String::Utf8Value(args[1]->ToString());
  Local<Object> options = args[2]->ToObject();
  std::string cf        = *v8::String::Utf8Value(args[3]->ToString());

  int target_level = -1, target_path_id = 0;

  if (options->Has(String::NewSymbol("target_level")) &&
      options->Get(String::NewSymbol("target_level"))->IsInt32()) {
    target_level = (int)(options->Get(
      String::NewSymbol("target_level")
    )->ToInt32()->Value());

    if (options->Has(String::NewSymbol("target_path_id")) ||
        options->Get(String::NewSymbol("target_path_id"))->IsInt32()) {
      target_path_id = (int)(options->Get(
        String::NewSymbol("target_path_id")
      )->ToInt32()->Value());
    }
  }

  db_wrapper->status = db_wrapper->db->CompactRange(
    db_wrapper->columnFamilies[cf],
    &begin,
    &end,
    true,
    target_level,
    target_path_id
  );

  return scope.Close(Boolean::New(db_wrapper->status.ok()));
}

Handle<Value> DBWrapper::CompactRange(const Arguments& args) {
  HandleScope scope;

  if (!args[0]->IsString() || !args[1]->IsString()) {
    return scope.Close(Boolean::New(false));
  }

  switch(args.Length()) {
  case 2:
    return CompactRangeDefault(args);
  case 3:
    return args[2]->IsString() ? CompactColumnFamily(args) :
                                 CompactOptions(args);
  default:
    return CompactAll(args);
  }
}

Handle<Value> DBWrapper::Close(const Arguments& args) {
  HandleScope scope;

  delete ObjectWrap::Unwrap<DBWrapper>(args.This());

  return scope.Close(Null());
}
