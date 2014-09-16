// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "port/port.h"
#include "util/testharness.h"

namespace rocksdb {

namespace {

// A compactor that takes a pointer to Env to manage its threading
class EnvCompactor : public EventListener, public EnvWrapper {
 public:
  // Note that EnvCompactor will not release the input env.
  explicit EnvCompactor(Env* t) : EnvWrapper(t), db_(nullptr), refs_(0) {}

  Status OpenDB(const Options& options, const std::string& name) {
    Status s = DB::Open(options, name, &db_);
    if (!s.ok()) {
      assert(db_ == nullptr);
      return s;
    }
    db_->AddListener(this);
    refs_ = 0;
    return s;
  }

  void ListenTo(DB* db) {
    assert(db);
    if (db_) {
      UnListen();
    }
    db_ = db;
    db_->AddListener(this);
    refs_ = 0;
  }

  void UnListen() {
    // wait until all external compaction jobs are done before
    // closing the db.
    while (RefCount() > 0) {
      SleepForMicroseconds(100);
    }
    db_->RemoveListener(this);
    db_ = nullptr;
  }

 protected:
  void Ref() { refs_++; }
  void Unref() { refs_--; }
  int RefCount() { return refs_.load(); }

  DB* db_;
  std::atomic_int refs_;
};

struct CompactionArg {
  EventListener* driver;
  DB* db;
  std::string cf_name;
  bool triggered_writes_slowdown;
  bool triggered_writes_stop;
};

// A compactor which always triggers full compaction whenever
// a flush happens and generates output file to the right
// level based on the size setting.
class EnvFullCompactor : public EnvCompactor {
 public:
  explicit EnvFullCompactor(Env* env) : EnvCompactor(env) {}

  virtual void OnFlushCompleted(
      DB* db, const std::string& cf_name,
      const std::string& file_path,
      bool triggered_writes_slowdown,
      bool triggered_writes_stop) override {
    // Increment reference count to allow CloseDB() to wait
    // until all submitted compaction jobs are either finished or aborted.
    Ref();

    CompactionArg* arg = new CompactionArg();
    arg->driver = this;
    arg->db = db;
    arg->cf_name = cf_name;
    arg->triggered_writes_slowdown = triggered_writes_slowdown;
    arg->triggered_writes_stop = triggered_writes_stop;
    Schedule(&EnvFullCompactor::CompactFiles, arg, Env::Priority::LOW);
  }

  CompactionJob* PickCompaction(DB* db, const std::string& cf_name) {
    DatabaseMetaData meta;
    db->GetDatabaseMetaData(&meta);
    std::vector<uint64_t> input_file_numbers;

    CompactionJob* job = new CompactionJob();
    job->db = db;
    job->output_path_id = 0;  // set output_path_id = 0 for now
    job->column_family_name = cf_name;
    job->compact_options = CompactionOptions();

    int last_level_with_files = 0;
    for (auto c : meta.column_families) {
      for (auto l : c.levels) {
        for (auto f : l.files) {
          if (f.being_compacted) {
            return nullptr;
          }
        }
        if (last_level_with_files < l.level && l.files.size() > 0) {
          last_level_with_files = l.level;
        }
      }
    }

    // set output_level
    job->output_level = last_level_with_files;
    const Options& opt = db->GetOptions();
    for (uint64_t max_size = opt.max_bytes_for_level_base;
         max_size < meta.size;
         max_size *= opt.max_bytes_for_level_multiplier) {
      job->output_level++;
    }
    if (job->output_level >= db->GetOptions().num_levels) {
      job->output_level = db->GetOptions().num_levels - 1;
    }

    // set input file numbers
    job->input_file_numbers.clear();
    ColumnFamilyMetaData* matched_cf_meta;
    for (auto& cf_meta : meta.column_families) {
      if (cf_meta.name == cf_name) {
        matched_cf_meta = &cf_meta;
        break;
      }
    }
    assert(matched_cf_meta);
    for (auto level_meta : matched_cf_meta->levels) {
      for (auto file_meta : level_meta.files) {
        job->input_file_numbers.push_back(file_meta.file_number);
      }
    }
    return job;
  }

 private:
  static void CompactFiles(void* arg) {
    CompactionArg* carg = reinterpret_cast<CompactionArg*>(arg);
    EnvFullCompactor* compactor =
        reinterpret_cast<EnvFullCompactor*>(carg->driver);
    assert(compactor->RefCount());

    // if writes are stopped due to too many L0 files, it will
    // keep trying compact files until it succeed.
    bool force = carg->triggered_writes_stop;
    do {
      CompactionJob* job = compactor->PickCompaction(carg->db, carg->cf_name);
      if (job != nullptr) {
        Status s;
        s = job->db->CompactFiles(
            job->compact_options, job->column_family_name,
            job->input_file_numbers, job->output_level,
            job->output_path_id);
        if (s.ok()) {
          force = false;
        }
        delete job;
      }
      if (force) {
        compactor->SleepForMicroseconds(10000);
      }
    } while (force);
    compactor->Unref();
    delete carg;
  }
};

}  // anonymous namespace

class CompactorTEST {
 public:
  CompactorTEST() {
    dbname_ = test::TmpDir() + "/compactor_test";
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  DBImpl* dbfull(DB* db) {
    return reinterpret_cast<DBImpl*>(db);
  }

  std::string dbname_;
};

TEST(CompactorTEST, PureExternalCompaction) {
  Options options;
  // disable RocksDB BG compaction
  options.compaction_style = kCompactionStyleNone;
  // configure DB in a way that it will hang if external
  // compactor is not working properly.
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.compression = kNoCompression;
  options.write_buffer_size = 45000;  // Small write buffer
  options.create_if_missing = true;
  options.target_file_size_base = 100000;
  options.max_bytes_for_level_base = 100000;
  options.max_bytes_for_level_multiplier = 2;
  options.IncreaseParallelism();

  EnvFullCompactor compactor(options.env);

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  compactor.ListenTo(db);

  WriteOptions wopts;
  for (int i = 0; i < 100; ++i) {
    db->Put(wopts, std::to_string(i), std::string(45000, 'x'));
  }

  // verify all kvs are still there.
  for (int i = 0; i < 100; ++i) {
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), std::to_string(i), &value));
    ASSERT_EQ(std::string(45000, 'x'), value);
  }

  compactor.UnListen();
  delete db;
}

namespace {
class FullCompactor : public Compactor {
 public:
  explicit FullCompactor(const Options* options) :
      options_(options) {}

  virtual Status SanitizeCompactionInputFiles(
      std::set<uint64_t>* input_files,
      const ColumnFamilyMetaData& cf_meta,
      const int output_level) override {
    return Status::OK();
  }

  // always include all files into a single compaction when possible.
  virtual Status PickCompaction(
      std::vector<uint64_t>* input_file_numbers, int* output_level,
      const ColumnFamilyMetaData& cf_meta) override {
    input_file_numbers->clear();
    *output_level = 0;
    for (auto level : cf_meta.levels) {
      for (auto file : level.files) {
        if (file.being_compacted) {
          input_file_numbers->clear();
          return Status::NotFound();
        }
        input_file_numbers->push_back(file.file_number);
      }
      if (*output_level < level.level) {
        *output_level = level.level;
      }
    }
    (*output_level)++;
    if (*output_level >=  options_->num_levels) {
      *output_level = options_->num_levels - 1;
    }
    return Status::OK();
  }

  virtual Status PickCompactionByRange(
      std::vector<uint64_t>* input_file_Numbers,
      const ColumnFamilyMetaData& cf_meta,
      const int input_level, const int output_level) override {
    return Status::NotSupported("");
  }

 private:
  const Options* options_;
};

class FullCompactorFactory : public CompactorFactory {
 public:
  explicit FullCompactorFactory(const Options* options) :
      options_(options) {}

  virtual Compactor* CreateCompactor() {
    return new FullCompactor(options_);
  }
 private:
  const Options* options_;
};

}  // namespace

TEST(CompactorTEST, PluggableCompactor) {
  Options options;
  // disable RocksDB BG compaction
  options.compaction_style = kCompactionStyleCustom;
  options.compactor_factory.reset(new FullCompactorFactory(&options));
  // configure DB in a way that it will hang if custom
  // compactor is not working properly.
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.compression = kNoCompression;
  options.write_buffer_size = 45000;  // Small write buffer
  options.create_if_missing = true;
  options.target_file_size_base = 100000;
  options.max_bytes_for_level_base = 100000;
  options.max_bytes_for_level_multiplier = 2;
  options.IncreaseParallelism();

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  WriteOptions wopts;
  for (int i = 0; i < 100; ++i) {
    db->Put(wopts, std::to_string(i), std::string(45000, 'x'));
  }

  // verify all kvs are still there.
  for (int i = 0; i < 100; ++i) {
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), std::to_string(i), &value));
    ASSERT_EQ(std::string(45000, 'x'), value);
  }

  delete db;
}

TEST(CompactorTEST, SanitizeOptionsTest) {
  Options options;
  // disable RocksDB BG compaction
  options.compaction_style = kCompactionStyleCustom;
  options.create_if_missing = true;

  DB* db = nullptr;
  ASSERT_TRUE(!DB::Open(options, dbname_, &db).ok());
  ASSERT_TRUE(db == nullptr);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}

