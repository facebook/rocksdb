//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "db/compaction/compaction_picker_level.h"
#include "port/stack_trace.h"
#include "port/port.h"
#include "rocksdb/experimental.h"
#include "rocksdb/utilities/convenience.h"
#include "test_util/sync_point.h"
#include <chrono>
#include <thread>
#include <cmath>
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/merge_operators/string_append/stringappend2.h"
#include "db/value_log.h"
#include <regex>

namespace rocksdb {

// SYNC_POINT is not supported in released Windows mode.
#if !defined(ROCKSDB_LITE)

class DBVLogTest : public DBTestBase {
 public:
  // these copied from CompactionPickerTest
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableCFOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  LevelCompactionPicker level_compaction_picker;
  std::string cf_name_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::unique_ptr<VersionStorageInfo> vstorage_;
  std::vector<std::unique_ptr<FileMetaData>> files_;
  // does not own FileMetaData
  std::unordered_map<uint32_t, std::pair<FileMetaData*, int>> file_map_;
  // input files to compaction process.
  std::vector<CompactionInputFiles> input_files_;
  int compaction_level_start_;

  DBVLogTest() : DBTestBase("/db_compaction_test"), ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        ioptions_(options_),
        mutable_cf_options_(options_),
        level_compaction_picker(ioptions_, &icmp_),
        cf_name_("dummy"),
        file_num_(1),
        vstorage_(nullptr) {
     fifo_options_.max_table_files_size = 1;
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    ioptions_.cf_paths.emplace_back("dummy",
                                    std::numeric_limits<uint64_t>::max());}

  void DeleteVersionStorage() {
    vstorage_.reset();
    files_.clear();
    file_map_.clear();
    input_files_.clear();
  }

  void NewVersionStorage(int num_levels, CompactionStyle style) {
    DeleteVersionStorage();
    options_.num_levels = num_levels;

    vstorage_.reset(new VersionStorageInfo(&icmp_, ucmp_, options_.num_levels,
                                           style, nullptr, false
    , static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd()  // needs to be &c_f_d for AR compactions
    ));
    vstorage_->CalculateBaseBytes(ioptions_, mutable_cf_options_);
  }

  void UpdateVersionStorageInfo() {
    vstorage_->CalculateBaseBytes(ioptions_, mutable_cf_options_);
    vstorage_->UpdateFilesByCompactionPri(ioptions_.compaction_pri);
    vstorage_->UpdateNumNonEmptyLevels();
    vstorage_->GenerateFileIndexer();
    vstorage_->GenerateLevelFilesBrief();
    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
    vstorage_->GenerateLevel0NonOverlapping();
    vstorage_->ComputeFilesMarkedForCompaction();
    vstorage_->SetFinalized();
  }

  void SetUpForActiveRecycleTest();

};

class DBVLogTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  DBVLogTestWithParam() : DBTestBase("/db_compaction_test") {
    max_subcompactions_ = std::get<0>(GetParam());
    exclusive_manual_compaction_ = std::get<1>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t max_subcompactions_;
  bool exclusive_manual_compaction_;
};

class DBCompactionDirectIOTest : public DBVLogTest,
                                 public ::testing::WithParamInterface<bool> {
 public:
  DBCompactionDirectIOTest() : DBVLogTest() {}
};

namespace {

class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() {}
  ~FlushedFileCollector() {}

  virtual void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (auto fname : flushed_files_) {
      result.push_back(fname);
    }
    return result;
  }

  void ClearFlushedFiles() { flushed_files_.clear(); }

 private:
  std::vector<std::string> flushed_files_;
  std::mutex mutex_;
};

static const int kCDTValueSize = 1000;
static const int kCDTKeysPerBuffer = 4;
static const int kCDTNumLevels = 8;
/* Unused Function DeletionTriggerOptions */
/*
Options DeletionTriggerOptions(Options options) {
  options.compression = kNoCompression;
  options.write_buffer_size = kCDTKeysPerBuffer * (kCDTValueSize + 24);
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number_to_maintain = 0;
  options.num_levels = kCDTNumLevels;
  options.level0_file_num_compaction_trigger = 1;
  options.target_file_size_base = options.write_buffer_size * 2;
  options.target_file_size_multiplier = 2;
  options.max_bytes_for_level_base =
      options.target_file_size_base * options.target_file_size_multiplier;
  options.max_bytes_for_level_multiplier = 2;
  options.disable_auto_compactions = false;
  return options;
}
*/

/* Unused Function HaveOverlappingKeyRanges */
/*
bool HaveOverlappingKeyRanges(
    const Comparator* c,
    const SstFileMetaData& a, const SstFileMetaData& b) {
  if (c->Compare(a.smallestkey, b.smallestkey) >= 0) {
    if (c->Compare(a.smallestkey, b.largestkey) <= 0) {
      // b.smallestkey <= a.smallestkey <= b.largestkey
      return true;
    }
  } else if (c->Compare(a.largestkey, b.smallestkey) >= 0) {
    // a.smallestkey < b.smallestkey <= a.largestkey
    return true;
  }
  if (c->Compare(a.largestkey, b.largestkey) <= 0) {
    if (c->Compare(a.largestkey, b.smallestkey) >= 0) {
      // b.smallestkey <= a.largestkey <= b.largestkey
      return true;
    }
  } else if (c->Compare(a.smallestkey, b.largestkey) <= 0) {
    // a.smallestkey <= b.largestkey < a.largestkey
    return true;
  }
  return false;
}
*/

// Identifies all files between level "min_level" and "max_level"
// which has overlapping key range with "input_file_meta".
/* Unused Function GetOverlappingFileNumbersForLevelCompaction */
/*
void GetOverlappingFileNumbersForLevelCompaction(
    const ColumnFamilyMetaData& cf_meta,
    const Comparator* comparator,
    int min_level, int max_level,
    const SstFileMetaData* input_file_meta,
    std::set<std::string>* overlapping_file_names) {
  std::set<const SstFileMetaData*> overlapping_files;
  overlapping_files.insert(input_file_meta);
  for (int m = min_level; m <= max_level; ++m) {
    for (auto& file : cf_meta.levels[m].files) {
      for (auto* included_file : overlapping_files) {
        if (HaveOverlappingKeyRanges(
                comparator, *included_file, file)) {
          overlapping_files.insert(&file);
          overlapping_file_names->insert(file.name);
          break;
        }
      }
    }
  }
}
*/

/* Unused Function VerifyCompactionResult */
/*
void VerifyCompactionResult(
    const ColumnFamilyMetaData& cf_meta,
    const std::set<std::string>& overlapping_file_numbers) {
#ifndef NDEBUG
  for (auto& level : cf_meta.levels) {
    for (auto& file : level.files) {
      assert(overlapping_file_numbers.find(file.name) ==
             overlapping_file_numbers.end());
    }
  }
#endif
}
*/

/* Unused Function PickFileRandomly */
/*
const SstFileMetaData* PickFileRandomly(
    const ColumnFamilyMetaData& cf_meta,
    Random* rand,
    int* level = nullptr) {
  auto file_id = rand->Uniform(static_cast<int>(
      cf_meta.file_count)) + 1;
  for (auto& level_meta : cf_meta.levels) {
    if (file_id <= level_meta.files.size()) {
      if (level != nullptr) {
        *level = level_meta.level;
      }
      auto result = rand->Uniform(file_id);
      return &(level_meta.files[result]);
    }
    file_id -= static_cast<uint32_t>(level_meta.files.size());
  }
  assert(false);
  return nullptr;
}
*/
}  // anonymous namespace


static void ListVLogFileSizes(DBVLogTest *db, std::vector<uint64_t>& vlogfilesizes){
  vlogfilesizes.clear();
  // Get the list of file in the DB, cull that down to VLog files
  std::vector<std::string> filenames;
  ASSERT_OK(db->env_->GetChildren(db->db_->GetOptions().db_paths.back().path, &filenames));
  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t number;
    FileType type;
    if (ParseFileName(filenames[i], &number, &type)) {
      if (type == kVLogFile) {
        // vlog file.  Extract the CF id from the file extension
        size_t dotpos=filenames[i].find_last_of('.');  // find start of extension
        std::string cfname(filenames[i].substr(dotpos+1+kRocksDbVLogFileExt.size()));
        uint64_t file_size; db->env_->GetFileSize(db->db_->GetOptions().db_paths.back().path+"/"+filenames[i], &file_size);
        vlogfilesizes.emplace_back(file_size);
      }
    }
  }
}

// Riffle through the stats return string and extract the info for the Rings.  frag is reported as a fraction between 0 and 1
static void DecodeRingStats(std::string stats, uint64_t& r0files, double& r0size, double& r0frag,
   uint64_t& sumfiles, double& sumsize, double& sumfrag) {
  std::smatch matchres;
  std::regex r0info (".*Ring Stats.*\\n.*\\n.*\\n.*R0 *([0-9]*) *([0-9.]*) *([KMGT])B *([0-9.]*) *\\n");
  ASSERT_EQ(regex_search(stats,matchres,r0info), true);
  r0files = std::stoll(matchres[1].str());
  r0size = std::stod(matchres[2].str()) * (double)(1LL<<(10 * (1 + std::string("KMGT").find(matchres[3].str()))));
  r0frag =  std::stod(matchres[4].str()) / 100.0;
  std::regex suminfo (".*Ring Stats.*\\n.*\\n.*\\n.*\\n.*Sum *([0-9]*) *([0-9.]*) *([KMGT])B *([0-9.]*) *\\n");
  ASSERT_EQ(regex_search(stats,matchres,suminfo), true);
  sumfiles = std::stoll(matchres[1].str());
  sumsize = std::stod(matchres[2].str()) * (double)(1LL<<(10 * (1 + std::string("KMGT").find(matchres[3].str()))));
  sumfrag =  std::stod(matchres[4].str()) / 100.0;
}




#if 1  // remove if no long functions enabled
static std::string LongKey(int i, int len) { return DBTestBase::Key(i).append(len,' '); }
#endif

#if 1  // turn on for sequential write test with multiple L0 compactions
TEST_F(DBVLogTest, SequentialWriteTest) {
 // Generate Puts as fast as we can, to overrun the compactor and  force multiple parallel L0 compactions
  const int32_t value_ref_size = 16;  // length of indirect reference
//  const int32_t vlogoverhead = 5;  // # bytes added for header & CRC //-Werror=unused-variable
// obsolete   int32_t value_size = 18;  // 10 KB
// obsolete   int32_t key_size = 10 * 1024 - value_size;
  int32_t value_size = 30;  // 10 KB
  int32_t key_size = 18;
  int32_t batch_size = 100000; // scaf 200000;   number of keys in database


  Options options = CurrentOptions();
  options.max_bytes_for_level_multiplier = 10;

  batch_size = std::max(batch_size,(int32_t)std::exp(3*std::log(options.max_bytes_for_level_multiplier)));  // at least 1000 keys in L1

  options.num_levels = (int)std::floor(std::log(batch_size)/std::log(options.max_bytes_for_level_multiplier)-0.9);  // 1000 keys=> L1 only, 200000 keys=>3 levels on disk: 1,2,3
  uint64_t kvsl1 = (uint64_t)std::exp(std::log(batch_size)-(options.num_levels-1-1)*std::log(options.max_bytes_for_level_multiplier));   // # kvs in L1
  uint64_t bytesperkvinsst=key_size+value_ref_size+20;
  uint64_t bytesperkvinmemtable=key_size+value_size+20;
  options.max_bytes_for_level_base = bytesperkvinsst*kvsl1;  // size of all SSTs for those kvs
  uint64_t L1filecount=10;  // desired # files in full L1
  uint64_t sstkvcount = kvsl1/L1filecount;  // # kvs in a single file
  double  nfilesperl0flush=3.5;   // when we get this many files, flush the memtables
  options.level0_file_num_compaction_trigger =6;
  options.target_file_size_base=bytesperkvinsst*sstkvcount;
  options.write_buffer_size = (uint64_t)((nfilesperl0flush/options.level0_file_num_compaction_trigger)*bytesperkvinmemtable*sstkvcount);
  options.level0_slowdown_writes_trigger = 30;
  options.level0_stop_writes_trigger = 50;
  options.max_background_compactions = 8;

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({20});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({15});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_size_trigger = std::vector<uint64_t>({(uint64_t)(0.8*batch_size*value_size)});   // start AR when the DB is almost full  scaf should be 0.8
  options.active_recycling_size_trigger = std::vector<uint64_t>({0});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.paranoid_file_checks = true;

  options.vlogfile_max_size = std::vector<uint64_t>({sstkvcount*((value_size+10))});  // amount of value in 1 sst, allowing for CRC & header
  options.max_compaction_bytes = (uint64_t)((options.max_bytes_for_level_multiplier + 12) * options.target_file_size_base);

  options.vlog_direct_IO = false;
  options.merge_operator.reset(new StringAppendOperator(','));
  options.stats_dump_period_sec=10;

  printf("Starting: #levels=%d, max_bytes_for_level_base=%zd target_file_size_base=%zd write_buffer_size=%zd vlogfile_max_size=%zd\n",options.num_levels,options.max_bytes_for_level_base, options.target_file_size_base,options.write_buffer_size,options.vlogfile_max_size[0]);
  DestroyAndReopen(options);

  int64_t numcompactions = 0;  // total number of non-AR compactions (coming from L1 or higher)
  int64_t numARs = 0;  // number of ARs
  int64_t numfinalcompactions = 0;  // compactions into last level
  double totalref0position = 0.0;  //  total of ref0 position as a % of ring size
//  int64_t vlogtotalsize = 0;  // number of bytes in VLog, after removing fragmentation
  const int64_t debugmode = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::InstallCompactionResults", [&](void* arg) {
        uint64_t *compact_stats = static_cast<uint64_t *>(arg);
        if(0&&(compact_stats[0]|compact_stats[1]|compact_stats[2])!=0){
          printf("Compaction: %zd bytes in, %zd written to %zd files, %zd remapped\n",compact_stats[1],compact_stats[0],compact_stats[3],compact_stats[2]);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionBuilder::PickCompaction", [&](void* arg) {
        int64_t *pickerinfo = static_cast<int64_t *>(arg);
        ColumnFamilyData *cfd=(ColumnFamilyData*)pickerinfo[6];
        int printreq = 0;
        if(cfd!=nullptr){
          if(pickerinfo[7]){ // Active recycle
            ++numARs;  // inc count of ARs
            if(debugmode&1){printf("PickCompaction: AR L%zd->%zd , #input SSTs=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[2],pickerinfo[3],pickerinfo[4]); printreq = 1;}
          } else {  // normal compaction
            ++numcompactions;
            if(pickerinfo[5]==(options.num_levels-1)){
              // compaction into final level
              if(debugmode&1){printf("PickCompaction: Level %zd->%zd, exp ref0=%zd, act ref0=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[1],pickerinfo[2],pickerinfo[3],pickerinfo[4]); printreq = 1;}
              if(pickerinfo[2]!=~0LL){  // don't count comps with no ref0.  They should not occur during the final data-collection pass
                ++numfinalcompactions;  // inc # final compactions
                totalref0position += (double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]);  // add in fractional position of ref0 file
if((double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]) > 1.0  || (double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]) < 0.0)
 printf("invalid ref0\n"); // scaf
              }
            }else{
              // compaction before final level
              if(debugmode&2){printf("PickCompaction: Level %zd->%zd, exp ref0=%zd, act ref0=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[1],pickerinfo[2],pickerinfo[3],pickerinfo[4]); printreq = 1;}
            }
          }
          if(printreq){
            for(int t=0;t<options.num_levels;){printf("%d",cfd->current()->storage_info()->NumLevelFiles(t)); if(++t==options.num_levels)break; printf(", ");}
            printf("}\n");
          }
        }
      });


  VLogRingRestartInfo restartinfo;
  SyncPoint::GetInstance()->SetCallBack(
      "VLogRingRestartInfo::Coalesce", [&](void* arg) {
         restartinfo = *static_cast<VLogRingRestartInfo *>(arg);
// obsolete printf("size=%zd, frag=%zd, net size=%zd\n",restartinfo.size,restartinfo.frag,restartinfo.size-restartinfo.frag);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // Whenever we write, we have to housekeep the vlogtotalsize: remove the deleted key, add the new

  Random rnd(301);
  const size_t valuexmax = 15;
  std::vector<std::string> values(valuexmax+1);

  //Create random values to use
  for(size_t i=0;i<values.size();++i)values[i] = RandomString(&rnd, value_size);
 
  for(int32_t k=0;k<2;++k) {  // scaf 2
    // Reinit stats variables so they get the last-pass data
    numcompactions = 0;  // total number of non-AR compactions (coming from L1 or higher)
    numARs = 0;  // number of ARs
    numfinalcompactions = 0;  // compactions into last level
    totalref0position = 0.0;  //  total of ref0 position as a % of ring size
    // Many files 4 [300 => 4300)
    for (int32_t i = 0; i < 2; i++) {  // scaf 5
      for (int32_t j = 0; j < batch_size+300; j++) {
        Status s = Put(LongKey(j,key_size), values[j&valuexmax]);

        if(!s.ok())
          printf("Put failed\n");
        ASSERT_OK(s);
      }
      printf("\n\nbatch\n\n");
      std::this_thread::sleep_for(std::chrono::seconds(5));  // give the compactor time to run
    }
//  ASSERT_OK(Flush());
//  dbfull()->TEST_WaitForFlushMemTable();
//  dbfull()->TEST_WaitForCompact();

    for (int32_t j = 0; j < batch_size; j++) {
      std::string getresult = Get(LongKey(j,key_size));
      ASSERT_EQ(getresult,values[j&valuexmax]);
    }
    printf("...verified.\n");
    TryReopen(options);
    printf("reopened...\n");
  }
}
#endif

#if 0  // turn on for long r/w test.  This is the TRocks random-load test, and verifies that compaction picking picks oldest references
TEST_F(DBVLogTest, IndirectCompactionPickingTest) {
  const int32_t value_ref_size = 16;  // length of indirect reference
// obsolete   int32_t value_size = 18;  // 10 KB
// obsolete   int32_t key_size = 10 * 1024 - value_size;
  int32_t value_size = 800;  // 10 KB
  int32_t key_size = 18;
  int32_t value_size_var = 20;
  int32_t batch_size = 20000; // if you make this smaller you'd better reduce num_levels accordingly 00

  Options options = CurrentOptions();

  options.num_levels = 4;  // 3 levels on disk: 1,2,3
  options.max_bytes_for_level_multiplier = 10;
  uint64_t kvsl1 = (uint64_t)std::exp(std::log(batch_size)-(options.num_levels-1-1)*std::log(options.max_bytes_for_level_multiplier));   // # kvs in L1
  uint64_t bytesperkvinsst=key_size+value_ref_size+20;
  uint64_t bytesperkvinmemtable=key_size+value_size+20;
  options.max_bytes_for_level_base = bytesperkvinsst*kvsl1;  // size of all SSTs for those kvs
  uint64_t L1filecount=10;  // desired # files in full L1
  uint64_t sstkvcount = kvsl1/L1filecount;  // # kvs in a single file
  double  nfilesperl0flush=2.5;   // when we get this many files, flush the memtable
  options.target_file_size_base=bytesperkvinsst*sstkvcount;
  options.write_buffer_size = (uint64_t)(nfilesperl0flush*bytesperkvinmemtable*sstkvcount);
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.max_background_compactions = 3;
  options.stats_dump_period_sec = 30;

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({20});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({15});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_size_trigger = std::vector<uint64_t>({(uint64_t)(0.8*batch_size*value_size)});   // start AR when the DB is almost full
  options.active_recycling_sst_minct = std::vector<int32_t>({15});
  options.active_recycling_sst_maxct = std::vector<int32_t>({25});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({15});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({sstkvcount*(value_size+10)});  // amount of value in 1 sst, allowing for CRC & header

  options.vlog_direct_IO = false;
  options.statistics = CreateDBStatistics();

  printf("Starting: max_bytes_for_level_base=%zd target_file_size_base=%zd write_buffer_size=%zd vlogfile_max_size=%zd\n",options.max_bytes_for_level_base, options.target_file_size_base,options.write_buffer_size,options.vlogfile_max_size[0]);
  DestroyAndReopen(options);

  int64_t numcompactions = 0;  // total number of non-AR compactions (coming from L1 or higher)
  int64_t numARs = 0;  // number of ARs
  int64_t numfinalcompactions = 0;  // compactions into last level
  double totalref0position = 0.0;  //  total of ref0 position as a % of ring size

  // We want our statistics to show the steady-state status.  To do that, we set numcompactions negative so that we ignore the startup transient and
  // take stats only after a delay.  We originally reset the stats inside the loop below, but that fails thread-safety since compactions are running
  // at the same time
  int64_t npasses=3;  // number of overall passes
  numcompactions = -8500*(npasses-1);  // set number to ignore - all but the last pass

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::InstallCompactionResults", [&](void* arg) {
        uint64_t *compact_stats = static_cast<uint64_t *>(arg);
        if(0&&(compact_stats[0]|compact_stats[1]|compact_stats[2])!=0){
          printf("Compaction: %zd bytes in, %zd written to %zd files, %zd remapped\n",compact_stats[1],compact_stats[0],compact_stats[3],compact_stats[2]);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionBuilder::PickCompaction", [&](void* arg) {
        int64_t *pickerinfo = static_cast<int64_t *>(arg);
        ColumnFamilyData *cfd=(ColumnFamilyData*)pickerinfo[6];
        if(numcompactions<0){  // if we are still counting in...
          ++numcompactions;  // count in and nothing else
        }else if(cfd!=nullptr){  // to reduce typeout, type only if coming out of L1 or higher
          int doprint = pickerinfo[0]>0;  // true if we type out
          doprint = 0;  // suppress print normally
          if(pickerinfo[7]){ // Active recycle
            ++numARs;  // inc count of ARs
            if(doprint)printf("PickCompaction: AR L%zd->%zd , #input SSTs=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[2],pickerinfo[3],pickerinfo[4]);
          } else {  // normal compaction
            ++numcompactions;
            if(doprint)printf("PickCompaction: Level %zd->%zd, exp ref0=%zd, act ref0=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[1],pickerinfo[2],pickerinfo[3],pickerinfo[4]);
            if(pickerinfo[5]==(options.num_levels-1)){
              // compaction into final level
              if(pickerinfo[2]!=~0LL){  // don't count comps with no ref0.  They should not occur during the final data-collection pass
                ++numfinalcompactions;  // inc # final compactions
                totalref0position += (double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]);  // add in fractional position of ref0 file
if((double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]) > 1.0  || (double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]) < 0.0)
 printf("invalid ref0\n"); // scaf
              }
            }
          }
          if(doprint){
            for(int t=0;t<options.num_levels;){printf("%d",cfd->current()->storage_info()->NumLevelFiles(t)); if(++t==options.num_levels)break; printf(", ");}
            printf("}\n");
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(Get(LongKey(0,key_size)), values[0]);
  ASSERT_EQ(Get(LongKey(299,key_size)), values[299]);

  // 2 files in L0
// obsolete   ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L2
//  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
  }
  ASSERT_OK(Flush());
  for (int32_t i = 300; i < 300+batch_size; i++) {
    std::string vstg =  RandomString(&rnd, value_size);
    // append compressible suffix
    vstg.append(rnd.Next()%value_size_var,'a');
    values[i] = vstg;
  }

  for(int32_t k=0;k<npasses;++k) {
    // Many files 4 [300 => 4300)
    for (int32_t i = 0; i <= 3; i++) {
      for (int32_t j = 300; j < batch_size+300; j++) {
//      if (j == 2300) {
//        ASSERT_OK(Flush());
//        dbfull()->TEST_WaitForFlushMemTable();
//      }
        int maxget = (i|k)?batch_size:j;   // don't read a slot we haven't written yet
        int putpos = (i|k)?rnd.Next()%batch_size:j;  // put into a randow slot, but only after the first pass has filled all slots
        if((rnd.Next()&0x7f)==0)values[putpos] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);  // replace one value in 100
        Status s = (Put(LongKey(putpos,key_size), values[putpos]));
        if(!s.ok())
          printf("Put failed\n");
        ASSERT_OK(s);
        for(int32_t m=0;m<2;++m){  // read a couple times for every Put
          int32_t randkey = (rnd.Next()) % maxget;  // make 2 random gets per put
          std::string getresult = Get(LongKey(randkey,key_size));
          ASSERT_EQ(getresult,values[randkey]);
          if(getresult.compare(values[randkey])!=0) {
            printf("mismatch: Get result=%s len=%zd\n",getresult.c_str(),getresult.size());
            printf("mismatch: Expected=%s len=%zd\n",values[randkey].c_str(),values[randkey].size());
            std::string getresult2 = Get(LongKey(randkey,key_size));
            if(getresult.compare(getresult2)==0)printf("unchanged on reGet\n");
            else if(getresult.compare(values[randkey])==0)printf("correct on reGet\n");
            else printf("after ReGet: Get result=%s len=%zd\n",getresult2.c_str(),getresult2.size());
          }
        }
      }
      printf("batch ");
      std::this_thread::sleep_for(std::chrono::seconds(2));  // give the compactor time to run
    }
//  ASSERT_OK(Flush());
//  dbfull()->TEST_WaitForFlushMemTable();
//  dbfull()->TEST_WaitForCompact();

    for (int32_t j = 0; j < batch_size+300; j++) {
      ASSERT_EQ(Get(LongKey(j,key_size)), values[j]);
    }
    printf("...verified.\n");
    TryReopen(options);
    printf("reopened.\n");
  }
  dbfull()->TEST_WaitForCompact();  // Prevent race condition accessing numcompactions
  printf("numcompactions=%zd, numARs=%zd, avg ref0 pos=%7.5f\n",numcompactions,numARs,totalref0position/numfinalcompactions);
  ASSERT_LT(totalref0position/numfinalcompactions,0.12);  // the number is empirical.  Make sure we are picking compactions from the oldest values first
  ASSERT_LT((double)numARs/(double)(numARs+numcompactions),0.20);  // the number is empirical.  Make sure ARs aren't much more than 15% of compactions
}
#endif
#if 0  // turn on for long-compaction test
TEST_F(DBVLogTest, HugeCompactionTest) {
 // To create a huge compaction, we change the default database shape by
 // 1. making the VLog files 1/10 the normal size
 // 2. setting the max compaction size to 12 SSTs
 // 3. 
  const int32_t value_ref_size = 16;  // length of indirect reference
  const int32_t vlogoverhead = 5;  // # bytes added for header & CRC
// obsolete   int32_t value_size = 18;  // 10 KB
// obsolete   int32_t key_size = 10 * 1024 - value_size;
  int32_t value_size = 800;  // 10 KB
  int32_t key_size = 18;
  int32_t value_size_var = 20;
  int32_t merge_append_size = 10;
  int32_t batch_size = 10000; // scaf 200000;   number of keys in database


  Options options = CurrentOptions();
  options.max_bytes_for_level_multiplier = 10;

  batch_size = std::max(batch_size,(int32_t)std::exp(3*std::log(options.max_bytes_for_level_multiplier)));  // at least 1000 keys in L1

  options.num_levels = (int)std::floor(std::log(batch_size)/std::log(options.max_bytes_for_level_multiplier)-0.9);  // 1000 keys=> L1 only, 200000 keys=>3 levels on disk: 1,2,3
  uint64_t kvsl1 = (uint64_t)std::exp(std::log(batch_size)-(options.num_levels-1-1)*std::log(options.max_bytes_for_level_multiplier));   // # kvs in L1
  uint64_t bytesperkvinsst=key_size+value_ref_size+20;
  uint64_t bytesperkvinmemtable=key_size+value_size+20;
  options.max_bytes_for_level_base = bytesperkvinsst*kvsl1;  // size of all SSTs for those kvs
  uint64_t L1filecount=10;  // desired # files in full L1
  uint64_t sstkvcount = kvsl1/L1filecount;  // # kvs in a single file
  double  nfilesperl0flush=3.5;   // when we get this many files, flush the memtables
  options.level0_file_num_compaction_trigger = 3;
  options.target_file_size_base=bytesperkvinsst*sstkvcount;
  options.write_buffer_size = (uint64_t)((nfilesperl0flush/options.level0_file_num_compaction_trigger)*bytesperkvinmemtable*sstkvcount);
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({20});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({15});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_size_trigger = std::vector<uint64_t>({(uint64_t)(0.8*batch_size*value_size)});   // start AR when the DB is almost full  scaf should be 0.8
  options.active_recycling_size_trigger = std::vector<uint64_t>({0});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});

  options.vlogfile_max_size = std::vector<uint64_t>({sstkvcount*((value_size+10)/10)});  // 1/10 of amount of value in 1 sst, allowing for CRC & header
  options.max_compaction_bytes = (uint64_t)((options.max_bytes_for_level_multiplier + 12) * options.target_file_size_base);

  options.vlog_direct_IO = false;
  options.merge_operator.reset(new StringAppendOperator(','));


  printf("Starting: #levels=%d, max_bytes_for_level_base=%zd target_file_size_base=%zd write_buffer_size=%zd vlogfile_max_size=%zd\n",options.num_levels,options.max_bytes_for_level_base, options.target_file_size_base,options.write_buffer_size,options.vlogfile_max_size[0]);
  DestroyAndReopen(options);

  int64_t numcompactions = 0;  // total number of non-AR compactions (coming from L1 or higher)
  int64_t numARs = 0;  // number of ARs
  int64_t numfinalcompactions = 0;  // compactions into last level
  double totalref0position = 0.0;  //  total of ref0 position as a % of ring size
  int64_t vlogtotalsize = 0;  // number of bytes in VLog, after removing fragmentation

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::InstallCompactionResults", [&](void* arg) {
        uint64_t *compact_stats = static_cast<uint64_t *>(arg);
        if(0&&(compact_stats[0]|compact_stats[1]|compact_stats[2])!=0){
          printf("Compaction: %zd bytes in, %zd written to %zd files, %zd remapped\n",compact_stats[1],compact_stats[0],compact_stats[3],compact_stats[2]);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionBuilder::PickCompaction", [&](void* arg) {
        int64_t *pickerinfo = static_cast<int64_t *>(arg);
        ColumnFamilyData *cfd=(ColumnFamilyData*)pickerinfo[6];
        int printreq = 0;
        if(cfd!=nullptr){
          if(pickerinfo[7]){ // Active recycle
            ++numARs;  // inc count of ARs
// obsolete            printf("PickCompaction: AR L%zd->%zd , #input SSTs=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[2],pickerinfo[3],pickerinfo[4]);
// obsolete             printreq = 1;
          } else {  // normal compaction
            ++numcompactions;
            if(pickerinfo[5]==(options.num_levels-1)){
              // compaction into final level
// obsolete               printf("PickCompaction: Level %zd->%zd, exp ref0=%zd, act ref0=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[1],pickerinfo[2],pickerinfo[3],pickerinfo[4]);
// obsolete               printreq = 1;
              if(pickerinfo[2]!=~0LL){  // don't count comps with no ref0.  They should not occur during the final data-collection pass
                ++numfinalcompactions;  // inc # final compactions
                totalref0position += (double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]);  // add in fractional position of ref0 file
if((double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]) > 1.0  || (double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]) < 0.0)
 printf("invalid ref0\n"); // scaf
              }
            }
          }
          if(printreq){
            for(int t=0;t<options.num_levels;){printf("%d",cfd->current()->storage_info()->NumLevelFiles(t)); if(++t==options.num_levels)break; printf(", ");}
            printf("}\n");
          }
        }
      });


  VLogRingRestartInfo restartinfo;
  SyncPoint::GetInstance()->SetCallBack(
      "VLogRingRestartInfo::Coalesce", [&](void* arg) {
         restartinfo = *static_cast<VLogRingRestartInfo *>(arg);
// obsolete printf("size=%zd, frag=%zd, net size=%zd\n",restartinfo.size,restartinfo.frag,restartinfo.size-restartinfo.frag);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // Whenever we write, we have to housekeep the vlogtotalsize: remove the deleted key, add the new

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
    vlogtotalsize += values[i].size()+vlogoverhead;
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
    vlogtotalsize += values[i].size()+vlogoverhead;
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(Get(LongKey(0,key_size)), values[0]);
  ASSERT_EQ(Get(LongKey(299,key_size)), values[299]);


  // 2 files in L0
// obsolete   ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ(Get(LongKey(0,key_size)), values[0]);
  // 2 files in L2
//  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    vlogtotalsize -= values[i].size()+vlogoverhead;
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
    vlogtotalsize += values[i].size()+vlogoverhead;
  }
  ASSERT_OK(Flush());
  for (int32_t i = 300; i < 300+batch_size; i++) {
  }


  for(int32_t k=0;k<2;++k) {  // scaf 2
    // Reinit stats variables so they get the last-pass data
    numcompactions = 0;  // total number of non-AR compactions (coming from L1 or higher)
    numARs = 0;  // number of ARs
    numfinalcompactions = 0;  // compactions into last level
    totalref0position = 0.0;  //  total of ref0 position as a % of ring size
    // Many files 4 [300 => 4300)
    for (int32_t i = 0; i < 2; i++) {  // scaf 5
      for (int32_t j = 300; j < batch_size+300; j++) {
//      if (j == 2300) {
//        ASSERT_OK(Flush());
//        dbfull()->TEST_WaitForFlushMemTable();
//      }
        int maxget = (i|k)?batch_size:j;   // don't read a slot we haven't written yet
        int putpos = (i|k)?rnd.Next()%batch_size:j;  // put into a randow slot, but only after the first pass has filled all slots
        // Figure out the size of the incumbent value, and remove it from the total
        if(!(i|k)){
          // First pass: create a random value to use
          std::string vstg =  RandomString(&rnd, value_size);
          // append compressible suffix
          vstg.append(rnd.Next()%value_size_var,'a');
          values[putpos] = vstg;
        } else {
          // Not first pass: we are deleting the old value, which may have 0 length.  0-length values have no overhead
          vlogtotalsize -= values[putpos].size()+(values[putpos].size()?vlogoverhead:0);
        }
        // Optionally modify the value; do Put, Merge, or Delete.  Leave the merged value
        Status s;
        int dice = rnd.Next()&0xf;  // roll the dice for the operation type
        if((i|k) && values[putpos].size() && dice<4){
          // Not first pass, value is not empty: do a Merge 25% of the time
          std::string appendstg = RandomString(&rnd, merge_append_size);
          values[putpos].append(1,',');   // emulate the merge operator: add ',' followed by the merge arg
          values[putpos].append(appendstg);
          s = (Merge(LongKey(putpos,key_size), appendstg));
        }else if((i|k) && values[putpos].size() && dice<5) {
          // Not first pass, value is not empty: do a Delete 6% of the time
          values[putpos].clear();  
          s = Delete(LongKey(putpos,key_size));
        }else{
          // Otherwise, do a Put.  Replace the value 6% of the time/
          if(dice==0xf)values[putpos] = RandomString(&rnd, value_size + rnd.Next()%value_size_var); 
          // If the value is empty, don't put.  That will ensure that the only empty values are deleted ones, which will show 'not found'
          if(values[putpos].size())s = (Put(LongKey(putpos,key_size), values[putpos]));
        }
        vlogtotalsize += values[putpos].size()+(values[putpos].size()?vlogoverhead:0);
        if(!s.ok())
          printf("Put failed\n");
        ASSERT_OK(s);
        for(int32_t m=0;m<2;++m){  // read a couple times for every Put
          int32_t randkey = (rnd.Next()) % maxget;  // make 2 random gets per put
          std::string getresult = Get(LongKey(randkey,key_size));
          if(values[randkey].size()){
            ASSERT_EQ(getresult,values[randkey]);
            if(getresult.compare(values[randkey])!=0) {
              printf("mismatch: Get result=%s len=%zd\n",getresult.c_str(),getresult.size());
              printf("mismatch: Expected=%s len=%zd\n",values[randkey].c_str(),values[randkey].size());
              std::string getresult2 = Get(LongKey(randkey,key_size));
              if(getresult.compare(getresult2)==0)printf("unchanged on reGet\n");
              else if(getresult.compare(values[randkey])==0)printf("correct on reGet\n");
              else printf("after ReGet: Get result=%s len=%zd\n",getresult2.c_str(),getresult2.size());
            }
          }else{
            // value was empty, key should not be found
            ASSERT_EQ(getresult,"NOT_FOUND");
          }
        }
      }
      printf("\n\nbatch\n\n");
      std::this_thread::sleep_for(std::chrono::seconds(5));  // give the compactor time to run
    }
//  ASSERT_OK(Flush());
//  dbfull()->TEST_WaitForFlushMemTable();
//  dbfull()->TEST_WaitForCompact();

    for (int32_t j = 0; j < batch_size+300; j++) {
      std::string getresult = Get(LongKey(j,key_size));
      if(values[j].size()){
        ASSERT_EQ(getresult,values[j]);
      }else{
        // value was empty, key should not be found
        ASSERT_EQ(getresult,"NOT_FOUND");
      }
    }
    printf("...verified.\n");
    TryReopen(options);
    printf("reopened...\n");
  }
  // Now compact everything down to the lowest level, so we have a true count of VLog bytes
// obsolete printf("Before compacting to level %d, FilesPerLevel=%s\n",options.num_levels-1,FilesPerLevel(0).c_str());
  CompactRangeOptions c_options;
  c_options.change_level = true;
  c_options.target_level = options.num_levels-1;
  ASSERT_OK(db_->CompactRange(c_options, nullptr, nullptr));
  for (int32_t j = 0; j < batch_size+300; j++) {
    std::string getresult = Get(LongKey(j,key_size));
    if(values[j].size()){
      ASSERT_EQ(getresult,values[j]);
    }else{
      // value was empty, key should not be found
      ASSERT_EQ(getresult,"NOT_FOUND");
    }
  }
  printf("...verified again.\n");
// obsolete printf("After compacting to level %d, FilesPerLevel=%s\n",options.num_levels-1,FilesPerLevel(0).c_str());
  // Verify that the VLog size is correct
  ASSERT_EQ(vlogtotalsize,restartinfo.size-restartinfo.frag);
  // Reopen the database and reverify, to make sure the file list was updated correctly
  TryReopen(options);
  printf("reopened after compaction...\n");
  for (int32_t j = 0; j < batch_size+300; j++) {
    std::string getresult = Get(LongKey(j,key_size));
    if(values[j].size()){
      ASSERT_EQ(getresult,values[j]);
    }else{
      // value was empty, key should not be found
      ASSERT_EQ(getresult,"NOT_FOUND");
    }
  }
  printf("...verified after compaction.\n");
}
#endif
#if 0  // turn on for VLog space-accounting test
TEST_F(DBVLogTest, SpaceAccountingTest) {
  const int32_t value_ref_size = 16;  // length of indirect reference
  const int32_t vlogoverhead = 5;  // # bytes added for header & CRC
// obsolete   int32_t value_size = 18;  // 10 KB
// obsolete   int32_t key_size = 10 * 1024 - value_size;
  int32_t value_size = 800;  // 10 KB
  int32_t key_size = 18;
  int32_t value_size_var = 20;
  int32_t merge_append_size = 10;
  int32_t batch_size = 10000; // scaf 200000;   


  Options options = CurrentOptions();
  options.max_bytes_for_level_multiplier = 10;

  batch_size = std::max(batch_size,(int32_t)std::exp(3*std::log(options.max_bytes_for_level_multiplier)));  // at least 1000 keys in L1

  options.num_levels = (int)std::floor(std::log(batch_size)/std::log(options.max_bytes_for_level_multiplier)-0.9);  // 1000 keys=> L1 only, 200000 keys=>3 levels on disk: 1,2,3
  uint64_t kvsl1 = (uint64_t)std::exp(std::log(batch_size)-(options.num_levels-1-1)*std::log(options.max_bytes_for_level_multiplier));   // # kvs in L1
  uint64_t bytesperkvinsst=key_size+value_ref_size+20;
  uint64_t bytesperkvinmemtable=key_size+value_size+20;
  options.max_bytes_for_level_base = bytesperkvinsst*kvsl1;  // size of all SSTs for those kvs
  uint64_t L1filecount=10;  // desired # files in full L1
  uint64_t sstkvcount = kvsl1/L1filecount;  // # kvs in a single file
  double  nfilesperl0flush=3.5;   // when we get this many files, flush the memtable
  options.target_file_size_base=bytesperkvinsst*sstkvcount;
  options.write_buffer_size = (uint64_t)(nfilesperl0flush*bytesperkvinmemtable*sstkvcount);
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({20});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({15});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_size_trigger = std::vector<uint64_t>({(uint64_t)(0.8*batch_size*value_size)});   // start AR when the DB is almost full  scaf should be 0.8
  options.active_recycling_size_trigger = std::vector<uint64_t>({0});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({sstkvcount*(value_size+10)});  // amount of value in 1 sst, allowing for CRC & header

  options.vlog_direct_IO = false;
  options.merge_operator.reset(new StringAppendOperator(','));


  printf("Starting: #levels=%d, max_bytes_for_level_base=%zd target_file_size_base=%zd write_buffer_size=%zd vlogfile_max_size=%zd\n",options.num_levels,options.max_bytes_for_level_base, options.target_file_size_base,options.write_buffer_size,options.vlogfile_max_size[0]);
  DestroyAndReopen(options);

  int64_t numcompactions = 0;  // total number of non-AR compactions (coming from L1 or higher)
  int64_t numARs = 0;  // number of ARs
  int64_t numfinalcompactions = 0;  // compactions into last level
  double totalref0position = 0.0;  //  total of ref0 position as a % of ring size
  int64_t vlogtotalsize = 0;  // number of bytes in VLog, after removing fragmentation

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::InstallCompactionResults", [&](void* arg) {
        uint64_t *compact_stats = static_cast<uint64_t *>(arg);
        if(0&&(compact_stats[0]|compact_stats[1]|compact_stats[2])!=0){
          printf("Compaction: %zd bytes in, %zd written to %zd files, %zd remapped\n",compact_stats[1],compact_stats[0],compact_stats[3],compact_stats[2]);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionBuilder::PickCompaction", [&](void* arg) {
        int64_t *pickerinfo = static_cast<int64_t *>(arg);
        ColumnFamilyData *cfd=(ColumnFamilyData*)pickerinfo[6];
        int printreq = 0;
        if(cfd!=nullptr){
          if(pickerinfo[7]){ // Active recycle
            ++numARs;  // inc count of ARs
// obsolete            printf("PickCompaction: AR L%zd->%zd , #input SSTs=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[2],pickerinfo[3],pickerinfo[4]);
// obsolete             printreq = 1;
          } else {  // normal compaction
            ++numcompactions;
            if(pickerinfo[5]==(options.num_levels-1)){
              // compaction into final level
// obsolete               printf("PickCompaction: Level %zd->%zd, exp ref0=%zd, act ref0=%zd, ring files=[%zd,%zd] SSTs={",pickerinfo[0],pickerinfo[5],pickerinfo[1],pickerinfo[2],pickerinfo[3],pickerinfo[4]);
// obsolete               printreq = 1;
              if(pickerinfo[2]!=~0LL){  // don't count comps with no ref0.  They should not occur during the final data-collection pass
                ++numfinalcompactions;  // inc # final compactions
                totalref0position += (double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]);  // add in fractional position of ref0 file
if((double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]) > 1.0  || (double)(pickerinfo[2]-pickerinfo[3]) / (double)(pickerinfo[4]-pickerinfo[3]) < 0.0)
 printf("invalid ref0\n"); // scaf
              }
            }
          }
          if(printreq){
            for(int t=0;t<options.num_levels;){printf("%d",cfd->current()->storage_info()->NumLevelFiles(t)); if(++t==options.num_levels)break; printf(", ");}
            printf("}\n");
          }
        }
      });


  VLogRingRestartInfo restartinfo;
  SyncPoint::GetInstance()->SetCallBack(
      "VLogRingRestartInfo::Coalesce", [&](void* arg) {
         restartinfo = *static_cast<VLogRingRestartInfo *>(arg);
// obsolete printf("size=%zd, frag=%zd, net size=%zd\n",restartinfo.size,restartinfo.frag,restartinfo.size-restartinfo.frag);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // Whenever we write, we have to housekeep the vlogtotalsize: remove the deleted key, add the new

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
    vlogtotalsize += values[i].size()+vlogoverhead;
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
    vlogtotalsize += values[i].size()+vlogoverhead;
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(Get(LongKey(0,key_size)), values[0]);
  ASSERT_EQ(Get(LongKey(299,key_size)), values[299]);


  // 2 files in L0
// obsolete   ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L2
//  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    vlogtotalsize -= values[i].size()+vlogoverhead;
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
    vlogtotalsize += values[i].size()+vlogoverhead;
  }
  ASSERT_OK(Flush());
  for (int32_t i = 300; i < 300+batch_size; i++) {
  }


  for(int32_t k=0;k<2;++k) {  // scaf 2
    // Reinit stats variables so they get the last-pass data
    numcompactions = 0;  // total number of non-AR compactions (coming from L1 or higher)
    numARs = 0;  // number of ARs
    numfinalcompactions = 0;  // compactions into last level
    totalref0position = 0.0;  //  total of ref0 position as a % of ring size
    // Many files 4 [300 => 4300)
    for (int32_t i = 0; i < 2; i++) {  // scaf 5
      for (int32_t j = 300; j < batch_size+300; j++) {
//      if (j == 2300) {
//        ASSERT_OK(Flush());
//        dbfull()->TEST_WaitForFlushMemTable();
//      }
        int maxget = (i|k)?batch_size:j;   // don't read a slot we haven't written yet
        int putpos = (i|k)?rnd.Next()%batch_size:j;  // put into a randow slot, but only after the first pass has filled all slots
        // Figure out the size of the incumbent value, and remove it from the total
        if(!(i|k)){
          // First pass: create a random value to use
          std::string vstg =  RandomString(&rnd, value_size);
          // append compressible suffix
          vstg.append(rnd.Next()%value_size_var,'a');
          values[putpos] = vstg;
        } else {
          // Not first pass: we are deleting the old value, which may have 0 length.  0-length values have no overhead
          vlogtotalsize -= values[putpos].size()+(values[putpos].size()?vlogoverhead:0);
        }
        // Optionally modify the value; do Put, Merge, or Delete.  Leave the merged value
        Status s;
        int dice = rnd.Next()&0xf;  // roll the dice for the operation type
        if((i|k) && values[putpos].size() && dice<4){
          // Not first pass, value is not empty: do a Merge 25% of the time
          std::string appendstg = RandomString(&rnd, merge_append_size);
          values[putpos].append(1,',');   // emulate the merge operator: add ',' followed by the merge arg
          values[putpos].append(appendstg);
          s = (Merge(LongKey(putpos,key_size), appendstg));
        }else if((i|k) && values[putpos].size() && dice<5) {
          // Not first pass, value is not empty: do a Delete 6% of the time
          values[putpos].clear();  
          s = Delete(LongKey(putpos,key_size));
        }else{
          // Otherwise, do a Put.  Replace the value 6% of the time/
          if(dice==0xf)values[putpos] = RandomString(&rnd, value_size + rnd.Next()%value_size_var); 
          // If the value is empty, don't put.  That will ensure that the only empty values are deleted ones, which will show 'not found'
          if(values[putpos].size())s = (Put(LongKey(putpos,key_size), values[putpos]));
        }
        vlogtotalsize += values[putpos].size()+(values[putpos].size()?vlogoverhead:0);
        if(!s.ok())
          printf("Put failed\n");
        ASSERT_OK(s);
        for(int32_t m=0;m<2;++m){  // read a couple times for every Put
          int32_t randkey = (rnd.Next()) % maxget;  // make 2 random gets per put
          std::string getresult = Get(LongKey(randkey,key_size));
          if(values[randkey].size()){
            ASSERT_EQ(getresult,values[randkey]);
            if(getresult.compare(values[randkey])!=0) {
              printf("mismatch: Get result=%s len=%zd\n",getresult.c_str(),getresult.size());
              printf("mismatch: Expected=%s len=%zd\n",values[randkey].c_str(),values[randkey].size());
              std::string getresult2 = Get(LongKey(randkey,key_size));
              if(getresult.compare(getresult2)==0)printf("unchanged on reGet\n");
              else if(getresult.compare(values[randkey])==0)printf("correct on reGet\n");
              else printf("after ReGet: Get result=%s len=%zd\n",getresult2.c_str(),getresult2.size());
            }
          }else{
            // value was empty, key should not be found
            ASSERT_EQ(getresult,"NOT_FOUND");
          }
        }
      }
      printf("\n\nbatch\n\n");
      std::this_thread::sleep_for(std::chrono::seconds(5));  // give the compactor time to run
    }
//  ASSERT_OK(Flush());
//  dbfull()->TEST_WaitForFlushMemTable();
//  dbfull()->TEST_WaitForCompact();

    for (int32_t j = 0; j < batch_size+300; j++) {
      std::string getresult = Get(LongKey(j,key_size));
      if(values[j].size()){
        ASSERT_EQ(getresult,values[j]);
      }else{
        // value was empty, key should not be found
        ASSERT_EQ(getresult,"NOT_FOUND");
      }
    }
    printf("...verified.\n");
    TryReopen(options);
    printf("reopened...\n");
  }
  // Now compact everything down to the lowest level, so we have a true count of VLog bytes
// obsolete printf("Before compacting to level %d, FilesPerLevel=%s\n",options.num_levels-1,FilesPerLevel(0).c_str());
  CompactRangeOptions c_options;
  c_options.change_level = true;
  c_options.target_level = options.num_levels-1;
  ASSERT_OK(db_->CompactRange(c_options, nullptr, nullptr));
  for (int32_t j = 0; j < batch_size+300; j++) {
    std::string getresult = Get(LongKey(j,key_size));
    if(values[j].size()){
      ASSERT_EQ(getresult,values[j]);
    }else{
      // value was empty, key should not be found
      ASSERT_EQ(getresult,"NOT_FOUND");
    }
  }
  printf("...verified again.\n");
// obsolete printf("After compacting to level %d, FilesPerLevel=%s\n",options.num_levels-1,FilesPerLevel(0).c_str());
  // Verify that the VLog size is correct
  ASSERT_EQ(vlogtotalsize,restartinfo.size-restartinfo.frag);
}
#endif

TEST_F(DBVLogTest, RemappingFractionTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size = 16384;  // k+v=16KB
  const int32_t nkeys = 100;  // number of files
  options.target_file_size_base = 10LL<< 20;  // large value

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({value_size+500});  // one value per vlog file

  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB

  double statsfilesize=0;   // filesize inferred from the stats line (0 to avoid warning)
  printf("Stopping at %d:",40);
  for(int32_t mappct=20; mappct<=40; mappct+= 5){
    double mapfrac = mappct * 0.01;
    printf(" %d",mappct);
    // set remapping fraction to n
    options.fraction_remapped_during_compaction = std::vector<int32_t>({mappct});
    DestroyAndReopen(options);
    // write 100 files in key order, flushing each to give it a VLog file
    std::string val0string;
    for(int key = 0;key<nkeys;++key){
      std::string keystring = Key(key);
      Slice keyslice = keystring;  // the key we will add
      std::string valstring = RandomString(&rnd, value_size);
      if(key==0)val0string = valstring;
      Slice valslice = valstring;
      ASSERT_OK(Put(keyslice, valslice));
      ASSERT_EQ(valslice, Get(Key(key)));
      // Flush into L0
      ASSERT_OK(Flush());
      // Compact into L1, which will write the VLog files.  At this point there is nothing to remap
      CompactRangeOptions compact_options;
      compact_options.change_level = true;
      compact_options.target_level = 1;
      ASSERT_OK(db_->CompactRange(compact_options, &keyslice, &keyslice));
      dbfull()->TEST_WaitForCompact();

      // Read the VLog stats
      std::string stats;
      (db_->GetProperty("rocksdb.stats", &stats));
      // Extract size/frag for Ring 0 and the total
      uint64_t r0files=0, sumfiles=0; double r0size=0, r0frag=0, sumsize=0, sumfrag=0;
      DecodeRingStats(stats, r0files, r0size, r0frag, sumfiles, sumsize, sumfrag);
      // If this is the first file, remember the size
      if(key==0)statsfilesize=r0size;
      // The ring size should be the size of one file * the number of files
      ASSERT_EQ(r0files, key+1);
      ASSERT_GT(r0size*0.005,std::abs(r0size-(r0files*statsfilesize)));
      // Fragmentation should be based on the size and actual value length
      ASSERT_GT(0.001,std::abs(r0frag-((statsfilesize-(value_size+5))/statsfilesize)));
    }
    // Verify 100 SSTs and 100 VLog files
    ASSERT_EQ("0,100", FilesPerLevel(0));
    std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
    ListVLogFileSizes(this,vlogfilesizes);
    ASSERT_EQ(100, vlogfilesizes.size());
    // Verify total file size is pretty close to right.  Filesize gets rounded up to alignment boundary
    int64_t onefilesize = vlogfilesizes[0];
    // file size should match what we inferred from the stats
    ASSERT_GT(10,std::abs(onefilesize-(int64_t)statsfilesize));
    // We really should verify what the alignment boundary is based on the EnvOptions in the VLog.  But we have no way to access that, and the files
    // we created long ago, and the boundary may be different depending on path, and may change between read/write... so we don't try to get them just right.
    // Instead we assume the length is right, and see what alignment was applied
    const int64_t bufferalignment = onefilesize&-onefilesize;
    int64_t totalsize=0;  // place to build file stats
    for(size_t i=0;i<vlogfilesizes.size();++i){
     totalsize += vlogfilesizes[i];
    }
    ASSERT_GT(1000, std::abs(totalsize-onefilesize*nkeys));
    // compact n-5% to n+5%
    CompactRangeOptions remap_options;
    remap_options.change_level = true;
    remap_options.target_level = 1;
    remap_options.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    std::string minkeystring(Key((int)(nkeys*(mapfrac-0.05)))), maxkeystring(Key((int)(nkeys*(mapfrac+0.05))));
    Slice minkey(minkeystring), maxkey(maxkeystring);
    ASSERT_OK(db_->CompactRange(remap_options, &minkey, &maxkey));
    dbfull()->TEST_WaitForCompact();
    // verify that the VLog file total has grown by 4%
    vlogfilesizes.clear();  // reinit list of files
    ListVLogFileSizes(this,vlogfilesizes);
    int64_t newtotalsize=0;  // place to build file stats
    for(size_t i=0;i<vlogfilesizes.size();++i){
     newtotalsize += vlogfilesizes[i];
    }
 
    // expected increase is 4% of the size.  Each filesize is rounded up
    int64_t expincr = (int64_t) (onefilesize * nkeys * 0.04);
    expincr = (expincr + (bufferalignment-1)) & -bufferalignment;
    ASSERT_EQ(104, vlogfilesizes.size());
    ASSERT_GT(1000, (int64_t)std::abs(newtotalsize-(totalsize+expincr)));  // not a tight match, but if it works throughout the range it's OK
  }
}

TEST_F(DBVLogTest, VlogFileSizeTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size = (1LL<<14);  // k+v=16KB
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({1});  // Don't write to VLog during Flush - it messes up our calculation
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB


  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB
  // Write keys 4MB+multiples of 1MB; Compact them to L1; count files & sizes.
  // # files should be correct, and files should be close to the same size
  printf("Stopping at %zd:",10*options.vlogfile_max_size[0]/value_size);
  for(size_t nkeys = options.vlogfile_max_size[0]/value_size; nkeys<10*options.vlogfile_max_size[0]/value_size; nkeys += (1LL<<20)/value_size){
    printf(" %zd",nkeys); // scaf for faster test  if(nkeys>options.vlogfile_max_size[0]/value_size)break;
    DestroyAndReopen(options);
    // write the kvs
    for(int key = 0;key<(int)nkeys;++key){
      ASSERT_OK(Put(Key(key), RandomString(&rnd, value_size)));
    }
    size_t vsize=nkeys*(value_size+5);  // bytes written to vlog.  5 bytes for compression header
    // compact them
    ASSERT_OK(Flush());
    // Compact them into L1, which will write the VLog files
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 1;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    dbfull()->TEST_WaitForCompact();
    // 1 file in L1, after compaction
    ASSERT_EQ("0,1", FilesPerLevel(0));
    std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
    ListVLogFileSizes(this,vlogfilesizes);
    // verify number of files is correct
    uint64_t expfiles = (uint64_t)std::max(std::ceil(vsize/((double)options.vlogfile_max_size[0]*1.25)),std::floor(vsize/((double)options.vlogfile_max_size[0])));
    ASSERT_EQ(expfiles, vlogfilesizes.size());
    // verify files have almost the same size
    uint64_t minsize=(uint64_t)~0, maxsize=0;  // place to build file stats
    for(size_t i=0;i<vlogfilesizes.size();++i){
     minsize=std::min(minsize,vlogfilesizes[i]); maxsize=std::max(maxsize,vlogfilesizes[i]);
    }
    ASSERT_LT(maxsize-minsize, 2*value_size);
  }
  printf("\n");
}

TEST_F(DBVLogTest, MinIndirectValSizeTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size_incr = 10;  // k+v=16KB
  const int32_t nkeys = 2000;  // k+v=16KB
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB

  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB
  // Write keys 4MB+multiples of 1MB; Compact them to L1; count files & sizes.
  // total # bytes in files should go down as the minimum remapping size goes up
  printf("Stopping at %d:",nkeys*value_size_incr);
  for(size_t minsize=0; minsize<nkeys*value_size_incr; minsize+=500){
    printf(" %zd",minsize); // scaf for faster test  if(minsize>0)break;
    options.min_indirect_val_size[0]=minsize;
    DestroyAndReopen(options);
    // write the kvs
    int value_size=0;  // we remember the ending value
    for(int key = 0;key<nkeys;++key){
      ASSERT_OK(Put(Key(key), RandomString(&rnd, value_size)));
      value_size += value_size_incr;
    }
    // compact them
    ASSERT_OK(Flush());
    // Compact them into L1, which will write the VLog files
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 1;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    dbfull()->TEST_WaitForCompact();
    // 1 file in L1, after compaction
    ASSERT_EQ("0,1", FilesPerLevel(0));
    std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
    ListVLogFileSizes(this,vlogfilesizes);
    // verify total file size is correct for the keys we remapped
    int64_t totalsize=0;  // place to build file stats
    for(size_t i=0;i<vlogfilesizes.size();++i){
     totalsize += vlogfilesizes[i];
    }
    // mapped size = #mapped rcds * average size of rcd
    int64_t nmappedkeys = nkeys * (value_size - minsize) / value_size;
    int64_t expmappedsize = nmappedkeys * ((minsize + value_size)+5) / 2;  // 5 for compression header
    ASSERT_LT(std::abs(expmappedsize-totalsize), 2*value_size);
  }
  printf("\n");
}


TEST_F(DBVLogTest, VLogCompressionTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size_incr = 10;  // k+v=16KB
  const int32_t nkeys = 2000;  // k+v=16KB
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB

  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB
  // Write keys 4MB+multiples of 1MB; Compact them to L1; count files & sizes.
  options.min_indirect_val_size[0]=0;
  DestroyAndReopen(options);
  // write the kvs
  int value_size=0;  // we remember the ending value
  for(int key = 0;key<nkeys;++key){
    std::string vstg(RandomString(&rnd, 10));
    vstg.resize(value_size,' ');  // extend string with compressible data
    ASSERT_OK(Put(Key(key), vstg));
    value_size += value_size_incr;
  }
  // compact them
  ASSERT_OK(Flush());
  // Compact them into L1, which will write the VLog files
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForCompact();
  // 1 file in L1, after compaction
  ASSERT_EQ("0,1", FilesPerLevel(0));
  std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
  ListVLogFileSizes(this,vlogfilesizes);
  // verify total file size is correct for the keys we remapped
  int64_t totalsize=0;  // place to build file stats
  for(size_t i=0;i<vlogfilesizes.size();++i){
   totalsize += vlogfilesizes[i];
  }

  // Repeat with VLog compression on
  options.ring_compression_style = std::vector<CompressionType>({kSnappyCompression});

  DestroyAndReopen(options);
  // write the kvs
  value_size=0;  // we remember the ending value
  for(int key = 0;key<nkeys;++key){
    std::string vstg(RandomString(&rnd, 10));
    vstg.resize(value_size,' ');  // extend string with compressible data
    ASSERT_OK(Put(Key(key), vstg));
    value_size += value_size_incr;
  }
  // compact them
  ASSERT_OK(Flush());
  // Compact them into L1, which will write the VLog files
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForCompact();
  // 1 file in L1, after compaction
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ListVLogFileSizes(this,vlogfilesizes);
  // verify total file size is correct for the keys we remapped
  int64_t totalsizecomp=0;  // place to build file stats
  for(size_t i=0;i<vlogfilesizes.size();++i){
   totalsizecomp += vlogfilesizes[i];
  }


  // Verify compression happened
  ASSERT_LT(totalsizecomp, 0.1*totalsize);
}

void DBVLogTest::SetUpForActiveRecycleTest() {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size = 16384-10;  // k+v=16KB, and make sure the value doesn't round up to over 0x4000 - that would make the internal frag ct high & change behavior
  const int32_t nkeys = 100;  // number of files
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB

  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB

  // set up 100 files in L1
  // set AR level to 10%
  // compact & remap 15% of the data near the beginning of the database
  // AR should happen, freeing the beginning of the db

  int32_t mappct=20;
  // turn off remapping
  options.fraction_remapped_during_compaction = std::vector<int32_t>({mappct});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  options.active_recycling_size_trigger = std::vector<uint64_t>({0});

  DestroyAndReopen(options);
  // write 100 files in key order, flushing each to give it a VLog file
  std::string val0string;
  for(int key = 0;key<nkeys;++key){
    std::string keystring = Key(key);
    Slice keyslice = keystring;  // the key we will add
    std::string valstring = RandomString(&rnd, value_size);
    if(key==0)val0string = valstring;
    Slice valslice = valstring;
    ASSERT_OK(Put(keyslice, valslice));
    ASSERT_EQ(valslice, Get(Key(key)));
    // Flush into L0
    ASSERT_OK(Flush());
    // Compact into L1, which will write the VLog files.  At this point there is nothing to remap
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 1;
    ASSERT_OK(db_->CompactRange(compact_options, &keyslice, &keyslice));
    dbfull()->TEST_WaitForCompact();
  }
  // Verify 100 SSTs and 100 VLog files
  ASSERT_EQ("0,100", FilesPerLevel(0));
  std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
  ListVLogFileSizes(this,vlogfilesizes);
  ASSERT_EQ(100, vlogfilesizes.size());
  // Verify total file size is pretty close to right.  Filesize gets rounded up to multiple of 4096
  const int64_t bufferalignment = 4096;
  int64_t onefilesize = ((value_size+5) + (bufferalignment-1)) & -bufferalignment;
  int64_t totalsize=0;  // place to build file stats
  for(size_t i=0;i<vlogfilesizes.size();++i){
   totalsize += vlogfilesizes[i];
  }
  ASSERT_GT(1000, std::abs(totalsize-onefilesize*nkeys));
  // compact between 5% and 20%.  This should remap all, leaving 15% fragmentation.
  CompactRangeOptions remap_options;
  remap_options.change_level = true;
  remap_options.target_level = 1;
  remap_options.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  remap_options.exclusive_manual_compaction = false;
  std::string minkeystring(Key((int)(nkeys*0.05))), maxkeystring(Key((int)(nkeys*0.20)));
  Slice minkey(minkeystring), maxkey(maxkeystring);
  ASSERT_OK(db_->CompactRange(remap_options, &minkey, &maxkey));
  dbfull()->TEST_WaitForCompact();
  // The user compaction and the AR compaction should both run.

  NewVersionStorage(6, kCompactionStyleLevel);
  UpdateVersionStorageInfo();

  // Now we have 5 VLog files, numbered 1-5, holding keys 0-4, referred to by SSTs 0-4
  // then files 6-19, which have no reference
  // then file 20, which holds key 19 and is referred to by the SST holding keys 5-20
  // then file 21, which holds key 20 and is not the earliest key in any SST
  // then files 22-101, holding keys 21-100

}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest1) {
  SetUpForActiveRecycleTest();

  // For some reason mutable_cf_options_ is not initialized at DestroyAndReopen

  // Verify that we don't pick a compaction when the database is too small
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.min_indirect_val_size = std::vector<size_t>({0});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({1LL<<30});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({15});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_TRUE(compaction.get() == nullptr);

  // Verify that we don't pick a compaction with a 20% size requirement
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({20});
  mutable_cf_options_.min_indirect_val_size = std::vector<size_t>({0});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({0});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({15});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_TRUE(compaction.get() == nullptr);

  // verify that we do pick a compaction at a 10% size requirement
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({0});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({15});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(15, compaction->inputs()->size());  // get the max 15 SSTs allowed
  ASSERT_EQ(30, compaction->lastfileno());   // pick up the VLog files for them, plus the 15 orphaned VLog files

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest2) {
  SetUpForActiveRecycleTest();

  // testing minct.  Set vlogfile_freed_min to 0, then verify responsiveness to min.  freed_min=0 is a debug mode that doesn't look for SSTs past the minimum
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({0});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({3});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({0});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(3, compaction->inputs()->size());
  ASSERT_EQ(3, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}


TEST_F(DBVLogTest, ActiveRecycleTriggerTest3) {
  SetUpForActiveRecycleTest();

  // testing minct.  Set vlogfile_freed_min to 0, then verify responsiveness to min.  freed_min=0 is a debug mode that doesn't look for SSTs past the minimum
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({0});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({4});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({0});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(4, compaction->inputs()->size());
  ASSERT_EQ(4, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest4) {
  SetUpForActiveRecycleTest();

  // testing maxct.  Move vlogfile_freed_min off of 0, then verify responsiveness to maxct.
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({0});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({3});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({4});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({1});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(4, compaction->inputs()->size());
  ASSERT_EQ(4, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest5) {
  SetUpForActiveRecycleTest();

  // testing maxct.  Move vlogfile_freed_min off of 0, then verify responsiveness to maxct.
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({0});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({3});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({1});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(5, compaction->inputs()->size());
  ASSERT_EQ(5, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest6) {
  SetUpForActiveRecycleTest();

  // testing freed_min.  set sst_minct to 0 (debugging mode), then verify responsiveness to freed_min.
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({0});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({0});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({2});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(2, compaction->inputs()->size());
  ASSERT_EQ(2, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest7) {
  SetUpForActiveRecycleTest();

  // testing freed_min.  set sst_minct to 0 (debugging mode), then verify responsiveness to freed_min.
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<uint64_t>({0});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({0});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({3});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(3, compaction->inputs()->size());
  ASSERT_EQ(3, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, VLogRefsTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  const int32_t key_size = 16384;
  const int32_t value_size = 16;  // k+v=16KB
  const int32_t max_keys_per_file = 3;  // number of keys we want in each file
  options.target_file_size_base = max_keys_per_file * (key_size-100);  // file size limit, to put 3 refs per file (first, middle, last).  Err on the low side since we make files at least the min size

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({0});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({99});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB
  options.active_recycling_size_trigger = std::vector<uint64_t>({100LL<<30});

  Random rnd(301);

  // Turn off remapping so that each value stays in its starting VLogFile
  // Create SSTs, with 1 key per file.  The key for files 1 and 6 are as long as 3 normal keys
  // flush each file to L0, then compact into L1 to produce 1 VLog file per SST

  // compact all the files into L2
  // this will produce 1 SST per 3 keys (1 key, for SSTs 1 and 6).

  // pick a compaction of everything, to get references to the files
  // go through the earliest-references, verifying that they match the expected

  // repeat total of 6 times, permuting the keys in each block of 3.  Results should not change

  // repeat again, making the very last key a single-key file.  Results should not change

  int keys_per_file[] = {max_keys_per_file,1,max_keys_per_file,max_keys_per_file,max_keys_per_file,1,max_keys_per_file};
  const int32_t num_files = sizeof(keys_per_file)/sizeof(keys_per_file[0]);
 
  // loop twice, once with the size of the last file set to 1
  printf("Running to permutation 2/6:");
  for(int finalkey=0;finalkey<2;finalkey++){
    // loop over each permutation of inputs
    for(int perm=0;perm<6;++perm){
      int permadd[3]; permadd[0]=perm>>1; permadd[1]=1^(permadd[0]&1); if(perm&1)permadd[1]=3-permadd[0]-permadd[1]; permadd[2]=3-permadd[0]-permadd[1];  // adjustment for each slot of 3
      printf(" %d/%d%d%d",finalkey,permadd[0],permadd[1],permadd[2]);

      // Create SSTs, with 1 key per file.  Flush each file to L0, then compact into L1 to produce 1 VLog file per SST
      DestroyAndReopen(options);

      int key=0;  // running key
      for(int kfx=0;kfx<num_files;++kfx){
        int kpf=keys_per_file[kfx];
        for(int slot=0;slot<kpf;++slot){   // for each key
          int permkey = key; if(kpf>1)permkey+=permadd[slot];  // next key to use: key, but permuted if part of a set
          int keylen = key_size; if(kpf==1)keylen = max_keys_per_file*key_size + (max_keys_per_file-1)*value_size ;  // len of key: if just 1 key per file, make it long so it fills the SST, including the missing 2 values
          if(kfx==(num_files-1) && slot==(kpf-1))keylen-=1000;  // shorten the last key so the SSTs end where we expect

          // Write the key
          std::string keystring = KeyInvInd(permkey,keylen,true);
          Slice keyslice = keystring;  // the key we will add
          std::string valstring = RandomString(&rnd, value_size);
          Slice valslice = valstring;
          ASSERT_OK(Put(keyslice, valslice));
          // Read it back
          ASSERT_EQ(valslice, Get(KeyInvInd(permkey,keylen,true)));
          // Flush into L0
          ASSERT_OK(Flush());
          // Compact into L1, which will write the VLog files.  At this point there is nothing to remap
          CompactRangeOptions compact_options;
          compact_options.change_level = true;
          compact_options.target_level = 1;
          ASSERT_OK(db_->CompactRange(compact_options, &keyslice, &keyslice));
          dbfull()->TEST_WaitForCompact();
        }
        key += kpf;  // skip to next key
      }

      // All files created now.  Verify # VLog files created
      // Verify 100 SSTs and 100 VLog files
      char buf[100]; sprintf(buf,"0,%d",key);
      ASSERT_EQ(buf, FilesPerLevel(0));
      std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
      ListVLogFileSizes(this,vlogfilesizes);
      ASSERT_EQ(key, vlogfilesizes.size());

      // compact all the files into L2
      // Verify correct # SSTs
      CompactRangeOptions remap_options;
      remap_options.change_level = true;
      remap_options.target_level = 2;
      remap_options.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      remap_options.exclusive_manual_compaction = false;
      ASSERT_OK(db_->CompactRange(remap_options,nullptr,nullptr));
      dbfull()->TEST_WaitForCompact();
      // The user compaction and the AR compaction should both run.
      sprintf(buf,"0,0,%d",num_files);
      ASSERT_EQ(buf, FilesPerLevel(0));
      ListVLogFileSizes(this,vlogfilesizes);
      ASSERT_EQ(key, vlogfilesizes.size());

      // Find the list of SST files.  There must be a good way to do this, but I can't figure one out.
      // Here we create a new version and comb through it until we get to the vset, where 'obsolete_files'
      // contains the files before the version was created
      NewVersionStorage(6, kCompactionStyleLevel);
      UpdateVersionStorageInfo();
      VersionStorageInfo *vstore(vstorage_.get());
//      vstore->cfd_->current_->vset_->obsolete_files_;
      std::vector<ObsoleteFileInfo>& currssts = vstore->GetCfd()->current()->version_set()->obsolete_files();

      // Go through the file & verify that the ref0 field has the key of the oldest VLog file in the SST.  This is always the same key,
      // but it appears in different positions depending on the permutation.
      int ref0=1;  // the first file number is 1
      for(int kfx=0;kfx<num_files;++kfx){
        ASSERT_EQ(ref0, currssts[kfx].metadata->indirect_ref_0[0]);
        ref0 += keys_per_file[kfx];  // skip to the batch of VLog values in the next file
      }
    }

    keys_per_file[num_files-1] = 1;  // make the last file 1 key long for the second run
  }
  printf("\n");

}
#endif
}  // namespace rocksdb


int main(int argc, char** argv) {
#if !defined(ROCKSDB_LITE)
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  return 0;
#endif
}
