#include "test_delete.h"


class ResultChecker{
  public:
  ResultChecker(uint64_t key_space){
    uint64_t check_num_block = std::ceil(key_space / block_size);
    // check_num_block += (key_space % block_size == 0) ? 0 : 1;
    data_.reserve(check_num_block);
    
    for (size_t i = 0; i < check_num_block; ++i) {
        data_.emplace_back(new boost::dynamic_bitset<>());
        if (i == check_num_block - 1){
            uint64_t sz = key_space - i * block_size;
            data_[i]->resize(sz);
        } else {
            data_[i]->resize(block_size);
        }
        data_[i]->reset();
    }
  }

  void InsertKey(uint64_t key){
    size_t block_idx = key / block_size;
    size_t key_idx = key % block_size;
    data_[block_idx]->set(key_idx, true);
  }

  void DeleteKey(uint64_t key){
    size_t block_idx = key / block_size;
    size_t key_idx = key % block_size;
    data_[block_idx]->set(key_idx, false);
  }

  void DeleteRange(uint64_t key, uint64_t len){
    size_t block_idx_begin = key / block_size;
    size_t key_idx_begin = key % block_size;
    size_t block_idx_end = (key + len) / block_size;
    assert (!(block_idx_begin > block_idx_end));
    if (block_idx_begin == block_idx_end){
        data_[block_idx_begin]->set(key_idx_begin, (len + 1), false);
    } else {
        uint64_t len_begin = block_size - key_idx_begin;
        data_[block_idx_begin]->set(key_idx_begin, len_begin, false);
        size_t key_idx_end = (key + len) % block_size + 1;
        data_[block_idx_end]->set(0, key_idx_end, false);
    }
  }

  bool CheckKey(uint64_t key){
    size_t block_idx = key / block_size;
    size_t key_idx = key % block_size;
    return data_[block_idx]->test(key_idx);
  }

  private:
  std::vector<boost::dynamic_bitset<> *> data_; //record the existence of inserted keys
  uint64_t block_size = 10000;
};

void PrepareDB(rocksdb::RangeDeleteDB* db, ResultChecker* checker) {
  std::cout << "Start to prepare DB ... " << std::endl;
  rocksdb::OperationGenerator key_gen(FLAGS_ksize, (FLAGS_kvsize - FLAGS_ksize), FLAGS_max_key);
  key_gen.InitForWrite(0, FLAGS_prep_num);
  int idx = 0;
  while (key_gen.Valid()) {
    auto status = db->EntryInsert(key_gen.Key(), key_gen.KeyString(), key_gen.Value());
    checker->InsertKey(key_gen.Key());
    idx ++;
    key_gen.Next();
  }
  std::cout << "DB prepared with  " << idx << "  entries" << std::endl;
}

void ExcuteWorkload(rocksdb::RangeDeleteDB* db) {
  rocksdb::RDTimer timer;
  std::cout << "Excute workload ... " << std::endl;
  rocksdb::OperationGenerator key_gen(FLAGS_ksize, (FLAGS_kvsize - FLAGS_ksize), FLAGS_max_key);
  key_gen.InitForMix(FLAGS_prep_num, FLAGS_write_num, FLAGS_read_num, FLAGS_seek_num, FLAGS_rdelete_num);
  uint64_t num_ops = key_gen.size();
  int idx = 0;
  timer.Start();
  while (key_gen.Valid()) {
    switch (key_gen.Operation()) {
      case 'w': {
        // std::cout << "Write key : " << key_gen.Key() << std::endl;
        auto status = db->EntryInsert(key_gen.Key(), key_gen.KeyString(), key_gen.Value());
        break;
      }
      case 'r': {
        // std::cout << "Point read key : " << key_gen.Key() << std::endl;
        std::string value;
        auto status = db->PointQuery(key_gen.Key(), key_gen.KeyString(), value);
        break;
      }
      case 's': {
        // std::cout << "Range query from key : " << key_gen.Key() << std::endl;
        auto status = db->RangeLookup(key_gen.Key(), key_gen.KeyString(), FLAGS_seek_len);
        break;
      }
      case 'g': {
        // std::cout << "Range delete from key : " << key_gen.Key() << std::endl;
        uint64_t key_right = key_gen.Key() + FLAGS_rdelete_len;
        auto status = db->RangeDelete(key_gen.Key(), key_gen.KeyString(), key_right, key_gen.ToKeyString(key_right), &key_gen);
        break;
      }
    }
    idx ++;
    if (idx % (num_ops / 10) == 0) {
      std::cout << "Mixed workload progress: " << idx << " / " << num_ops << std::endl;
    }
    key_gen.Next();
  }
  timer.PauseMS();
  std::cout << "Complete with duration: " << timer.duration << " ms" << std::endl;
}

// else if (FLAGS_workload == "test_1" || FLAGS_workload == "test_2" || FLAGS_workload == "test_3") {
//     ExcuteTestWorkload(db);
//   }
void ExcuteTestWorkload(rocksdb::RangeDeleteDB* db) {
  rocksdb::RDTimer timer;
  std::cout << "Excute workload ... " << std::endl;
  rocksdb::OperationGenerator key_gen(FLAGS_ksize, (FLAGS_kvsize - FLAGS_ksize), FLAGS_max_key);
  if (FLAGS_workload == "test_1"){
    std::cout << "Excute workload Test not found: " << std::endl;
    key_gen.InitForTestNotfound(FLAGS_prep_num, FLAGS_write_num, FLAGS_read_num, FLAGS_seek_num, FLAGS_rdelete_num);
  } else if (FLAGS_workload == "test_2"){
    std::cout << "Excute workload Test valid: " << std::endl;
    key_gen.InitForTestValid(FLAGS_prep_num, FLAGS_write_num, FLAGS_read_num, FLAGS_seek_num, FLAGS_rdelete_num);
  } else if (FLAGS_workload == "test_3"){
    std::cout << "Excute workload Test deleted: " << std::endl;
    key_gen.InitForTestDeleted(FLAGS_prep_num, FLAGS_write_num, FLAGS_read_num, FLAGS_seek_num, FLAGS_rdelete_num);
  } else if (FLAGS_workload == "test_4"){
    std::cout << "Excute workload Test not found (before): " << std::endl;
    key_gen.InitForTestNotfoundBefore(FLAGS_prep_num, FLAGS_write_num, FLAGS_read_num, FLAGS_seek_num, FLAGS_rdelete_num);
  }
  bool flag = false;
  uint64_t num_ops = key_gen.size();
  int idx = 0;
  while (key_gen.Valid()) {
    switch (key_gen.Operation()) {
      case 'w': {
        // std::cout << "Write key : " << key_gen.Key() << std::endl;
        auto status = db->EntryInsert(key_gen.Key(), key_gen.KeyString(), key_gen.Value());
        break;
      }
      case 'r': {
        // std::cout << "Point read key : " << key_gen.Key() << std::endl;
        if (!flag){
          timer.Start();
          db->StartStatic();
          flag = true;
        }
        std::string value;
        auto status = db->PointQuery(key_gen.Key(), key_gen.KeyString(), value);
        break;
      }
      case 's': {
        // std::cout << "Range query from key : " << key_gen.Key() << std::endl;
        auto status = db->RangeLookup(key_gen.Key(), key_gen.KeyString(), FLAGS_seek_len);
        break;
      }
      case 'g': {
        // std::cout << "Range delete from key : " << key_gen.Key() << std::endl;
        uint64_t key_right = key_gen.Key() + FLAGS_rdelete_len;
        auto status = db->RangeDelete(key_gen.Key(), key_gen.KeyString(), key_right, key_gen.ToKeyString(key_right), &key_gen);
        break;
      }
    }
    idx ++;
    if (idx % (num_ops / 10) == 0) {
      std::cout << "Mixed workload progress: " << idx << " / " << num_ops << std::endl;
    }
    key_gen.Next();
  }
  timer.PauseMS();
  std::cout << "Complete with duration: " << timer.duration << " ms" << std::endl;
}

void ExcuteTestWithChecker(rocksdb::RangeDeleteDB* db, ResultChecker* checker) {
  rocksdb::RDTimer timer;
  std::cout << "Excute Filter Test ... " << std::endl;
  rocksdb::OperationGenerator key_gen(FLAGS_ksize, (FLAGS_kvsize - FLAGS_ksize), FLAGS_max_key);
  // key_gen.InitForFilterTest();
  key_gen.InitForMix(FLAGS_prep_num, FLAGS_write_num, FLAGS_read_num, FLAGS_seek_num, FLAGS_rdelete_num);
  uint64_t num_ops = key_gen.size();
  int idx = 0;
  timer.Start();
  while (key_gen.Valid()) {
    switch (key_gen.Operation()) {
      case 'w': {
        // std::cout << "Write key : " << key_gen.Key() << std::endl;
        auto status = db->EntryInsert(key_gen.Key(), key_gen.KeyString(), key_gen.Value());
        checker->InsertKey(key_gen.Key());
        break;
      }
      case 'r': {
        // std::cout << "Point read key : " << key_gen.Key() << std::endl;
        uint64_t key_query = key_gen.Key();
        std::string value;
        auto status = db->PointQuery(key_gen.Key(), key_gen.KeyString(), value);

        bool check_res = checker->CheckKey(key_gen.Key());
        if(check_res){
          if (status != rocksdb::Result::kOk && status != rocksdb::Result::kNotRangeDeleted){
            std::cerr << "Fail: cannot get existing key " << key_gen.Key() << " " << status << std::endl;
            exit(1);
          }
        } else {
          if (status == rocksdb::Result::kOk){
            std::cerr << "Fail: get deleted/non-exist key " << key_gen.Key() << " " << status << std::endl;
            exit(1);
          }
        }
        break;
      }
      case 's': {
        // std::cout << "Range query from key : " << key_gen.Key() << std::endl;
        auto status = db->RangeLookup(key_gen.Key(), key_gen.KeyString(), FLAGS_seek_len);
        break;
      }
      case 'g': {
        // std::cout << "Range delete from key : " << key_gen.Key() << " to " << key_gen.Key() + FLAGS_rdelete_len << std::endl;
        uint64_t key_right = key_gen.Key() + FLAGS_rdelete_len;
        // WF: key_right_str is for RocksDB use, for which right boundary is exclusive, so we add 1
        std::string key_right_str = key_gen.ToKeyString(key_right + 1);
        auto status = db->RangeDelete(key_gen.Key(), key_gen.KeyString(), key_right, key_right_str, &key_gen);
        checker->DeleteRange(key_gen.Key(), FLAGS_rdelete_len);
        break;
      }
      // case 'g': {
      //   std::cout << "Range delete from key : " << key_gen.Key() << " to " << key_gen.Key() + FLAGS_rdelete_len << std::endl;
      //   uint64_t key_query_rep = key_gen.Key() + 3;
      //   std::string key_query_rep_str = key_gen.ToKeyString(key_query_rep);
      //   auto status = db->EntryInsert(key_query_rep, key_query_rep_str, key_gen.Value());
      //   checker->InsertKey(key_query_rep);

      //   uint64_t key_query_filter = key_gen.Key() + 120;
      //   std::string key_query_filter_str = key_gen.ToKeyString(key_query_filter);
      //   status = db->EntryInsert(key_query_filter, key_query_filter_str, key_gen.Value());
      //   checker->InsertKey(key_query_filter);
      //   ///////////

      //   uint64_t key_right = key_gen.Key() + FLAGS_rdelete_len;
      //   // WF: key_right_str is for RocksDB use, for which right boundary is exclusive, so we add 1
      //   std::string key_right_str = key_gen.ToKeyString(key_right + 1);
      //   status = db->RangeDelete(key_gen.Key(), key_gen.KeyString(), key_right, key_right_str, &key_gen);
      //   checker->DeleteRange(key_gen.Key(), FLAGS_rdelete_len);

      //   ///////////
      //   std::string value;
      //   status = db->PointQuery(key_query_rep, key_query_rep_str, value);
      //   bool check_res = checker->CheckKey(key_query_rep);
      //   if(check_res){
      //     if (status != rocksdb::Result::kOk && status != rocksdb::Result::kNotRangeDeleted){
      //       std::cerr << "Fail: cannot get existing key " << key_gen.Key() << " " << status << std::endl;
      //       exit(1);
      //     }
      //   } else {
      //     if (status == rocksdb::Result::kOk){
      //       std::cerr << "Fail: get deleted/non-exist key " << key_gen.Key() << " " << status << std::endl;
      //       exit(1);
      //     }
      //   }
      //   status = db->PointQuery(key_query_filter, key_query_filter_str, value);
      //   check_res = checker->CheckKey(key_query_filter);
      //   if(check_res){
      //     if (status != rocksdb::Result::kOk && status != rocksdb::Result::kNotRangeDeleted){
      //       std::cerr << "Fail: cannot get existing key " << key_gen.Key() << " " << status << std::endl;
      //       exit(1);
      //     }
      //   } else {
      //     if (status == rocksdb::Result::kOk){
      //       std::cerr << "Fail: get deleted/non-exist key " << key_gen.Key() << " " << status << std::endl;
      //       exit(1);
      //     }
      //   }
      //   //////////////////////////
      //   break;
      // }
    }
    idx ++;
    key_gen.Next();
  }
  timer.Pause();
  std::cout << "Complete with duration: " << timer.duration << " ms" << std::endl;
}

void PrintSetting(){
  std::cout << "LSM Setting" << std::endl;
  std::cout << "Key size : " << FLAGS_ksize << " bytes;  KV size: " << FLAGS_kvsize << " bytes" << std::endl;
  std::cout << "LSM buffer size : " << FLAGS_buffer_size << " MB" << std::endl;
  std::cout << "LSM Bloom filter BPK : " << FLAGS_bpk_filter << std::endl;

  if (FLAGS_mode == "grd"){
    std::cout << "GRD Filter Setting" << std::endl;
    std::cout << "Enable GRD filter : " << FLAGS_enable_rdfilter << std::endl;
    std::cout << "GRD filter BPK : " << FLAGS_bpk_rd_filter << std::endl;
    
    std::cout << "GRD Rep Setting" << std::endl;
    std::cout << "GRD Rep buffer size : " << FLAGS_rep_buffer_size << " KB" << std::endl;
    std::cout << "GRD Rep size ratio : " << FLAGS_rep_size_ratio << std::endl;
    std::cout << "GRD Rep run number : " << FLAGS_rep_run_num << std::endl;
    std::cout << "Use Full R-tree : " << FLAGS_full_rtree << std::endl;
    std::cout << "level start to involve GDR in compaction : " << FLAGS_level_comp << std::endl;
  }
  
  std::cout << "Workload" << std::endl;
  std::cout << "Maxkey : " << FLAGS_max_key << ",  Prepare: " << FLAGS_prep_num << " keys" << std::endl;
  std::cout << "workload: Write " << FLAGS_write_num << ", Point query " << FLAGS_read_num 
            << ", Range query " << FLAGS_seek_num << ", Range delete " << FLAGS_rdelete_num << " Delete length: " << FLAGS_rdelete_len << std::endl;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true); 
  rocksdb::range_delete_db_opt options;
  bool set_inti = get_default_options(options);
  if (!set_inti) {
    return 1;
  }
  PrintSetting();

  rocksdb::RangeDeleteDB* db = new rocksdb::RangeDeleteDB(options);

  ResultChecker* checker = new ResultChecker(FLAGS_max_key + 1);

  if (FLAGS_workload == "prepare") {
    PrepareDB(db, checker);
  } else if (FLAGS_workload == "test") {
    PrepareDB(db, checker);
    db->StartStatic();
    ExcuteWorkload(db);
  } else if (FLAGS_workload == "test_checker") {
    PrepareDB(db, checker);
    db->StartStatic();
    ExcuteTestWithChecker(db, checker);
  } else if (FLAGS_workload == "test_1" || FLAGS_workload == "test_2" || FLAGS_workload == "test_3" || FLAGS_workload == "test_4") {
    ExcuteTestWorkload(db);
  }

  db->PrintStatic();

  // std::cout << "statistics: " << options.db_conf.statistics->ToString() << std::endl;

  db->close();
  delete db;
  return 0;
}