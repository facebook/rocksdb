#include "test_delete.h"

inline std::string ItoSWithPadding(const uint64_t key, uint64_t size) {
  std::string key_str = std::to_string(key);
  std::string padding_str(size - key_str.size(), '0');
  key_str = padding_str + key_str;
  return key_str;
}

class OperationGenerator {
 public:
  OperationGenerator(uint64_t key_size, uint64_t value_size, 
                      bool shuffle = true) {
    idx_ = 0;
    key_size_ = key_size;
    value_size_ = value_size;
    shuffle_ = shuffle;
  }

  uint64_t Key() const { return keys_[idx_]; }

  std::string KeyString() const { return ItoSWithPadding(keys_[idx_], key_size_); }
  std::string ToKeyString(uint64_t other) const { return ItoSWithPadding(other, key_size_); }

  std::string Value() const {
    return ItoSWithPadding(keys_[idx_], value_size_);
  }

  char Operation() const {return ops_[idx_]; }

  void Next() {
    idx_++;
  }

  bool Valid() {
    return idx_ < keys_.size();
  }

  void InitForWrite(const uint64_t &start, const uint64_t &end) {
    srand(100);
    // for (uint64_t i = start; i < end; i++) {
    //   keys_.push_back(i);
    //   ops_.push_back('w');
    // }
    // if (shuffle_) {
    //   auto rng = std::default_random_engine{};
    //   std::shuffle(std::begin(keys_), std::end(keys_), rng);
    // }
    for (uint64_t i = start; i < end; i++) {
      uint64_t key = shuffle_ ? (rand() % FLAGS_max_key) : i;
      keys_.push_back(key);
      ops_.push_back('w');
    }
    assert(keys_.size() == ops_.size());
    idx_ = 0;
  }

  void InitForMix(const uint64_t &start, const uint64_t &write, const uint64_t &read, 
                  const uint64_t &seek, const uint64_t &rdelete) {
    std::vector<uint64_t> key_w;
    srand(100);
    for (uint64_t i = 0; i < write; i++) {
      ops_.push_back('w');
      // key_w.push_back(i + start);
    }
    for (uint64_t i = 0; i < read; i++) {
      ops_.push_back('r');
    }
    for (uint64_t i = 0; i < seek; i++) {
      ops_.push_back('s');
    }
    for (uint64_t i = 0; i < rdelete; i++) {
      ops_.push_back('g');
    }

    if (shuffle_) {
      auto rng = std::default_random_engine{};
      // std::shuffle(std::begin(key_w), std::end(key_w), rng);
      std::shuffle(std::begin(ops_), std::end(ops_), rng);
    }

    uint64_t j = 0;
    for (uint64_t i = 0; i < ops_.size(); i++) {
      uint64_t key = rand() % FLAGS_max_key;
      keys_.push_back(key);
      // if(ops_[i] == 'w'){
      //   keys_.push_back(key_w[j++]);
      // } else {
      //   uint64_t key = rand() % start;
      //   keys_.push_back(key);
      // }
    }
    assert(keys_.size() == ops_.size());
    idx_ = 0;
  }

  void InitForFilterTest() {
    uint64_t rd_keys[10] = {11, 562, 1093, 2024, 3345, 4766, 5782, 6628, 7129, 9811};
    uint64_t r_keys[10] = {8, 552, 995, 2031, 3349, 4444, 5735, 6655, 7132, 9818};
    for (size_t i = 0; i < 10; i++)
    {
      ops_.push_back('g');
      keys_.push_back(rd_keys[i]);
    }
    for (size_t i = 0; i < 10; i++)
    {
      ops_.push_back('r');
      keys_.push_back(r_keys[i]);
    }
    assert(keys_.size() == ops_.size());
    idx_ = 0;
  }

  uint64_t size(){
    return keys_.size();
  }

 private:
  uint64_t idx_;
  std::vector<uint64_t> keys_;
  std::vector<char> ops_;
  uint64_t key_size_;
  uint64_t value_size_;
  bool shuffle_;
};

class ResultChecker{
  public:
  ResultChecker(uint64_t key_space){
    uint64_t check_num_block = std::ceil(key_space / block_size);
    check_num_block += (key_space % block_size == 0) ? 0 : 1;
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
  OperationGenerator key_gen(FLAGS_ksize, FLAGS_kvsize - FLAGS_ksize);
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
  OperationGenerator key_gen(FLAGS_ksize, FLAGS_kvsize - FLAGS_ksize);
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
        auto status = db->RangeDelete(key_gen.Key(), key_gen.KeyString(), key_right, key_gen.ToKeyString(key_right));
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
  OperationGenerator key_gen(FLAGS_ksize, FLAGS_kvsize - FLAGS_ksize);
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
        auto status = db->RangeDelete(key_gen.Key(), key_gen.KeyString(), key_right, key_right_str);
        checker->DeleteRange(key_gen.Key(), FLAGS_rdelete_len);
        break;
      }
    }
    idx ++;
    key_gen.Next();
  }
  timer.Pause();
  std::cout << "Complete with duration: " << timer.duration << " ms" << std::endl;
}

void PrintSetting(){
  std::cout << "kv size : " << FLAGS_kvsize << " bytes" << std::endl;
  std::cout << "db_opts.buffer_cap : " << FLAGS_buffer_size << " MB" << std::endl;
  std::cout << "rep_opts.buffer_cap : " << FLAGS_rep_buffer_size << " KB" << std::endl;
  std::cout << "filter_opts.bit_per_key : " << FLAGS_bpk_rd_filter << std::endl;
  std::cout << "workload: Write " << FLAGS_write_num << ", Point query " << FLAGS_read_num 
            << ", Range query " << FLAGS_seek_num << ", Range delete " << FLAGS_rdelete_num << std::endl;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true); 
  rocksdb::range_delete_db_opt options;
  if (FLAGS_mode == "default") {
    std::cout << "Default setting initializing ..." << std::endl;
    options = get_default_options();
  } else if (FLAGS_mode == "grd") {
    std::cout << "GRD setting initializing ..." << std::endl;
    options = get_default_options();
    options.enable_global_rd = true;
  } else if (FLAGS_mode == "filter") {
    std::cout << "Filter test setting initializing ..." << std::endl;
    options = get_default_options();
    options.enable_global_rd = true;
    options.filter_conf.min_key = 0;
    options.filter_conf.max_key = 10000;
    options.filter_conf.num_keys = 100;
    options.filter_conf.bit_per_key = 1;
    options.filter_conf.num_blocks = 10;
  } else {
    std::cerr << "Unknown compaction style: " << FLAGS_mode << std::endl;
    return 1;
  }

  PrintSetting();

  rocksdb::RangeDeleteDB* db = new rocksdb::RangeDeleteDB(options);

  ResultChecker* checker = new ResultChecker(FLAGS_max_key);

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
  }

  db->PrintStatic();

  db->close();
  delete db;
  return 0;
}