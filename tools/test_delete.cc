#include "test_delete.h"

// sudo ./tools/mytest --db_path=/home/wangfan/global-delete/range-delete/build/testdb --mode=write --num=2000000 --write_num=1000000 --read_num=0 --seek_num=0 --seek_keys=1000000 --zread_num=0
// sudo ./tools/mytest --mode=mix --num=2000000 --write_num=1000000 --read_num=0 --seek_num=0 --seek_keys=1000000 --zread_num=0
// sudo ./tools/test_delete --db_path=/home/wangfan/delete/global-range-delete/build/testdb --mode=mixseq --num=1000000 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --zread_num=0 --seek_num=10000 --seek_keys=100 --range_del_num=10000

DEFINE_string(mode, "write", "");
DEFINE_uint64(num, 2000000, "");
DEFINE_string(db_path, "/tmp/delete_test", "");  //path /home/wangfan/global-delete/range-delete/build/mydb
DEFINE_string(method, "default", "");  // grdr: enable global range deleter; default: level compaction
DEFINE_uint64(write_num, 1000000, "");
DEFINE_uint64(prep_num, 1000000, "");
DEFINE_uint64(read_num, 1000000, "");
DEFINE_uint64(zread_num, 1000000, "");
DEFINE_uint64(seek_num, 1000000, "");
DEFINE_uint64(seek_keys, 2000, "");
DEFINE_uint64(range_del_num, 1000000, "");
DEFINE_uint64(del_len, 20, "");
DEFINE_int64(filter_bits, 30 * 1024 * 1024, "");
DEFINE_bool(global_range, false, "");

std::string ConvertToString(uint64_t in_num){
  uint64_t total_num = FLAGS_num + FLAGS_write_num + FLAGS_prep_num;
	int bits = std::floor(std::log10(total_num)) + 1;

	std::stringstream ss;
	ss << std::setw(bits) << std::setfill('0') << in_num ;
	std::string str;
	ss >> str;         //transfer to str
	//str = ss.str();  //also works
	return str;
}

std::string val_suffix(100, 'v');

rocksdb::Status RangeQuery(uint64_t start, uint64_t len, rocksdb::DB* db){
  rocksdb::ReadOptions rr_opt = rocksdb::ReadOptions();
  if (FLAGS_method == "grdr" && FLAGS_global_range) {
    // rr_opt.use_global_range_deleter_range_query = true;
  }

  auto it = db->NewIterator(rr_opt);

  uint64_t curr_seq = std::numeric_limits<uint64_t>::max();
  it->Seek(rocksdb::Slice(ConvertToString(start)));
  uint64_t j = 0;
  uint64_t i = 0;
  while(j < len){
    if (!it->Valid()) {
      break;
    }
    // it->GetCurrentSequence(&curr_seq);
    std::string key_str = it->key().ToString();
    // if (db->IsGlobalRangeDeleter()){
    if (FLAGS_global_range){
      // if (!(db->CheckGlobalRangeDeleter(key_str, curr_seq))){
      //   rocksdb::Slice val = it->value();
      //   rocksdb::Slice key = it->key();
      //   if (val.ToString() != key.ToString() + val_suffix) {
      //     std::cout << "invalid val for seek: " << val.ToString() << std::endl;
      //     std::cout << "expected: " << key.ToString() + val_suffix << std::endl;
      //     exit(1);
      //   }
      //   j++;
      // }
      // i++;
    } else {
      rocksdb::Slice val = it->value();
      rocksdb::Slice key = it->key();
      // std::cout << "key: " << key.ToString() << " val: " << val.ToString() << std::endl;
      if (val.ToString() != key.ToString() + val_suffix) {
        std::cout << "invalid val for seek: " << val.ToString() << std::endl;
        std::cout << "expected: " << key.ToString() + val_suffix << std::endl;
        exit(1);
      }
      j++;
    }
    // std::cout << "j is " << j << ", i is " << i << ", key: " << it->key().ToString() << " seq: " << curr_seq << std::endl;
    it->Next(); 
  }
  delete it;
  // std::cout << "Counter with filter: " << j << " without filter: " << i << std::endl;
  return rocksdb::Status::OK();
}

void WriteKeys(uint64_t num, rocksdb::DB* db) {
  std::vector<uint64_t> keys;
  for (uint64_t i = 0; i < num; i++) {
    keys.push_back(i);
  }
  auto rng = std::default_random_engine{};
  std::vector<std::thread> thrds;
  std::atomic<uint64_t> fin(0);
  std::shuffle(std::begin(keys), std::end(keys), rng);
  for (uint64_t i = 0; i < 10; i++) {
    uint64_t start = i * num / 10;
    uint64_t end = (i + 1) * num / 10;
    if (i == 9) {
      end = num;
    }
    auto t = std::thread([=, &keys, &fin]() {
      for (uint64_t j = start; j < end; j++) {
        // std::string key = std::to_string(keys[j]);
        std::string key = ConvertToString(keys[j]);
        std::string val = key + val_suffix;
        rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, val);
        if (!s.ok()) {
          std::cout << "fail to put " << key << " because of " << s.ToString()
                    << std::endl;
          exit(1);
        }
        fin++;
      }
    });
    thrds.push_back(std::move(t));
  }
  for (uint64_t i = 0; i < 10; i++) {
    thrds[i].join();
  }
  db->Flush(rocksdb::FlushOptions());
  WaitForCompaction(db);
  std::cout << "finish writing" << std::endl;
}

void WriteSeqKeys(uint64_t num, rocksdb::DB* db) {
  std::vector<uint64_t> keys;
  for (uint64_t i = 0; i < num; i++) {
    keys.push_back(i);
  }
  // auto rng = std::default_random_engine{};
  std::vector<std::thread> thrds;
  std::atomic<uint64_t> fin(0);
  // std::shuffle(std::begin(keys), std::end(keys), rng);
  for (uint64_t i = 0; i < 10; i++) {
    uint64_t start = i * num / 10;
    uint64_t end = (i + 1) * num / 10;
    if (i == 9) {
      end = num;
    }
    auto t = std::thread([=, &keys, &fin]() {
      for (uint64_t j = start; j < end; j++) {
        // std::string key = std::to_string(keys[j]);
        std::string key = ConvertToString(keys[j]);
        std::string val = key + val_suffix;
        rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, val);
        if (!s.ok()) {
          std::cout << "fail to put " << key << " because of " << s.ToString()
                    << std::endl;
          exit(1);
        }
        fin++;
      }
    });
    thrds.push_back(std::move(t));
  }
  for (uint64_t i = 0; i < 10; i++) {
    thrds[i].join();
  }
  db->Flush(rocksdb::FlushOptions());
  WaitForCompaction(db);
  std::cout << "finish writing" << std::endl;
}

void ReadKeys(uint64_t num, rocksdb::DB* db) {
  for (uint64_t i = 0; i < num; i++) {
    std::string val;
    // std::string key = std::to_string(i);
    std::string key = ConvertToString(i);
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
    // if (s.ok()) {
    //   std::cout << "result: " << val << std::endl;
    // }
    if (!s.ok()) {
      std::cout << "fail to read " << key << " because of " << s.ToString()
                << std::endl;
      exit(1);
    }
    // if (val != std::to_string(i) + val_suffix) {
    if (val != ConvertToString(i) + val_suffix) {
      std::cout << "invalid val " << val << std::endl;
      exit(1);
    }
    if (i % (num / 10) == 0) {
      std::cout << "reading progress: " << i << "/" << num << std::endl;
    }
  }
  std::cout << "finish reading" << std::endl;
}

void IterKeys(uint64_t num, rocksdb::DB* db) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < num; i++) {
    // keys.push_back(std::to_string(i));
    keys.push_back(ConvertToString(i));
  }
  std::sort(keys.begin(), keys.end());
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  it->SeekToFirst();
  std::cout << "start iterating" << std::endl;
  for (size_t i = 0; i < keys.size(); i++) {
    rocksdb::Slice key = it->key();
    rocksdb::Slice val = it->value();
    std::string expected_key = keys[i];
    std::string expected_val = keys[i] + val_suffix;
    if (key.ToString() != expected_key || val.ToString() != expected_val) {
      std::cout << "invalid key/val: " << key.ToString() << "/"
                << val.ToString() << std::endl;
      std::cout << "valid key/val: " << expected_key << "/" << expected_val
                << std::endl;
      exit(1);
    }
    if (i % (num / 10) == 0) {
      std::cout << "iter progress: " << i << "/" << num << std::endl;
    }
    it->Next();
  }
  delete it;
  std::cout << "finish iterating" << std::endl;
}

void MixedWorkload(uint64_t num, rocksdb::DB* db) {
  srand(100);
  std::vector<uint64_t> keys;
  std::vector<std::string> skeys;
  std::vector<char> ops;
  for (uint64_t i = 0; i < FLAGS_read_num; i++) {
    ops.push_back('r');
  }
  for (uint64_t i = 0; i < FLAGS_zread_num; i++) {
    ops.push_back('z');
  }
  for (uint64_t i = 0; i < FLAGS_write_num; i++) {
    ops.push_back('w');
    keys.push_back(i);
  }
  for (uint64_t i = 0; i < FLAGS_seek_num; i++) {
    ops.push_back('s');
  }
  for (uint64_t i = 0; i < FLAGS_range_del_num; i++) {
    ops.push_back('g');
  }
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(keys), std::end(keys), rng);
  std::shuffle(std::begin(ops), std::end(ops), rng);
  uint64_t idx = 0;
  for (size_t i = 0; i < ops.size(); i++) {
    auto op = ops[i];
    if (op == 'w') {
      // write
      // std::string key = std::to_string(keys[idx]) + "-new";
      std::string key = ConvertToString(keys[idx]) + "-new";
      std::string val = key + val_suffix;
      idx++;
      rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, val);
      if (!s.ok()) {
        std::cout << "fail to put " << key << " because of " << s.ToString()
                  << std::endl;
        exit(1);
      }
    } else if (op == 'r') {
      // read
      uint64_t rand_num = rand() % num;
      std::string val;
      // std::string key = std::to_string(rand_num);
      std::string key = ConvertToString(rand_num);
      rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
      if (!s.ok()) {
        std::cout << "fail to read " << key << " because of " << s.ToString()
                  << std::endl;
        exit(1);
      }
    //   if (val != std::to_string(rand_num) + val_suffix) {
      if (val != ConvertToString(rand_num) + val_suffix) {  
        std::cout << "invalid val " << val << std::endl;
        exit(1);
      }
    //   if (s.ok()) {
    //     if (val != ConvertToString(rand_num) + val_suffix) {
    //       std::cout << "invalid val " << val << std::endl;
    //       exit(1);
    //     }
    //   }
    } else if (op == 'z') {
      // zero read
      std::string val;
      // std::string key = std::to_string(rand() % num) + ".5";
      std::string key = ConvertToString(rand() % num) + ".5";
      rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
      if (!s.IsNotFound()) {
        std::cout << "fail to zero read " << key << " because of "
                  << s.ToString() << std::endl;
        std::cout << "key: " << key << ", val: " << val << std::endl;
        exit(1);
      }
    } else if (op == 's') {
      // seek
      uint64_t rand_start = rand() % num;
      rocksdb::Status s = RangeQuery(rand_start, FLAGS_seek_keys, db);
    } else if (op == 'g') {
      // range delete
      uint64_t rd_start = rand() % num;
      uint64_t rd_end = rd_start + FLAGS_del_len;
      std::string rd_start_str = ConvertToString(rd_start);
      std::string rd_end_str = ConvertToString(rd_end);
      if (!(rd_start_str < rd_end_str)){
        std::cout << "Invalid range delete range" << std::endl;
        exit(1);
      }
      rocksdb::Status s;
      if (FLAGS_method == "grdr"){
        // s = db->GlobalDeleteRange(rocksdb::WriteOptions(), rd_start_str, rd_end_str);
      } else {
        s = db->DeleteRange(rocksdb::WriteOptions(), rd_start_str, rd_end_str);
      }
      if (!s.ok()) {
        std::cout << "fail to delete range [" << rd_start_str << " , " << rd_end_str << "] because of " << s.ToString()
                  << std::endl;
        exit(1);
      }
    } else {
      std::cout << "should not reach here" << std::endl;
      exit(1);
    }
    if (i % (ops.size() / 10) == 0) {
      std::cout << "mix progress: " << i << "/" << ops.size() << std::endl;
    }
  }
}

// mixed workload with preparation
void MixedSeqWorkload(rocksdb::DB* db) {
  std::cout << "mixed workload with preparation ... " << std::endl;
  srand(100);
  // generate prepare entries
  std::vector<uint64_t> keys_prep;
  for (uint64_t i = 0; i < FLAGS_prep_num; i++) {
    keys_prep.push_back(i);
  }
  // generate operations
  std::vector<uint64_t> keys;
  std::vector<char> ops;
  uint64_t total_write_num = FLAGS_prep_num + FLAGS_write_num;
  for (uint64_t i = 0; i < FLAGS_read_num; i++) {
    ops.push_back('r');
  }
  for (uint64_t i = 0; i < FLAGS_zread_num; i++) {
    ops.push_back('z');
  }
  for (uint64_t i = FLAGS_prep_num; i < total_write_num; i++) {
    ops.push_back('w');
    keys.push_back(i);
  }
  for (uint64_t i = 0; i < FLAGS_seek_num; i++) {
    ops.push_back('s');
  }
  for (uint64_t i = 0; i < FLAGS_range_del_num; i++) {
    ops.push_back('g');
  }
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(keys_prep), std::end(keys_prep), rng);
  std::shuffle(std::begin(keys), std::end(keys), rng);
  std::shuffle(std::begin(ops), std::end(ops), rng);
  uint64_t idx = 0;
  uint64_t w_count = FLAGS_prep_num;

  std::cout << "Preparing ..." << std::endl;
  for (size_t n = 0; n < keys_prep.size(); n++) {
    std::string key = ConvertToString(keys_prep[n]);
    std::string val = key + val_suffix;
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, val);
    if (!s.ok()) {
      std::cout << "fail to put " << key << " because of " << s.ToString()
                << std::endl;
      exit(1);
    }
  }

  std::cout << "Processing workload ..." << std::endl;

  uint64_t iter_build_time = 0;

  for (size_t i = 0; i < ops.size(); i++) {
    auto op = ops[i];
    if (op == 'w') {
      // std::cout << "Put ";
      // write
      std::string key = ConvertToString(keys[idx]);
      std::string val = key + val_suffix;
      idx++;
      w_count++;
      rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, val);
      // std::cout << key << " status: " << s.ToString() << std::endl;
      if (!s.ok()) {
        std::cout << "fail to put " << key << " because of " << s.ToString() << std::endl;
        exit(1);
      }
    } else if (op == 'r') {
      // read
      uint64_t rand_num = 0;
      if (w_count > 0) {
        rand_num = rand() % w_count;
      }
      std::string val;
      // std::string key = std::to_string(rand_num);
      std::string key = ConvertToString(rand_num);
      rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
      // std::cout << key << " status: " << s.ToString() << std::endl;
      if (s.ok()) {
        if (val != ConvertToString(rand_num) + val_suffix) {
          std::cout << "invalid val " << val << std::endl;
          exit(1);
        }
      }
    } else if (op == 'z') {
      // zero read
      uint64_t rand_num = 0;
      if (w_count > 0) {
        rand_num = rand() % w_count;
      }
      std::string val;
      // std::string key = std::to_string(rand() % num) + ".5";
      std::string key = ConvertToString(rand_num) + ".5";
      rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
      // std::cout << key << " status: " << s.ToString() << std::endl;
      // if (!s.IsNotFound()) {
      //   std::cout << "fail to zero read " << key << " because of "
      //             << s.ToString() << std::endl;
      //   std::cout << "key: " << key << ", val: " << val << std::endl;
      //   exit(1);
      // }
    } else if (op == 's') {
      // seek
      uint64_t rand_start = 0;
      if (w_count > 0) {
        rand_start = rand() % w_count;
      }
      // rocksdb::Status s = RangeQuery(rand_start, FLAGS_seek_keys, iter_build_time, db);
      rocksdb::Status s = RangeQuery(rand_start, FLAGS_seek_keys, db);
    } else if (op == 'g') {
      // std::cout << "DeleteRange ";
      // range delete
      uint64_t rd_start = 0;
      if (w_count > 0) {
        rd_start = rand() % w_count;
      }
      uint64_t rd_end = rd_start + FLAGS_del_len;
      std::string rd_start_str = ConvertToString(rd_start);
      std::string rd_end_str = ConvertToString(rd_end);
      if (!(rd_start_str < rd_end_str)){
        std::cout << "Invalid range delete range" << std::endl;
        exit(1);
      }
      rocksdb::Status s;
      if (FLAGS_method == "grdr"){
        // s = db->GlobalDeleteRange(rocksdb::WriteOptions(), rd_start_str, rd_end_str);
      } else {
        s = db->DeleteRange(rocksdb::WriteOptions(), rd_start_str, rd_end_str);
      }
      // std::cout << "Delete Range: " << rd_start_str << " , " << rd_end_str << std::endl;
      // std::cout << rd_start_str << " status: " << s.ToString() << std::endl;
      if (!s.ok()) {
        std::cout << "fail to delete range [" << rd_start_str << " , " << rd_end_str << "] because of " << s.ToString()
                  << std::endl;
        exit(1);
      }
    } else {
      std::cout << "should not reach here" << std::endl;
      exit(1);
    }
    if (i % (ops.size() / 10) == 0) {
      std::cout << "mix progress: " << i << "/" << ops.size() << std::endl;

      // uint64_t deleter_size = 0;
      // db->GlobalDeleteRangeSize(deleter_size);
      // std::cout << "Szie of global deleter in Bytes: " << deleter_size << std::endl;
    }
  }

  std::cout
      << "Iter construction time : "
      << (iter_build_time / FLAGS_seek_num)
      << " ms"
      << std::endl;
}


int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  uint64_t total_num = FLAGS_num;
  rocksdb::DB* db;
  rocksdb::Options options;

  if (FLAGS_method == "default") {
    options = get_default_options();
  } 
  else if (FLAGS_method == "grdr") {
    options = get_GRDR_options();
  } else {
    std::cout << "Invalid method ..." << std::endl;
    exit(1);
  }
  
  rocksdb::Status status = rocksdb::DB::Open(options, FLAGS_db_path, &db);
  if (!status.ok()) {
    std::cout << "Fail to open DB, " << status.ToString() << std::endl;
    exit(1);
  }
  std::cout << "DB is opened. " << std::endl;
  
  std::chrono::steady_clock::time_point begin =
      std::chrono::steady_clock::now();
  if (FLAGS_mode == std::string("write")) {
    WriteKeys(total_num, db);
  } else if (FLAGS_mode == std::string("writeseq")) {
    WriteSeqKeys(total_num, db);
  } else if (FLAGS_mode == std::string("read")) {
    ReadKeys(total_num, db);
  } else if (FLAGS_mode == std::string("iter")) {
    IterKeys(total_num, db);
  } else if (FLAGS_mode == std::string("mix")) {
    MixedWorkload(total_num, db);
  } else if (FLAGS_mode == std::string("mixseq")) {
    MixedSeqWorkload(db);
  } else {
    std::cout << "invalid mode" << std::endl;
    exit(1);
  }
  
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  std::cout
      << "Time Elapsed: "
      << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count()
      << std::endl;
  std::cout << "validation complete" << std::endl;
  
  delete db;
  return 0;
}

