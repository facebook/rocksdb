#pragma once

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <functional>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/mock_table.h"
#include "util/string_util.h"
#include "rocksdb/statistics.h"

#include "rocksdb/range_delete_filter/bucket_wrapper.hpp"
#include "rocksdb/range_delete_rep/lsm.hpp"

namespace ROCKSDB_NAMESPACE {

enum Method : unsigned char {
  mDefault = 0,
  mScanAndDelete = 1,
  mDecomposition = 2,
  mGlobalRangeDelete = 3,
  mNone
};

enum Result : unsigned char {
  kOk = 0,
  kNotFound = 1,
  kRDFilter = 2,
  kNoRDFilter = 3,
  kRDRep = 4,
  kNotRangeDeleted = 5,
  kMaxCode
};

rocksdb::Result ConvertStaToRes(rocksdb::Status &s){
  if(s.ok()){
    return kOk;
  } else if (s.IsNotFound()){
    return kNotFound;
  }
  return kMaxCode;
}

inline std::string ItoSWithPadding(const uint64_t key, uint64_t size) {
  std::string key_str = std::to_string(key);
  std::string padding_str(size - key_str.size(), '0');
  key_str = padding_str + key_str;
  return key_str;
}

class OperationGenerator {
 public:
  OperationGenerator(uint64_t key_size, uint64_t value_size, uint64_t max_key,
                      bool shuffle = true) {
    idx_ = 0;
    key_size_ = key_size;
    value_size_ = value_size;
    max_key_ = max_key;
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

  bool Valid() const {
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
      uint64_t key = shuffle_ ? (rand() % max_key_) : i;
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
      uint64_t key = rand() % max_key_;
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

  uint64_t size() const {
    return keys_.size();
  }

 private:
  uint64_t idx_;
  std::vector<uint64_t> keys_;
  std::vector<char> ops_;
  uint64_t key_size_;
  uint64_t value_size_;
  uint64_t max_key_;
  bool shuffle_;
};

struct RDTimer {
  uint64_t duration = 0;
  uint64_t count = 0;

  std::chrono::steady_clock::time_point begin;

  void Start(){
    begin = std::chrono::steady_clock::now();
  }
  void Pause(){
    // duration += std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();
    duration += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin).count();
  }
  void PauseMS(){
    duration += std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();
  }
  void AddCount(){
    count++;
  }
};

struct RDDBStat
{
  RDTimer op_write;
  RDTimer op_read;
  RDTimer op_rread;
  RDTimer op_rdelete;
  RDTimer rd_filter;
  RDTimer rd_rep;
  RDTimer op_rdelete_filter;
  RDTimer op_rdelete_rep;
  // uint64_t break_filter = 0; //#operations filtered by filter
  // uint64_t break_rep = 0;    //#calls range delete rep
  uint64_t filter_tn = 0;
  uint64_t filter_fp = 0;
  uint64_t filter_tp = 0;
  std::vector<uint64_t> rtree_node_cnt;
  std::vector<uint64_t> rtree_leaf_cnt;
  // std::vector<uint64_t> gc_times; // garbage collection

  bool enable_global_rd = false;

  void Print();
};

uint64_t GetMin(std::vector<uint64_t> &vec){
  if (vec.size() == 0){
    return 0;
  }
  return *std::min_element(vec.begin(), vec.end());
}

uint64_t GetMax(std::vector<uint64_t> &vec){
  if (vec.size() == 0){
    return 0;
  }
  return *std::max_element(vec.begin(), vec.end());
}

uint64_t GetAvg(std::vector<uint64_t> &vec){
  if (vec.size() == 0){
    return 0;
  }
  uint64_t sum = std::accumulate(vec.begin(), vec.end(), 0);
  return sum / vec.size();
}

void RDDBStat::Print(){
  std::cout << "Test Information: " << std::endl;
  std::cout << "    Point query:  count: " << op_read.count << " , duration: " << op_read.duration 
            << " us, avg latency: " << op_read.duration/std::max(op_read.count, uint64_t(1)) << std::endl;
  std::cout << "    Range query: count:  " << op_rread.count << " , duration: " << op_rread.duration
            << " us, avg latency: " << op_rread.duration/std::max(op_rread.count, uint64_t(1)) << std::endl;
  std::cout << "    Range delete: count:  " << op_rdelete.count << " , duration: " << op_rdelete.duration 
            << " us, avg latency: " << op_rdelete.duration/std::max(op_rdelete.count, uint64_t(1)) << std::endl;
  std::cout << "    write:  count: " << op_write.count << " , duration: " << op_write.duration 
            << " us, avg latency: " << op_write.duration/std::max(op_write.count, uint64_t(1)) << std::endl;
  std::cout << "            write range delete filter: " << op_rdelete_filter.duration 
            << " us, write range delete representation " << op_rdelete_rep.duration << " us." << std::endl;

  if(enable_global_rd){
    std::cout << "  GRD Information: " << std::endl;
    std::cout << "    Read RD filter: count:  " << rd_filter.count << " , duration: " << rd_filter.duration 
              << " us, avg latency: " << rd_filter.duration/std::max(rd_filter.count, uint64_t(1)) << std::endl;
    std::cout << "              True negative: " << filter_tn << " True positive: " << filter_tp << " False positive: " << filter_fp << std::endl;

    std::cout << "    Read RD rep: count:  " << rd_rep.count << " , duration: " << rd_rep.duration 
              << " us, avg latency: " << rd_rep.duration/std::max(rd_rep.count, uint64_t(1)) << std::endl;
    std::cout << "       number of accessed internal nodes (min - max): " <<  GetMin(rtree_node_cnt) << " - " << GetMax(rtree_node_cnt)
              << " average value: " << GetAvg(rtree_node_cnt) << std::endl;
    std::cout << "       number of accessed leaf nodes (min - max): " <<  GetMin(rtree_leaf_cnt) << " - " << GetMax(rtree_leaf_cnt)
              << " average value: " << GetAvg(rtree_leaf_cnt) << std::endl;
    // std::cout << "  Garbage collection: " <<  gc_times.size() << " times, taking " << GetAvg(gc_times) << " us on average value: " << std::endl;
    // for (size_t i = 0; i < gc_times.size(); i++){
    //   std::cout << "       Garbage collection " <<  i << " takes " << gc_times[i] << " us; " << std::endl;
    // }
    // std::cout << "       number of accessed leaf nodes (min - max): " << *std::min_element(rtree_leaf_cnt.begin(), rtree_leaf_cnt.end()) 
    //           << " - " << *std::max_element(rtree_leaf_cnt.begin(), rtree_leaf_cnt.end()) << std::endl;
  }
  
  std::cout << std::endl;
}

}