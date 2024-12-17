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
  // uint64_t break_filter = 0; //#operations filtered by filter
  // uint64_t break_rep = 0;    //#calls range delete rep
  uint64_t filter_tn = 0;
  uint64_t filter_fp = 0;
  uint64_t filter_tp = 0;
  std::vector<uint64_t> rtree_node_cnt;
  std::vector<uint64_t> rtree_leaf_cnt;

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
  std::cout << "    write:  count: " << op_write.count << " , duration: " << op_write.duration 
            << " , avg latency: " << op_write.duration/std::max(op_write.count, uint64_t(1)) << std::endl;
  std::cout << "    Point query:  count: " << op_read.count << " , duration: " << op_read.duration 
            << " , avg latency: " << op_read.duration/std::max(op_read.count, uint64_t(1)) << std::endl;
  std::cout << "    Range query: count:  " << op_rread.count << " , duration: " << op_rread.duration
            << " , avg latency: " << op_rread.duration/std::max(op_rread.count, uint64_t(1)) << std::endl;
  std::cout << "    Range delete: count:  " << op_rdelete.count << " , duration: " << op_rdelete.duration 
            << " , avg latency: " << op_rdelete.duration/std::max(op_rdelete.count, uint64_t(1)) << std::endl;

  if(enable_global_rd){
    std::cout << "  GRD Information: " << std::endl;
    std::cout << "    RD filter: count:  " << rd_filter.count << " , duration: " << rd_filter.duration 
              << " , avg latency: " << rd_filter.duration/std::max(rd_filter.count, uint64_t(1)) << std::endl;
    std::cout << "              True negative: " << filter_tn << " True positive: " << filter_tp << " False positive: " << filter_fp << std::endl;

    std::cout << "    RD rep: count:  " << rd_rep.count << " , duration: " << rd_rep.duration 
              << " , avg latency: " << rd_rep.duration/std::max(rd_rep.count, uint64_t(1)) << std::endl;
    std::cout << "       number of accessed internal nodes (min - max): " <<  GetMin(rtree_node_cnt) << " - " << GetMax(rtree_node_cnt)
              << " average value: " << GetAvg(rtree_node_cnt) << std::endl;
    std::cout << "       number of accessed leaf nodes (min - max): " <<  GetMin(rtree_leaf_cnt) << " - " << GetMax(rtree_leaf_cnt)
              << " average value: " << GetAvg(rtree_leaf_cnt) << std::endl;
    // std::cout << "       number of accessed leaf nodes (min - max): " << *std::min_element(rtree_leaf_cnt.begin(), rtree_leaf_cnt.end()) 
    //           << " - " << *std::max_element(rtree_leaf_cnt.begin(), rtree_leaf_cnt.end()) << std::endl;
  }
  
  std::cout << std::endl;
}

}