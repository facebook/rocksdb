#pragma once

#include "run.hpp"
#include "diskLevel.hpp"
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <stdlib.h>
#include <future>
#include <vector>
#include <mutex>
#include <thread>
#include "RTree.h"
#include "RTree_util.hpp"
#include <iostream>

namespace rangedelete_rep{

struct rd_rep_opt {
    bool use_full_rtree;
    uint64_t buffer_cap; // num entry fits in mem
    size_t T; // size ratio
    size_t page_size = 100;
    std::string path;
};

template <class K, class V>
class LSM {
 public:
    //To make private
    uint64_t buffer_cap; // num entry fits in mem
    size_t T; // size ratio
    uint64_t buffer_size; // num of existing entry in mem 
    // size_t num_disk_levels; // num of existing entry
    size_t page_size;
    
    mutex *mergeLock;
    thread mergeThread;
    
    LSM<K,V>(const LSM<K,V> &other) = default;
    LSM<K,V>(LSM<K,V> &&other) = default;
    
    LSM<K,V>(const rd_rep_opt opt)
            : buffer_cap(opt.buffer_cap), T(opt.T), use_full_rtree_(opt.use_full_rtree),
            page_size(opt.page_size), path(opt.path), base_(0){

        buffer_size = 0;
        mem_ = new RTreeType();

        mem_min_point_.init_min();
        mem_max_point_.init_max();
        mem_min_upper_ = std::numeric_limits<uint64_t>::max();

        mergeLock = new mutex();

        DiskLevel<K,V> * diskLevel = new DiskLevel<K, V>(path, page_size, 1, T, use_full_rtree_);
        diskLevels.emplace_back(diskLevel);
        // num_disk_levels = 1;
        curr_rtrees_.reserve(1000);
    }
    ~LSM<K,V>(){
        if (mergeThread.joinable()){
            mergeThread.join();
        }
        delete mergeLock;
        delete mem_;
        // for (int i = 0; i < diskLevels.size(); ++i){
        //     delete diskLevels[i];
        // }
    }

    void ResetMem(){
        mem_->RemoveAll();
        buffer_size = 0;
        mem_min_point_.init_min();
        mem_max_point_.init_max();
        mem_min_upper_ = std::numeric_limits<uint64_t>::max();
    }

    uint64_t GetBase() const{
        return base_;
    }

    void UpdateBase(uint64_t base){
        if(base_ < base){
            base_ = base;
        }  
    }

    bool IsValidLevel(size_t level){
        return (level <= diskLevels.size() && level >= 0);
    }
    
    void InsertRect(K &key);
    
    // compact all files in src_level to tar_level
    void CompactRunsToLevel(size_t src_level, size_t tar_level);

    bool QueryRect(K &key, bool is_point);

    bool QueryRect(K &key, bool is_point, uint64_t & node_cnt, uint64_t & leaf_cnt);

    // check the lsm file defined by &level and &run
    bool QueryNextFile(K &key, bool is_point, size_t &level, size_t &run, uint64_t &sequence, uint64_t &node_cnt, uint64_t &leaf_cnt);

    // Query the in memory parts of the LSM tree, which is loaded from the RTree_ of each diskrun
    bool QueryRectCurr(K &key, bool is_point);

    bool ExtractSubtree(K &key);

    void ExcuteGarbageCollection(const uint64_t & sequence);

    void ResetCurrentRTree();

 private:
    // RTree in memory
    bool use_full_rtree_ = false;
    RTreeType * mem_ = nullptr;
    Point mem_min_point_;
    Point mem_max_point_;
    uint64_t mem_min_upper_;  //minimum sequence number of range deletes in mem
    uint64_t base_;           //minimum effective sequence number of the tree, start from 0 and updated after GC
    // RTree on disks
    std::vector<DiskLevel<K,V> *> diskLevels;
    std::vector<RTreeType *> curr_rtrees_;
    std::string path;

};

template <class K, class V>
void LSM<K, V>::InsertRect(K &key) {
    buffer_size++;
    
    if (buffer_size >= buffer_cap){
        CompactRunsToLevel(0, 1);
    }

    mem_min_point_.set_min(key.min);
    mem_max_point_.set_max(key.max);
    mem_min_upper_ = std::min(mem_min_upper_, key.max.y);

    RectForRTree rectr(key);

    mem_->Insert(rectr.min, rectr.max, true);
}

// compact all files in src_level to tar_level
template <class K, class V>
void LSM<K, V>::CompactRunsToLevel(size_t src_level, size_t tar_level) {

    if (tar_level > diskLevels.size()){
        for (size_t idx = diskLevels.size() + 1 ; idx <= tar_level; idx++){
            DiskLevel<K,V> * newLevel = new DiskLevel<K, V>(path, page_size, idx, T, use_full_rtree_);
            diskLevels.push_back(newLevel);
            // num_disk_levels++;
        }
    }
    //Note that idx for disklevel start from 0, thus disklevel_idx = target/src_level - 1
    if (src_level == 0){
        // if source is mem
        // std::cout << "Compact from level : " << src_level << std::endl;
        mergeLock->lock();
        diskLevels[tar_level - 1]->AddRunFromMem(mem_, mem_min_point_, mem_max_point_, mem_min_upper_);
        ResetMem();
        mergeLock->unlock();
    } else {
        // std::cout << "Compact from level : " << src_level << std::endl;
        vector<DiskRun<K, V> *> runsToMerge = diskLevels[src_level - 1]->GetRunsToMerge();
        mergeLock->lock();
        diskLevels[tar_level - 1]->AddRun(runsToMerge);
        diskLevels[src_level - 1]->FreeMergedRuns(runsToMerge);
        // std::cout << "FreeMergedRuns Done" << std::endl;
        mergeLock->unlock();
    }
    
    if (diskLevels[tar_level - 1]->to_merge()) {
        CompactRunsToLevel(tar_level, tar_level + 1); // merge down one, recursively
    }
}

template <class K, class V>
bool LSM<K, V>::QueryRect(K &key, bool is_point){
    if (key.max < mem_min_point_ || key.min > mem_max_point_){
        return false;
    }
    // query mem, if not, look at disk
    if (is_point){
        assert(key.min == key.max);
    }
    RectForRTree rectr(key);
    //////////////////////////////////
    uint64_t node_cnt_ = 0;
    uint64_t leaf_cnt_ = 0;
    bool is_in_mem = mem_->Cover(rectr.min, rectr.max, node_cnt_, leaf_cnt_);
    // std::cout << "Memtable checked ..." << std::endl;
    // std::cout << "mem_->Cover : " << is_in_mem << ", node_cnt_ " << node_cnt_ << ", leaf_cnt_ " << leaf_cnt_ << std::endl;
    // node_cnt += node_cnt_;
    // leaf_cnt += leaf_cnt_;
    // is_in_mem = mem_->Cover(rectr.min, rectr.max);
    ////////////////////////////////////
    if (! is_in_mem){
        if (mergeThread.joinable()){
            // make sure that there isn't a merge happening as you search the disk
            mergeThread.join();
        }
        for (int i = 0; i < diskLevels.size(); i++){
            node_cnt_ = 0;
            leaf_cnt_ = 0;
            bool res = diskLevels[i]->QueryRect(key, node_cnt_, leaf_cnt_);
            if (res) {
                // std::cout << "Level " << (i+1) << " checked : found ..." << std::endl;
                return true;
            }
            // std::cout << "Level " << (i+1) << " checked : not exists ..." << std::endl;
            // if (diskLevels[i]->QueryRect(key, node_cnt_, leaf_cnt_)){
            //     return true;
            // }
        }
        return false;
    }
    
    return true;
}

template <class K, class V>
bool LSM<K, V>::QueryRect(K &key, bool is_point, uint64_t & node_cnt, uint64_t & leaf_cnt){
    // query mem, if not, look at disk
    if (is_point){
        assert(key.min == key.max);
    }
    RectForRTree rectr(key);
    uint64_t node_cnt_ = 0;
    uint64_t leaf_cnt_ = 0;

    bool is_in_mem = false;
    if (!(key.max < mem_min_point_ || key.min > mem_max_point_)){
        is_in_mem = mem_->Cover(rectr.min, rectr.max, node_cnt_, leaf_cnt_);
    }
    node_cnt_ = 0;
    leaf_cnt_ = 0;

    if (! is_in_mem){
        if (mergeThread.joinable()){
            // make sure that there isn't a merge happening as you search the disk
            mergeThread.join();
        }
        for (int i = 0; i < diskLevels.size(); i++){
            node_cnt_ = 0;
            leaf_cnt_ = 0;
            bool res = diskLevels[i]->QueryRect(key, node_cnt_, leaf_cnt_);
            node_cnt += node_cnt_;
            leaf_cnt += leaf_cnt_;
            if (res) {
                // std::cout << "Level " << (i+1) << " checked : found ..." << std::endl;
                return true;
            }
            // std::cout << "Level " << (i+1) << " checked : not exists ..." << std::endl;
            // if (diskLevels[i]->QueryRect(key, node_cnt_, leaf_cnt_)){
            //     return true;
            // }
        }
        return false;
    }
    
    return true;
}

template <class K, class V>
bool LSM<K, V>::QueryNextFile(K &key, bool is_point, size_t &level, size_t &run, uint64_t &sequence, uint64_t &node_cnt, uint64_t &leaf_cnt){
    if (is_point){
        assert(key.min == key.max);
    }
    RectForRTree rectr(key);
    uint64_t node_cnt_ = 0;
    uint64_t leaf_cnt_ = 0;

    bool result = false;
    if (level == 0 && run == 0)
    {
        sequence = mem_min_upper_;
        Point bound_min = mem_min_point_;
        Point bound_max = mem_max_point_;
        if (key.OverlapInX(bound_min, bound_max)){
            result = mem_->QueryMaxSequence(rectr.min, rectr.max, sequence);
        }
        level++;
    } else if(level <= diskLevels.size()){
        if (mergeThread.joinable()){
            mergeThread.join();
        }
        result = diskLevels[level - 1]->QueryRectAtRun(key, level, run, sequence, node_cnt_, leaf_cnt_);
    }
    node_cnt += node_cnt_;
    leaf_cnt += leaf_cnt_;
    
    return result;
}

template <class K, class V>
bool LSM<K, V>::QueryRectCurr(K &key, bool is_point){
    // query mem, if not, look at disk
    if (is_point){
        assert(key.min == key.max);
    }

    RectForRTree rectr(key);

    bool res = false;

    for (int i = 0; i < curr_rtrees_.size(); i++){
        if (curr_rtrees_[i]->IsEmpty()){
            continue;
        }
        res = curr_rtrees_[i]->Cover(rectr.min, rectr.max);
        if (res) {
            return true;
        }
    }
}

template <class K, class V>
bool LSM<K, V>::ExtractSubtree(K &key){
    uint64_t node_cnt = 0;
    uint64_t leaf_cnt = 0;

    ResetCurrentRTree();

    if (mergeThread.joinable()){
        // make sure that there isn't a merge happening as you search the disk
        mergeThread.join();
    }
    for (int i = 0; i < diskLevels.size(); i++){
        uint64_t node_cnt_ = 0;
        uint64_t leaf_cnt_ = 0;
        bool res = diskLevels[i]->ExtractSubtreeFromDisk(key, node_cnt_, leaf_cnt_, curr_rtrees_);
        node_cnt += node_cnt_;
        leaf_cnt += leaf_cnt_;
    }
    std::cout << "Extracting subtree for LSM Rtree: " << node_cnt << " nodes, " << leaf_cnt << " leaves." << std::endl;
    return true;
}

template <class K, class V>
void LSM<K, V>::ExcuteGarbageCollection(const uint64_t & sequence){
    for(size_t i = diskLevels.size(); i > 0; i--){
        diskLevels[i-1]->ExcuteGarbageCollection(sequence);
    }
}

template <class K, class V>
void LSM<K, V>::ResetCurrentRTree(){
    for(size_t i = 0; i < curr_rtrees_.size(); i++){
        delete curr_rtrees_[i];
    }
    curr_rtrees_.clear();
    curr_rtrees_.reserve(1000);
}

}
