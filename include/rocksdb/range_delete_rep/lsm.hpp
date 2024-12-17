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

typedef RTree<bool, uint64_t, 2, float, 16> RTreeType;

struct rd_rep_opt {
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
    size_t num_disk_levels; // num of existing entry
    size_t page_size;
    
    mutex *mergeLock;
    thread mergeThread;
    
    LSM<K,V>(const LSM<K,V> &other) = default;
    LSM<K,V>(LSM<K,V> &&other) = default;
    
    LSM<K,V>(const rd_rep_opt opt)
            : buffer_cap(opt.buffer_cap), T(opt.T), 
            page_size(opt.page_size), path(opt.path){

        buffer_size = 0;
        mem_ = new RTree<bool, uint64_t, 2, float, 16>();

        mem_min_point_.init_min();
        mem_max_point_.init_max();

        mergeLock = new mutex();

        DiskLevel<K,V> * diskLevel = new DiskLevel<K, V>(path, page_size, 1, T);
        diskLevels.emplace_back(diskLevel);
        num_disk_levels = 1;
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
    }
    
    void InsertRect(K &key) {
        buffer_size++;
        
        if (buffer_size >= buffer_cap){
            CompactRunsToLevel(0, 1);
        }

        mem_min_point_.set_min(key.min);
        mem_max_point_.set_max(key.max);

        RectForRTree rectr(key);

        mem_->Insert(rectr.min, rectr.max, true);
    }
    
    // compact all files in src_level to tar_level
    void CompactRunsToLevel(size_t src_level, size_t tar_level) {

        if (tar_level > diskLevels.size()){
            for (size_t idx = diskLevels.size() + 1 ; idx <= tar_level; idx++){
                DiskLevel<K,V> * newLevel = new DiskLevel<K, V>(path, page_size, idx, T);
                diskLevels.push_back(newLevel);
                num_disk_levels++;
            }
        }
        //Note that idx for disklevel start from 0, thus disklevel_idx = target/src_level - 1
        if (src_level == 0){
            // if source is mem
            // std::cout << "Compact from level : " << src_level << std::endl;
            diskLevels[tar_level - 1]->AddRunFromMem(mem_, mem_min_point_, mem_max_point_);
            mergeLock->lock();
            ResetMem();
            mergeLock->unlock();
        } else {
            // std::cout << "Compact from level : " << src_level << std::endl;
            vector<DiskRun<K, V> *> runsToMerge = diskLevels[src_level - 1]->GetRunsToMerge();
            diskLevels[tar_level - 1]->AddRun(runsToMerge);
            mergeLock->lock();
            diskLevels[src_level - 1]->FreeMergedRuns(runsToMerge);
            // std::cout << "FreeMergedRuns Done" << std::endl;
            mergeLock->unlock();
        }
        
        if (diskLevels[tar_level - 1]->to_merge()) {
            CompactRunsToLevel(tar_level, tar_level + 1); // merge down one, recursively
        }
    }

    bool QueryRect(K &key, bool is_point){
        if (key.max < mem_min_point_ || key.min > mem_max_point_){
            return false;
        }
        // query mem, if not, look at disk
        if (is_point){
            assert(key.min == key.max);
        }
        RectForRTree rectr(key);
        if (! mem_->Cover(rectr.min, rectr.max)){
            if (mergeThread.joinable()){
                // make sure that there isn't a merge happening as you search the disk
                mergeThread.join();
            }
            for (int i = 0; i < diskLevels.size(); i++){
                if (diskLevels[i]->QueryRect(key)){
                    return true;
                }
            }
        }
        
        return false;
    }

    bool QueryRect(K &key, bool is_point, uint64_t & node_cnt, uint64_t & leaf_cnt){
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

 private:
    // RTree in memory
    RTree<bool, uint64_t, 2, float, 16> * mem_ = nullptr;
    Point mem_min_point_;
    Point mem_max_point_;
    // RTree on disks
    std::vector<DiskLevel<K,V> *> diskLevels;
    std::string path;

};

}
