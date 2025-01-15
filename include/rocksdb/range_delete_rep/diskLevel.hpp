//
//  diskLevel.hpp
//  lsm-tree
//
//    sLSM: Skiplist-Based LSM Tree
//    Copyright © 2017 Aron Szanto. All rights reserved.
//
//    This program is free software: you can redistribute it and/or modify
//    it under the terms of the GNU General Public License as published by
//    the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.
//
//    This program is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU General Public License for more details.
//
//        You should have received a copy of the GNU General Public License
//        along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
#pragma once

#include <vector>
#include <cstdint>
#include <string>
#include <cstring>
#include "run.hpp"
// #include "diskRun.hpp"
#include "diskRTree.hpp"
#include "RTree_util.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <cassert>
#include <algorithm>
#include <iostream>

// #define LEFTCHILD(x) 2 * x + 1
// #define RIGHTCHILD(x) 2 * x + 2
// #define PARENT(x) (x - 1) / 2

// int TOMBSTONE = INT_MIN;

// using namespace std;

namespace rangedelete_rep{

template <class K, class V>
class DiskLevel {
 public:
    int level_;
    size_t page_size_; // number of elements per fence pointer
    size_t T_; // number of runs in a level
    // unsigned _numRuns; // index of active run
    // unsigned _numMerges; // number of merges in a level
    mutex *curr_read_lock; //WF: Used to protect read RTree in each run during compaction TODO: try to use other methods
    
    DiskLevel<K,V>( std::string path, size_t page_size, int level, size_t T, bool use_full_rtree)
                    :T_(T), level_(level), page_size_(page_size), path_(path), use_full_rtree_(use_full_rtree)
    {
        // min_point_.init_min();
        // max_point_.init_max();
        // min_upper_ = std::numeric_limits<uint64_t>::max();
        runs_.reserve(100); //temperaly set it to 100, while we expect at most T runs in this level
        curr_read_lock = new mutex();
    }
    
    ~DiskLevel<K,V>(){
        for (int i = 0; i< runs_.size(); i++){
            delete runs_[i];
        }
        delete curr_read_lock;
    }
    
    void AddRun(vector<DiskRun<K, V> *> &runList);

    void AddRunFromMem(RTreeType * mem, Point & minP, Point & maxP, uint64_t & minU);
    
    vector<DiskRun<K,V> *> GetRunsToMerge();
    
    void FreeMergedRuns(vector<DiskRun<K,V> *> &toFree);

    bool to_merge(){
        return (runs_.size() >= T_);
    }

    bool level_empty(){
        return (runs_.size() == 0);
    }

    bool QueryRect (const K &key);

    bool QueryRect (const K &key, uint64_t & rtree_cnt, uint64_t & node_cnt, uint64_t & leaf_cnt);

    bool QueryRectAtRun(const K &key, size_t &level, size_t &run, uint64_t &sequence, uint64_t & node_cnt, uint64_t & leaf_cnt);

    void ExcuteGarbageCollection(const uint64_t & sequence);

    bool ExtractSubtreeFromDisk (const K &key, uint64_t & node_cnt, uint64_t & leaf_cnt, std::vector<RTreeType *> & rtrees);

 private:
    // Point min_point_;
    // Point max_point_;
    // uint64_t min_upper_;
    bool use_full_rtree_;
    std::string path_;
    std::vector<DiskRun<K,V> *> runs_;
};

template <class K, class V>
void DiskLevel<K, V>::AddRun(vector<DiskRun<K, V> *> &runList) {
    RTreeType * RTree_res;
    RTree_res = new RTreeType();
    Point minP;
    Point maxP;
    uint64_t minU;
    // merge existing rtrees
    for (int i = 0; i < runList.size(); i++){
        if (i == 0)
        {
            runList[i]->LoadTo(RTree_res);
            minP = runList[i]->min_point_;
            maxP = runList[i]->max_point_;
            minU = runList[i]->min_upper_;
        }else {
            runList[i]->PrepareRTree();
            // update boundary
            minP.set_min(runList[i]->min_point_);
            maxP.set_max(runList[i]->max_point_);
            minU = std::min(minU, runList[i]->min_upper_);

            // insert entries in new RTree
            RTreeType::Iterator it;
            uint64_t boundsMin[2] = {0, 0};
            uint64_t boundsMax[2] = {0, 0};

            for (runList[i]->RTree_->GetFirst(it); !runList[i]->RTree_->IsNull(it); runList[i]->RTree_->GetNext(it)) {
                it.GetBounds(boundsMin, boundsMax);
                RTree_res->Insert(boundsMin, boundsMax, *it);
            }
        }
    }
    // insert into target slot
    size_t runID = runs_.size();
    runs_.emplace_back(new DiskRun<K,V>(path_, page_size_, level_, runID, minP, maxP, minU, use_full_rtree_, RTree_res));
}

template <class K, class V>
void DiskLevel<K, V>::AddRunFromMem(RTreeType * mem, Point & minP, Point & maxP, uint64_t & minU) {
    // insert into level_ 1
    size_t runID = runs_.size();
    runs_.emplace_back(new DiskRun<K,V>(path_, page_size_, level_, runID, minP, maxP, minU, use_full_rtree_, mem));

    // RTreeType::Iterator it;
    // uint64_t boundsMin[2] = {0, 0};
    // uint64_t boundsMax[2] = {0, 0};
    // for (mem.GetFirst(it); !mem.IsNull(it); mem.GetNext(it)) {
    //     it.GetBounds(boundsMin, boundsMax);
    //     if (boundsMin[0] < minP.data[0]){
    //         minP.data[0] = boundsMin[0];
    //     }
    //     if (boundsMin[1] < minP.data[1]){
    //         minP.data[1] = boundsMin[1];
    //     }
    //     if (boundsMax[0] > maxP.data[0]){
    //         maxP.data[0] = boundsMax[0];
    //     }
    //     if (boundsMax[1] > maxP.data[1]){
    //         maxP.data[1] = boundsMax[1];
    //     }
    // }        
}

template <class K, class V>
vector<DiskRun<K,V> *> DiskLevel<K, V>::GetRunsToMerge(){
    vector<DiskRun<K, V> *> toMerge;
    for (int i = 0; i < runs_.size(); i++){
        toMerge.push_back(runs_[i]);
    }
    return toMerge;
}

template <class K, class V>
void DiskLevel<K, V>::FreeMergedRuns(vector<DiskRun<K,V> *> &toFree){
    for (int i = 0; i < toFree.size(); i++){
        assert(toFree[i]->level_ == level_);
        delete toFree[i];
    }
    assert(toFree.size() <= runs_.size());
    size_t numRuns = runs_.size() - toFree.size();
    runs_.erase(runs_.begin(), runs_.begin() + toFree.size());
    for (int i = 0; i < numRuns; i++){
        runs_[i]->runID_ = i;
        string newName = (path_ + "C_" + to_string(runs_[i]->level_) + "_" + to_string(runs_[i]->runID_) + ".dat");
        
        if (rename(runs_[i]->filename_.c_str(), newName.c_str())){
            perror(("Error renaming file " + runs_[i]->filename_ + " to " + newName).c_str());
            exit(EXIT_FAILURE);
        }
        runs_[i]->filename_ = newName;
    }
}

template <class K, class V>
bool DiskLevel<K, V>::QueryRect (const K &key) {
    int maxRunToSearch = runs_.size();
    if (maxRunToSearch == 0){
        return false;
    }
    for (int i = maxRunToSearch - 1; i >= 0; i--){
        if (key.max < runs_[i]->min_point_ || key.min > runs_[i]->max_point_){
            continue;
        }
        if (runs_[i]->query(key)) {
            return true;
        }
    }
    return false;
}

template <class K, class V>
bool DiskLevel<K, V>::QueryRect (const K &key, uint64_t & rtree_cnt, uint64_t & node_cnt, uint64_t & leaf_cnt) {
    int maxRunToSearch = runs_.size();
    if (maxRunToSearch == 0){
        return false;
    }
    for (int i = maxRunToSearch - 1; i >= 0; i--){
        if (key.max < runs_[i]->min_point_ || key.min > runs_[i]->max_point_){
            continue;
        }
        // if (runs_[i]->query(key, node_cnt, leaf_cnt)) {
        //     return true;
        // }
        uint64_t node_cnt_ = 0;
        uint64_t leaf_cnt_ = 0;
        // bool res = runs_[i]->query(key, node_cnt_, leaf_cnt_);
        bool res = runs_[i]->QueryDisk(key, node_cnt_, leaf_cnt_);
        node_cnt += node_cnt_;
        leaf_cnt += leaf_cnt_;
        rtree_cnt += 1;
        if (res) {return true;}
    }
    return false;
}

template <class K, class V>
bool DiskLevel<K, V>::QueryRectAtRun(const K &key, size_t &level, size_t &run, uint64_t &sequence, uint64_t & node_cnt, uint64_t & leaf_cnt){
    bool result = false;
    int maxRunToSearch = runs_.size();
    assert(level == level_);
    if((run + 1) > maxRunToSearch || maxRunToSearch == 0){
        run = 0;
        level++;
        return result;
    }
    uint64_t node_cnt_ = 0;
    uint64_t leaf_cnt_ = 0;
    size_t run_id = maxRunToSearch - 1 - run;
    sequence = runs_[run_id]->min_upper_;
    Point bound_min = runs_[run_id]->min_point_;
    Point bound_max = runs_[run_id]->max_point_;
    if (key.OverlapInX(bound_min, bound_max)){
        result = runs_[run_id]->QueryDiskMaxSequence(key, sequence, node_cnt_, leaf_cnt_);
        node_cnt += node_cnt_;
        leaf_cnt += leaf_cnt_;
    }
    // update level & run_id of next file
    if(run_id > 0){
        run++;
    } else if (run_id == 0){
        run = 0;
        level++;
    }
    return result;
}

template <class K, class V>
void DiskLevel<K, V>::ExcuteGarbageCollection(const uint64_t & sequence){
    // return if all runs are created after sequence
    if(runs_.size() < 1){
        return;
    }
    // find out runs to delete / prune
    vector<DiskRun<K, V> *> runs_to_delete;
    vector<DiskRun<K, V> *> runs_to_prune;
    for (int i = 0; i < runs_.size(); i++){
        if (runs_[i]->max_point_.y < sequence){
            runs_to_delete.push_back(runs_[i]);
        } else if (runs_[i]->max_point_.y > sequence && runs_[i]->min_upper_ < sequence){
            runs_to_prune.push_back(runs_[i]);
        }
    }
    // we assume all runs to be deleted are before that to be pruned
    assert((runs_to_delete.size() + runs_to_prune.size()) <= runs_.size());
    if (runs_to_delete.size() > 0){
        assert(runs_to_delete[runs_to_delete.size() - 1]->runID_ < runs_to_prune[0]->runID_);
    }
    // prune runs before deletion
    // there could be optimized: if the run is too small after pruning, merge it to adjacent run
    for (int i = 0; i < runs_to_prune.size(); i++)
    {
        assert(runs_to_prune[i]->level_ == level_);
        runs_to_prune[i]->PruneBeforeUpper(sequence);
    }
    // delete obsolete runs
    FreeMergedRuns(runs_to_delete);
}

template <class K, class V>
bool DiskLevel<K, V>::ExtractSubtreeFromDisk (const K &key, uint64_t & node_cnt, uint64_t & leaf_cnt, std::vector<RTreeType *> & rtrees) {
    int maxRunToAccess = runs_.size() - 1;
    for (int i = maxRunToAccess; i >= 0; i--){
        // std::cout << "ExtractSubtreeFromDisk Level " << level_ << " run " << i << std::endl;
        if (key.max < runs_[i]->min_point_ || key.min > runs_[i]->max_point_){
            continue;
        }
        uint64_t node_cnt_ = 0;
        uint64_t leaf_cnt_ = 0;
        // bool res = runs_[i]->query(key, node_cnt_, leaf_cnt_);
        RTreeType * tree_tmp = new RTreeType();
        bool res = runs_[i]->ExtractSubtreeFromDisk(key, node_cnt_, leaf_cnt_, tree_tmp);
        if (tree_tmp != nullptr){
            if (!tree_tmp->IsEmpty()){
                rtrees.emplace_back(tree_tmp);
            }
        }
        node_cnt += node_cnt_;
        leaf_cnt += leaf_cnt_;
    }
    return true;
}

}