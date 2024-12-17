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

// #define LEFTCHILD(x) 2 * x + 1
// #define RIGHTCHILD(x) 2 * x + 2
// #define PARENT(x) (x - 1) / 2

// int TOMBSTONE = INT_MIN;

// using namespace std;

namespace rangedelete_rep{

typedef RTree<bool, uint64_t, 2, float, 16> RTreeType;

template <class K, class V>
class DiskLevel {
 public:
    int level_;
    size_t page_size_; // number of elements per fence pointer
    size_t T_; // number of runs in a level
    // unsigned _numRuns; // index of active run
    // unsigned _numMerges; // number of merges in a level
    
    DiskLevel<K,V>( std::string path, size_t page_size, int level, size_t T)
                    :T_(T), level_(level), page_size_(page_size), path_(path){
        min_point_.init_min();
        max_point_.init_max();
        runs_.reserve(100); //temperaly set it to 100, while we expect at most T runs in this level
        // std::cout << "Disk level " << level_ << " created ..." << std::endl;
        // _numRuns = 0;        
        // for (int i = 0; i < _T; i++){
        //     DiskRun<K,V> * run = new DiskRun<K, V>(_runSize, pageSize, level, i, _bf_fp);
        //     runs.push_back(run);
        // }
    }
    
    ~DiskLevel<K,V>(){
        for (int i = 0; i< runs_.size(); i++){
            delete runs_[i];
        }
    }
    
    void AddRun(vector<DiskRun<K, V> *> &runList) {
        RTree<bool, uint64_t, 2, float, 16> * RTree_res;
        RTree_res = new RTree<bool, uint64_t, 2, float, 16>();
        Point minP;
        Point maxP;
        // merge existing rtrees
        for (int i = 0; i < runList.size(); i++){
            if (i == 0)
            {
                runList[i]->LoadTo(RTree_res);
                minP = runList[i]->min_point_;
                maxP = runList[i]->max_point_;
            }else {
                runList[i]->PrepareRTree();
                // update boundary
                minP.set_min(runList[i]->min_point_);
                maxP.set_max(runList[i]->max_point_);

                // insert entries in new RTree
                RTree<bool, uint64_t, 2, float, 16>::Iterator it;
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
        runs_.emplace_back(new DiskRun<K,V>(path_, page_size_, level_, runID, minP, maxP, RTree_res));
    }


    void AddRunFromMem(RTree<bool, uint64_t, 2, float, 16> * mem, Point & minP, Point & maxP) {
        // insert into level_ 1
        size_t runID = runs_.size();
        runs_.emplace_back(new DiskRun<K,V>(path_, page_size_, level_, runID, minP, maxP, mem));

        // RTree<bool, uint64_t, 2, float, 16>::Iterator it;
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
    
    
    vector<DiskRun<K,V> *> GetRunsToMerge(){
        vector<DiskRun<K, V> *> toMerge;
        for (int i = 0; i < runs_.size(); i++){
            toMerge.push_back(runs_[i]);
        }
        return toMerge;
    }
    
    void FreeMergedRuns(vector<DiskRun<K,V> *> &toFree){
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

    bool to_merge(){
        return (runs_.size() >= T_);
    }

    bool level_empty(){
        return (runs_.size() == 0);
    }

    bool QueryRect (const K &key) {
        int maxRunToSearch = runs_.size() - 1;
        for (int i = maxRunToSearch; i >= 0; i--){
            if (key.max < runs_[i]->min_point_ || key.min > runs_[i]->max_point_){
                continue;
            }
            if (runs_[i]->query(key)) {
                return true;
            }
        }
        return false;
    }

    bool QueryRect (const K &key, uint64_t & node_cnt, uint64_t & leaf_cnt) {
        int maxRunToSearch = runs_.size() - 1;
        for (int i = maxRunToSearch; i >= 0; i--){
            if (key.max < runs_[i]->min_point_ || key.min > runs_[i]->max_point_){
                continue;
            }
            // if (runs_[i]->query(key, node_cnt, leaf_cnt)) {
            //     return true;
            // }
            uint64_t node_cnt_ = 0;
            uint64_t leaf_cnt_ = 0;
            // bool res = runs_[i]->query(key, node_cnt_, leaf_cnt_);
            bool res = runs_[i]->diskquery(key, node_cnt_, leaf_cnt_);
            node_cnt += node_cnt_;
            leaf_cnt += leaf_cnt_;
            if (res) {return true;}
        }
        return false;
    }

 private:
    Point min_point_;
    Point max_point_;
    std::string path_;
    std::vector<DiskRun<K,V> *> runs_;
};
}