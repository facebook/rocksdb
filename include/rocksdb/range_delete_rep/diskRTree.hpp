#pragma once
#include <vector>
#include <cstdint>
#include <cstring>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <cassert>
#include <algorithm>
#include "RTree_util.hpp"

namespace rangedelete_rep{

// typedef RTree<bool, uint64_t, 2, float, 16> RTreeType;

template <class K, class V> class DiskLevel;

template <class K, class V>
class DiskRun {
    friend class DiskLevel<K,V>;
public:
    DiskRun<K,V> (std::string path, size_t page_size, int level, size_t runID, 
                    Point min_point, Point max_point, uint64_t min_upper, RTree<bool, uint64_t, 2, float, 16> * Tree)
                  :level_(level), page_size_(page_size), runID_(runID), 
                  min_point_(min_point), max_point_(max_point), min_upper_(min_upper){
        filename_ = path + "C_" + to_string(level_) + "_" + to_string(runID_) + ".dat";
        Tree->Save(filename_.c_str());
        RTree_ = new RTree<bool, uint64_t, 2, float, 16>();
    }

    ~DiskRun<K,V>(){
        delete(RTree_);
        if (remove(filename_.c_str())){
            perror(("Error removing file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
    }
    
    bool query(const K &key){
        PrepareRTree();
        RectForRTree rectr(key);
        return RTree_->Cover(rectr.min, rectr.max); //search r-tree
    }

    // print the number of accessed internal nodes and leaf nodes
    bool query(const K &key, uint64_t &node_cnt, uint64_t &leaf_cnt){ 
        PrepareRTree();
        RectForRTree rectr(key);
        return RTree_->Cover(rectr.min, rectr.max, node_cnt, leaf_cnt); //search r-tree
    }

    // // query current RTree_
    // bool QueryCurr(const K &key){ 
    //     if(RTree_ == nullptr){
    //         return false;
    //     } else if (RTree_->IsEmpty()){
    //         return false;
    //     }
    //     RectForRTree rectr(key);
    //     return RTree_->Cover(rectr.min, rectr.max); //search r-tree
    // }

    // print the number of accessed internal nodes and leaf nodes
    bool QueryDisk(const K &key, uint64_t &node_cnt, uint64_t &leaf_cnt){ 
        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        RectForRTree rectr(key);
        bool res = false;
        RTree<bool, uint64_t, 2, float, 16>* tree = new RTree<bool, uint64_t, 2, float, 16>();
        res = tree->QueryFromFile(rectr.min, rectr.max, node_cnt, leaf_cnt, filename_.c_str()); //search r-tree
        // delete tree;
        return res;
    }

    // return whether the key is covered and record the max sequence if the cover rects exists
    bool QueryDiskMaxSequence(const K &key, uint64_t &sequence, uint64_t &node_cnt, uint64_t &leaf_cnt){ 
        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        RectForRTree rectr(key);
        bool res = false;
        RTree<bool, uint64_t, 2, float, 16>* tree = new RTree<bool, uint64_t, 2, float, 16>();
        res = tree->QueryMaxSequenceFromFile(rectr.min, rectr.max, sequence, node_cnt, leaf_cnt, filename_.c_str()); //search r-tree
        tree->RemoveRootNode();
        return res;
    }

    void PrepareRTree(){
        if (! loaded_){
            RTree_->RemoveAll();
            Load();
            loaded_ = true;
        }
    }

    // remove all rect whose sequence is smaller than a threshold (upper)
    void PruneBeforeUpper(const uint64_t & upper){
        // create a new rtree to store the prune result
        RTreeType* tree = new RTreeType();
        
        // load rects whose sequence is smaller than upper and return the boundary 
        std::vector<uint64_t> bounds_min(2, std::numeric_limits<uint64_t>::max());
        std::vector<uint64_t> bounds_max(2, 0);
        uint64_t new_upper = std::numeric_limits<uint64_t>::max();
        bool res = tree->ExtractSubtreeFromFileBeforeUpper(upper, new_upper, bounds_min, bounds_max, filename_.c_str());
        
        if (!res){
            return;
        }

        // update rtree and boundary (min point / max point / min upper) if necessary
        if(new_upper > min_upper_ || min_point_.update(bounds_min[0], bounds_min[1]) || max_point_.update(bounds_max[0], bounds_max[1])){            
            // update upper
            min_upper_ = new_upper;
            // save rtree
            string newName = filename_.substr(0, filename_.length() - 4) + "_tmp.dat";
            tree->Save(newName.c_str());
            // rename
            if (rename(newName.c_str(), filename_.c_str())){
                perror(("Error renaming file " + filename_ + " to " + newName).c_str());
                exit(EXIT_FAILURE);
            }
        }
    }

    bool ExtractSubtreeFromDisk(const K &key, uint64_t &node_cnt, uint64_t &leaf_cnt, RTreeType * tree){ 
        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        RectForRTree rectr(key);
        tree->RemoveAll();
        //reload RTree_ with elements overlapping with rectr in file
        bool res = tree->ExtractSubtreeFromFile(rectr.min, rectr.max, node_cnt, leaf_cnt, filename_.c_str()); 
        return res;
    }
    
    
private:
    std::string filename_;
    int level_;
    size_t runID_;

    bool loaded_ = false;

    RTree<bool, uint64_t, 2, float, 16> * RTree_ = nullptr;

    size_t page_size_;
    Point min_point_;
    Point max_point_;
    uint64_t min_upper_;
                            
    void Save(){
        RTree_->Save(filename_.c_str());
    }
    
    void Load(){
        // std::cout << "Load filename_ : " << filename_ << std::endl;

        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        RTree_->Load(filename_.c_str());
    }

    void LoadTo(RTree<bool, uint64_t, 2, float, 16> *tree){
        // std::cout << "Load filename_ : " << filename_ << " to tmp rtree" << std::endl;

        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        tree->Load(filename_.c_str());
    }
    
};
}

