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
#include "RTree.h"

namespace rangedelete_rep{

typedef RTree<bool, uint64_t, 2, float, 16> RTreeType;

template <class K, class V> class DiskLevel;

template <class K, class V>
class DiskRun {
    friend class DiskLevel<K,V>;
public:
    DiskRun<K,V> (std::string path, size_t page_size, int level, size_t runID, 
                    Point min_point, Point max_point, RTree<bool, uint64_t, 2, float, 16> * Tree)
                  :level_(level), page_size_(page_size), runID_(runID), 
                  min_point_(min_point), max_point_(max_point){
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

    // print the number of accessed internal nodes and leaf nodes
    bool diskquery(const K &key, uint64_t &node_cnt, uint64_t &leaf_cnt){ 
        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        RectForRTree rectr(key);
        RTree<bool, uint64_t, 2, float, 16>* tree = new RTree<bool, uint64_t, 2, float, 16>();
        bool res = tree->QueryFromFile(rectr.min, rectr.max, node_cnt, leaf_cnt, filename_.c_str()); //search r-tree
        return res;
    }

    void PrepareRTree(){
        if (! loaded_){
            RTree_->RemoveAll();
            Load();
            loaded_ = true;
        }
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

