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

template <class K, class V> class DiskLevel;

template <class K, class V>
class DiskRun {
    friend class DiskLevel<K,V>;
public:
    DiskRun<K,V> (std::string path, size_t page_size, int level, size_t runID, 
                    Point min_point, Point max_point, uint64_t min_upper, bool use_full_rtree, 
                    RTreeType * Tree)
                  :level_(level), page_size_(page_size), runID_(runID), min_point_(min_point), 
                  max_point_(max_point), min_upper_(min_upper), use_full_rtree_(use_full_rtree)
    {
        filename_ = path + "C_" + to_string(level_) + "_" + to_string(runID_) + ".dat";
        // Index_RTree_ = Tree;
        Tree->SaveLeafToFile(filename_.c_str());
        Index_RTree_ = new RTreeType();
        Index_RTree_->CopyWithoutLeaf(Tree);
        RTree_ = new RTreeType();
    }

    ~DiskRun<K,V>(){
        delete(RTree_);
        delete(Index_RTree_);
        if (remove(filename_.c_str())){
            perror(("Error removing file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
    }

    void UpdateInfo(Point min_point, Point max_point, uint64_t min_upper)
    {
        min_point_ = min_point; 
        max_point_ = max_point;
        min_upper_ = min_upper;
    }

    void UpdateRtree(RTreeType * tree){
        // RTreeType* old_tree = Index_RTree_;
        std::string newName = filename_.substr(0, filename_.length() - 4) + "_tmp.dat";
        tree->SaveLeafToFile(newName.c_str());
        Index_RTree_->RemoveAll();
        Index_RTree_->CopyWithoutLeaf(tree);
        // rename
        if (rename(newName.c_str(), filename_.c_str())){
            perror(("Error renaming file " + filename_ + " to " + newName).c_str());
            exit(EXIT_FAILURE);
        }
        loaded_ = false;
    }
    

    // print the number of accessed internal nodes and leaf nodes
    bool QueryDisk(const K &key, uint64_t &node_cnt, uint64_t &leaf_cnt) const{
        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        RectForRTree rectr(key);
        bool res = false;
        //search index r-tree and search leaf file
        res = Index_RTree_->QueryFromLeafFile(rectr.min, rectr.max, node_cnt, leaf_cnt, filename_.c_str()); 
        // RTreeType* tree = new RTreeType();
        // res = tree->QueryFromFile(rectr.min, rectr.max, node_cnt, leaf_cnt, filename_.c_str(), use_full_rtree_); //search r-tree
        return res;
    }

    // return whether the key is covered and record the max sequence if the cover rects exists
    // Obsolete
    bool QueryDiskMaxSequence(const K &key, uint64_t &sequence, uint64_t &node_cnt, uint64_t &leaf_cnt){ 
        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        RectForRTree rectr(key);
        bool res = false;
        RTreeType* tree = new RTreeType();
        // need to add full version
        res = tree->QueryMaxSequenceFromFile(rectr.min, rectr.max, sequence, node_cnt, leaf_cnt, filename_.c_str()); //search r-tree
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
        bool res = tree->ExtractSubtreeBeforeUpperFromIndexedLeafFile(upper, new_upper, bounds_min, bounds_max, Index_RTree_, filename_.c_str());        
        if (!res){
            return;
        }

        // update rtree and boundary (min point / max point / min upper) if necessary
        if(new_upper > min_upper_ || min_point_.update(bounds_min[0], bounds_min[1]) || max_point_.update(bounds_max[0], bounds_max[1])){            
            // update upper
            min_upper_ = new_upper;
            // save rtree
            UpdateRtree(tree);
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
        bool res = tree->ExtractSubtreeFromIndexedLeafFile(rectr.min, rectr.max, Index_RTree_, filename_.c_str());
        return res;
    }
    
    
private:
    RTreeType * Index_RTree_ = nullptr;
    std::string filename_;
    int level_;
    size_t runID_;

    bool use_full_rtree_ = false;
    bool loaded_ = false;

    RTreeType * RTree_ = nullptr;

    size_t page_size_;
    Point min_point_;
    Point max_point_;
    uint64_t min_upper_;
                            
    // void Save(){
    //     RTree_->Save(filename_.c_str(), use_full_rtree_);
    // }
    
    // load full rtree from Index_RTree_ & disk file to RTree_
    void Load(){
        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        RTree_->CopyWithoutLeaf(Index_RTree_);
        RTree_->LoadLeafFromFile(filename_.c_str());
    }

    // Load full tree to the target "tree" from Index_RTree_ & disk file
    void LoadTo(RTreeType *tree){
        struct stat buffer;
        if (stat(filename_.c_str(), &buffer) != 0){
            perror(("Loading RTree Error: No file " + string(filename_)).c_str());
            exit(EXIT_FAILURE);
        }
        tree->CopyWithoutLeaf(Index_RTree_);
        tree->LoadLeafFromFile(filename_.c_str());
    }
    
};
}

