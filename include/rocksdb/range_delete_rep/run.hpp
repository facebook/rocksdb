//
//  run.hpp
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

#ifndef RUN_H
#define RUN_H
#include <stdio.h>
#include <cstdint>
#include <vector>
using namespace std;


template <typename K, typename V>
struct KVPair {
    
    K key;
    V value;
    
    // bool tombstone;
    
    
    
    bool operator==(KVPair kv) const {
        return (kv.key == key && kv.value == value);
    }
    bool operator!=(KVPair kv) const {
        return (kv.key != key != kv.value != value);
    }
    
    bool operator<(KVPair kv) const{
        return key < kv.key;
    }
    
    bool operator>(KVPair kv) const{
        return key > kv.key;
    }
    
    
};



    template <class K, class V>
    class Run {
        
    public:
        virtual K get_min() = 0;
        virtual K get_max() = 0;
        virtual void insert_key(const K &key, const V &value) = 0;
        virtual void delete_key(const K &key) = 0;
        virtual V lookup(const K &key, bool &found) = 0;
        virtual unsigned long long num_elements() = 0;
        virtual void set_size(const unsigned long size) = 0;
        virtual vector<KVPair<K,V>> get_all() = 0;
        virtual vector<KVPair<K,V>> get_all_in_range(const K &key1, const K &key2) = 0;
        virtual ~Run() { }
    };
    
    


#endif /* run_h */

