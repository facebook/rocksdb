#pragma once

#include "rocksdb/range_delete_db_util.hpp"

namespace ROCKSDB_NAMESPACE {

struct range_delete_db_opt
{
  bool enable_global_rd = false;
  rocksdb::Method method = rocksdb::Method::mNone;
  bool enable_rdfilter = true;
  bool enable_crosscheck = false;
  std::string db_path = "/tmp/rdtest";
  rocksdb::Options db_conf;
  rangedelete_filter::rd_filter_opt filter_conf;
  rangedelete_rep::rd_rep_opt rep_conf;
};

class RangeDeleteDB {
  public:
    RangeDeleteDB(rocksdb::range_delete_db_opt & options) 
      : enable_global_rd_(options.enable_global_rd),
      method_(options.method),
      enable_rdfilter_(options.enable_rdfilter),
      enable_crosscheck_(options.enable_crosscheck){
      rocksdb::Status s = rocksdb::DB::Open(options.db_conf, options.db_path, &db_);
      if (!s.ok()) {
        std::cerr << "Failed to open db: " << s.ToString() << std::endl;
      }
      stat = new RDDBStat();
      rd_filter_ = new rangedelete_filter::BucketWrapper();
      rd_filter_->Construct(options.filter_conf);
      rd_rep_ = new rangedelete_rep::LSM<rangedelete_rep::Rectangle, bool>(options.rep_conf);
      db_->SetRangeDeleteRep(rd_rep_);
      rangedelete_filter::BucketWrapper* filter_ = new rangedelete_filter::BucketWrapper();
      // db_->SetRangeDeleteFilter(filter_);
    }

    ~RangeDeleteDB() {
      delete db_;
      delete rd_filter_;
      delete rd_rep_;
    }

    rocksdb::Result EntryInsert(const uint64_t &key, const string &key_str, const string &value);

    rocksdb::Result PointQuery(const uint64_t &key, const string &key_str, string &value);

    rocksdb::Result PointDelete(const uint64_t &key, const string &key_str);

    //delete the key range [left, right]
    rocksdb::Result RangeDelete(const uint64_t &key_left, const string &key_left_str, const uint64_t &key_right, const string &key_right_str, const OperationGenerator* key_gen);

    //query key range [left, right)
    rocksdb::Result RangeLookup(const uint64_t &key_start, const string &key_start_str, const size_t &len);

    void StartStatic(){
      stat = new RDDBStat;
      stat->enable_global_rd = enable_global_rd_;
      use_stat = true;
    }

    void PrintStatic(){
      stat->Print();
    }
    
    void close(){
      db_->ReSetRangeDeleteRep();
      // db_->ReSetRangeDeleteFilter();
      db_->Close();
    }

  private:
    bool enable_global_rd_;
    rocksdb::Method method_;
    bool enable_crosscheck_;
    bool enable_rdfilter_;
    rocksdb::DB* db_;
    rangedelete_filter::RangeDeleteFliterWrapper * rd_filter_ = nullptr;
    rangedelete_rep::LSM<rangedelete_rep::Rectangle, bool> * rd_rep_ = nullptr;   // <key_type, value_type(not used)>
    bool use_stat = false;
    // RDDBStat * stat = nullptr;
    RDDBStat * stat;

    rocksdb::Result IsRangeDeleted(const uint64_t &key, const uint64_t &sequence) const;
};

rocksdb::Result RangeDeleteDB::EntryInsert(const uint64_t &key, const string &key_str, const string &value){
  if(use_stat){ stat->op_write.Start(); }

  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), key_str, value);
  if (!s.ok()) {
    std::cerr << "Failed to put key " << key << std::endl;
    exit(1);
  }
  if(use_stat){ 
    stat->op_write.Pause(); 
    stat->op_write.AddCount();
  }
  return ConvertStaToRes(s);
}

rocksdb::Result RangeDeleteDB::IsRangeDeleted(const uint64_t &key, const uint64_t &sequence) const{
  if(use_stat){ stat->rd_filter.Start(); }
  bool filter_res = true;
  if(enable_rdfilter_){
    filter_res = rd_filter_->Query(key);
  }
  if(use_stat){ 
    stat->rd_filter.Pause(); 
    stat->rd_filter.AddCount();
  }

  if(!filter_res){
    stat->filter_tn += 1;
    // std::cout << "   Filter: True Negative " << std::endl; 
    return rocksdb::Result::kNotRangeDeleted;
  } else {
    // rangedelete_rep::Rectangle query_rect(key, sequence, key, sequence); 
    rangedelete_rep::Rectangle query_rect(key, sequence, key, sequence); 
    bool rep_res;
    if(use_stat){
      uint64_t rtree_cnt = 0;
      uint64_t node_cnt = 0;
      uint64_t leaf_cnt = 0;
      stat->rd_rep.Start();

      rep_res = rd_rep_->QueryRect(query_rect, true, rtree_cnt, node_cnt, leaf_cnt);

      stat->rd_rep.Pause();
      stat->rd_rep.AddCount();
      stat->rtree_cnt.emplace_back(rtree_cnt);
      stat->rtree_node_cnt.emplace_back(node_cnt);
      stat->rtree_leaf_cnt.emplace_back(leaf_cnt);
    } else {
      rep_res = rd_rep_->QueryRect(query_rect, true);
    }

    if (rep_res){
      stat->filter_tp += 1;
      // std::cout << "   Filter: True positive " << std::endl;
      return rocksdb::Result::kRDRep;
    } else {
      stat->filter_fp += 1;
      // std::cout << "   Filter: False positive" << std::endl;
      return rocksdb::Result::kNotRangeDeleted;
    }
  }
  return rocksdb::Result::kMaxCode;
}

rocksdb::Result RangeDeleteDB::PointQuery(const uint64_t &key, const string &key_str, string &value){
  if(use_stat){ stat->op_read.Start(); }

  if (!enable_global_rd_){
    rocksdb::ReadOptions read_options;
    read_options.ignore_range_deletions = false;
    // read_options.ignore_range_deletions = true;
    auto s = db_->Get(read_options, key_str, &value);
    if (!s.ok() && !s.IsNotFound()) {
      std::cerr << "Failed to get key " << key << std::endl;
      exit(1);
    }
    if(use_stat){ 
      stat->op_read.Pause(); 
      stat->op_read.AddCount();
      if (s.IsNotFound()){
        stat->read_notfound += 1;
      }
    }
    return ConvertStaToRes(s);
  }
  else {
    rocksdb::Result result = rocksdb::Result::kNotFound;
    if(enable_crosscheck_){
      rocksdb::ReadOptions read_options;
      read_options.use_cross_check = true;
      auto s = db_->Get(read_options, key_str, &value);
      if (!s.ok() && !s.IsNotFound()) {
        std::cerr << "Failed to get key " << key << std::endl;
        exit(1);
      }
      if(use_stat){ 
        if (s.IsNotFound()){
          stat->read_notfound += 1;
        }
      }
      result = ConvertStaToRes(s);
    } else {
      uint64_t seq = 0;
      rocksdb::Status s = db_->GetWithSeq(rocksdb::ReadOptions(), key_str, &value, seq);
      if (!s.ok() && !s.IsNotFound()) {
        std::cerr << "Failed to get key " << key << std::endl;
        exit(1);
      } else if (s.ok()){
        result = IsRangeDeleted(key, seq);
      }
      if(use_stat){ 
        if (s.IsNotFound()){
          stat->read_notfound += 1;
        }
      }
    }
    if(use_stat){
      stat->op_read.Pause(); 
      stat->op_read.AddCount();
    }
    return result;
  }
}


rocksdb::Result RangeDeleteDB::PointDelete(const uint64_t &key, const string &key_str){
  rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), key_str);
  return ConvertStaToRes(s);
}


rocksdb::Result RangeDeleteDB::RangeDelete(const uint64_t &key_left, const string &key_left_str, const uint64_t &key_right, const string &key_right_str, 
                                            const OperationGenerator* key_gen){
  if(use_stat){ stat->op_rdelete.Start(); }

  if (method_ == rocksdb::Method::mDefault){
    rocksdb::Status s = db_->DeleteRange(rocksdb::WriteOptions(), key_left_str, key_right_str);
    if(use_stat){ 
      stat->op_rdelete.Pause(); 
      stat->op_rdelete.AddCount();
    }
    return ConvertStaToRes(s);
  }
  else if (method_ == rocksdb::Method::mDecomposition){
    rocksdb::Status s = rocksdb::Status::OK();
    std::string k_str;
    for (uint64_t k = key_left; k <= key_right; k++)
    {
      k_str = key_gen->ToKeyString(k);
      s = db_->Delete(rocksdb::WriteOptions(), k_str);
      if (!s.ok()) {
        std::cout << "Failed to delete key " << k << std::endl;
        // std::cerr << "Failed to delete key " << k << std::endl;
        // exit(1);
      }
    }
    if(use_stat){ 
      stat->op_rdelete.Pause(); 
      stat->op_rdelete.AddCount();
    }
    return ConvertStaToRes(s);
  }
  else if (method_ == rocksdb::Method::mScanAndDelete){
    //////////////////////////////
    rocksdb::Status s = rocksdb::Status::OK();
    auto it = db_->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(rocksdb::Slice(key_left_str)); it->Valid() && std::stoull(it->key().ToString()) <= key_right; it->Next())
    {
      db_->Delete(rocksdb::WriteOptions(), it->key());
    }
    delete it;
    //////////////////////////////
    // rocksdb::Status s = rocksdb::Status::OK();
    // std::string v;
    // std::string k_str;
    // for (uint64_t k = key_left; k <= key_right; k++)
    // {
    //   k_str = key_gen->ToKeyString(k);
    //   s = db_->Get(rocksdb::ReadOptions(), k_str, &v);
    //   if (s.ok()) {
    //     s = db_->Delete(rocksdb::WriteOptions(), k_str);
    //     if (!s.ok()) {
    //       std::cerr << "Failed to delete key " << k << std::endl;
    //       exit(1);
    //     }
    //   }
    // }
    //////////////////////////////
    if(use_stat){
      stat->op_rdelete.Pause(); 
      stat->op_rdelete.AddCount();
    }
    return ConvertStaToRes(s);
  }
  else if(method_ == rocksdb::Method::mGlobalRangeDelete){
    // // check garbage collection
    // uint64_t sequence = 0;
    // bool gc_trigger = false;
    // db_->GetGCInfo(gc_trigger, sequence);
    // if(gc_trigger){
    //   // garbage collection
    //   size_t gcsz = sizeof(stat->gc_times);
    //   RDTimer gc_timer;
    //   gc_timer.Start();
    //   ExcuteGarbageCollection(sequence);
    //   gc_timer.Pause();
    //   stat->gc_times.emplace_back(gc_timer.duration);
    // }
    // update range filter
    if(use_stat){ stat->op_rdelete_filter.Start(); }

    rd_filter_->InsertRange(key_left, key_right);
    
    if(use_stat){ stat->op_rdelete_filter.Pause(); }

    uint64_t seq_curr = db_->GetLatestSequenceNumber();
    rangedelete_rep::Rectangle in_rect(key_left, rd_rep_->GetBase(), key_right, seq_curr);

    if(use_stat){ stat->op_rdelete_rep.Start(); }

    rd_rep_->InsertRect(in_rect);
    
    if(use_stat){ 
      stat->op_rdelete_rep.Pause();
      stat->op_rdelete.Pause(); 
      stat->op_rdelete.AddCount();
    }

    return rocksdb::Result::kOk;
  }
}

rocksdb::Result RangeDeleteDB::RangeLookup(const uint64_t &key_start, const string &key_start_str, const size_t &len){
  if(use_stat){ stat->op_rread.Start(); }

  auto it = db_->NewIterator(rocksdb::ReadOptions());
  it->Seek(rocksdb::Slice(key_start_str));
  if (!it->Valid()) {
    std::cerr << "Failed to seek key " << key_start << std::endl;
    exit(1);
  }
  size_t j = 0;
  while(j < len){
    if (!it->Valid()) {
      break;
    }
    uint64_t seq = 0;
    it->GetCurrentSequence(&seq);
    uint64_t key = std::stoull(it->key().ToString());
    j += (enable_global_rd_ ? (IsRangeDeleted(key, seq) == rocksdb::Result::kNotRangeDeleted) : 1);
    
    // std::cout << "     " << it->key().ToString() << std::endl;

    it->Next();
  }
  delete it;
  if(use_stat){ 
    stat->op_rread.Pause(); 
    stat->op_rread.AddCount();
  }
  return rocksdb::Result::kOk;
}

// void RangeDeleteDB::ExcuteGarbageCollection(const uint64_t & sequence){
//   rd_rep_->ExcuteGarbageCollection(sequence);
//   rd_rep_->UpdateBase(sequence);
//   ResetGarbageCollection();
// }

// void RangeDeleteDB::ResetGarbageCollection(){
//   db_->ResetGCInfo();
// }

}  // namespace rocksdb
