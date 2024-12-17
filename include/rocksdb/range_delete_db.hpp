#pragma once

#include "rocksdb/range_delete_db_util.hpp"

namespace ROCKSDB_NAMESPACE {

struct range_delete_db_opt
{
  bool enable_global_rd = false;
  std::string db_path = "/tmp/rdtest";
  rocksdb::Options db_conf;
  rangedelete_filter::rd_filter_opt filter_conf;
  rangedelete_rep::rd_rep_opt rep_conf;
};

class RangeDeleteDB {
  public:
    RangeDeleteDB(rocksdb::range_delete_db_opt & options) 
      : enable_global_rd_(options.enable_global_rd){
      rocksdb::Status s = rocksdb::DB::Open(options.db_conf, options.db_path, &db_);
      if (!s.ok()) {
        std::cerr << "Failed to open db: " << s.ToString() << std::endl;
      }
      rd_filter_ = new rangedelete_filter::BucketWrapper();
      rd_filter_->Construct(options.filter_conf);
      rd_rep_ = new rangedelete_rep::LSM<rangedelete_rep::Rectangle, bool>(options.rep_conf);
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
    rocksdb::Result RangeDelete(const uint64_t &key_left, const string &key_left_str, const uint64_t &key_right, const string &key_right_str);

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
      db_->Close();
    }

    /* [left, right] */
    // bool query(const uint64_t left, const uint64_t right);

    // auto serialize() const -> std::pair<uint8_t *, size_t>;
    // static auto deserialize(uint8_t *ser) -> Bucket *;

    // auto size() const -> size_t;

  private:
    bool enable_global_rd_;
    rocksdb::DB* db_;
    rangedelete_filter::RangeDeleteFliterWrapper * rd_filter_ = nullptr;
    rangedelete_rep::LSM<rangedelete_rep::Rectangle, bool> * rd_rep_ = nullptr;   // <key_type, value_type(not used)>
    bool use_stat = false;
    RDDBStat * stat = nullptr;

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
  bool filter_res = rd_filter_->Query(key);
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
      uint64_t node_cnt = 0;
      uint64_t leaf_cnt = 0;
      stat->rd_rep.Start();

      rep_res = rd_rep_->QueryRect(query_rect, true, node_cnt, leaf_cnt);

      stat->rd_rep.Pause();
      stat->rd_rep.AddCount();
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
    auto s = db_->Get(rocksdb::ReadOptions(), key_str, &value);
    if (!s.ok() && !s.IsNotFound()) {
      std::cerr << "Failed to get key " << key << std::endl;
      exit(1);
    }
    if(use_stat){ 
      stat->op_read.Pause(); 
      stat->op_read.AddCount();
    }
    return ConvertStaToRes(s);
  }
  else {
    uint64_t seq = 0;
    rocksdb::Status s = db_->GetWithSeq(rocksdb::ReadOptions(), key_str, &value, seq);

    rocksdb::Result result = rocksdb::Result::kNotFound;

    if (!s.ok() && !s.IsNotFound()) {
      std::cerr << "Failed to get key " << key << std::endl;
      exit(1);
    } else if (s.ok()){
      // std::cout << "  key : " << key << " seq: " << seq  << " status ok: " << s.ok() << " ststus notfound :" << s.IsNotFound() << std::endl;
      result = IsRangeDeleted(key, seq);
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


rocksdb::Result RangeDeleteDB::RangeDelete(const uint64_t &key_left, const string &key_left_str, const uint64_t &key_right, const string &key_right_str){
  if(use_stat){ stat->op_rdelete.Start(); }

  if (!enable_global_rd_){
    rocksdb::Status s = db_->DeleteRange(rocksdb::WriteOptions(), key_left_str, key_right_str);
    if(use_stat){ 
      stat->op_rdelete.Pause(); 
      stat->op_rdelete.AddCount();
    }
    return ConvertStaToRes(s);
  }
  else{
    uint64_t seq_curr = db_->GetLatestSequenceNumber();
    rd_filter_->InsertRange(key_left, key_right);
    // insert Rect
    rangedelete_rep::Rectangle in_rect(key_left, 0, key_right, seq_curr);

    // std::cout << "   seq: " << seq_curr << std::endl;

    rd_rep_->InsertRect(in_rect);
    if(use_stat){ 
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

}  // namespace rocksdb
