#include <iostream>
#include <fstream>
#include <vector>

namespace rocksdb
{
  enum EventLogType{
    kNoUseEvent,
    kCompactionEvent,
    kFlushEvent,
    kStatisticsEvent,
    kOptionsEvent,
    kDeleteEvent
  };

  class EventLog{
  private:
    time_t event_time_;
    
  public:
    EventLog(){};
    virtual ~EventLog(){};
    void SetEventTime(time_t time){event_time_=time;};
    time_t GetEventTime(){return event_time_;};
    
    virtual bool Parse(std::string log_string){return false;};
    virtual bool Parse(std::vector<std::string> log_string){return false;};
    int event_usec_;
  };
  
  class CompactionEvent : public EventLog
  {
  public:
    CompactionEvent():base_layer_(-1){};
    ~CompactionEvent(){};
    
    bool Parse(std::string log_string);
    
    enum CompactionType{
      kCompactionCreate=0,
      kCompactionFinish=1
    };
    int base_version_id_;
    int base_layer_;
    int l0_file_num_;
    int l1_file_num_;
    int l2_file_num_;
    int l3_file_num_;
    int l4_file_num_;
    int l5_file_num_;
    int l6_file_num_;
    float write_mb_per_sec_;
    float read_mb_per_sec_;
    float input_file_n_mb_;
    float input_file_np1_mb_;
    float output_file_mb_;
    float read_am_;
    float write_am_;
    int records_in_;
    CompactionType compaction_type_;
    
  private:
    std::vector<int> target_file_ids_base_;
    std::vector<int> target_file_ids_high_;
  };

  class FlushEvent : public EventLog
  {
  public:
    FlushEvent():write_bytes_(-1){};
    ~FlushEvent(){};

    bool Parse(std::string log_string);

    enum FlushType{
      kFlushCreate,
      kFlushFinish
    };
    int version_id_;
    FlushType flush_type_;
    long write_bytes_;

  private:
  };

  class StatisticsEvent: public EventLog
  {
  public:
    StatisticsEvent(){};
    ~StatisticsEvent(){};

    bool Parse(std::vector<std::string> log_strings);

    struct compaction_infos{
      char name[256];
      char files[256];
      int size_mb;
      float score;
      float Read_gb;
      float Rn_gb;
      float Rnp1_gb;
      float Write_gb;
      float Wnew_gb;
      float RW_Amp;
      float W_Amp;
      float Rd_mb_per_s;
      float Wr_mb_per_s;
      int Rn_cnt;
      int Rnp1_cnt;
      int Wnp1_cnt;
      int Wnew_cnt;
      int Comp_sec;
      int Comp_cnt;
      float Avg_sec;
      float Stall_sec;
      int Stall_cnt;
      float Avg_ms;
      int RecordIn;
      int RecordDrop;
    };

    struct flush_infos{
      float accumulative;
      float interval;
    };

    struct stalls_secs{
      float level0_slowdown;
      float level0_numfiles;
      float memtable_compaction;
      float leveln_slowdown_soft;
      float leveln_slowdown_hard;
    };

    struct stalls_count{
      int level0_slowdown;
      int level0_numfiles;
      int memtable_compaction;
      int leveln_slowdown_soft;
      int leveln_slowdown_hard;
    };

    struct uptime_secs{
      float total;
      float interval;
    };
    
    struct cumulative_writes{
      int writes;
      int batches;
      float writes_per_batch;
      float gb_user_ingest;
    };

    struct cumulative_wal{
      int writes;
      int syncs;
      float writes_per_sync;
      float gb_written;
    };

    struct interval_writes{
      int writes;
      int batches;
      float writes_per_batch;
      float mb_user_ingest;
    };

    struct interval_wal{
      int writes;
      int syncs;
      float writes_per_sync;
      float mb_written;
    };

    flush_infos flush_infos_;
    stalls_secs stalls_secs_;
    stalls_count stalls_count_;
    uptime_secs uptime_secs_;
    cumulative_writes cumulative_writes_;
    cumulative_wal cumulative_wal_;
    interval_writes interval_writes_;
    interval_wal interval_wal_;
    std::vector<compaction_infos> compaction_infos_vector_;
  };

  class OptionsEvent: public EventLog
  {
  public:
    OptionsEvent(){};
    ~OptionsEvent(){};

    bool Parse(std::vector<std::string> log_strings);

    std::vector<std::string> options_vector_;
  };

  class DeleteEvent: public EventLog
  {
  public:
    DeleteEvent(){};
    ~DeleteEvent(){};

    bool Parse(std::string log_strings);

    char filename_[512];
    int type_;
    long id_;
  };
};//namespace rocksdb

namespace rocksdb
{
  class LOGParser{
  private:
    std::vector<CompactionEvent*> parsed_compaction_event_logs_;
    std::vector<FlushEvent*> parsed_flush_event_logs_;
    std::vector<StatisticsEvent*> parsed_statistics_event_logs_;
    std::vector<DeleteEvent*> parsed_delete_event_logs_;
    std::vector<OptionsEvent*> parsed_options_event_logs_;
    
  public:
    std::vector<CompactionEvent*> getCompactionEvent(){return parsed_compaction_event_logs_;};
    std::vector<FlushEvent*> getFlushEvent(){return parsed_flush_event_logs_;};
    std::vector<OptionsEvent*> getOptionsEvent(){return parsed_options_event_logs_;};
    std::vector<StatisticsEvent*> getStatisticsEvent(){return parsed_statistics_event_logs_;};
    std::vector<DeleteEvent*> getDeleteEvent(){return parsed_delete_event_logs_;};
    LOGParser(){};
    ~LOGParser(){
      for (size_t i = 0; i < parsed_compaction_event_logs_.size(); i++)
	delete parsed_compaction_event_logs_.at(i);
      for (size_t i = 0; i < parsed_flush_event_logs_.size(); i++)
	delete parsed_flush_event_logs_.at(i);
      for (size_t i = 0; i < parsed_options_event_logs_.size(); i++)
	delete parsed_options_event_logs_.at(i);
      for (size_t i = 0; i < parsed_statistics_event_logs_.size(); i++)
	delete parsed_statistics_event_logs_.at(i);
      for (size_t i = 0; i < parsed_delete_event_logs_.size(); i++)
	delete parsed_delete_event_logs_.at(i);
    };

    void Parse(std::string filename);
    bool CheckConcatenateEnd(std::string log_string);
    EventLogType CheckNeedConcatenate(std::string log_string);
    EventLogType GetTypeOfToken(std::string token);
  };
};//namespace rocksdb
