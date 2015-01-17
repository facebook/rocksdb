#include "tools/parse_db_log.h"

namespace rocksdb
{
  bool CompactionEvent::Parse(std::string log_string)
  {
    if (log_string.find("Origin") != std::string::npos){
      struct tm t;
      char thread_id[128];
      int now_sec;
      std::sscanf(log_string.c_str(),
		  "(Original Log Time %04d/%02d/%02d-%02d:%02d:%02d.%06d) %s",
		  &t.tm_year,
		  &t.tm_mon,
		  &t.tm_mday,
		  &t.tm_hour,
		  &t.tm_min,
		  &t.tm_sec,
		  &now_sec,
		  thread_id
		  );
      t.tm_year -= 1900;
      t.tm_mon  -= 1;
      time_t unix_timestamp = mktime(&t);
      this->SetEventTime(unix_timestamp);
      this->event_usec_ = now_sec;
      log_string = log_string.substr(log_string.find(")") + 1);
    }
      
    if (log_string.find("Compaction start") != std::string::npos){
      compaction_type_ = kCompactionCreate;
      log_string = log_string.substr(log_string.find(":") + 1);
      char file_input_ids_origin[4096], file_input_ids_high[4096];
      std::sscanf(log_string.c_str(), " Base version %d Base level %d, seek compaction:0, inputs: [%s], [%s]",
		  &base_version_id_, &base_layer_, file_input_ids_origin, file_input_ids_high);
      return true;
    }else if(log_string.find("Compacted to") != std::string::npos){
      compaction_type_ = kCompactionFinish;
      log_string = log_string.substr(log_string.find(":") + 1);
      int n_file_nums, np1_file_nums, output_file_nums;
      std::sscanf(log_string.c_str(), " Base version %d files[%d %d %d %d %d %d %d], MB/sec: %f rd, %f wr, level %d, files in(%d, %d) out(%d) MB in(%f, %f) out(%f), read-write-amplify(%f) write-amplify(%f) OK, records in: %d, records dropped: 0",
		  &base_version_id_, &l0_file_num_, &l1_file_num_, &l2_file_num_, &l3_file_num_, &l4_file_num_, &l5_file_num_, &l6_file_num_, &read_mb_per_sec_, &write_mb_per_sec_,
		  &base_layer_, &n_file_nums, &np1_file_nums, &output_file_nums, &input_file_n_mb_, &input_file_np1_mb_, &output_file_mb_, &read_am_, &write_am_, &records_in_
		  );
      return true;
    };
    return false;
  }

  bool FlushEvent::Parse(std::string log_string)
  {
    log_string = log_string.substr(log_string.find("]") + 2);
    if(log_string.find("Level-0 flush") != std::string::npos){
      if(log_string.find("started") != std::string::npos){
	flush_type_ = kFlushCreate;
	std::sscanf(log_string.c_str(), "Level-0 flush table #%d", &version_id_);
	return true;
      }
      if(log_string.find("bytes") != std::string::npos){
	flush_type_ = kFlushFinish;
	std::sscanf(log_string.c_str(), " Level-0 flush table #%d: %ld bytes OK", &version_id_, &write_bytes_);
	return true;
      }
    }
    return false;
  };

  bool StatisticsEvent::Parse(std::vector<std::string> log_strings)
  {

    for(size_t i = 0; i < log_strings.size()-1; i++){
      std::string log_string = log_strings[i];
      //Skip Unneeded Strings
      if(i < 5)
	continue;

      //For Other Check the first words
      std::string tokens = log_string.substr(0, log_string.find(" "));
      if(!tokens.compare(std::string("Flush(GB):"))){
	std::sscanf(log_string.c_str(), "Flush(GB): accumulative %f, interval %f",
		    &flush_infos_.accumulative, &flush_infos_.interval);
      }else if(!tokens.compare("Stalls(secs):")){
	std::sscanf(log_string.c_str(), "Stalls(secs): %f level0_slowdown, %f level0_numfiles, %f memtable_compaction, %f leveln_slowdown_soft, %f leveln_slowdown_hard",
		    &stalls_secs_.level0_slowdown,&stalls_secs_.level0_numfiles,&stalls_secs_.memtable_compaction,&stalls_secs_.leveln_slowdown_soft,&stalls_secs_.leveln_slowdown_hard);
      }else if(!tokens.compare("Stalls(count):")){
	std::sscanf(log_string.c_str(), "Stalls(secs): %d level0_slowdown, %d level0_numfiles, %d memtable_compaction, %d leveln_slowdown_soft, %d leveln_slowdown_hard",
		    &stalls_count_.level0_slowdown,&stalls_count_.level0_numfiles,&stalls_count_.memtable_compaction,&stalls_count_.leveln_slowdown_soft,&stalls_count_.leveln_slowdown_hard);
      }else if(!tokens.compare("**")){
      }else if(!tokens.compare("Uptime(secs):")){
	std::sscanf(log_string.c_str(), "Uptime(secs): %f total, %f interval",
		    &uptime_secs_.total, &uptime_secs_.interval);
      }else if(!tokens.compare("Cumulative")){
	if(log_string.find("writes:") != std::string::npos){
	  std::sscanf(log_string.c_str(), "Cumulative writes: %d writes, %d batches, %f writes per batch, %f GB user ingest",
		      &cumulative_writes_.writes, &cumulative_writes_.batches, &cumulative_writes_.writes_per_batch, &cumulative_writes_.gb_user_ingest);
	}else if(log_string.find("WAL:") != std::string::npos){
	  std::sscanf(log_string.c_str(), "Cumulative WAL: %d writes, %d syncs, %f writes per sync, %f GB written",
		      &cumulative_wal_.writes, &cumulative_wal_.syncs, &cumulative_wal_.writes_per_sync, &cumulative_wal_.gb_written);
	}
      }else if(!tokens.compare("Interval")){
	if(log_string.find("writes:") != std::string::npos){
	  std::sscanf(log_string.c_str(), "Interval writes: %d writes, %d batches, %f writes per batch, %f MB user ingest",
		      &interval_writes_.writes, &interval_writes_.batches, &interval_writes_.writes_per_batch, &interval_writes_.mb_user_ingest);
	}else if(log_string.find("WAL:") != std::string::npos){
	  std::sscanf(log_string.c_str(), "Interval WAL: %d writes, %d syncs, %f writes per sync, %f MB written",
		      &interval_wal_.writes, &interval_wal_.syncs, &interval_wal_.writes_per_sync, &interval_wal_.mb_written);
	}
      }else{
	if(log_string.length()){
	  struct compaction_infos tmp_compaction_infos;
	  std::sscanf(log_string.c_str(), "%s %s %d %f %f %f %f %f %f %f %f %f %f %d %d %d %d %d %d %f %f %d %f %d %d",
		      tmp_compaction_infos.name,
		      tmp_compaction_infos.files,
		      &tmp_compaction_infos.size_mb,
		      &tmp_compaction_infos.score,
		      &tmp_compaction_infos.Read_gb,
		      &tmp_compaction_infos.Rn_gb,
		      &tmp_compaction_infos.Rnp1_gb,
		      &tmp_compaction_infos.Write_gb,
		      &tmp_compaction_infos.Wnew_gb,
		      &tmp_compaction_infos.RW_Amp,
		      &tmp_compaction_infos.W_Amp,
		      &tmp_compaction_infos.Rd_mb_per_s,
		      &tmp_compaction_infos.Wr_mb_per_s,
		      &tmp_compaction_infos.Rn_cnt,
		      &tmp_compaction_infos.Rnp1_cnt,
		      &tmp_compaction_infos.Wnp1_cnt,
		      &tmp_compaction_infos.Wnew_cnt,
		      &tmp_compaction_infos.Comp_sec,
		      &tmp_compaction_infos.Comp_cnt,
		      &tmp_compaction_infos.Avg_ms,
		      &tmp_compaction_infos.Stall_sec,
		      &tmp_compaction_infos.Stall_cnt,
		      &tmp_compaction_infos.Avg_ms,
		      &tmp_compaction_infos.RecordIn,
		      &tmp_compaction_infos.RecordDrop);
	  compaction_infos_vector_.push_back(tmp_compaction_infos);
	}
      }
    }
    return true;
  }

  bool OptionsEvent::Parse(std::vector<std::string> log_strings)
  {
    for(size_t i = 0; i < log_strings.size() - 1; i++){
      std::string log_string = log_strings[i];

      if(i < 3)
	continue;

      std::string tokens = log_string.substr(0, log_string.find(" "));
	
      struct tm t;
      char thread_id[128];
      int now_sec;
      char prev_term[150], next_term[1024];
      int ret = std::sscanf(log_string.c_str(),
			    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s %s %s",
			    &t.tm_year,
			    &t.tm_mon,
			    &t.tm_mday,
			    &t.tm_hour,
			    &t.tm_min,
			    &t.tm_sec,
			    &now_sec,
			    thread_id,
			    prev_term,
			    next_term
			    );
      if(ret != 0){
	options_vector_.push_back(std::string(next_term));
      }
    }
    return true;
  }

  bool DeleteEvent::Parse(std::string log_string)
  {
    std::sscanf(log_string.c_str(),
		"Delete %s type=%d #%ld",
		filename_,
		&type_,
		&id_
		);
    return true;
  }
};//namespace rocksdb

namespace rocksdb
{
  void LOGParser::Parse(std::string filename)
  {
    std::cout << "Parse start" << std::endl;

    std::ifstream LOGfile(filename);
    std::string line;
    int counter = 0;
    std::vector<std::string> string_concatenate_buffer;
    bool concatenate_span = false;
    EventLogType concatenate_log_type = kNoUseEvent;
    while (std::getline(LOGfile, line))
      {
	struct tm t;
	char thread_id[128];
	int now_sec;
	int ret = std::sscanf(line.c_str(),
			      "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s",
			      &t.tm_year,
			      &t.tm_mon,
			      &t.tm_mday,
			      &t.tm_hour,
			      &t.tm_min,
			      &t.tm_sec,
			      &now_sec,
			      thread_id
			      );

	//If need Concatenate
	if(concatenate_span){
	  string_concatenate_buffer.push_back(line);
	  if(CheckConcatenateEnd(line)){
	    concatenate_span = false;
	      
	    if(concatenate_log_type == kStatisticsEvent){
	      StatisticsEvent* event_log = new StatisticsEvent();
	      if(event_log->Parse(string_concatenate_buffer))
		parsed_statistics_event_logs_.push_back(event_log);
	    }else if(concatenate_log_type == kOptionsEvent){
	      OptionsEvent* event_log = new OptionsEvent();
	      if(event_log->Parse(string_concatenate_buffer))
		parsed_options_event_logs_.push_back(event_log);
	    }
	    concatenate_log_type = kNoUseEvent;
	    string_concatenate_buffer.clear();
	    continue;
	  }
	  //Continue to next line
	  else{
	    continue;
	  }
	}

	if(ret != 0){
	  t.tm_year -= 1900;
	  t.tm_mon  -= 1;
	  time_t unix_timestamp = mktime(&t);
	    
	  std::string thread_string(thread_id);
	  int cutoff_offset = 28 + (int)thread_string.length();
	  std::string log_string = line.substr(cutoff_offset).c_str();

	  //Check concatenate timing string
	  EventLogType need_concatenate = CheckNeedConcatenate(log_string);
	  if(need_concatenate != kNoUseEvent){
	    concatenate_span = true;
	    string_concatenate_buffer.push_back(log_string);
	    concatenate_log_type = need_concatenate;
	    continue;
	  }

	  //Not need  to concatenate
	  EventLogType log_type = GetTypeOfToken(log_string);
	  if ( log_type == kCompactionEvent){
	    CompactionEvent* event_log = new CompactionEvent();
	    event_log->SetEventTime(unix_timestamp);
	    event_log->event_usec_ = now_sec;
	    if(event_log->Parse(log_string))
	      parsed_compaction_event_logs_.push_back(event_log);
	  }else if( log_type == kFlushEvent ){
	    FlushEvent* event_log = new FlushEvent();
	    event_log->SetEventTime(unix_timestamp);
	    event_log->event_usec_ = now_sec;
	    if(event_log->Parse(log_string))
	      parsed_flush_event_logs_.push_back(event_log);
	  }else if( log_type == kDeleteEvent ){
	    DeleteEvent* event_log = new DeleteEvent();
	    event_log->SetEventTime(unix_timestamp);
	    event_log->event_usec_ = now_sec;
	    if(event_log->Parse(log_string))
	      parsed_delete_event_logs_.push_back(event_log);
	  }
	}
	counter++;
      }
    std::cout << "Total line : " << counter <<  " Parse End" << std::endl; 
  }

  bool LOGParser::CheckConcatenateEnd(std::string log_string){
    if(log_string.find("DUMPING STATS END") != std::string::npos){
      return true;
    }else if(log_string.find("Recovered from") != std::string::npos){
      return true;
    }
    return false;
  }

  EventLogType LOGParser::CheckNeedConcatenate(std::string log_string){
    if(log_string.find("DUMPING STATS") != std::string::npos){
      return kStatisticsEvent;
    }else if(log_string.find("DB SUMMARY") != std::string::npos){
      return kOptionsEvent;
    }
    return kNoUseEvent;
  }

  EventLogType LOGParser::GetTypeOfToken(std::string token){
    if (token.find(" Compact") != std::string::npos)
      return kCompactionEvent;
    else if(token.find("Level-0") != std::string::npos)
      return kFlushEvent;
    else if(token.find("Delete") != std::string::npos)
      return kDeleteEvent;
    else
      return kNoUseEvent;
  };
};//namespace rocksdb
