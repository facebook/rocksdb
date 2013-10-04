#include "scribe_logger.h"

namespace rocksdb {

const std::string ScribeLogger::COL_SEPERATOR = "\x1";
const std::string ScribeLogger::DEPLOY_STATS_CATEGORY = "leveldb_deploy_stats";

ScribeLogger::ScribeLogger(const std::string& host, int port,
    int retry_times, uint32_t retry_intervals)
  : host_(host),
    port_(port),
    retry_times_(retry_times),
    retry_intervals_ (retry_intervals) {
  shared_ptr<TSocket> socket(new TSocket(host_, port_));
  shared_ptr<TFramedTransport> framedTransport(new TFramedTransport(socket));
  framedTransport->open();
  shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(framedTransport));
  scribe_client_ = new scribeClient(protocol);
}

void ScribeLogger::Log(const std::string& category,
    const std::string& message) {
  LogEntry entry;
  entry.category = category;
  entry.message = message;

  std::vector<LogEntry> logs;
  logs.push_back(entry);

  logger_mutex_.Lock();
  ResultCode  ret = scribe_client_->Log(logs);
  int retries_left = retry_times_;
  while (ret == TRY_LATER && retries_left > 0) {
    Env::Default()->SleepForMicroseconds(retry_intervals_);
    ret = scribe_client_->Log(logs);
    retries_left--;
  }

  logger_mutex_.Unlock();
}

void ScribeLogger::MakeScribeMessage(std::string& output,
    std::vector<std::string>& cols) {
  int sz = cols.size();
  int i = 0;
  for (; i < sz - 1; i++) {
    std::string& col = cols.at(i);
    output += col;
    output += ScribeLogger::COL_SEPERATOR;
  }
  std::string& col = cols.at(i);
  output+=col;
}

void ScribeLogger::Log_Deploy_Stats(
    const std::string& db_version,
    const std::string& machine_info,
    const std::string& data_dir,
    const uint64_t data_size,
    const uint32_t file_number,
    const std::string& data_size_per_level,
    const std::string& file_number_per_level,
    const int64_t& ts_unix) {
  std::string message;
  std::vector<std::string> cols;
  cols.push_back(db_version);
  cols.push_back(machine_info);
  cols.push_back(data_dir);
  cols.push_back(boost::lexical_cast<std::string>(data_size));
  cols.push_back(boost::lexical_cast<std::string>(file_number));
  cols.push_back(data_size_per_level);
  cols.push_back(file_number_per_level);
  cols.push_back(boost::lexical_cast<std::string>(ts_unix));
  MakeScribeMessage(message, cols);
  return Log(ScribeLogger::DEPLOY_STATS_CATEGORY, message);
}

ScribeLogger::~ScribeLogger(){
  delete scribe_client_;
}

}
