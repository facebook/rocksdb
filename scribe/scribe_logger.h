#ifndef SCRIBE_LOGGER_H_
#define SCRIBE_LOGGER_H_

#include "scribe/if/gen-cpp/scribe.h"
#include "scribe/if/gen-cpp/scribe_types.h"
#include "thrift/lib/cpp/protocol/TProtocol.h"
#include "thrift/lib/cpp/transport/TSocket.h"
#include "thrift/lib/cpp/protocol/TBinaryProtocol.h"
#include "thrift/lib/cpp/transport/TBufferTransports.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "util/stats_logger.h"

#include "boost/lexical_cast.hpp"

using namespace Tleveldb;
using Tleveldb::scribeClient;

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using boost::shared_ptr;


using namespace  ::Tleveldb;

namespace leveldb {

class ScribeLogger : public StatsLogger{
private:
  std::string host_;
  int port_;
  int batch_size_;

  scribeClient* scribe_client_;
  std::vector<LogEntry> logs_;
  port::Mutex logger_mutex_;

  int retry_times_;
  uint32_t  retry_intervals_;

  void MakeScribeMessage(std::string& output, std::vector<std::string>& cols);

public:

  static const std::string COL_SEPERATOR;
  static const std::string DEPLOY_STATS_CATEGORY;

  ScribeLogger(const std::string& host, int port,
      int retry_times=3, uint32_t retry_intervals=1000000,
      int batch_size=1);
  virtual ~ScribeLogger();

  virtual void Log(const std::string& category, const std::string& message);

  virtual void Log_Deploy_Stats(
      const std::string& db_version,
      const std::string& machine_info,
      const std::string& data_dir,
      const uint64_t data_size,
      const uint32_t file_number,
      const std::string& data_size_per_level,
      const std::string& file_number_per_level,
      const int64_t& ts_unix
      );

};
}

#endif /* SCRIBE_LOGGER_H_ */
