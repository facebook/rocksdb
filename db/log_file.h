// Copyright 2008-present Facebook. All Rights Reserved.

#ifndef STORAGE_LEVELDB_DB_LOG_FILE_H_
#define STORAGE_LEVELDB_DB_LOG_FILE_H_

namespace leveldb {

enum  WalFileType {
  kArchivedLogFile = 0,
  kAliveLogFile = 1
} ;

class LogFile {

 public:
  uint64_t logNumber;
  WalFileType type;

  LogFile(uint64_t logNum,WalFileType logType) :
    logNumber(logNum),
    type(logType) {}

  LogFile(const LogFile& that) {
   logNumber = that.logNumber;
   type = that.type;
  }

  bool operator < (const LogFile& that) const {
    return logNumber < that.logNumber;
  }

  std::string ToString() const {
    char response[100];
    const char* typeOfLog;
    if (type == kAliveLogFile) {
      typeOfLog = "Alive Log";
    } else {
      typeOfLog = "Archived Log";
    }
    sprintf(response,
            "LogNumber  : %ld LogType : %s",
            logNumber,
            typeOfLog);
    return std::string(response);
  }
};
}  //  namespace leveldb
#endif  // STORAGE_LEVELDB_DB_LOG_FILE_H_
