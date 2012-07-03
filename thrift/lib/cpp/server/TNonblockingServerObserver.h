#ifndef THRIFT_SERVER_TNONBLOCKINGSERVEROBSERVER_H_
#define THRIFT_SERVER_TNONBLOCKINGSERVEROBSERVER_H_ 1

#include <stdint.h>

namespace apache { namespace thrift { namespace server {

class TNonblockingServerObserver {
 public:

  virtual ~TNonblockingServerObserver() {}

  TNonblockingServerObserver() : sampleRate_(0) {}
  TNonblockingServerObserver(uint32_t sampleRate) : sampleRate_(sampleRate) {}

  class CallTimestamps {
  public:
    uint64_t readBegin;
    uint64_t readEnd;
    uint64_t processBegin;
    uint64_t processEnd;
    uint64_t writeBegin;
    uint64_t writeEnd;

    CallTimestamps() {
      init();
    }

    void init() {
      readBegin = readEnd = 0;
      processBegin = processEnd = 0;
      writeBegin = writeEnd = 0;
    }

  };

  // Notifications for various events on the TNonblockingServer
  virtual void connDropped() = 0;

  virtual void taskKilled() = 0;

  virtual void callCompleted(const CallTimestamps& runtimes) = 0;

  // The observer has to specify a sample rate for callCompleted notifications
  inline uint32_t getSampleRate() const {
    return sampleRate_;
  }

 protected:
  uint32_t sampleRate_;
};

}}} // apache::thrift::server
#endif
