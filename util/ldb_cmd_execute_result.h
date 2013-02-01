#ifndef LEVELDB_UTIL_LDB_CMD_EXECUTE_RESULT_H_
#define LEVELDB_UTIL_LDB_CMD_EXECUTE_RESULT_H_

namespace leveldb {

class LDBCommandExecuteResult {
public:
  enum State {
    EXEC_NOT_STARTED = 0, EXEC_SUCCEED = 1, EXEC_FAILED = 2,
  };

  LDBCommandExecuteResult() {
    state_ = EXEC_NOT_STARTED;
    message_ = "";
  }

  LDBCommandExecuteResult(State state, std::string& msg) {
    state_ = state;
    message_ = msg;
  }

  std::string ToString() {
    std::string ret;
    switch (state_) {
    case EXEC_SUCCEED:
      break;
    case EXEC_FAILED:
      ret.append("Failed: ");
      break;
    case EXEC_NOT_STARTED:
      ret.append("Not started: ");
    }
    if (!message_.empty()) {
      ret.append(message_);
    }
    return ret;
  }

  void Reset() {
    state_ = EXEC_NOT_STARTED;
    message_ = "";
  }

  bool IsSucceed() {
    return state_ == EXEC_SUCCEED;
  }

  bool IsNotStarted() {
    return state_ == EXEC_NOT_STARTED;
  }

  bool IsFailed() {
    return state_ == EXEC_FAILED;
  }

  static LDBCommandExecuteResult SUCCEED(std::string msg) {
    return LDBCommandExecuteResult(EXEC_SUCCEED, msg);
  }

  static LDBCommandExecuteResult FAILED(std::string msg) {
    return LDBCommandExecuteResult(EXEC_FAILED, msg);
  }

private:
  State state_;
  std::string message_;

  bool operator==(const LDBCommandExecuteResult&);
  bool operator!=(const LDBCommandExecuteResult&);
};

}

#endif
