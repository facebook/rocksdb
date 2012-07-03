/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef THRIFT_TEST_LOADGEN_WORKER_H_
#define THRIFT_TEST_LOADGEN_WORKER_H_ 1

#include "thrift/lib/cpp/test/loadgen/WorkerIf.h"

#include "thrift/lib/cpp/test/loadgen/IntervalTimer.h"
#include "thrift/lib/cpp/test/loadgen/LoadConfig.h"
#include "thrift/lib/cpp/test/loadgen/ScoreBoard.h"
#include "thrift/lib/cpp/concurrency/Util.h"
#include "thrift/lib/cpp/TLogging.h"

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace loadgen {

/**
 * Main Worker implementation
 *
 * If you are implementing a new load generator, you should define your own
 * subclass of Worker, and implement the createConnection() and
 * performOperation() methods.
 *
 * This is templatized on the client type.
 *
 * The Config type is also templatized for convenience.  This allows
 * subclasses to store their own more-specific configuration type that derives
 * from LoadConfig.
 */
template <typename ClientT, typename ConfigT = LoadConfig>
class Worker : public WorkerIf, public boost::noncopyable {
 public:
  typedef ClientT ClientType;
  typedef ConfigT ConfigType;

  enum ErrorAction {
    EA_CONTINUE,
    EA_NEXT_CONNECTION,
    EA_DROP_THREAD,
    EA_ABORT
  };

  Worker()
    : id_(-1)
    , alive_(false)
    , intervalTimer_(NULL)
    , config_()
    , scoreboard_() {}

  /**
   * Initialize the Worker.
   *
   * This is separate from the constructor so that developers writing new
   * Worker implementations don't have to pass through additional constructor
   * arguments.  If the subclass doesn't need any special initialization, they
   * can just use the default constructor.
   *
   * If a Worker implementation does need to perform additional implementation-
   * specific initialization after the config object has been set, it can
   * override init().
   */
  void init(int id,
            const boost::shared_ptr<ConfigT>& config,
            const boost::shared_ptr<ScoreBoard>& scoreboard,
            IntervalTimer* itimer) {
    assert(id_ == -1);
    assert(!config_);
    id_ = id;
    config_ = config;
    scoreboard_ = scoreboard;
    intervalTimer_ = itimer;
    alive_ = true;
  }

  virtual ~Worker() {}

  int getID() const {
    return id_;
  }

  /**
   * Create a new connection to the server.
   *
   * Subclasses must implement this method.
   */
  virtual boost::shared_ptr<ClientT> createConnection() = 0;

  /**
   * Perform an operation on a connection.
   *
   * Subclasses must implement this method.
   */
  virtual void performOperation(const boost::shared_ptr<ClientT>& client,
                                uint32_t opType) = 0;

  /**
   * Determine how to handle an exception raised by createConnection().
   *
   * The default behavior is to log an error message and abort.
   * Subclasses may override this function to provide alternate behavior.
   */
  virtual ErrorAction handleConnError(const std::exception& ex) {
    T_ERROR("worker %d caught %s exception while connecting: %s",
            id_, typeid(ex).name(), ex.what());
    return EA_ABORT;
  }

  /**
   * Determine how to handle an exception raised by performOperation().
   *
   * The default behavior is to log an error message and continue processing on
   * a new connection.  Subclasses may override this function to provide
   * alternate behavior.
   */
  virtual ErrorAction handleOpError(uint32_t opType, const std::exception& ex) {
    T_ERROR("worker %d caught %s exception performing operation %s: %s",
            id_, typeid(ex).name(), config_->getOpName(opType).c_str(),
            ex.what());
    return EA_NEXT_CONNECTION;
  }

  /**
   * Get the LoadConfig for this worker.
   *
   * (Returns a templatized config type for convenience, so subclasses can
   * store a subclass of LoadConfig, and retrieve it without having to cast it
   * back to the subclass type.)
   */
  const boost::shared_ptr<ConfigT>& getConfig() const {
    return config_;
  }

  /**
   * The main worker method.
   *
   * Loop forever creating connections and performing operations on them.
   * (May return if an error occurs and the error handler returns
   * EA_DROP_THREAD.)
   */
  virtual void run() {
    while (true) {
      // Create a new connection
      boost::shared_ptr<ClientT> client;
      try {
        client = createConnection();
      } catch (const std::exception& ex) {
        ErrorAction action = handleConnError(ex);
        if (action == EA_CONTINUE || action == EA_NEXT_CONNECTION) {
          // continue the next connection loop
          continue;
        } else if (action == EA_DROP_THREAD) {
          T_ERROR("worker %d exiting after connection error", id_);
          alive_ = false;
          return;
        } else if (action == EA_ABORT) {
          T_ERROR("worker %d causing abort after connection error", id_);
          abort();
        } else {
          T_ERROR("worker %d received unknown conn error action %d; aborting",
                  id_, action);
          abort();
        }
      }

      // Determine how many operations to perform on this connection
      uint32_t nops = config_->pickOpsPerConnection();

      // Perform operations on the connection
      for (uint32_t n = 0; n < nops; ++n) {
       // Only send as fast as requested
        if (!intervalTimer_->sleep()) {
          T_ERROR("can't keep up with requested QPS rate");
        }
        uint32_t opType = config_->pickOpType();
        scoreboard_->opStarted(opType);
        try {
          performOperation(client, opType);
          scoreboard_->opSucceeded(opType);
        } catch (const std::exception& ex) {
          scoreboard_->opFailed(opType);
          ErrorAction action = handleOpError(opType, ex);
          if (action == EA_CONTINUE) {
            // nothing to do; continue trying to use this connection
          } else if (action == EA_NEXT_CONNECTION) {
            // break out of the op loop,
            // continue the next connection loop
            break;
          } else if (action == EA_DROP_THREAD) {
            T_ERROR("worker %d exiting after op %d error", id_, opType);
            // return from run()
            alive_ = false;
            return;
          } else if (action == EA_ABORT) {
            T_ERROR("worker %d causing abort after op %d error", id_, opType);
            abort();
          } else {
            T_ERROR("worker %d received unknown op error action %d; aborting",
                    id_, action);
            abort();
          }
        }
      }
    }

    assert(false);
    alive_ = false;
  }

  bool isAlive() const {
    return alive_;
  }

 protected:
  // Methods needed for overriding ::run
  const boost::shared_ptr<ScoreBoard>& getScoreBoard() const {
    return scoreboard_;
  }

  void stopWorker() {
    alive_ = false;
  }

 private:
  int id_;
  bool alive_;
  IntervalTimer* intervalTimer_;
  boost::shared_ptr<ConfigT> config_;
  boost::shared_ptr<ScoreBoard> scoreboard_;
};


/**
 * Default WorkerFactory implementation.
 *
 * This factory creates Worker objects using the default constructor,
 * then calls init(id, config, scoreboard) on each worker before returning it.
 */
template<typename WorkerT, typename ConfigT = LoadConfig>
class SimpleWorkerFactory : public WorkerFactory {
 public:
  SimpleWorkerFactory(const boost::shared_ptr<ConfigT>& config)
    : config_(config) {}

  virtual WorkerT* newWorker(int id,
                             const boost::shared_ptr<ScoreBoard>& scoreboard,
                             IntervalTimer* itimer) {
    std::auto_ptr<WorkerT> worker(new WorkerT);
    worker->init(id, config_, scoreboard, itimer);
    return worker.release();
  }

  boost::shared_ptr<ConfigT> config_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_WORKER_H_
