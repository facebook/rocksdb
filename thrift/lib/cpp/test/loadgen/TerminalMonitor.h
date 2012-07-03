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
#ifndef THRIFT_TEST_LOADGEN_TERMINALMONITOR_H_
#define THRIFT_TEST_LOADGEN_TERMINALMONITOR_H_ 1

#include "thrift/lib/cpp/test/loadgen/Monitor.h"

namespace apache { namespace thrift { namespace loadgen {

/**
 * A Monitor object that prints statistics to a terminal.
 *
 * It handles printing header lines, and re-printing the header whenever it
 * scrolls off of the screen.
 */
class TerminalMonitor : public Monitor {
 public:
  TerminalMonitor();

  virtual void redisplay(uint64_t intervalUsec);

  /**
   * Initialize monitoring information.
   *
   * This method is called immediately after all of the Workers have started,
   * just before the initial monitoring interval.  It can be used to get
   * initial counter values from all of the Workers, so that statistics
   * reported in the first call to printInfo() are accurate.
   *
   * If subclasses override initializeInfo(), they should make sure to call
   * TerminalMonitor::initializeInfo() in their method.
   */
  virtual void initializeInfo();

  /**
   * Print header lines.
   *
   * This is called when monitoring first starts.  If the screen height can
   * be determined, this is also called every time the previous header scrolls
   * off the screen.
   *
   * @return Returns the number of lines printed.
   */
  virtual uint32_t printHeader() = 0;

  /**
   * Print monitoring information.
   *
   * This is called once every specified interval.
   *
   * @param intervalUsec The number of microseconds since the previous call to
   *                     printInfo().  The first time printInfo() is called,
   *                     this is the number of microseconds since
   *                     initializeInfo() was called.
   *
   * @return Returns the number of lines printed.
   */
  virtual uint32_t printInfo(uint64_t intervalUsec) = 0;

 protected:
  int32_t getScreenHeight();

  int32_t screenHeight_;
  uint32_t linesPrinted_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_TERMINALMONITOR_H_
