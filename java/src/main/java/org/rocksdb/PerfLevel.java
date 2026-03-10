//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum PerfLevel {
  /**
   * Unknown setting
   */
  UNINITIALIZED((byte) 0),
  /**
   * disable perf stats
   */
  DISABLE((byte) 1),
  /**
   * enable only count stats
   */
  ENABLE_COUNT((byte) 2),
  /**
   * Other than count stats, also enable wait/delay time stats for
   * user threads blocked in RocksDB
   */
  ENABLE_WAIT((byte) 3),
  /**
   * Also enable time stats for write operations (WAL write time,
   * scheduling time, etc.) without the overhead of read-path timers
   */
  ENABLE_TIME_FOR_WRITE((byte) 4),
  /**
   * Also enable time stats for all operations except for mutexes
   */
  ENABLE_TIME_EXCEPT_FOR_MUTEX((byte) 5),
  /**
   * Other than time, also measure CPU time counters. Still don't measure
   * time (neither wall time nor CPU time) for mutexes
   */
  ENABLE_TIME_AND_CPU_TIME_EXCEPT_FOR_MUTEX((byte) 6),
  /**
   * enable count and time stats
   */
  ENABLE_TIME((byte) 7),

  /**
   * Do not use
   * @deprecated It's here to just keep parity with C++ API.
   */
  @Deprecated OUT_OF_BOUNDS((byte) 8);

  PerfLevel(byte _value) {
    this._value = _value;
  }

  private final byte _value;

  public byte getValue() {
    return _value;
  }

  public static PerfLevel getPerfLevel(byte level) {
    for (PerfLevel l : PerfLevel.values()) {
      if (l.getValue() == level) {
        return l;
      }
    }
    throw new IllegalArgumentException("Uknknown PerfLevel constant : " + level);
  }
}
