// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * RocksDB HDFS environment.
 */
public class RocksHdfsEnv extends Env {

  /**
   * <p>Creates a new RocksDB environment that stores its data
   * on specified HDFS filesystem.</p>
   * @param path fully qualified path, hdfs://host:port/path
   */
  public RocksHdfsEnv(String path) {
    super(createHdfsEnv(path));
  }

  private static native long createHdfsEnv(String path);
  @Override protected final native void disposeInternal(final long handle);
}
