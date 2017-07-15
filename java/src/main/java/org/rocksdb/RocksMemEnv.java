// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * RocksDB memory environment.
 */
public class RocksMemEnv extends Env {

  /**
   * <p>Creates a new RocksDB environment that stores its data
   * in memory and delegates all non-file-storage tasks to
   * base_env. The caller must delete the result when it is
   * no longer needed.</p>
   *
   * <p>{@code *base_env} must remain live while the result is in use.</p>
   */
  public RocksMemEnv() {
    super(createMemEnv());
  }

  private static native long createMemEnv();
  @Override protected final native void disposeInternal(final long handle);
}
