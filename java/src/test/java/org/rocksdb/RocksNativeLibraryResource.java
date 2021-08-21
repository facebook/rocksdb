// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.rules.ExternalResource;

/**
 * Resource to load the RocksDB JNI library.
 */
public class RocksNativeLibraryResource extends ExternalResource {
  @Override
  protected void before() {
    RocksDB.loadLibrary();
  }
}
