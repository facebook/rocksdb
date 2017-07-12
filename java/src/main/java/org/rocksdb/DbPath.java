// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.nio.file.Path;

/**
 * Tuple of database path and target size
 */
public class DbPath {
  final Path path;
  final long targetSize;

  public DbPath(final Path path, final long targetSize) {
    this.path = path;
    this.targetSize = targetSize;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DbPath dbPath = (DbPath) o;

    if (targetSize != dbPath.targetSize) {
      return false;
    }

    return path != null ? path.equals(dbPath.path) : dbPath.path == null;
  }

  @Override
  public int hashCode() {
    int result = path != null ? path.hashCode() : 0;
    result = 31 * result + (int) (targetSize ^ (targetSize >>> 32));
    return result;
  }
}
