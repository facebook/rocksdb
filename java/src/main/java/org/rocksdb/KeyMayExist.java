// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

public class KeyMayExist {
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final KeyMayExist that = (KeyMayExist) o;
    return (valueLength == that.valueLength && exists == that.exists);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exists, valueLength);
  }

  public enum KeyMayExistEnum { kNotExist, kExistsWithoutValue, kExistsWithValue }
  ;

  public KeyMayExist(final KeyMayExistEnum exists, final int valueLength) {
    this.exists = exists;
    this.valueLength = valueLength;
  }

  public final KeyMayExistEnum exists;
  public final int valueLength;
}
