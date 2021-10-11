package org.rocksdb;

import java.util.Objects;

class KeyMayExist {
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

  enum KeyMayExistEnum { kNotExist, kExistsWithoutValue, kExistsWithValue }
  ;

  public KeyMayExist(final KeyMayExistEnum exists, final int valueLength) {
    this.exists = exists;
    this.valueLength = valueLength;
  }

  final KeyMayExistEnum exists;
  final int valueLength;
}
