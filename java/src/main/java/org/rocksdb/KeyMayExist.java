package org.rocksdb;

import java.util.Objects;

class KeyMayExist {
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    KeyMayExist that = (KeyMayExist) o;
    return valueLength == that.valueLength && exists == that.exists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(exists, valueLength);
  }

  enum KeyMayExistEnum { kNotExist, kExistsWithoutValue, kExistsWithValue }
  ;

  public KeyMayExist(KeyMayExistEnum exists, int valueLength) {
    this.exists = exists;
    this.valueLength = valueLength;
  }

  KeyMayExistEnum exists = KeyMayExistEnum.kNotExist;
  int valueLength = 0;
}
