package org.rocksdb;

import java.util.Arrays;
import java.util.Objects;

public abstract class ColumnFamilyHandle extends RocksNative {

  protected ColumnFamilyHandle(long nativeReference) {
    super(nativeReference);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ColumnFamilyHandle that = (ColumnFamilyHandle) o;
    try {
      return equalsByHandle(getNative(), that.getNative()) &&
          getID() == that.getID() &&
          Arrays.equals(getName(), that.getName());
    } catch (RocksDBException e) {
      throw new RuntimeException("Cannot compare column family handles", e);
    }
  }

  @Override
  public int hashCode() {
    try {
      int result = Objects.hash(getID(), getNative());
      result = 31 * result + Arrays.hashCode(getName());
      return result;
    } catch (RocksDBException e) {
      throw new RuntimeException("Cannot calculate hash code of column family handle", e);
    }
  }

  protected abstract boolean equalsByHandle(final long nativeReference, long otherNativeReference);

  /**
   * Gets the name of the Column Family.
   *
   * @return The name of the Column Family.
   *
   * @throws RocksDBException if an error occurs whilst retrieving the name.
   */
  protected byte[] getName() throws RocksDBException {
    return getName(getNative());
  }
  protected abstract byte[] getName(final long handle) throws RocksDBException;

  /**
   * Gets the ID of the Column Family.
   *
   * @return the ID of the Column Family.
   */
  protected int getID() throws RocksDBException {
    return getID(getNative());
  }
  protected abstract int getID(final long handle);

  /**
   * Gets the up-to-date descriptor of the column family
   * associated with this handle. Since it fills "*desc" with the up-to-date
   * information, this call might internally lock and release DB mutex to
   * access the up-to-date CF options. In addition, all the pointer-typed
   * options cannot be referenced any longer than the original options exist.
   *
   * Note that this function is not supported in RocksDBLite.
   *
   * @return the up-to-date descriptor.
   *
   * @throws RocksDBException if an error occurs whilst retrieving the
   *     descriptor.
   */
  protected ColumnFamilyDescriptor getDescriptor() throws RocksDBException {
    return getDescriptor(getNative());
  }
  protected abstract ColumnFamilyDescriptor getDescriptor(final long handle) throws RocksDBException;

  protected abstract boolean isDefaultColumnFamily();
}
