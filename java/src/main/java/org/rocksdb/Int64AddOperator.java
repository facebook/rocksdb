package org.rocksdb;

public class Int64AddOperator extends MergeOperator {
  protected Int64AddOperator() {
    super(newSharedInt64AddOperator());
  }

  @Override protected native void disposeInternal(final long handle);

  private static native long newSharedInt64AddOperator();
}
