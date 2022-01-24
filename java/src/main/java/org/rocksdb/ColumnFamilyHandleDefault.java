package org.rocksdb;

public class ColumnFamilyHandleDefault extends ColumnFamilyHandle {

  ColumnFamilyHandleDefault(long nativeReference) {
    super(nativeReference);
  }

  @Override protected native boolean equalsByHandle(final long nativeReference, long otherNativeReference);
  @Override protected native byte[] getName(long handle);
  @Override protected native int getID(long handle);
  @Override protected native ColumnFamilyDescriptor getDescriptor(long handle);
  @Override protected boolean isDefaultColumnFamily() {
    return true;
  }
  @Override protected native void nativeClose(long nativeReference);
  @Override protected boolean isLastReference(long nativeAPIReference) {
    return true;
  }
}
