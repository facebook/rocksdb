package org.rocksdb.test;

import org.rocksdb.AbstractEventListener;

public class TestableEventListener extends AbstractEventListener {
  public void invokeAllCallbacks() {
    invokeAllCallbacks(nativeHandle_);
  }

  private static native void invokeAllCallbacks(final long handle);
}
