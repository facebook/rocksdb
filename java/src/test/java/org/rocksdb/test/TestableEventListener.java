package org.rocksdb.test;

import org.rocksdb.AbstractEventListener;

public class TestableEventListener extends AbstractEventListener {
  public TestableEventListener() {
    super();
  }

  public TestableEventListener(final EnabledEventCallback... enabledEventCallbacks) {
    super(enabledEventCallbacks);
  }

  public void invokeAllCallbacks() {
    invokeAllCallbacks(nativeHandle_);
  }

  private static native void invokeAllCallbacks(final long handle);
}
