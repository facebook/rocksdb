//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
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
