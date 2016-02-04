// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * AbstractNativeReference is the base-class of all RocksDB classes that have
 * a pointer to a native C++ {@code rocksdb} object.
 * <p>
 * AbstractNativeReference has the {@link AbstractNativeReference#dispose()}
 * method, which frees its associated C++ object.</p>
 * <p>
 * This function should be called manually, however, if required it will be
 * called automatically during the regular Java GC process via
 * {@link AbstractNativeReference#finalize()}.</p>
 * <p>
 * Note - Java can only see the long member variable (which is the C++ pointer
 * value to the native object), as such it does not know the real size of the
 * object and therefore may assign a low GC priority for it; So it is strongly
 * suggested that you manually dispose of objects when you are finished with
 * them.</p>
 */
public abstract class AbstractNativeReference {

  /**
   * Returns true if we are responsible for freeing the underlying C++ object
   *
   * @return true if we are responsible to free the C++ object
   * @see #dispose()
   */
  protected abstract boolean isOwningHandle();

  /**
   * Frees the underlying C++ object
   * <p>
   * It is strong recommended that the developer calls this after they
   * have finished using the object.</p>
   * <p>
   * Note, that once an instance of {@link AbstractNativeReference} has been
   * disposed, calling any of its functions will lead to undefined
   * behavior.</p>
   */
  public abstract void dispose();

  /**
   * Simply calls {@link AbstractNativeReference#dispose()} to free
   * any underlying C++ object reference which has not yet been manually
   * released.
   */
  @Override
  protected void finalize() throws Throwable {
    dispose();
    super.finalize();
  }
}
