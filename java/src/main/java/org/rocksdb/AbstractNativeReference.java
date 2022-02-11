// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * AbstractNativeReference is the base-class of all RocksDB classes that have
 * a pointer to a native C++ {@code rocksdb} object.
 * <p>
 * AbstractNativeReference has the {@link AbstractNativeReference#close()}
 * method, which frees its associated C++ object.</p>
 * <p>
 * This function should be called manually, or even better, called implicitly using a
 * <a
 * href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">try-with-resources</a>
 * statement, when you are finished with the object. It is no longer
 * called automatically during the regular Java GC process via
 * {@link AbstractNativeReference#finalize()}.</p>
 * <p>
 * Explanatory note - When or if the Garbage Collector calls {@link Object#finalize()}
 * depends on the JVM implementation and system conditions, which the programmer
 * cannot control. In addition, the GC cannot see through the native reference
 * long member variable (which is the C++ pointer value to the native object),
 * and cannot know what other resources depend on it.
 * </p>
 */
public abstract class AbstractNativeReference implements AutoCloseable {
  /**
   * Returns true if we are responsible for freeing the underlying C++ object
   *
   * @return true if we are responsible to free the C++ object
   */
  protected abstract boolean isOwningHandle();

  /**
   * Frees the underlying C++ object
   * <p>
   * It is strong recommended that the developer calls this after they
   * have finished using the object.</p>
   * <p>
   * Note, that once an instance of {@link AbstractNativeReference} has been
   * closed, calling any of its functions will lead to undefined
   * behavior.</p>
   */
  @Override public abstract void close();
}
