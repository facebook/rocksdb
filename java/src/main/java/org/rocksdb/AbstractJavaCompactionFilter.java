// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for compaction filters implemented in Java
 */
// TODO(benclay): Ideally this should extend RocksCallbackObject, I didn't see a clean way though
// without converting the basic primitive for compaction filters to an interface.
public abstract class AbstractJavaCompactionFilter extends AbstractCompactionFilter<Slice> {

  // Static outputs representing the two immutable choices, avoiding object churn
  protected static final CompactionOutput KEEP_OUTPUT = new CompactionOutput(
          CompactionDecision.kKeep);
  protected static final CompactionOutput REMOVE_OUTPUT = new CompactionOutput(
          CompactionDecision.kRemove);

  // We use threadlocal caches for DirectSlice to avoid finalization on each compaction call.
  // Long-term we can get rid of this if AbstractNativeReference removes its finalize() method.
  private static ThreadLocal<DirectSlice> keyCache =
      new ThreadLocal<DirectSlice>() {
        @Override
        protected DirectSlice initialValue() {
          return new DirectSlice();
        }
      };
  private static ThreadLocal<DirectSlice> existingValueCache =
      new ThreadLocal<DirectSlice>() {
        @Override
        protected DirectSlice initialValue() {
          return new DirectSlice();
        }
      };

  public AbstractJavaCompactionFilter() {
    super(newCompactionFilter());
    //
    // Ideally we would construct the native-side CompactionFilter AND initialize it with all
    // the caching of Java method + field IDs in a single native call.  We cannot for the
    // following reasons:
    //
    // 1. CompactionFilter inherits from AbstractCompactionFilter, which way up the chain has
    //    a constructor that will take a native handle. We need to provide this super() a new
    //    native handle which we generate ourselves - when Java constructor calls, we jump into
    //    native and generate a native-side handle representing the native compaction filter
    //    (in this case, an instance of CompactionFilterJniCallback).
    // 2. Native constructor counterparts are denoted private native static because the Java
    //    object doesn't exist yet - we are constructing it when they're called, and hence
    //    they're represented as static. Therefore they have the signature (JNIEnv *, jclass)
    //    instead of an object method's (JNIEnv *, jobject)
    // 3. We need to cache two sets of things on native side around the time of Java object
    //    construction: a bunch of method and field IDs (just require the jclass references)
    //    AND a jobject reference to our Java-side CompactionFilter. The former are for
    //    performance reasons, but the latter is so that we call the Name() and FilterV2()
    //    methods on our CompactionFilter object - these are non-static methods on Java side
    //    and hence require an object. We could conceivably represent Name() as a static
    //    method, but not FilterV2().
    // 4. Java requires super() to be the very first call within a constructor, so that's when
    //    we need to generate the native-side handle. That in turn requires us to call the
    //    static newCompactionFilter().
    // 5. We then follow it with a call to the non-static initializeFilter() to cache our Java
    //    method and field IDs for speed.
    //
    if (!initializeFilter(nativeHandle_)) {
      throw new RuntimeException("Unable to cache field IDs in AbstractJavaCompactionFilter");
    }
  }

  /**
   * Native handle accessor. We can think about making it package-private later.
   *
   * @return
   */
  public long nativeHandle() {
    return nativeHandle_;
  }

  /**
   * Java interface for rocksdb::CompactionFilter::FilterV2.
   *
   * See compaction_filter.h for a description of parameters. We have Java-side representations of
   * the enums but their meaning is the same.
   *
   * @return  CompactionOutput struct type, which lets us avoid the native FilterV2() method's usage
   *          of output parameters.
   *          NOTE: If you return null from this method it will be treated as Decision.kKeep!
   */
  public abstract CompactionOutput FilterV2(
          int level, DirectSlice key, CompactionValueType valueType, DirectSlice existingValue);

  /**
   * Internal callback function that is directly called from the native side. We perform input marshalling within the JVM instead of requiring native to do it.
   *
   * @param level                     Level
   * @param keyHandle                 Handle of the key Slice
   * @param valueTypeValue            Byte representing the CompactionValueType enum value
   * @param existingValueHandle       Handle of the existingValue DirectSlice
   * @return                          CompactionOutput object
   */
  @SuppressWarnings("unused")
  private CompactionOutput FilterV2Internal(
          int level, long keyHandle, byte valueTypeValue, long existingValueHandle) {
    try {
      DirectSlice key = keyCache.get();
      key.setNativeHandle(keyHandle, false);
      CompactionValueType valueType = CompactionValueType.getCompactionValueType(valueTypeValue);
      DirectSlice existingValue = existingValueCache.get();
      existingValue.setNativeHandle(existingValueHandle, false);
      return this.FilterV2(level, key, valueType, existingValue);
    } catch (Exception e) {
      // We'd rather not let this thread die due to bugs in Java, so catch + return a default
      // decision.
      return KEEP_OUTPUT;
    }
  }

  /**
   * Unique name of this compaction filter
   */
  public abstract String Name();

  /* Native method references below */
  private native static long newCompactionFilter();
  private native boolean initializeFilter(long handle);

}
