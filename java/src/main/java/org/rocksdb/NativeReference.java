package org.rocksdb;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class NativeReference {

    /**
     * A flag indicating whether the current {@code RocksObject} is responsible to
     * release the c++ object stored in its {@code nativeHandle_}.
     */
    private final AtomicBoolean owningHandle_;

    protected NativeReference(final boolean owningHandle) {
        this.owningHandle_ = new AtomicBoolean(owningHandle);
    }

    public boolean isOwningHandle() {
        return owningHandle_.get();
    }

    /**
     * Revoke ownership of the native object.
     * <p>
     * This will prevent the object from attempting to delete the underlying
     * native object in its finalizer. This must be used when another object
     * takes over ownership of the native object or both will attempt to delete
     * the underlying object when garbage collected.
     * <p>
     * When {@code disOwnNativeHandle()} is called, {@code dispose()} will simply set
     * {@code nativeHandle_} to 0 without releasing its associated C++ resource.
     * As a result, incorrectly use this function may cause memory leak, and this
     * function call will not affect the return value of {@code isInitialized()}.
     * </p>
     * @see #dispose()
     */
    protected final void disOwnNativeHandle() {
        owningHandle_.set(false);
    }

    /**
     * Release the c++ object manually pointed by the native handle.
     * <p>
     * Note that {@code dispose()} will also be called during the GC process
     * if it was not called before its {@code RocksObject} went out-of-scope.
     * However, since Java may wrongly wrongly assume those objects are
     * small in that they seems to only hold a long variable. As a result,
     * they might have low priority in the GC process.  To prevent this,
     * it is suggested to call {@code dispose()} manually.
     * </p>
     * <p>
     * Note that once an instance of {@code RocksObject} has been disposed,
     * calling its function will lead undefined behavior.
     * </p>
     */
    public final void dispose() {
        if (owningHandle_.compareAndSet(true, false)) {
            disposeInternal();
        }
    }

    /**
     * The helper function of {@code dispose()} which all subclasses of
     * {@code RocksObject} must implement to release their associated
     * C++ resource.
     */
    protected abstract void disposeInternal();

    /**
     * Simply calls {@code dispose()} and release its c++ resource if it has not
     * yet released.
     */
    @Override
    protected void finalize() throws Throwable {
        dispose();
        super.finalize();
    }
}
