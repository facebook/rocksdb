package org.rocksdb;

public abstract class RocksMutableObject extends NativeReference {

    private final boolean shouldOwnHandle;
    protected volatile long nativeHandle_;

    protected RocksMutableObject() {
        super(false);
        this.shouldOwnHandle = false;
    }

    protected RocksMutableObject(final long nativeHandle) {
        super(true);
        this.shouldOwnHandle = true;
        this.nativeHandle_ = nativeHandle;
    }

    @Override
    public boolean isOwningHandle() {
        return ((!shouldOwnHandle) || super.isOwningHandle()) && nativeHandle_ != 0;
    }

    /**
     * Deletes underlying C++ object pointer.
     */
    @Override
    protected void disposeInternal() {
        disposeInternal(nativeHandle_);
    }

    protected abstract void disposeInternal(final long handle);
}
