package org.rocksdb;

public abstract class RocksMutableObject /*extends NativeReference*/ {

    private long nativeHandle_;
    private boolean owningHandle_;

    protected RocksMutableObject() {
    }

    protected RocksMutableObject(final long nativeHandle) {
        this.nativeHandle_ = nativeHandle;
        this.owningHandle_ = true;
    }

    public synchronized void setNativeHandle(final long nativeHandle, final boolean owningNativeHandle) {
        this.nativeHandle_ = nativeHandle;
        this.owningHandle_ = owningNativeHandle;
    }

    //@Override
    protected synchronized boolean isOwningHandle() {
        return this.owningHandle_;
    }

    protected synchronized long getNativeHandle() {
        assert(this.nativeHandle_ != 0);
        return this.nativeHandle_;
    }

    public synchronized final void dispose() {
        if(isOwningHandle()) {
            disposeInternal();
            this.owningHandle_ = false;
            this.nativeHandle_ = 0;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        dispose();
        super.finalize();
    }

    protected void disposeInternal() {
        disposeInternal(nativeHandle_);
    }

    protected abstract void disposeInternal(final long handle);
}
