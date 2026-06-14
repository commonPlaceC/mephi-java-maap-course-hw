package com.alexey.reactive;

import java.util.concurrent.atomic.AtomicBoolean;

final class SubscriptionToken implements Disposable {

    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private volatile Disposable child;

    void attach(Disposable disposable) {
        this.child = disposable;
        if (disposed.get() && disposable != null) {
            disposable.dispose();
        }
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            Disposable current = child;
            if (current != null) {
                current.dispose();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
