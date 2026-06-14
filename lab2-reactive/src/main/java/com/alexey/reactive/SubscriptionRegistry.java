package com.alexey.reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class SubscriptionRegistry implements Disposable {

    private final List<Disposable> children = new ArrayList<>();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    synchronized void track(Disposable disposable) {
        if (!disposed.get()) {
            children.add(disposable);
        }
    }

    @Override
    public synchronized void dispose() {
        if (disposed.compareAndSet(false, true)) {
            for (Disposable child : children) {
                child.dispose();
            }
            children.clear();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
