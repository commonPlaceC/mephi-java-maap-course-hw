package com.alexey.reactive;

import java.util.concurrent.atomic.AtomicBoolean;

final class GuardedObserver<T> implements Observer<T> {

    private final Observer<T> downstream;
    private final Disposable token;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    GuardedObserver(Observer<T> downstream, Disposable token) {
        this.downstream = downstream;
        this.token = token;
    }

    @Override
    public void onNext(T item) {
        if (finished.get() || token.isDisposed()) {
            return;
        }
        try {
            downstream.onNext(item);
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!finished.compareAndSet(false, true) || token.isDisposed()) {
            return;
        }
        downstream.onError(t);
    }

    @Override
    public void onComplete() {
        if (!finished.compareAndSet(false, true) || token.isDisposed()) {
            return;
        }
        downstream.onComplete();
    }
}
