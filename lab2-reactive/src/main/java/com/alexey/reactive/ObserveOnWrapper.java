package com.alexey.reactive;

final class ObserveOnWrapper {

    private ObserveOnWrapper() {
    }

    static <T> Observer<T> wrap(Observer<T> target, Scheduler scheduler, Disposable token) {
        return new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (token.isDisposed()) {
                    return;
                }
                scheduler.execute(() -> {
                    if (!token.isDisposed()) {
                        target.onNext(item);
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                if (token.isDisposed()) {
                    return;
                }
                scheduler.execute(() -> {
                    if (!token.isDisposed()) {
                        target.onError(t);
                    }
                });
            }

            @Override
            public void onComplete() {
                if (token.isDisposed()) {
                    return;
                }
                scheduler.execute(() -> {
                    if (!token.isDisposed()) {
                        target.onComplete();
                    }
                });
            }
        };
    }
}
