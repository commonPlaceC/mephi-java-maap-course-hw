package com.alexey.reactive;

import java.util.function.Predicate;

final class FilterOperator {

    private FilterOperator() {
    }

    static <T> Observable<T> apply(Observable<T> upstream, Predicate<T> predicate,
                                   Scheduler upstreamScheduler, Scheduler downstreamScheduler) {
        return new Observable<>((observer, registry) -> {
            Disposable parent = upstream.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (registry.isDisposed()) {
                        return;
                    }
                    try {
                        if (predicate.test(item)) {
                            observer.onNext(item);
                        }
                    } catch (Throwable t) {
                        observer.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    observer.onError(t);
                }

                @Override
                public void onComplete() {
                    observer.onComplete();
                }
            });
            Operators.link(registry, parent);
        }, upstreamScheduler, downstreamScheduler);
    }
}
