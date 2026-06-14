package com.alexey.reactive;

import java.util.function.Function;

final class MapOperator {

    private MapOperator() {
    }

    static <T, R> Observable<R> apply(Observable<T> upstream, Function<T, R> mapper,
                                      Scheduler upstreamScheduler, Scheduler downstreamScheduler) {
        return new Observable<>((observer, registry) -> {
            Disposable parent = upstream.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (registry.isDisposed()) {
                        return;
                    }
                    try {
                        observer.onNext(mapper.apply(item));
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
