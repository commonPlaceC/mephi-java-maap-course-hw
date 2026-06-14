package com.alexey.reactive;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

final class FlatMapOperator {

    private FlatMapOperator() {
    }

    static <T, R> Observable<R> apply(Observable<T> upstream, Function<T, Observable<R>> mapper,
                                        Scheduler upstreamScheduler, Scheduler downstreamScheduler) {
        return new Observable<>((observer, registry) -> {
            AtomicInteger openStreams = new AtomicInteger(0);
            AtomicBoolean sourceDone = new AtomicBoolean(false);
            AtomicBoolean closed = new AtomicBoolean(false);

            Disposable parent = upstream.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (registry.isDisposed() || closed.get()) {
                        return;
                    }
                    try {
                        Observable<R> nested = mapper.apply(item);
                        openStreams.incrementAndGet();

                        Disposable nestedSub = nested.subscribe(new Observer<R>() {
                            @Override
                            public void onNext(R value) {
                                if (!registry.isDisposed() && !closed.get()) {
                                    observer.onNext(value);
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                finishWithError(t);
                            }

                            @Override
                            public void onComplete() {
                                if (openStreams.decrementAndGet() == 0 && sourceDone.get()) {
                                    finishNormally();
                                }
                            }
                        });
                        Operators.link(registry, nestedSub);
                    } catch (Throwable t) {
                        finishWithError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    finishWithError(t);
                }

                @Override
                public void onComplete() {
                    sourceDone.set(true);
                    if (openStreams.get() == 0) {
                        finishNormally();
                    }
                }

                private void finishWithError(Throwable t) {
                    if (closed.compareAndSet(false, true)) {
                        registry.dispose();
                        observer.onError(t);
                    }
                }

                private void finishNormally() {
                    if (closed.compareAndSet(false, true)) {
                        observer.onComplete();
                    }
                }
            });
            Operators.link(registry, parent);
        }, upstreamScheduler, downstreamScheduler);
    }
}
