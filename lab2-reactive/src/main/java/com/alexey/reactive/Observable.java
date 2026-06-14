package com.alexey.reactive;

import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {

    @FunctionalInterface
    interface Publisher<T> {
        void connect(Observer<T> observer, Disposable registry);
    }

    private final Publisher<T> publisher;
    private final Scheduler upstreamScheduler;
    private final Scheduler downstreamScheduler;

    protected Observable(Publisher<T> publisher, Scheduler upstreamScheduler, Scheduler downstreamScheduler) {
        this.publisher = publisher;
        this.upstreamScheduler = upstreamScheduler;
        this.downstreamScheduler = downstreamScheduler;
    }

    public static <T> Observable<T> create(StreamEmitter<T> source) {
        return new Observable<>((observer, registry) -> {
            StreamEmitter.Emitter<T> outlet = new StreamEmitter.Emitter<T>() {
                @Override
                public void onNext(T item) {
                    if (!registry.isDisposed()) {
                        observer.onNext(item);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (!registry.isDisposed()) {
                        observer.onError(t);
                    }
                }

                @Override
                public void onComplete() {
                    if (!registry.isDisposed()) {
                        observer.onComplete();
                    }
                }
            };
            try {
                source.subscribe(outlet);
            } catch (Throwable t) {
                observer.onError(t);
            }
        }, null, null);
    }

    public Disposable subscribe(Observer<T> observer) {
        SubscriptionToken token = new SubscriptionToken();
        Observer<T> target = new GuardedObserver<>(observer, token);

        if (downstreamScheduler != null) {
            target = ObserveOnWrapper.wrap(target, downstreamScheduler, token);
        }

        Observer<T> sink = target;
        Runnable connect = () -> {
            SubscriptionRegistry registry = new SubscriptionRegistry();
            token.attach(registry);
            publisher.connect(sink, registry);
        };

        if (upstreamScheduler != null) {
            upstreamScheduler.execute(connect);
        } else {
            connect.run();
        }

        return token;
    }

    public <R> Observable<R> map(Function<T, R> mapper) {
        return MapOperator.apply(this, mapper, upstreamScheduler, downstreamScheduler);
    }

    public Observable<T> filter(Predicate<T> predicate) {
        return FilterOperator.apply(this, predicate, upstreamScheduler, downstreamScheduler);
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return FlatMapOperator.apply(this, mapper, upstreamScheduler, downstreamScheduler);
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new Observable<>(publisher, scheduler, downstreamScheduler);
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new Observable<>(publisher, upstreamScheduler, scheduler);
    }
}
