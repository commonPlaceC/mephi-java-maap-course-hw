package com.alexey.reactive;

@FunctionalInterface
public interface StreamEmitter<T> {
    void subscribe(Emitter<T> emitter) throws Exception;

    interface Emitter<T> {
        void onNext(T item);

        void onError(Throwable t);

        void onComplete();
    }
}
