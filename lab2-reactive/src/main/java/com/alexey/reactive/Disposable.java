package com.alexey.reactive;

public interface Disposable {
    void dispose();

    boolean isDisposed();
}
