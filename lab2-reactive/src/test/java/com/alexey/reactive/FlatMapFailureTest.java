package com.alexey.reactive;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FlatMapFailureTest {

    @Test
    void shouldPropagateInnerStreamError() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(7);
            emitter.onComplete();
        })
                .flatMap(v -> Observable.<String>create(inner ->
                        inner.onError(new IllegalStateException("inner broke"))))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        failure.set(t);
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class);
        assertThat(failure.get().getMessage()).isEqualTo("inner broke");
    }
}
