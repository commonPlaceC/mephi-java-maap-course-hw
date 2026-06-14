package com.alexey.reactive;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class StreamErrorTest {

    @Test
    void shouldDeliverSourceError() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Object>create(emitter -> emitter.onError(new IllegalStateException("source broke")))
                .subscribe(new Observer<Object>() {
                    @Override
                    public void onNext(Object item) {
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

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class);
        assertThat(failure.get().getMessage()).isEqualTo("source broke");
    }

    @Test
    void shouldDeliverMapperError() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Function<Integer, Integer> broken = n -> {
            throw new RuntimeException("mapper broke");
        };

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        })
                .map(broken)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
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

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isInstanceOf(RuntimeException.class);
        assertThat(failure.get().getMessage()).isEqualTo("mapper broke");
    }

    @Test
    void shouldInvokeOnErrorOnlyOnce() throws Exception {
        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onError(new RuntimeException("first"));
            emitter.onError(new RuntimeException("second"));
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable t) {
                errors.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(errors.get()).isEqualTo(1);
    }
}
