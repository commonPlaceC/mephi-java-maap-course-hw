package com.alexey.reactive;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SubscriptionCancellationTest {

    @Test
    void disposeShouldStopFurtherDelivery() throws Exception {
        List<Integer> received = new ArrayList<>();
        AtomicReference<Disposable> tokenRef = new AtomicReference<>();

        Disposable token = Observable.<Integer>create(emitter -> {
            for (int i = 1; i <= 10; i++) {
                emitter.onNext(i);
                Thread.sleep(50);
            }
            emitter.onComplete();
        })
                .subscribeOn(Schedulers.single())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        received.add(item);
                        if (item == 1) {
                            tokenRef.get().dispose();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        tokenRef.set(token);
        Thread.sleep(500);
        assertThat(token.isDisposed()).isTrue();
        assertThat(received).containsExactly(1);
    }

    @Test
    void isDisposedShouldReflectTokenState() {
        Disposable token = Observable.<Object>create(emitter -> {
        }).subscribe(new Observer<Object>() {
            @Override
            public void onNext(Object item) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        });

        assertThat(token.isDisposed()).isFalse();
        token.dispose();
        assertThat(token.isDisposed()).isTrue();
    }
}
