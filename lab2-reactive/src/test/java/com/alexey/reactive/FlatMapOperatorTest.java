package com.alexey.reactive;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class FlatMapOperatorTest {

    @Test
    void shouldMergeNestedStreams() throws Exception {
        List<String> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<String>create(emitter -> {
            emitter.onNext("x");
            emitter.onNext("y");
            emitter.onComplete();
        })
                .flatMap(key -> Observable.<String>create(inner -> {
                    inner.onNext(key + "1");
                    inner.onNext(key + "2");
                    inner.onComplete();
                }))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactlyInAnyOrder("x1", "x2", "y1", "y2");
    }
}
