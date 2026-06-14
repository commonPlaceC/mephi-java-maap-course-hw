package com.alexey.reactive;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerProviderTest {

    @Test
    void ioSchedulerUsesDistinctThreads() throws Exception {
        Set<String> names = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(3);

        Scheduler io = Schedulers.io();
        for (int i = 0; i < 3; i++) {
            io.execute(() -> {
                names.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(names).hasSizeGreaterThan(1);
        assertThat(names).anyMatch(n -> n.startsWith("alex-io-"));
    }

    @Test
    void computationSchedulerHasExpectedPrefix() throws Exception {
        String[] holder = new String[1];
        CountDownLatch latch = new CountDownLatch(1);

        Schedulers.computation().execute(() -> {
            holder[0] = Thread.currentThread().getName();
            latch.countDown();
        });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(holder[0]).startsWith("alex-compute-");
    }

    @Test
    void singleSchedulerRunsSequentiallyOnOneThread() throws Exception {
        Set<String> names = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(3);

        Scheduler single = Schedulers.single();
        for (int i = 0; i < 3; i++) {
            single.execute(() -> {
                names.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(names).hasSize(1);
        assertThat(names.iterator().next()).startsWith("alex-single-");
    }
}
