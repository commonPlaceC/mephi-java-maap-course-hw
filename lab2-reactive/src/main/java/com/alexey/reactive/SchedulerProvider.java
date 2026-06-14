package com.alexey.reactive;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class SchedulerProvider {

    private static final Scheduler IO = new DelegatingScheduler(
            Executors.newCachedThreadPool(new NamedThreads("alex-io")),
            "io"
    );
    private static final Scheduler COMPUTATION = new DelegatingScheduler(
            Executors.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors(),
                    new NamedThreads("alex-compute")
            ),
            "computation"
    );
    private static final Scheduler SINGLE = new DelegatingScheduler(
            Executors.newSingleThreadExecutor(new NamedThreads("alex-single")),
            "single"
    );

    private SchedulerProvider() {
    }

    public static Scheduler io() {
        return IO;
    }

    public static Scheduler computation() {
        return COMPUTATION;
    }

    public static Scheduler single() {
        return SINGLE;
    }

    private static final class DelegatingScheduler implements Scheduler {
        private final ExecutorService backing;
        private final String label;

        private DelegatingScheduler(ExecutorService backing, String label) {
            this.backing = backing;
            this.label = label;
        }

        @Override
        public void execute(Runnable task) {
            backing.execute(task);
        }

        @Override
        public String toString() {
            return "Scheduler(" + label + ")";
        }
    }

    private static final class NamedThreads implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger seq = new AtomicInteger(1);

        private NamedThreads(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, prefix + "-" + seq.getAndIncrement());
        }
    }
}
