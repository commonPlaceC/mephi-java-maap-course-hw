package com.alexey.executor;

import java.util.concurrent.TimeUnit;

public record ExecutorSettings(
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        TimeUnit timeUnit,
        int queueSize,
        int minSpareThreads,
        OverflowAction overflowAction
) {
    public ExecutorSettings {
        if (corePoolSize < 0) {
            throw new IllegalArgumentException("corePoolSize must be >= 0");
        }
        if (maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize");
        }
        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must be > 0");
        }
        if (minSpareThreads < 0) {
            throw new IllegalArgumentException("minSpareThreads must be >= 0");
        }
        if (keepAliveTime < 0) {
            throw new IllegalArgumentException("keepAliveTime must be >= 0");
        }
        if (timeUnit == null) {
            throw new IllegalArgumentException("timeUnit must not be null");
        }
        if (overflowAction == null) {
            throw new IllegalArgumentException("overflowAction must not be null");
        }
    }

    public static ExecutorSettings of(
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            TimeUnit timeUnit,
            int queueSize,
            int minSpareThreads,
            OverflowAction overflowAction
    ) {
        return new ExecutorSettings(
                corePoolSize, maxPoolSize, keepAliveTime, timeUnit,
                queueSize, minSpareThreads, overflowAction
        );
    }
}
