package com.alexey.executor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class LabeledThreadFactory implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger sequence = new AtomicInteger(1);

    public LabeledThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String name = prefix + "-" + sequence.getAndIncrement();
        ExecutorEventLog.factory("Creating new thread: " + name);
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(false);
        return thread;
    }
}
