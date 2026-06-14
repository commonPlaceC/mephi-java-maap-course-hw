package com.alexey.reactive;

public interface Scheduler {
    void execute(Runnable task);
}
