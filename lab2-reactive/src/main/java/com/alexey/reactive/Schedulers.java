package com.alexey.reactive;

public final class Schedulers {

    private Schedulers() {
    }

    public static Scheduler io() {
        return SchedulerProvider.io();
    }

    public static Scheduler computation() {
        return SchedulerProvider.computation();
    }

    public static Scheduler single() {
        return SchedulerProvider.single();
    }
}
