package com.alexey.executor;

import java.util.concurrent.RejectedExecutionException;

final class OverflowHandler {

    private OverflowHandler() {
    }

    static void handle(Runnable task, OverflowAction action) {
        String label = taskLabel(task);
        switch (action) {
            case ABORT -> {
                ExecutorEventLog.rejected("Task " + label + " was rejected due to overload!");
                throw new RejectedExecutionException("Task " + label + " rejected due to overload");
            }
            case CALLER_RUNS -> {
                ExecutorEventLog.rejected("Task " + label + " will be executed in caller thread due to overload.");
                task.run();
            }
            case DISCARD -> ExecutorEventLog.rejected("Task " + label + " was discarded due to overload!");
        }
    }

    private static String taskLabel(Runnable task) {
        return task.getClass().getSimpleName() + "@" + Integer.toHexString(task.hashCode());
    }
}
