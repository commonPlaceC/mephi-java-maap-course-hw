package com.alexey.executor;

import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public final class ExecutorEventLog {

    private static final Logger LOGGER = Logger.getLogger("ManagedExecutorPool");

    static {
        LOGGER.setUseParentHandlers(false);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                return record.getMessage() + System.lineSeparator();
            }
        });
        handler.setLevel(Level.INFO);
        LOGGER.addHandler(handler);
        LOGGER.setLevel(Level.INFO);
    }

    private ExecutorEventLog() {
    }

    public static void factory(String message) {
        LOGGER.info("[ThreadFactory] " + message);
    }

    public static void pool(String message) {
        LOGGER.info("[Pool] " + message);
    }

    public static void worker(String message) {
        LOGGER.info("[Worker] " + message);
    }

    public static void rejected(String message) {
        LOGGER.info("[Rejected] " + message);
    }
}
