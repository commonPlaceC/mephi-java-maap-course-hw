package com.alexey.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ExecutorLab {

    private static final Logger LOG = Logger.getLogger(ExecutorLab.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "benchmark".equals(args[0])) {
            comparePools();
            System.out.println();
            sweepParameters();
            return;
        }

        runSteadyLoad();
        System.out.println();
        runOverloadScenario();
        System.out.println();
        runGracefulStop();
        System.out.println();
        comparePools();
        System.out.println();
        sweepParameters();
    }

    private static void runSteadyLoad() throws Exception {
        LOG.info("=== Scenario: steady workload ===");

        ExecutorSettings cfg = ExecutorSettings.of(
                2, 4, 5, TimeUnit.SECONDS, 5, 1, OverflowAction.ABORT
        );
        ManagedExecutorPool pool = new ManagedExecutorPool(cfg);

        List<Future<String>> results = new ArrayList<>();
        for (int id = 1; id <= 12; id++) {
            int taskId = id;
            results.add(pool.submit(delayedCall(taskId, 500 + (taskId % 3) * 500L)));
        }

        for (Future<String> future : results) {
            LOG.info("Completed: " + future.get(30, TimeUnit.SECONDS));
        }

        pool.shutdown();
        boolean done = pool.awaitTermination(30, TimeUnit.SECONDS);
        LOG.info("Shutdown result: " + done + ", workers left: " + pool.activeWorkers());
    }

    private static void runOverloadScenario() {
        LOG.info("=== Scenario: queue overflow ===");

        ExecutorSettings cfg = ExecutorSettings.of(
                2, 2, 2, TimeUnit.SECONDS, 2, 0, OverflowAction.ABORT
        );
        ManagedExecutorPool pool = new ManagedExecutorPool(cfg);

        int ok = 0;
        int dropped = 0;
        for (int id = 1; id <= 25; id++) {
            try {
                pool.execute(delayedRun(id, 200));
                ok++;
            } catch (RejectedExecutionException ex) {
                dropped++;
            }
        }

        LOG.info("Accepted: " + ok + ", rejected: " + dropped);
        pool.shutdownNow();
    }

    private static void runGracefulStop() throws Exception {
        LOG.info("=== Scenario: graceful stop ===");

        ExecutorSettings cfg = ExecutorSettings.of(
                2, 4, 5, TimeUnit.SECONDS, 5, 1, OverflowAction.ABORT
        );
        ManagedExecutorPool pool = new ManagedExecutorPool(cfg);

        for (int id = 1; id <= 8; id++) {
            pool.execute(delayedRun(id, 1000));
        }

        pool.shutdown();
        boolean done = pool.awaitTermination(60, TimeUnit.SECONDS);
        LOG.info("Graceful stop: " + done + ", workers left: " + pool.activeWorkers());
    }

    private static void comparePools() throws Exception {
        LOG.info("=== Benchmark: ManagedExecutorPool vs ThreadPoolExecutor ===");

        int tasks = 500;
        long sleepMs = 10;

        BenchStats custom = benchManaged(tasks, sleepMs);
        BenchStats standard = benchJdk(tasks, sleepMs);

        LOG.info(String.format("ManagedExecutorPool: throughput=%.2f tasks/sec, avg latency=%.2f ms",
                custom.throughput(), custom.avgLatencyMs()));
        LOG.info(String.format("ThreadPoolExecutor: throughput=%.2f tasks/sec, avg latency=%.2f ms",
                standard.throughput(), standard.avgLatencyMs()));
    }

    private static void sweepParameters() throws Exception {
        LOG.info("=== Parameter sweep ===");
        LOG.info("corePoolSize | maxPoolSize | queueSize | throughput (tasks/sec)");

        int[][] matrix = {{2, 4, 50}, {4, 8, 50}, {8, 16, 50}};
        for (int[] row : matrix) {
            BenchStats stats = benchManagedWith(row[0], row[1], row[2], 200, 10);
            LOG.info(String.format("%12d | %11d | %9d | %18.2f",
                    row[0], row[1], row[2], stats.throughput()));
        }
    }

    private static BenchStats benchManaged(int taskCount, long sleepMs) throws Exception {
        return benchManagedWith(4, 8, 150, taskCount, sleepMs);
    }

    private static BenchStats benchManagedWith(
            int core, int max, int queueSize, int taskCount, long sleepMs) throws Exception {
        ExecutorSettings cfg = ExecutorSettings.of(
                core, max, 60, TimeUnit.SECONDS, queueSize, 0, OverflowAction.ABORT
        );
        ManagedExecutorPool pool = new ManagedExecutorPool(cfg);
        return measure(pool::submit, pool::shutdown, pool::awaitTermination, taskCount, sleepMs);
    }

    private static BenchStats benchJdk(int taskCount, long sleepMs) throws Exception {
        ThreadPoolExecutor jdkPool = new ThreadPoolExecutor(
                4, 8, 60, TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(1200),
                new LabeledThreadFactory("jdk-pool"),
                new ThreadPoolExecutor.AbortPolicy()
        );
        return measure(jdkPool::submit, jdkPool::shutdown, jdkPool::awaitTermination, taskCount, sleepMs);
    }

    private static BenchStats measure(
            TaskSource source,
            Runnable stop,
            Awaiter wait,
            int taskCount,
            long sleepMs) throws Exception {
        List<Future<Long>> futures = new ArrayList<>(taskCount);
        long t0 = System.nanoTime();

        for (int i = 0; i < taskCount; i++) {
            futures.add(source.submit(timedCall(sleepMs)));
        }

        long latencySum = 0;
        for (Future<Long> f : futures) {
            latencySum += f.get(5, TimeUnit.MINUTES);
        }

        long elapsed = System.nanoTime() - t0;
        stop.run();
        wait.await(5, TimeUnit.MINUTES);

        return new BenchStats(
                taskCount / (elapsed / 1_000_000_000.0),
                latencySum / (double) taskCount
        );
    }

    private static Callable<String> delayedCall(int id, long ms) {
        return () -> {
            LOG.info("Job-" + id + " begin");
            Thread.sleep(ms);
            LOG.info("Job-" + id + " end");
            return "Job-" + id + " ok";
        };
    }

    private static Runnable delayedRun(int id, long ms) {
        return () -> {
            LOG.info("Job-" + id + " begin");
            try {
                Thread.sleep(ms);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            LOG.info("Job-" + id + " end");
        };
    }

    private static Callable<Long> timedCall(long ms) {
        return () -> {
            long begin = System.nanoTime();
            Thread.sleep(ms);
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
        };
    }

    @FunctionalInterface
    private interface TaskSource {
        <T> Future<T> submit(Callable<T> callable);
    }

    @FunctionalInterface
    private interface Awaiter {
        boolean await(long timeout, TimeUnit unit) throws InterruptedException;
    }

    private record BenchStats(double throughput, double avgLatencyMs) {
    }
}
