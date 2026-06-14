package com.alexey.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ManagedExecutorPool implements CustomExecutor {

    private final ExecutorSettings settings;
    private final LabeledThreadFactory threadFactory;
    private final long keepAliveNanos;
    private final QueueBalancer balancer = new QueueBalancer();
    private final List<PoolWorker> workers = new ArrayList<>();
    private final List<BlockingQueue<Runnable>> taskQueues = new ArrayList<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final AtomicBoolean forceShutdown = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();

    public ManagedExecutorPool(ExecutorSettings settings) {
        this(settings, new LabeledThreadFactory("alex-pool"));
    }

    public ManagedExecutorPool(ExecutorSettings settings, LabeledThreadFactory threadFactory) {
        this.settings = settings;
        this.threadFactory = threadFactory;
        this.keepAliveNanos = settings.timeUnit().toNanos(settings.keepAliveTime());

        synchronized (lifecycleLock) {
            for (int i = 0; i < settings.corePoolSize(); i++) {
                spawnWorkerLocked();
            }
        }
    }

    LabeledThreadFactory threadFactory() {
        return threadFactory;
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException("command");
        }
        if (shuttingDown.get()) {
            throw new RejectedExecutionException("Pool is shut down");
        }

        synchronized (lifecycleLock) {
            maintainSpareCapacityLocked();

            int queueIndex = balancer.tryEnqueue(command, taskQueues);
            if (queueIndex >= 0) {
                ExecutorEventLog.pool("Task accepted into queue #" + queueIndex + ": "
                        + command.getClass().getSimpleName());
                return;
            }

            if (workers.size() < settings.maxPoolSize()) {
                int newIndex = spawnWorkerLocked();
                if (taskQueues.get(newIndex).offer(command)) {
                    ExecutorEventLog.pool("Task accepted into queue #" + newIndex + ": "
                            + command.getClass().getSimpleName());
                    return;
                }
            }

            OverflowHandler.handle(command, settings.overflowAction());
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) {
            throw new NullPointerException("callable");
        }
        FutureTask<T> task = new FutureTask<>(callable);
        execute(task);
        return task;
    }

    @Override
    public void shutdown() {
        shuttingDown.set(true);
        ExecutorEventLog.pool("Shutdown initiated. No new tasks will be accepted.");
        synchronized (lifecycleLock) {
            for (PoolWorker worker : workers) {
                worker.poke();
            }
            lifecycleLock.notifyAll();
        }
    }

    @Override
    public void shutdownNow() {
        shuttingDown.set(true);
        forceShutdown.set(true);
        ExecutorEventLog.pool("ShutdownNow initiated. Interrupting workers.");

        synchronized (lifecycleLock) {
            for (PoolWorker worker : workers) {
                worker.halt();
            }
            for (BlockingQueue<Runnable> queue : taskQueues) {
                queue.clear();
            }
            lifecycleLock.notifyAll();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (lifecycleLock) {
            while (!workers.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return workers.isEmpty();
                }
                lifecycleLock.wait(TimeUnit.NANOSECONDS.toMillis(remaining));
            }
        }
        return true;
    }

    public int activeWorkers() {
        synchronized (lifecycleLock) {
            return workers.size();
        }
    }

    public int idleWorkers() {
        synchronized (lifecycleLock) {
            int idle = 0;
            for (PoolWorker worker : workers) {
                if (!worker.isProcessing()) {
                    idle++;
                }
            }
            return idle;
        }
    }

    boolean isShuttingDown() {
        return shuttingDown.get();
    }

    boolean isForceShutdown() {
        return forceShutdown.get();
    }

    boolean mayRetireIdleWorker() {
        synchronized (lifecycleLock) {
            return workers.size() > settings.corePoolSize();
        }
    }

    void onWorkerIdle(PoolWorker worker) {
        synchronized (lifecycleLock) {
            maintainSpareCapacityLocked();
            lifecycleLock.notifyAll();
        }
    }

    void onWorkerExit(PoolWorker worker) {
        synchronized (lifecycleLock) {
            int index = workers.indexOf(worker);
            if (index >= 0) {
                workers.remove(index);
                taskQueues.remove(index);
            }
            lifecycleLock.notifyAll();
        }
    }

    private void maintainSpareCapacityLocked() {
        while (countIdleLocked() < settings.minSpareThreads()
                && workers.size() < settings.maxPoolSize()) {
            spawnWorkerLocked();
        }
    }

    private int countIdleLocked() {
        int idle = 0;
        for (PoolWorker worker : workers) {
            if (!worker.isProcessing()) {
                idle++;
            }
        }
        return idle;
    }

    private int spawnWorkerLocked() {
        int id = workers.size();
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(settings.queueSize());
        PoolWorker worker = new PoolWorker(id, queue, this, keepAliveNanos);
        taskQueues.add(queue);
        workers.add(worker);
        worker.launch();
        return id;
    }
}
