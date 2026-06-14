package com.alexey.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PoolWorker implements Runnable {

    private final int id;
    private final BlockingQueue<Runnable> inbox;
    private final ManagedExecutorPool pool;
    private final long idleWaitNanos;
    private final Thread thread;
    private final AtomicBoolean alive = new AtomicBoolean(true);
    private volatile boolean processing;

    public PoolWorker(int id, BlockingQueue<Runnable> inbox, ManagedExecutorPool pool, long idleWaitNanos) {
        this.id = id;
        this.inbox = inbox;
        this.pool = pool;
        this.idleWaitNanos = idleWaitNanos;
        this.thread = pool.threadFactory().newThread(this);
    }

    public void launch() {
        thread.start();
    }

    public int id() {
        return id;
    }

    public boolean isProcessing() {
        return processing;
    }

    public void halt() {
        alive.set(false);
        thread.interrupt();
    }

    public void poke() {
        thread.interrupt();
    }

    @Override
    public void run() {
        try {
            while (alive.get()) {
                if (pool.isShuttingDown() && inbox.isEmpty()) {
                    break;
                }

                Runnable job = inbox.poll(idleWaitNanos, TimeUnit.NANOSECONDS);
                if (job == null) {
                    if (pool.mayRetireIdleWorker()) {
                        ExecutorEventLog.worker(thread.getName() + " idle timeout, stopping.");
                        break;
                    }
                    continue;
                }

                if (pool.isForceShutdown()) {
                    break;
                }

                processing = true;
                try {
                    ExecutorEventLog.worker(thread.getName() + " executes "
                            + job.getClass().getSimpleName() + "@" + Integer.toHexString(job.hashCode()));
                    job.run();
                } catch (RuntimeException ex) {
                    ExecutorEventLog.worker(thread.getName() + " failed: " + ex.getMessage());
                    throw ex;
                } finally {
                    processing = false;
                    pool.onWorkerIdle(this);
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            alive.set(false);
            pool.onWorkerExit(this);
            ExecutorEventLog.worker(thread.getName() + " terminated.");
        }
    }
}
