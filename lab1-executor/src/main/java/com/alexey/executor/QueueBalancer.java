package com.alexey.executor;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

final class QueueBalancer {

    private final AtomicInteger cursor = new AtomicInteger(0);

    int selectQueue(List<BlockingQueue<Runnable>> queues) {
        return Math.floorMod(cursor.getAndIncrement(), Math.max(queues.size(), 1));
    }

    int tryEnqueue(Runnable task, List<BlockingQueue<Runnable>> queues) {
        if (queues.isEmpty()) {
            return -1;
        }
        int start = selectQueue(queues);
        for (int offset = 0; offset < queues.size(); offset++) {
            int index = (start + offset) % queues.size();
            if (queues.get(index).offer(task)) {
                return index;
            }
        }
        return -1;
    }
}
