package ru.example.crawler.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class BoundedBufferTest {
    @Test
    void validatesArgumentsAndPreservesFifo() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new BoundedBuffer<>(0));
        assertThrows(IllegalArgumentException.class, () -> new BoundedBuffer<>(-1));
        var buffer = new BoundedBuffer<Integer>(2);
        assertThrows(NullPointerException.class, () -> buffer.put(null));
        buffer.put(1);
        buffer.put(2);
        assertEquals(1, buffer.take());
        buffer.put(3);
        assertEquals(2, buffer.take());
        assertEquals(3, buffer.take());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void consumersAndProducersWaitAndResume(boolean wakeAll) throws Exception {
        var buffer = new BoundedBuffer<Integer>(1, wakeAll);
        var take = new FutureTask<>(buffer::take);
        Thread consumer = new Thread(take);
        consumer.start();
        try {
            awaitWaiting(consumer);
            assertFalse(take.isDone());
            buffer.put(1);
            assertEquals(1, take.get(2, TimeUnit.SECONDS));
        } finally {
            stop(consumer);
        }
        buffer.put(2);
        var put = new FutureTask<Void>(() -> { buffer.put(3); return null; });
        Thread producer = new Thread(put);
        producer.start();
        try {
            awaitWaiting(producer);
            assertFalse(put.isDone());
            assertEquals(2, buffer.take());
            put.get(2, TimeUnit.SECONDS);
            assertEquals(3, buffer.take());
        } finally {
            stop(producer);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void interruptionLeavesBufferUsable(boolean producer) throws Exception {
        var buffer = new BoundedBuffer<Integer>(1);
        if (producer) {
            buffer.put(7);
        }
        var task = new FutureTask<Void>(() -> {
            assertThrows(InterruptedException.class, () -> {
                if (producer) buffer.put(8); else buffer.take();
            });
            return null;
        });
        Thread thread = new Thread(task);
        thread.start();
        try {
            awaitWaiting(thread);
            thread.interrupt();
            task.get(2, TimeUnit.SECONDS);
            if (producer) assertEquals(7, buffer.take());
            buffer.put(9);
            assertEquals(9, buffer.take());
        } finally {
            stop(thread);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void multipleProducersAndConsumersDeliverEveryItemOnce(boolean wakeAll) throws Exception {
        var buffer = new BoundedBuffer<Integer>(3, wakeAll);
        var pool = Executors.newFixedThreadPool(8);
        var start = new CountDownLatch(1);
        var seen = ConcurrentHashMap.<Integer>newKeySet();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int worker = 0; worker < 4; worker++) {
                int base = worker * 2_000;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 2_000; i++) buffer.put(base + i);
                    return null;
                }));
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 2_000; i++) {
                        int value = buffer.take();
                        assertTrue(value >= 0 && value < 8_000);
                        assertTrue(seen.add(value), "Duplicate: " + value);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) future.get(10, TimeUnit.SECONDS);
            assertEquals(8_000, seen.size());
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    private static void awaitWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (thread.getState() != Thread.State.WAITING) {
            if (!thread.isAlive() || System.nanoTime() - deadline >= 0) {
                fail("Worker did not await: " + thread.getState());
            }
            Thread.sleep(1);
        }
    }

    private static void stop(Thread thread) throws InterruptedException {
        thread.interrupt();
        thread.join(2_000);
        assertFalse(thread.isAlive());
    }
}
