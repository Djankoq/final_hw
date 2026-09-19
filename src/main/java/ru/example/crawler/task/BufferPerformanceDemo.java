package ru.example.crawler.task;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/** Учебное сравнение: одинаковые данные, прогрев и чередование порядка запусков. */
public final class BufferPerformanceDemo {
    private static final int WORKERS = 4;
    private static final int ITEMS_PER_PRODUCER = 25_000;
    private static final int CAPACITY = 64;

    interface Buffer {
        void put(Integer item) throws InterruptedException;
        Integer take() throws InterruptedException;
    }

    private static Buffer createBuffer(int variant) {
        if (variant == 2) {
            return new MonitorBuffer();
        }
        var buffer = new BoundedBuffer<Integer>(CAPACITY, variant == 1);
        return new Buffer() {
            public void put(Integer item) throws InterruptedException { buffer.put(item); }
            public Integer take() throws InterruptedException { return buffer.take(); }
        };
    }

    private static final class MonitorBuffer implements Buffer {
        private final ArrayDeque<Integer> items = new ArrayDeque<>();

        public synchronized void put(Integer item) throws InterruptedException {
            while (items.size() == CAPACITY) {
                wait();
            }
            items.addLast(item);
            notifyAll();
        }

        public synchronized Integer take() throws InterruptedException {
            while (items.isEmpty()) {
                wait();
            }
            Integer item = items.removeFirst();
            notifyAll();
            return item;
        }
    }

    private static long run(Buffer buffer) throws Exception {
        var pool = Executors.newFixedThreadPool(WORKERS * 2);
        var ready = new CountDownLatch(WORKERS * 2);
        var start = new CountDownLatch(1);
        List<Future<Long>> futures = new ArrayList<>();
        try {
            for (int worker = 0; worker < WORKERS; worker++) {
                final int base = worker * ITEMS_PER_PRODUCER;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    for (int i = 0; i < ITEMS_PER_PRODUCER; i++) {
                        buffer.put(base + i);
                    }
                    return 0L;
                }));
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    long sum = 0;
                    for (int i = 0; i < ITEMS_PER_PRODUCER; i++) {
                        sum += buffer.take();
                    }
                    return sum;
                }));
            }
            if (!ready.await(10, TimeUnit.SECONDS)) {
                throw new TimeoutException("Workers did not start");
            }
            long began = System.nanoTime();
            start.countDown();
            long sum = 0;
            for (Future<Long> future : futures) {
                long remaining = TimeUnit.SECONDS.toNanos(30) - (System.nanoTime() - began);
                sum += future.get(Math.max(0, remaining), TimeUnit.NANOSECONDS);
            }
            long elapsed = System.nanoTime() - began;
            long count = (long) WORKERS * ITEMS_PER_PRODUCER;
            if (sum != count * (count - 1) / 2) {
                throw new IllegalStateException("Incorrect consumed checksum: " + sum);
            }
            return elapsed;
        } finally {
            pool.shutdownNow();
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Workers did not terminate");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String[] names = {"ReentrantLock + signal", "ReentrantLock + signalAll", "synchronized + notifyAll"};
        long[][] measurements = new long[3][7];
        for (int round = -3; round < 7; round++) {
            for (int offset = 0; offset < 3; offset++) {
                int variant = Math.floorMod(round + offset, 3);
                long elapsed = run(createBuffer(variant));
                if (round >= 0) {
                    measurements[variant][round] = elapsed;
                }
            }
        }
        System.out.printf("%d producers, %d consumers, capacity=%d, %,d items/run%n",
                WORKERS, WORKERS, CAPACITY, WORKERS * ITEMS_PER_PRODUCER);
        for (int variant = 0; variant < 3; variant++) {
            Arrays.sort(measurements[variant]);
            long median = measurements[variant][3];
            System.out.printf("%-28s median: %8.2f ms; %,.0f items/s%n",
                    names[variant], median / 1_000_000.0,
                    WORKERS * (double) ITEMS_PER_PRODUCER * 1_000_000_000 / median);
        }
        System.out.println("Illustrative benchmark; results depend on JVM, hardware and workload.");
    }
}
