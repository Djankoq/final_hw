package ru.example.crawler.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public final class AtomicPerformanceDemo {
    private static final int ITERATIONS = 200_000;

    interface State {
        boolean stopped();
        void stop();
        void increment();
        int count();
        String cached();
    }

    static final class AtomicState implements State {
        private final StopFlag flag = new StopFlag();
        private final AtomicCounter counter = new AtomicCounter();

        public boolean stopped() { return flag.isStopped(); }
        public void stop() { flag.requestStop(); }
        public void increment() { counter.increment(); }
        public int count() { return counter.get(); }
        public String cached() { return SingletonCache.getInstance().get(); }
    }

    private static final class MonitorState implements State {
        private boolean stopped;
        private int count;
        private String cached;

        public synchronized boolean stopped() { return stopped; }
        public synchronized void stop() { stopped = true; }
        public synchronized void increment() { count++; }
        public synchronized int count() { return count; }
        public synchronized String cached() {
            if (cached == null) {
                cached = new String("cached value");
            }
            return cached;
        }
    }

    record Result(long nanos, long stopNanos, long operations) { }
    private record WorkerResult(long operations, String cached) { }

    // В режиме untilStopped цикл заканчивается только по общему флагу.
    static Result run(State state, int threads, boolean untilStopped) throws Exception {
        var pool = Executors.newFixedThreadPool(threads);
        var ready = new CountDownLatch(threads);
        var start = new CountDownLatch(1);
        var working = new CountDownLatch(threads);
        List<Future<WorkerResult>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    long local = 0;
                    String first = null;
                    while (!state.stopped() && (untilStopped || local < ITERATIONS)) {
                        String cached = state.cached();
                        if (first == null) {
                            first = cached;
                        } else if (first != cached) {
                            throw new IllegalStateException("Cache identity changed");
                        }
                        state.increment();
                        if (++local == 1) {
                            working.countDown();
                        }
                    }
                    return new WorkerResult(local, first);
                }));
            }
            if (!ready.await(10, TimeUnit.SECONDS)) {
                throw new TimeoutException("Workers not ready");
            }
            long began = System.nanoTime();
            start.countDown();
            long stopAt = 0;
            if (untilStopped) {
                if (!working.await(10, TimeUnit.SECONDS)) {
                    throw new TimeoutException("Workers did not perform work");
                }
                Thread.sleep(200);
                stopAt = System.nanoTime();
                state.stop();
            }
            long total = 0;
            String shared = null;
            for (Future<WorkerResult> future : futures) {
                long remaining = TimeUnit.SECONDS.toNanos(30) - (System.nanoTime() - began);
                WorkerResult result = future.get(Math.max(0, remaining), TimeUnit.NANOSECONDS);
                total += result.operations();
                if (shared == null) {
                    shared = result.cached();
                } else if (shared != result.cached()) {
                    throw new IllegalStateException("Workers received different cache objects");
                }
            }
            long ended = System.nanoTime();
            if (total != state.count() || (!untilStopped && total != (long) threads * ITERATIONS)) {
                throw new IllegalStateException("Lost increments: " + state.count() + " expected " + total);
            }
            return new Result(ended - began, untilStopped ? ended - stopAt : 0, total);
        } finally {
            state.stop();
            start.countDown();
            pool.shutdownNow();
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Workers did not terminate");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Java " + System.getProperty("java.version")
                + "; available processors: " + Runtime.getRuntime().availableProcessors());
        Result stopped = run(new AtomicState(), 4, true);
        System.out.printf("Volatile stop: %,d increments verified; stop + collection: %.3f ms%n",
                stopped.operations(), stopped.stopNanos() / 1_000_000.0);
        for (int threads : new int[]{1, 4, 8}) {
            long[][] samples = new long[2][5];
            for (int round = -2; round < 5; round++) {
                for (int offset = 0; offset < 2; offset++) {
                    int variant = Math.floorMod(round + offset, 2);
                    Result result = run(variant == 0 ? new AtomicState() : new MonitorState(), threads, false);
                    if (round >= 0) {
                        samples[variant][round] = result.nanos();
                    }
                }
            }
            for (int variant = 0; variant < 2; variant++) {
                Arrays.sort(samples[variant]);
                double seconds = samples[variant][2] / 1_000_000_000.0;
                System.out.printf("%d threads, %-12s: median %.3f ms; %,.0f iterations/s; count=%d OK%n",
                        threads, variant == 0 ? "volatile/CAS" : "synchronized",
                        seconds * 1000, threads * ITERATIONS / seconds, threads * ITERATIONS);
            }
        }
    }
}
