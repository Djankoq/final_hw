package ru.example.crawler.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class BankPerformanceDemo {
    private static final int THREADS = 8;
    private static final int OPERATIONS = 50_000;

    public static void main(String[] args) throws Exception {
        new BankPerformanceDemo().run();
    }

    public void run() throws Exception {
        BankDataCollector bank = new BankDataCollector();
        bank.openAccount("A", 1_000_000);
        bank.openAccount("B", 1_000_000);
        runConcurrent(THREADS, worker -> {
            for (int i = 0; i < 5_000; i++) {
                String from = worker % 2 == 0 ? "A" : "B";
                String to = worker % 2 == 0 ? "B" : "A";
                var item = new BankDataCollector.Item(worker + ":" + i, from, to, 1);
                if (!bank.collectItem(item) || bank.collectItem(item)) {
                    throw new IllegalStateException("Transfer or deduplication failed");
                }
                var snapshot = bank.getBalances();
                if (snapshot.get("A") + snapshot.get("B") != 2_000_000) {
                    throw new IllegalStateException("Money was lost");
                }
            }
        });
        if (bank.getBalance("A") != 1_000_000 || bank.getBalance("B") != 1_000_000
                || bank.getProcessedCount() != 40_000) {
            throw new IllegalStateException("Incorrect final bank state");
        }
        System.out.println("Bank stress test: 8 threads, 40000 transfers; balances and duplicates verified.");

        for (int i = 0; i < 3; i++) {
            measure(true);
            measure(false);
        }
        double[] safe = new double[7];
        double[] unsafe = new double[7];
        for (int i = 0; i < safe.length; i++) {
            Result first = measure(i % 2 == 0);
            Result second = measure(i % 2 != 0);
            Result synchronizedResult = i % 2 == 0 ? first : second;
            Result unsynchronizedResult = i % 2 == 0 ? second : first;
            safe[i] = synchronizedResult.millis();
            unsafe[i] = unsynchronizedResult.millis();
            System.out.printf(Locale.ROOT,
                    "Round %d: synchronized=%.3f ms; unsafe=%.3f ms; unsafe balances=%d/%d; count=%d%n",
                    i + 1, safe[i], unsafe[i], unsynchronizedResult.left(),
                    unsynchronizedResult.right(), unsynchronizedResult.count());
        }
        Arrays.sort(safe);
        Arrays.sort(unsafe);
        System.out.printf(Locale.ROOT, "Median: synchronized=%.3f ms; unsafe=%.3f ms%n", safe[3], unsafe[3]);
        System.out.println("Expected balances: 1000000/1000000; count: 400000. "
                + "Unsafe timings measure incorrect code; races may not appear in every run. Educational benchmark, not JMH.");
    }

    private record Result(double millis, long left, long right, long count) { }

    /** Одинаковая работа в обеих ветках, без журнала и выделения объектов в цикле. */
    private static final class Ledger {
        private long left = 1_000_000;
        private long right = 1_000_000;
        private long count;

        private void transfer(boolean forward) {
            if (forward) {
                left--;
                right++;
            } else {
                right--;
                left++;
            }
            count++;
        }

        private synchronized void safeTransfer(boolean forward) {
            transfer(forward);
        }
    }

    private Result measure(boolean synchronizedAccess) throws Exception {
        Ledger ledger = new Ledger();
        double millis = runConcurrent(THREADS, worker -> {
            for (int i = 0; i < OPERATIONS; i++) {
                if (synchronizedAccess) {
                    ledger.safeTransfer(worker % 2 == 0);
                } else {
                    ledger.transfer(worker % 2 == 0);
                }
            }
        });
        if (synchronizedAccess && (ledger.left != 1_000_000 || ledger.right != 1_000_000
                || ledger.count != (long) THREADS * OPERATIONS)) {
            throw new IllegalStateException("Synchronized benchmark failed");
        }
        return new Result(millis, ledger.left, ledger.right, ledger.count);
    }

    @FunctionalInterface
    interface Worker {
        void run(int index) throws Exception;
    }

    static double runConcurrent(int threads, Worker action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                int index = i;
                tasks.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    action.run(index);
                    return null;
                }));
            }
            if (!ready.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Workers did not start");
            }
            long begin = System.nanoTime();
            start.countDown();
            for (Future<?> task : tasks) {
                task.get(30, TimeUnit.SECONDS);
            }
            return (System.nanoTime() - begin) / 1_000_000.0;
        } finally {
            start.countDown();
            pool.shutdownNow();
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Workers did not terminate");
            }
        }
    }
}
