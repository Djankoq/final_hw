package ru.example.crawler.task;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Independent demonstrations; the deadlock mode intentionally never terminates. */
public final class ConcurrencyProblemsDemo {
    private static final int RETRIES = 20;
    private static final int OPERATIONS = 10_000;

    private ConcurrencyProblemsDemo() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "fixed" : args[0];
        switch (mode) {
            case "deadlock" -> deadlock();
            case "deadlock-fixed" -> orderedLocks();
            case "livelock" -> livelock(false);
            case "livelock-fixed" -> livelock(true);
            case "starvation" -> starvation();
            case "starvation-fixed" -> fairAccess();
            case "fixed" -> {
                orderedLocks();
                livelock(true);
                fairAccess();
            }
            default -> throw new IllegalArgumentException("Modes: deadlock, deadlock-fixed, "
                    + "livelock, livelock-fixed, starvation, starvation-fixed, fixed");
        }
    }

    private static void deadlock() throws InterruptedException {
        Object resourceA = new Object();
        Object resourceB = new Object();
        CountDownLatch firstLocks = new CountDownLatch(2);
        Thread first = new Thread(() -> lockOpposite(resourceA, resourceB, firstLocks), "Deadlock-1");
        Thread second = new Thread(() -> lockOpposite(resourceB, resourceA, firstLocks), "Deadlock-2");
        System.out.println("PID=" + ProcessHandle.current().pid());
        first.start();
        second.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (ManagementFactory.getThreadMXBean().findMonitorDeadlockedThreads() == null) {
            if (System.nanoTime() - deadline >= 0) {
                throw new IllegalStateException("Deadlock was not detected within 10 seconds");
            }
            Thread.sleep(10);
        }
        System.out.printf("DEADLOCK CONFIRMED: %s=%s, %s=%s%n",
                first.getName(), first.getState(), second.getName(), second.getState());
        System.out.flush();
        first.join();
        second.join();
    }

    private static void lockOpposite(Object first, Object second, CountDownLatch firstLocks) {
        synchronized (first) {
            firstLocks.countDown();
            try {
                firstLocks.await();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
            synchronized (second) {
                throw new AssertionError("Unreachable: both first resources are already held");
            }
        }
    }

    static int orderedLocks() throws Exception {
        Object resourceA = new Object();
        Object resourceB = new Object();
        AtomicInteger completed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(2);
        Callable<Void> worker = () -> {
            start.countDown();
            start.await();
            synchronized (resourceA) {
                synchronized (resourceB) {
                    completed.incrementAndGet();
                }
            }
            return null;
        };
        runPair(worker, worker);
        System.out.println("deadlock-fixed: completed=" + completed.get());
        return completed.get();
    }

    record LivelockResult(int attempts, int completed) {
    }

    static LivelockResult livelock(boolean fixed) throws Exception {
        ReentrantLock resourceA = new ReentrantLock();
        ReentrantLock resourceB = new ReentrantLock();
        CyclicBarrier phase = new CyclicBarrier(2);
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        runPair(
                retryWorker(resourceA, resourceB, resourceA, resourceB, phase, attempts, completed, fixed),
                retryWorker(resourceB, resourceA, resourceA, resourceB, phase, attempts, completed, fixed));
        System.out.printf("%s: attempts=%d, completed=%d%n",
                fixed ? "livelock-fixed" : "livelock", attempts.get(), completed.get());
        return new LivelockResult(attempts.get(), completed.get());
    }

    private static Callable<Void> retryWorker(ReentrantLock first, ReentrantLock second,
                                              ReentrantLock resourceA, ReentrantLock resourceB,
                                              CyclicBarrier phase, AtomicInteger attempts,
                                              AtomicInteger completed, boolean fixed) {
        return () -> {
            for (int round = 0; round < RETRIES; round++) {
                first.lockInterruptibly();
                try {
                    // Both workers hold their first resource before trying the second.
                    phase.await(5, TimeUnit.SECONDS);
                    attempts.incrementAndGet();
                    if (second.tryLock()) {
                        try {
                            completed.incrementAndGet();
                        } finally {
                            second.unlock();
                        }
                    }
                    // Neither worker releases its first resource before both attempts finish.
                    phase.await(5, TimeUnit.SECONDS);
                } finally {
                    first.unlock();
                }
                // Both release and retry together, reproducing the same collision.
                phase.await(5, TimeUnit.SECONDS);
            }
            if (fixed) {
                // A retry budget breaks livelock; ordered blocking acquisition ensures progress.
                resourceA.lockInterruptibly();
                try {
                    resourceB.lockInterruptibly();
                    try {
                        completed.incrementAndGet();
                    } finally {
                        resourceB.unlock();
                    }
                } finally {
                    resourceA.unlock();
                }
            }
            return null;
        };
    }

    record AccessResult(int highCompleted, int lowCompleted) {
    }

    /** Application-level strict priority with a permanently nonempty high-priority queue. */
    private static final class PriorityResource {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private boolean highPending = true;
        private boolean stopped;
        private int highCompleted;
        private int lowCompleted;

        void awaitLowAccess(CountDownLatch lowRequested) throws InterruptedException {
            lock.lockInterruptibly();
            try {
                lowRequested.countDown();
                while (highPending && !stopped) {
                    changed.await();
                }
                if (!stopped) {
                    lowCompleted++;
                }
            } finally {
                lock.unlock();
            }
        }

        void serveHigh() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                highPending = false;
                highCompleted++;
                // A new high-priority request arrives before the resource is released.
                highPending = true;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void stopObservation() {
            lock.lock();
            try {
                stopped = true;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    static AccessResult starvation() throws Exception {
        PriorityResource resource = new PriorityResource();
        CountDownLatch lowRequested = new CountDownLatch(1);
        runPair(() -> {
            resource.awaitLowAccess(lowRequested);
            return null;
        }, () -> {
            try {
                lowRequested.await();
                for (int i = 0; i < OPERATIONS; i++) {
                    resource.serveHigh();
                }
            } finally {
                resource.stopObservation();
            }
            return null;
        });
        System.out.printf("starvation: high=%d, low=%d (observation stopped)%n",
                resource.highCompleted, resource.lowCompleted);
        return new AccessResult(resource.highCompleted, resource.lowCompleted);
    }

    static AccessResult fairAccess() throws Exception {
        ReentrantLock resource = new ReentrantLock(true);
        CountDownLatch start = new CountDownLatch(2);
        int[] completed = new int[2];
        runPair(fairWorker(resource, start, completed, 0), fairWorker(resource, start, completed, 1));
        System.out.printf("starvation-fixed: worker1=%d, worker2=%d%n", completed[0], completed[1]);
        return new AccessResult(completed[0], completed[1]);
    }

    private static Callable<Void> fairWorker(ReentrantLock resource, CountDownLatch start,
                                              int[] completed, int index) {
        return () -> {
            start.countDown();
            start.await();
            for (int i = 0; i < OPERATIONS; i++) {
                resource.lockInterruptibly();
                try {
                    completed[index]++;
                } finally {
                    resource.unlock();
                }
            }
            return null;
        };
    }

    private static void runPair(Callable<Void> first, Callable<Void> second) throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try {
            var results = executor.invokeAll(List.of(first, second), 15, TimeUnit.SECONDS);
            for (var result : results) {
                result.get();
            }
        } finally {
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Workers did not terminate");
            }
        }
    }
}
